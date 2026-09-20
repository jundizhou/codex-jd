//! Dispatch commitment, completion and administrative recovery share scheduler state.
use super::*;

#[derive(Default)]
pub(super) struct Attempt {
    pub(super) fingerprint: String,
    pub(super) finalizing: Mutex<Option<Instant>>,
    pub(super) outcome: Mutex<Option<(Instant, Observation)>>,
    pub(super) done: Condvar,
    pub(super) failed: AtomicBool,
    pub(super) local_finished: AtomicBool,
}

impl Attempt {
    pub(super) fn finalize(&self) {
        self.finalizing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert_with(|| {
                Instant::now() + crate::app_server_reader::RAW_RESPONSE_CONTROL_TIMEOUT
            });
    }
}

pub(crate) struct Lease {
    pub(super) scheduler: Arc<Scheduler>,
    pub(super) key: Key,
    pub dispatch: Dispatch,
    pub(super) evidence: Evidence,
}

impl Lease {
    pub(crate) fn identity_outcome(&self) -> crate::conversations::Outcome {
        if self.dispatch.started.load(Ordering::Acquire)
            && self
                .dispatch
                .attempt
                .outcome
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none()
        {
            if self.dispatch.attempt.local_finished.load(Ordering::Acquire) {
                crate::conversations::Outcome::Recoverable
            } else {
                crate::conversations::Outcome::Unknown
            }
        } else {
            crate::conversations::Outcome::Released
        }
    }

    pub(crate) fn settle(&self) {
        if !self.dispatch.started.load(Ordering::Acquire) {
            return;
        }
        let mut outcome = self
            .dispatch
            .attempt
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if outcome.is_none() && !self.dispatch.attempt.failed.load(Ordering::Acquire) {
            self.dispatch.attempt.finalize();
            let deadline = self
                .dispatch
                .finalizing_deadline()
                .unwrap_or_else(Instant::now);
            while outcome.is_none()
                && !self.dispatch.attempt.failed.load(Ordering::Acquire)
                && Instant::now() < deadline
            {
                (outcome, _) = self
                    .dispatch
                    .attempt
                    .done
                    .wait_timeout(outcome, deadline.saturating_duration_since(Instant::now()))
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut state = self
            .scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let outcome = self
            .dispatch
            .attempt
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let started = self.dispatch.started.load(Ordering::Acquire);
        if started && outcome.is_none() {
            // Closing a socket does not establish that model computation stopped.
            if state.journal.quarantine(&self.dispatch.id).is_err() {
                state.uncertain = true;
            }
            eprintln!(
                "queue request quarantined: request={} conversation={}",
                self.dispatch.id,
                self.key.digest()
            );
            state
                .quarantined
                .insert(self.dispatch.id.clone(), Some(self.key.digest()));
            state.running.remove(&self.key);
            state.attempts.remove(&self.dispatch.id);
            state.bindings.remove(&self.key);
        } else {
            if !started {
                state.dispatching = None;
            }
            state.running.remove(&self.key);
            state.attempts.remove(&self.dispatch.id);
            if started && state.journal.finish(&self.dispatch.id).is_err() {
                state.uncertain = true;
            }
            if !started
                && state
                    .bindings
                    .get(&self.key)
                    .is_some_and(|b| b.finished.is_none())
            {
                state.bindings.remove(&self.key);
            } else if let Some(binding) = state.bindings.get_mut(&self.key)
                && started
            {
                let (at, output) =
                    outcome.unwrap_or_else(|| (Instant::now(), Observation::default()));
                binding.finished = Some(at);
                binding.signals = Signals {
                    input: std::mem::take(&mut self.evidence),
                    output,
                    finished: Some(at),
                };
            }
        }
        self.scheduler.changed.notify_all();
    }
}

#[derive(Clone)]
pub(crate) struct Dispatch {
    pub(super) scheduler: Arc<Scheduler>,
    pub(super) started: Arc<AtomicBool>,
    pub(super) attempt: Arc<Attempt>,
    pub(super) key: Key,
    pub(super) id: String,
    pub(super) fingerprint: String,
    pub(super) deadline: Instant,
}

impl Dispatch {
    pub(crate) fn start(&self) -> anyhow::Result<()> {
        let mut state = self
            .scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.uncertain {
            return Err(Rejection::Unavailable.into());
        }
        if state.paused || state.throttle.blocked {
            return Err(Rejection::Paused.into());
        }
        let now = Instant::now();
        if let Some(until) = state.throttle.until
            && until > now
        {
            return Err(Rejection::Cooldown(until.duration_since(now).as_secs() + 1).into());
        }
        if now >= self.deadline {
            return Err(Rejection::Expired.into());
        }
        if state
            .cancelled
            .contains_key(&Key(self.key.0.clone(), self.id.clone()))
        {
            return Err(Rejection::Cancelled.into());
        }
        // The durable write is the dispatch commitment. A crash from here onwards
        // requires explicit recovery, including if the network send never happened.
        if let Err(error) =
            state
                .journal
                .start(&self.id, &self.fingerprint, Some(&self.key.digest()))
        {
            if error == Rejection::Unavailable {
                state.uncertain = true;
            }
            return Err(error.into());
        }
        self.started.store(true, Ordering::Release);
        state.dispatching = None;
        state.next_start = Some(Instant::now() + self.scheduler.config.start_gap);
        self.scheduler.changed.notify_all();
        Ok(())
    }

    pub(crate) fn feedback(
        &self,
        status: u16,
        headers: &std::collections::HashMap<String, String>,
    ) {
        let mut state = self
            .scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let maximum = state.max_running;
        state
            .throttle
            .observe(status, headers, Instant::now(), maximum);
        let State {
            journal,
            throttle,
            uncertain,
            ..
        } = &mut *state;
        if journal.save_account(throttle).is_err() {
            *uncertain = true;
        }
        self.scheduler.changed.notify_all();
    }

    pub(crate) fn confirmed(&self, observation: Observation) {
        if observation.quota_exhausted {
            self.feedback(402, &HashMap::new());
        } else if observation.rate_limited {
            self.feedback(429, &HashMap::new());
        } else if observation.successful {
            self.feedback(200, &HashMap::new());
        }
        *self
            .attempt
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some((Instant::now(), observation));
        self.attempt.done.notify_all();
    }

    pub(crate) fn finalize(&self) {
        self.attempt.finalize();
    }

    // A terminal RPC proves the local handler ended, not that upstream generation ended.
    pub(crate) fn local_finished(&self) {
        self.attempt.local_finished.store(true, Ordering::Release);
    }

    pub(crate) fn unconfirmed(&self) {
        let _outcome = self
            .attempt
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.attempt.failed.store(true, Ordering::Release);
        self.attempt.done.notify_all();
    }

    pub(crate) fn finalizing_deadline(&self) -> Option<Instant> {
        *self
            .attempt
            .finalizing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Key {
    pub(crate) fn digest(&self) -> String {
        digest(&[self.0.as_bytes(), self.1.as_bytes()])
    }
}

impl Pending {
    pub(super) fn gap(&self, binding: &Binding, config: &Config, now: Instant) -> Duration {
        match self.evidence.kind(&binding.signals, now) {
            Kind::User => config.user_gap,
            Kind::Tool => config.tool_gap,
            Kind::Unknown => config.gap,
        }
    }
}

impl Scheduler {
    pub(crate) fn pause(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .paused = true;
        self.changed.notify_all();
    }

    pub(crate) fn resume(&self) -> Result<(), Rejection> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.uncertain || state.throttle.blocked {
            return Err(Rejection::Unavailable);
        }
        state.paused = false;
        self.changed.notify_all();
        Ok(())
    }

    // Only an operator who independently confirmed upstream termination may use this.
    pub(crate) fn recover(&self) -> Result<(), Rejection> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.paused
            || state.inbound != 0
            || !state.pending.is_empty()
            || !state.running.is_empty()
        {
            return Err(Rejection::Duplicate);
        }
        state
            .journal
            .acknowledge_unknown()
            .map_err(|_| Rejection::Unavailable)?;
        state.quarantined.clear();
        state.attempts.clear();
        state.dispatching = None;
        state.uncertain = false;
        state.throttle.blocked = false;
        state.throttle.blocked_reason = None;
        let State {
            journal, throttle, ..
        } = &mut *state;
        journal
            .save_account(throttle)
            .map_err(|_| Rejection::Unavailable)?;
        // Do not erase a legitimate Retry-After cooldown during recovery.
        self.changed.notify_all();
        Ok(())
    }
}
