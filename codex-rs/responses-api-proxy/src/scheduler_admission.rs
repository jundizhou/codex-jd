//! Bounded admission and waiting; all eligibility gates are checked under one lock.
use super::*;

impl Scheduler {
    pub(crate) fn admit(self: &Arc<Self>, bytes: usize) -> Result<Admission, Rejection> {
        if bytes > MAX_BODY {
            return Err(Rejection::TooLarge);
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let configured_max = state.configured_max(&self.config);
        if state.inbound >= MAX_PENDING + configured_max || state.bytes + bytes > MAX_RESIDENT {
            return Err(Rejection::Full);
        }
        state.inbound += 1;
        state.bytes += bytes;
        Ok(Admission {
            scheduler: Arc::clone(self),
            bytes,
        })
    }

    pub(crate) fn acquire(self: &Arc<Self>, mut pending: Pending) -> Result<Lease, Rejection> {
        let now = Instant::now();
        pending.deadline = pending.deadline.min(now + self.config.timeout);
        pending.id = digest(&[pending.key.0.as_bytes(), pending.id.as_bytes()]);
        let id = pending.id.clone();
        let key = pending.key.clone();
        let conversation = key.digest();
        let cancel_key = Key(key.0.clone(), id.clone());
        let deadline = pending.deadline;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let retired: Vec<_> = state
            .bindings
            .iter()
            .filter(|(key, binding)| {
                binding.finished.is_some_and(|at| {
                    now.duration_since(at)
                        >= self
                            .config
                            .idle_ttl
                            .max(self.config.gap)
                            .max(self.config.user_gap)
                            .max(self.config.tool_gap)
                }) && !state.running.contains_key(key)
                    && !state.pending.iter().any(|p| &p.key == *key)
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in retired {
            state.bindings.remove(&key);
        }
        // Timing evidence is a bounded cache, not ownership of an execution slot.
        if state.bindings.len() >= 1024 && !state.bindings.contains_key(&key) {
            let gap = self
                .config
                .gap
                .max(self.config.user_gap)
                .max(self.config.tool_gap);
            let oldest = state
                .bindings
                .iter()
                .filter(|(key, binding)| {
                    !state.running.contains_key(key)
                        && !state.pending.iter().any(|pending| &pending.key == *key)
                        && binding
                            .finished
                            .is_some_and(|at| now.duration_since(at) >= gap)
                })
                .min_by_key(|(_, binding)| binding.finished)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                state.bindings.remove(&oldest);
            } else {
                return Err(Rejection::Full);
            }
        }
        state.journal.check(&id, &pending.fingerprint)?;
        state.cancelled.retain(|_, expiry| *expiry > now);
        if state.cancelled.contains_key(&cancel_key) {
            return Err(Rejection::Cancelled);
        }
        if let Some(fingerprint) = state
            .pending
            .iter()
            .find(|p| p.id == id)
            .map(|p| &p.fingerprint)
            .or_else(|| state.attempts.get(&id).map(|attempt| &attempt.fingerprint))
        {
            return Err(if fingerprint == &pending.fingerprint {
                Rejection::Duplicate
            } else {
                Rejection::Conflict
            });
        }
        if state.pending.len() >= MAX_PENDING
            || state.pending.iter().map(|p| p.bytes).sum::<usize>() + pending.bytes
                > 64 * 1024 * 1024
        {
            return Err(Rejection::Full);
        }
        if state.pending.iter().filter(|p| p.key == key).count() >= 2
            || state.pending.iter().filter(|p| p.key.0 == key.0).count() >= 8
        {
            return Err(Rejection::Limited);
        }
        if !state.tenants.contains(&key.0) {
            state.tenants.push_back(key.0.clone());
        }
        if !state.conversations.contains(&key) {
            state.conversations.push_back(key.clone());
        }
        state.pending.push(pending);
        self.changed.notify_all();
        loop {
            let now = Instant::now();
            let rejection = if state.cancelled.contains_key(&cancel_key) {
                Some(Rejection::Cancelled)
            } else if state.paused || state.throttle.blocked {
                Some(Rejection::Paused)
            } else if state
                .throttle
                .until
                .is_some_and(|until| until > now && until >= deadline)
            {
                Some(Rejection::Cooldown(
                    state
                        .throttle
                        .until
                        .map(|until| until.saturating_duration_since(now).as_secs() + 1)
                        .unwrap_or(1),
                ))
            } else if state.uncertain
                || (!self.config.automatic_recovery
                    && (state.quarantined.len() >= state.running_cap(&self.config)
                        || state
                            .quarantined
                            .values()
                            .any(|key| key.as_deref() == Some(&conversation))))
            {
                Some(Rejection::Unavailable)
            } else if now >= deadline {
                Some(Rejection::Expired)
            } else {
                None
            };
            if let Some(error) = rejection {
                let _ = state.remove(&id);
                self.changed.notify_all();
                return Err(error);
            }
            if state.selected(now, &self.config) == Some(id.as_str()) {
                let pending = state.remove(&id).ok_or(Rejection::Unavailable)?;
                let kind = state.bindings.get(&key).map_or(Kind::Unknown, |binding| {
                    pending.evidence.kind(&binding.signals, now)
                });
                state.tool_streak = if kind == Kind::Tool {
                    let count = state
                        .tool_streak
                        .as_ref()
                        .filter(|(previous, _)| previous == &key)
                        .map_or(1, |(_, count)| count.saturating_add(1));
                    Some((key.clone(), count))
                } else {
                    None
                };
                if pending.sticky {
                    state.bindings.entry(key.clone()).or_insert(Binding {
                        finished: None,
                        signals: Signals::default(),
                    });
                }
                let attempt = Arc::new(Attempt {
                    fingerprint: pending.fingerprint.clone(),
                    ..Attempt::default()
                });
                state.attempts.insert(id.clone(), Arc::clone(&attempt));
                state.running.insert(key.clone(), id.clone());
                state.dispatching = Some(id.clone());
                self.changed.notify_all();
                return Ok(Lease {
                    scheduler: Arc::clone(self),
                    key: key.clone(),
                    evidence: pending.evidence,
                    dispatch: Dispatch {
                        scheduler: Arc::clone(self),
                        started: Arc::new(AtomicBool::new(false)),
                        attempt,
                        fingerprint: pending.fingerprint,
                        key,
                        id,
                        deadline,
                    },
                });
            }
            let wake = state
                .next_start
                .into_iter()
                .chain(state.throttle.until)
                .chain(state.bindings.get(&key).and_then(|binding| {
                    binding.finished.map(|at| {
                        at + state
                            .pending
                            .iter()
                            .find(|p| p.id == id)
                            .map_or(self.config.gap, |p| p.gap(binding, &self.config, now))
                    })
                }))
                .filter(|t| *t > now)
                .fold(deadline, Instant::min);
            (state, _) = self
                .changed
                .wait_timeout(state, wake.saturating_duration_since(now))
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}
