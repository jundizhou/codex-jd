//! Opt-in availability recovery. A successful probe never certifies remote termination.
use super::*;
use crate::app_server_reader::AppServerIdentityClient;
use crate::queue_store::now;

impl State {
    fn recovery_allowed(&self) -> bool {
        !self.uncertain
            && !self.switching
            && !self.paused
            && !self.throttle.blocked
            && self
                .throttle
                .until
                .is_none_or(|until| Instant::now() >= until)
    }
}

impl Scheduler {
    pub(crate) fn start_recovery(
        self: &Arc<Self>,
        identity: &Arc<AppServerIdentityClient>,
    ) -> anyhow::Result<()> {
        let scheduler = Arc::downgrade(self);
        let identity = Arc::downgrade(identity);
        std::thread::Builder::new()
            .name("queue-recovery".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(Duration::from_secs(1));
                    let (Some(scheduler), Some(identity)) =
                        (scheduler.upgrade(), identity.upgrade())
                    else {
                        break;
                    };
                    let due = scheduler
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .journal
                        .recovery_due(now());
                    for (id, key) in due {
                        let allowed = scheduler
                            .state
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .recovery_allowed();
                        let result = if allowed {
                            identity.recover_conversation(key.clone())
                        } else {
                            Err(anyhow::anyhow!("queue recovery gated"))
                        };
                        scheduler.complete_recovery(&id, &key, result);
                    }
                }
            })?;
        Ok(())
    }

    pub(super) fn complete_recovery(&self, id: &str, key: &str, mut result: anyhow::Result<()>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.quarantined.get(id).and_then(Option::as_deref) != Some(key) {
            return;
        }
        // Account feedback or an administrative pause may arrive while the check runs.
        if !state.recovery_allowed() {
            result = Err(anyhow::anyhow!("queue recovery gated"));
        }
        if state.journal.recovery_result(id, &result, now()).is_err() {
            state.uncertain = true;
        } else if result.is_ok() {
            state.quarantined.remove(id);
            eprintln!(
                "queue recovery released reservation: request={id} conversation={key}; upstream outcome remains unknown"
            );
        } else {
            eprintln!(
                "queue recovery check failed: request={id} conversation={key}; retry scheduled"
            );
        }
        self.changed.notify_all();
    }
}
