//! Administrative changes are serialized with request admission.
use super::*;

impl Scheduler {
    pub(crate) fn set_limit(
        &self,
        limit: usize,
        save: impl FnOnce() -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        save()?;
        let old = state.max_running;
        state.max_running = limit;
        state.throttle.limit = if state.throttle.limit == old {
            limit
        } else {
            state.throttle.limit.min(limit)
        };
        self.changed.notify_all();
        Ok(())
    }

    pub(crate) fn switch_idle(
        &self,
        activate: impl FnOnce() -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        anyhow::ensure!(
            state.running.is_empty()
                && state.pending.is_empty()
                && state.quarantined.is_empty()
                && state.inbound == 0
                && !state.uncertain,
            "仍有运行中、排队或结果未知的请求，请等待请求结束后重试"
        );
        activate()?;
        // Allow the existing app-server auth-file watcher to observe the replacement.
        std::thread::sleep(Duration::from_secs(2));
        state.throttle = Throttle {
            limit: state.max_running,
            ..Throttle::default()
        };
        let State {
            journal,
            throttle,
            uncertain,
            ..
        } = &mut *state;
        if let Err(error) = journal.save_account(throttle) {
            *uncertain = true;
            return Err(error);
        }
        self.changed.notify_all();
        Ok(())
    }
}
