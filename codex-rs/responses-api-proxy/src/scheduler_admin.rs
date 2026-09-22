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
}
