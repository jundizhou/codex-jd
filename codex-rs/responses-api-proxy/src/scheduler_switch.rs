//! Drains real leases while retaining bounded pending requests during account changes.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Activation {
    Changed,
    Restored,
}

impl Scheduler {
    pub(crate) fn rotation_enabled(&self, enabled: bool) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.rotation_enabled = enabled;
        if !enabled {
            state.rotation_hold = false;
        }
        self.changed.notify_all();
    }

    pub(crate) fn hold_for_rotation(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .rotation_hold = true;
        self.changed.notify_all();
    }

    pub(crate) fn switch_account(
        &self,
        activate: impl FnOnce() -> anyhow::Result<Activation>,
    ) -> anyhow::Result<Activation> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        anyhow::ensure!(!state.switching, "已有账号切换正在进行");
        anyhow::ensure!(
            !state.uncertain && state.quarantined.is_empty(),
            "结果未知的请求未处理，不能切换账号"
        );
        state.switching = true;
        self.changed.notify_all();
        let deadline = Instant::now() + Duration::from_secs(90);
        while !state.running.is_empty() {
            if state.uncertain || !state.quarantined.is_empty() || Instant::now() >= deadline {
                state.switching = false;
                self.changed.notify_all();
                anyhow::bail!("等待在途请求结束超时或出现未知结果，未切换账号");
            }
            state = self
                .changed
                .wait_timeout(state, deadline.saturating_duration_since(Instant::now()))
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
        if state.uncertain || !state.quarantined.is_empty() {
            state.switching = false;
            self.changed.notify_all();
            anyhow::bail!("在途请求出现未知结果，未切换账号");
        }
        drop(state);
        let result = activate();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.switching = false;
        match &result {
            Ok(Activation::Changed) => {
                state.account_epoch = state.account_epoch.wrapping_add(1);
                state.rotation_hold = false;
                state.bindings.clear();
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
                if journal.save_account(throttle).is_err() {
                    *uncertain = true;
                }
            }
            Ok(Activation::Restored) => {}
            Err(_) => {
                state.uncertain = true;
            }
        }
        self.changed.notify_all();
        anyhow::ensure!(
            !state.uncertain,
            "账号状态未能确认，已停止派发；请检查认证或恢复服务"
        );
        result
    }
}
