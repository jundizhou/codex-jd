//! Fixed-capacity pool of model-access sessions.

use std::collections::VecDeque;
use std::sync::Condvar;
use std::sync::Mutex;

use anyhow::Result;

use super::identity::SessionIdentity;

const POOL_SIZE: usize = 5;

pub(crate) struct SessionPool {
    idle: Mutex<VecDeque<SessionIdentity>>,
    available: Condvar,
}

impl SessionPool {
    pub(crate) fn new(identities: Vec<SessionIdentity>) -> Result<Self> {
        if identities.len() != POOL_SIZE {
            return Err(anyhow::anyhow!(
                "expected {POOL_SIZE} app-server sessions, received {}",
                identities.len()
            ));
        }

        Ok(Self {
            idle: Mutex::new(identities.into()),
            available: Condvar::new(),
        })
    }

    pub(crate) fn acquire(&self) -> SessionIdentity {
        let mut idle = self
            .idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if let Some(identity) = idle.pop_front() {
                return identity;
            }
            idle = self
                .available
                .wait(idle)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    pub(crate) fn release(&self, identity: SessionIdentity) {
        let mut idle = self
            .idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        idle.push_back(identity);
        self.available.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::SessionPool;
    use crate::identity::SessionIdentity;
    use pretty_assertions::assert_eq;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    fn identity(index: usize) -> SessionIdentity {
        SessionIdentity {
            installation_id: format!("install-{index}"),
            session_id: format!("session-{index}"),
            thread_id: format!("thread-{index}"),
            window_id: format!("thread-{index}:0"),
            parent_thread_id: None,
            turn_id: None,
            root_turn_id: None,
            parent_turn_id: None,
        }
    }

    #[test]
    fn pool_limits_concurrent_leases_to_configured_size() {
        let pool = Arc::new(
            SessionPool::new((0..5).map(identity).collect()).expect("create session pool"),
        );
        let mut handles = Vec::new();
        for index in 0..5 {
            let pool = Arc::clone(&pool);
            handles.push(thread::spawn(move || {
                let lease = pool.acquire();
                thread::sleep(Duration::from_millis(20));
                pool.release(lease);
                index
            }));
        }
        for handle in handles {
            handle.join().expect("worker should finish");
        }
        assert_eq!(pool.idle.lock().expect("pool mutex").len(), 5);
    }

    #[test]
    fn release_wakes_waiting_acquire() {
        let pool = Arc::new(
            SessionPool::new((0..5).map(identity).collect()).expect("create session pool"),
        );
        let mut held = Vec::new();
        for _ in 0..5 {
            held.push(pool.acquire());
        }
        let waiter = {
            let pool = Arc::clone(&pool);
            thread::spawn(move || pool.acquire())
        };
        thread::sleep(Duration::from_millis(20));
        pool.release(held.pop().expect("held identity"));
        let acquired = waiter.join().expect("waiter should finish");
        pool.release(acquired);
        for identity in held {
            pool.release(identity);
        }
    }

    #[test]
    fn requires_exactly_five_sessions() {
        let result = SessionPool::new((0..4).map(identity).collect());

        assert_eq!(
            result.err().map(|error| error.to_string()),
            Some("expected 5 app-server sessions, received 4".to_string())
        );
    }
}
