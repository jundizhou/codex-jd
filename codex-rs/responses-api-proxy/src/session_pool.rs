//! Fixed-capacity pool of model-access sessions.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Condvar;
use std::sync::Mutex;

use anyhow::Result;

use super::identity::SessionIdentity;

const POOL_SIZE: usize = 5;

pub(crate) struct SessionPool {
    state: Mutex<PoolState>,
    available: Condvar,
}

struct PoolState {
    idle: VecDeque<SessionIdentity>,
    affinity: HashMap<String, String>,
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
            state: Mutex::new(PoolState {
                idle: identities.into(),
                affinity: HashMap::new(),
            }),
            available: Condvar::new(),
        })
    }

    pub(crate) fn acquire(&self, affinity_key: Option<&str>) -> SessionIdentity {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if let Some(key) = affinity_key
                && let Some(thread_id) = state.affinity.get(key).cloned()
            {
                if let Some(index) = state
                    .idle
                    .iter()
                    .position(|identity| identity.thread_id == thread_id)
                {
                    match state.idle.remove(index) {
                        Some(identity) => return identity,
                        None => continue,
                    }
                }
                state = self
                    .available
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                continue;
            }

            if let Some(identity) = state.idle.pop_front() {
                if let Some(key) = affinity_key {
                    state
                        .affinity
                        .retain(|_, thread_id| thread_id != &identity.thread_id);
                    state
                        .affinity
                        .insert(key.to_string(), identity.thread_id.clone());
                }
                return identity;
            }

            state = self
                .available
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    pub(crate) fn release(&self, identity: SessionIdentity) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.idle.push_back(identity);
        // Waiters may require different slots, so every predicate must be rechecked.
        self.available.notify_all();
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
                let lease = pool.acquire(None);
                thread::sleep(Duration::from_millis(20));
                pool.release(lease);
                index
            }));
        }
        for handle in handles {
            handle.join().expect("worker should finish");
        }
        assert_eq!(pool.state.lock().expect("pool mutex").idle.len(), 5);
    }

    #[test]
    fn release_wakes_waiting_acquire() {
        let pool = Arc::new(
            SessionPool::new((0..5).map(identity).collect()).expect("create session pool"),
        );
        let mut held = Vec::new();
        for _ in 0..5 {
            held.push(pool.acquire(None));
        }
        let waiter = {
            let pool = Arc::clone(&pool);
            thread::spawn(move || pool.acquire(None))
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

    #[test]
    fn affinity_reuses_the_same_session_after_release() {
        let pool = SessionPool::new((0..5).map(identity).collect()).expect("create session pool");
        let first = pool.acquire(Some("session-a"));
        let first_thread = first.thread_id.clone();
        pool.release(first);

        let second = pool.acquire(Some("session-a"));
        assert_eq!(second.thread_id, first_thread);
        pool.release(second);
    }

    #[test]
    fn affinity_keeps_five_distinct_sessions_available() {
        let pool = SessionPool::new((0..5).map(identity).collect()).expect("create session pool");
        let mut leases = Vec::new();
        for index in 0..5 {
            leases.push(pool.acquire(Some(&format!("session-{index}"))));
        }
        let mut thread_ids: Vec<_> = leases
            .iter()
            .map(|identity| identity.thread_id.clone())
            .collect();
        thread_ids.sort();
        thread_ids.dedup();
        assert_eq!(thread_ids.len(), 5);
        for identity in leases {
            pool.release(identity);
        }
        assert_eq!(pool.state.lock().expect("pool mutex").idle.len(), 5);
    }

    #[test]
    fn releasing_one_slot_wakes_its_affine_waiter() {
        let pool = Arc::new(SessionPool::new((0..5).map(identity).collect()).expect("pool"));
        let mut held: Vec<_> = (0..5)
            .map(|index| pool.acquire(Some(&format!("key-{index}"))))
            .collect();
        let barrier = Arc::new(std::sync::Barrier::new(6));
        let (tx, rx) = std::sync::mpsc::channel();
        let workers: Vec<_> = (0..5)
            .map(|index| {
                let pool = Arc::clone(&pool);
                let barrier = Arc::clone(&barrier);
                let tx = tx.clone();
                thread::spawn(move || {
                    barrier.wait();
                    let lease = pool.acquire(Some(&format!("key-{index}")));
                    tx.send(index).expect("send result");
                    pool.release(lease);
                })
            })
            .collect();
        barrier.wait();
        thread::sleep(Duration::from_millis(50));
        pool.release(held.pop().expect("last slot"));
        let result = rx.recv_timeout(Duration::from_secs(2));
        for lease in held {
            pool.release(lease);
        }
        for worker in workers {
            worker.join().expect("worker finishes");
        }
        assert_eq!(result, Ok(4));
    }
}
