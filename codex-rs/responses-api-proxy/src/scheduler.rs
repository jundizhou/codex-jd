//! Bounded, tenant-fair admission with independent conversation identities.
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use crate::queue_signals::Evidence;
use crate::queue_signals::Kind;
use crate::queue_signals::Observation;
use crate::queue_signals::Signals;
use crate::queue_store::Journal;
use crate::queue_store::digest;
use crate::queue_throttle::Throttle;
#[path = "scheduler_admin.rs"]
mod admin;
#[path = "scheduler_admission.rs"]
mod admission;
#[path = "scheduler_lifecycle.rs"]
mod lifecycle;
#[path = "scheduler_recovery.rs"]
mod recovery;
use lifecycle::Attempt;
pub(crate) use lifecycle::Dispatch;
pub(crate) use lifecycle::Lease;

pub(crate) const MAX_PENDING: usize = 24;
pub(crate) const MAX_QUEUE_WAIT: Duration = Duration::from_secs(120);
pub(crate) const MAX_BODY: usize = 32 * 1024 * 1024;
const MAX_RESIDENT: usize = 128 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct Config {
    pub automatic_recovery: bool,
    pub max_running: usize,
    pub gap: Duration,
    pub user_gap: Duration,
    pub tool_gap: Duration,
    pub idle_ttl: Duration,
    pub start_gap: Duration,
    pub timeout: Duration,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct Key(pub String, pub String);

pub(crate) struct Pending {
    pub id: String,
    pub key: Key,
    pub sticky: bool,
    pub fingerprint: String,
    pub evidence: Evidence,
    pub bytes: usize,
    pub deadline: Instant,
}

struct Binding {
    finished: Option<Instant>,
    signals: Signals,
}

#[derive(Default)]
struct State {
    max_running: usize,
    pending: Vec<Pending>,
    running: HashMap<Key, String>,
    tenants: VecDeque<String>,
    conversations: VecDeque<Key>,
    bindings: HashMap<Key, Binding>,
    cancelled: HashMap<Key, Instant>,
    next_start: Option<Instant>,
    uncertain: bool,
    // Persisted request IDs and conversation digests still consuming upstream capacity.
    quarantined: HashMap<String, Option<String>>,
    paused: bool,
    journal: Journal,
    throttle: Throttle,
    attempts: HashMap<String, Arc<Attempt>>,
    tool_streak: Option<(Key, u8)>,
    dispatching: Option<String>,
    inbound: usize,
    bytes: usize,
}

pub(crate) struct Scheduler {
    config: Config,
    state: Mutex<State>,
    changed: Condvar,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Rejection {
    Full,
    Limited,
    Expired,
    Cancelled,
    Duplicate,
    Unavailable,
    TooLarge,
    Conflict,
    Completed,
    OutcomeUnknown,
    BindingLost,
    IdentityUnavailable,
    Paused,
    Cooldown(u64),
}

impl std::fmt::Display for Rejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.response().1)
    }
}

impl std::error::Error for Rejection {}

impl Rejection {
    pub(crate) fn response(self) -> (u16, &'static str) {
        match self {
            Self::Conflict => (409, "idempotency_conflict"),
            Self::Completed => (409, "request_already_completed"),
            Self::OutcomeUnknown => (409, "request_outcome_unknown"),
            Self::IdentityUnavailable => (503, "worker_identity_unavailable"),
            Self::BindingLost => (409, "conversation_binding_lost"),
            Self::Paused => (503, "worker_unavailable"),
            Self::Cooldown(_) => (429, "account_cooldown"),
            Self::Full => (503, "worker_overloaded"),
            Self::Limited => (429, "queue_limit_exceeded"),
            Self::Expired => (503, "queue_deadline_exceeded"),
            Self::Cancelled => (409, "request_cancelled"),
            Self::Duplicate => (409, "request_in_progress"),
            Self::Unavailable => (503, "worker_outcome_unknown"),
            Self::TooLarge => (413, "request_too_large"),
        }
    }
}

impl State {
    fn configured_max(&self, config: &Config) -> usize {
        if self.max_running == 0 {
            config.max_running
        } else {
            self.max_running
        }
    }

    fn running_cap(&self, config: &Config) -> usize {
        let configured = self.configured_max(config);
        configured.min(if self.throttle.limit == 0 {
            configured
        } else {
            self.throttle.limit
        })
    }

    fn remove(&mut self, id: &str) -> Option<Pending> {
        let index = self.pending.iter().position(|p| p.id == id)?;
        let pending = self.pending.remove(index);
        self.tenants.retain(|t| t != &pending.key.0);
        self.conversations.retain(|key| key != &pending.key);
        if self.pending.iter().any(|p| p.key.0 == pending.key.0) {
            self.tenants.push_back(pending.key.0.clone());
        }
        if self.pending.iter().any(|p| p.key == pending.key) {
            self.conversations.push_back(pending.key.clone());
        }
        Some(pending)
    }

    fn selected(&self, now: Instant, config: &Config) -> Option<&str> {
        if self.uncertain
            || self.paused
            || self.throttle.blocked
            || self.throttle.until.is_some_and(|until| now < until)
            || self.dispatching.is_some()
            || self.running.len() + self.quarantined.len() >= self.running_cap(config)
            || self.next_start.is_some_and(|next| now < next)
        {
            return None;
        }
        let idle_tenants = self
            .tenants
            .iter()
            .filter(|tenant| !self.running.keys().any(|key| &key.0 == *tenant));
        let busy_tenants = self
            .tenants
            .iter()
            .filter(|tenant| self.running.keys().any(|key| &key.0 == *tenant));
        for tenant in idle_tenants.chain(busy_tenants) {
            let mut eligible = Vec::new();
            for key in self.conversations.iter().filter(|key| &key.0 == tenant) {
                if self
                    .quarantined
                    .values()
                    .any(|digest| digest.as_deref() == Some(&key.digest()))
                {
                    continue;
                }
                let Some(pending) = self.pending.iter().find(|pending| &pending.key == key) else {
                    continue;
                };
                let binding = self.bindings.get(key);
                let ready = binding.is_none_or(|binding| {
                    binding
                        .finished
                        .is_none_or(|finished| now >= finished + pending.gap(binding, config, now))
                });
                if ready && pending.deadline > now && !self.running.contains_key(key) {
                    let kind = binding.map_or(Kind::Unknown, |binding| {
                        pending.evidence.kind(&binding.signals, now)
                    });
                    eligible.push((pending, kind));
                }
            }
            // Preserve tenant round-robin. Within a tenant, deadlines take
            // precedence and verified tool work gets at most two consecutive starts.
            let selected = eligible
                .iter()
                .filter(|(pending, _)| {
                    pending.deadline.saturating_duration_since(now) <= Duration::from_secs(1)
                })
                .min_by_key(|(pending, _)| pending.deadline)
                .or_else(|| {
                    eligible.iter().find(|(pending, kind)| {
                        *kind == Kind::Tool
                            && !self
                                .tool_streak
                                .as_ref()
                                .is_some_and(|(key, count)| key == &pending.key && *count >= 2)
                    })
                })
                .or_else(|| eligible.first());
            if let Some((pending, _)) = selected {
                return Some(&pending.id);
            }
        }
        None
    }
}

impl Scheduler {
    pub(crate) fn new(config: Config, journal: Journal) -> Arc<Self> {
        let quarantined = journal.unfinished();
        // Legacy records without a conversation cannot safely isolate that binding.
        let uncertain = quarantined.values().any(Option::is_none);
        let (cooldown, blocked, limit) = journal.account();
        let until = cooldown.and_then(|duration| Instant::now().checked_add(duration));
        let throttle = Throttle {
            until,
            blocked: blocked || (cooldown.is_some() && until.is_none()),
            limit: if limit == 0 {
                config.max_running
            } else {
                limit.min(config.max_running)
            },
            ..Throttle::default()
        };
        // A fresh timing cache must not shorten a pre-restart conversation gap.
        let startup_gap = config
            .gap
            .max(config.user_gap)
            .max(config.tool_gap)
            .max(config.start_gap);
        let next_start = (!startup_gap.is_zero()).then(|| Instant::now() + startup_gap);
        Arc::new(Self {
            config: config.clone(),
            state: Mutex::new(State {
                max_running: config.max_running,
                quarantined,
                uncertain,
                next_start,
                journal,
                throttle,
                ..State::default()
            }),
            changed: Condvar::new(),
        })
    }

    pub(crate) fn cancel(&self, tenant: String, id: String) -> Result<(), Rejection> {
        let id = digest(&[tenant.as_bytes(), id.as_bytes()]);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .running
            .iter()
            .any(|(key, running)| key.0 == tenant && running == &id)
            && state.dispatching.as_deref() != Some(id.as_str())
        {
            if let Some(attempt) = state.attempts.get(&id) {
                attempt.finalize();
            }
            return Ok(());
        }
        let now = Instant::now();
        state.cancelled.retain(|_, expiry| *expiry > now);
        let key = Key(tenant, id);
        if state.cancelled.len() >= 128 && !state.cancelled.contains_key(&key) {
            return Err(Rejection::Full);
        }
        state.cancelled.insert(key, now + Duration::from_secs(60));
        self.changed.notify_all();
        Ok(())
    }

    pub(crate) fn status(&self) -> serde_json::Value {
        let s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        serde_json::json!({"pending":s.pending.len(),"running":s.running.len() + s.quarantined.len(),
            "max_running":s.max_running,"conversations":s.bindings.len(),
            "body_bytes_reserved":s.bytes,"outcome_unknown":s.uncertain || !s.quarantined.is_empty(),
            "quarantined":s.quarantined.len(), "worker_fault":s.uncertain,
            "automatic_recovery": self.config.automatic_recovery, "recovery": s.journal.recovery_status(),
            "effective_max_running":s.throttle.limit, "paused":s.paused, "account_unavailable":s.throttle.blocked,
            "account_unavailable_reason":s.throttle.blocked_reason,
            "cooldown_seconds":s.throttle.until.map(|until| {
                let left = until.saturating_duration_since(Instant::now());
                left.as_secs() + u64::from(left.subsec_nanos() != 0)
            }).unwrap_or(0)})
    }
}

pub(crate) struct Admission {
    scheduler: Arc<Scheduler>,
    bytes: usize,
}

impl Drop for Admission {
    fn drop(&mut self) {
        let mut state = self
            .scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.inbound -= 1;
        state.bytes -= self.bytes;
    }
}

#[cfg(test)]
#[path = "scheduler_tests.rs"]
mod tests;
