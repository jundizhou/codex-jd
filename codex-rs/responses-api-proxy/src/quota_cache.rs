//! Shared, bounded quota snapshots. Timers never perform network I/O without demand.
use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Condvar;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Failure {
    pub message: String,
    pub retry_after: Option<u64>,
    pub auth_invalid: bool,
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for Failure {}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default)]
struct Record {
    value: Option<Value>,
    failure: Option<Failure>,
    retry_at: u64,
    failures: usize,
    network_queries: u64,
    last_query_at: u64,
}
#[derive(Default)]
struct State {
    records: HashMap<String, Record>,
    active: HashSet<String>,
}
#[derive(Default)]
pub(crate) struct Cache {
    state: Mutex<State>,
    changed: Condvar,
    path: OnceLock<PathBuf>,
}
pub(crate) static CACHE: LazyLock<Cache> = LazyLock::new(Cache::default);
#[derive(Clone, Copy)]
pub(crate) enum Freshness {
    Manual,
    Active,
    Candidate,
}

pub(crate) fn key(auth: &Value) -> String {
    crate::queue_store::digest(&[auth.to_string().as_bytes()])
}
pub(crate) fn remaining(value: &Value) -> Option<f64> {
    let main = value["limits"]
        .as_array()?
        .iter()
        .find(|v| v["name"] == "Codex")?;
    if main["limit_reached"] == true
        || main["allowed"] == false
        || value["spend_control_reached"] == true
    {
        return Some(0.0);
    }
    main["windows"]
        .as_array()?
        .iter()
        .filter_map(|w| w["remaining_percent"].as_f64())
        .reduce(f64::min)
}
pub(crate) fn eligible(value: &Value) -> bool {
    value["partial"] != true
        && remaining(value).is_some_and(|left| left > 5.0)
        && value["limits"].as_array().is_some_and(|groups| {
            groups.iter().filter(|g| g["name"] == "Codex").all(|g| {
                g["windows"].as_array().is_some_and(|windows| {
                    !windows.is_empty()
                        && windows.iter().all(|w| w["remaining_percent"].is_number())
                })
            })
        })
}
pub(crate) fn due(value: &Value, mode: Freshness) -> u64 {
    let at = value["fetched_at"].as_u64().unwrap_or(0);
    let interval = match mode {
        Freshness::Manual => 30,
        Freshness::Active if remaining(value).is_some_and(|v| v <= 10.0) => 120,
        Freshness::Active => 600,
        Freshness::Candidate => 120,
    };
    let reset_threshold = match mode {
        Freshness::Manual => None,
        Freshness::Active => Some(2.0),
        Freshness::Candidate => Some(5.0),
    };
    let reset = if reset_threshold
        .is_some_and(|threshold| remaining(value).is_some_and(|v| v <= threshold))
    {
        value["limits"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|g| g["name"] == "Codex")
            .flat_map(|g| g["windows"].as_array().into_iter().flatten())
            .filter(|w| w["remaining_percent"].as_f64().is_some_and(|v| v <= 5.0))
            .filter_map(|w| w["reset_at"].as_u64())
            .max()
            .unwrap_or(at.saturating_add(900))
    } else {
        0
    };
    at.saturating_add(interval).max(reset)
}

impl Cache {
    pub(crate) fn observe(&self, key: &str, headers: &HashMap<String, String>, now: u64) {
        let mut windows = Vec::new();
        for slot in ["primary", "secondary"] {
            let used = headers
                .get(&format!("x-codex-{slot}-used-percent"))
                .and_then(|s| s.parse::<f64>().ok())
                .filter(|v| v.is_finite() && *v >= 0.0);
            let seconds = headers
                .get(&format!("x-codex-{slot}-window-minutes"))
                .and_then(|s| s.parse::<u64>().ok())
                .and_then(|v| v.checked_mul(60))
                .filter(|v| *v > 0);
            if let (Some(used), Some(seconds)) = (used, seconds) {
                windows.push(serde_json::json!({"seconds":seconds,"used_percent":used,"remaining_percent":(100.0-used).clamp(0.0,100.0),
                    "reset_at":headers.get(&format!("x-codex-{slot}-reset-at")).and_then(|v| v.parse::<u64>().ok())}));
            }
        }
        if windows.is_empty() {
            return;
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(record) = state.records.get_mut(key) else {
            return;
        };
        let Some(value) = record.value.as_mut() else {
            return;
        };
        let Some(main) = value["limits"]
            .as_array_mut()
            .and_then(|groups| groups.iter_mut().find(|g| g["name"] == "Codex"))
        else {
            return;
        };
        let Some(existing) = main["windows"].as_array_mut() else {
            return;
        };
        let complete = !existing.is_empty()
            && existing
                .iter()
                .all(|old| windows.iter().any(|new| new["seconds"] == old["seconds"]));
        for window in windows {
            if let Some(old) = existing
                .iter_mut()
                .find(|old| old["seconds"] == window["seconds"])
            {
                if old["reset_at"] == window["reset_at"]
                    && old["remaining_percent"]
                        .as_f64()
                        .zip(window["remaining_percent"].as_f64())
                        .is_some_and(|(a, b)| a < b)
                {
                    continue;
                }
                *old = window;
            } else if existing.len() < 2 {
                existing.push(window);
            }
        }
        // Partial observations can lower availability, but cannot erase a known block.
        if complete {
            main["limit_reached"] = false.into();
            main["allowed"] = true.into();
            value["fetched_at"] = now.into();
        }
    }
    pub(crate) fn block(&self, key: String, auth_invalid: bool, now: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.records.len() >= 64 && !state.records.contains_key(&key) {
            return;
        }
        let record = state.records.entry(key).or_default();
        if auth_invalid {
            record.failure = Some(Failure {
                message: "账号认证失效".into(),
                retry_after: None,
                auth_invalid: true,
            });
            record.retry_at = u64::MAX;
        } else if let Some(value) = &mut record.value {
            if let Some(groups) = value["limits"].as_array_mut() {
                for group in groups.iter_mut().filter(|g| g["name"] == "Codex") {
                    group["limit_reached"] = true.into();
                }
            }
        } else {
            record.value = Some(
                serde_json::json!({"fetched_at":now,"limits":[{"name":"Codex","limit_reached":true,"windows":[]}]}),
            );
        }
    }
    pub(crate) fn initialize(&self, path: PathBuf) -> Result<()> {
        let file = match std::fs::File::open(&path) {
            Ok(file) => Some(file),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        if let Some(file) = file {
            let mut bytes = Vec::new();
            file.take(1024 * 1024 + 1).read_to_end(&mut bytes)?;
            anyhow::ensure!(bytes.len() <= 1024 * 1024, "quota cache too large");
            let records: HashMap<String, Record> = serde_json::from_slice(&bytes)?;
            anyhow::ensure!(records.len() <= 64, "too many quota cache records");
            self.state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .records = records;
        }
        let _ = self.path.set(path);
        Ok(())
    }

    pub(crate) fn status(&self, key: &str, mode: Freshness) -> Value {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match state.records.get(key) {
            Some(record) => {
                serde_json::json!({"quota":record.value,"error":record.failure.as_ref().map(|f| &f.message),
                "network_queries":record.network_queries,"last_query_at":record.last_query_at,
                "auth_invalid":record.failure.as_ref().is_some_and(|f| f.auth_invalid),
                "next_check_at":if record.retry_at == u64::MAX { None } else { Some(record.retry_at.max(record.value.as_ref().map_or(0, |v| due(v, mode)))) }})
            }
            None => {
                serde_json::json!({"quota":null,"error":null,"auth_invalid":false,"next_check_at":0})
            }
        }
    }

    pub(crate) fn get(
        &self,
        key: String,
        mode: Freshness,
        now: u64,
        fetch: impl FnOnce() -> Result<Value>,
    ) -> Result<Value> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let deadline = Instant::now() + Duration::from_secs(20);
        while state.active.contains(&key) {
            let wait = deadline.saturating_duration_since(Instant::now());
            anyhow::ensure!(!wait.is_zero(), "额度查询正在进行，请稍后重试");
            state = self
                .changed
                .wait_timeout(state, wait)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
        if let Some(record) = state.records.get(&key) {
            if now < record.retry_at
                && let Some(error) = &record.failure
            {
                return Err(error.clone().into());
            }
            if let Some(value) = &record.value
                && now < due(value, mode)
            {
                let mut result = value.clone();
                result["cached"] = true.into();
                return Ok(result);
            }
        }
        anyhow::ensure!(state.active.len() < 4, "额度查询繁忙，请稍后重试");
        state.active.insert(key.clone());
        drop(state);
        let result = fetch();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.records.len() >= 64
            && !state.records.contains_key(&key)
            && let Some(oldest) = state
                .records
                .iter()
                .filter(|(k, _)| !state.active.contains(*k))
                .min_by_key(|(_, r)| {
                    r.value
                        .as_ref()
                        .and_then(|v| v["fetched_at"].as_u64())
                        .unwrap_or(0)
                })
                .map(|(k, _)| k.clone())
        {
            state.records.remove(&oldest);
        }
        let record = state.records.entry(key.clone()).or_default();
        let queries = record.network_queries.saturating_add(1);
        let result = match result {
            Ok(mut value) => {
                value["cached"] = false.into();
                *record = Record {
                    value: Some(value.clone()),
                    ..Record::default()
                };
                Ok(value)
            }
            Err(error) => {
                let failure = error.downcast_ref::<Failure>().cloned().unwrap_or(Failure {
                    message: error.to_string(),
                    retry_after: None,
                    auth_invalid: false,
                });
                record.failures = record.failures.saturating_add(1);
                let delay = [60, 300, 900][record.failures.saturating_sub(1).min(2)]
                    .max(failure.retry_after.unwrap_or(0));
                record.retry_at = if failure.auth_invalid {
                    u64::MAX
                } else {
                    now.saturating_add(delay)
                };
                record.failure = Some(failure.clone());
                Err(failure.into())
            }
        };
        record.network_queries = queries;
        record.last_query_at = now;
        state.active.remove(&key);
        let persisted = self
            .path
            .get()
            .map(|path| {
                crate::admin_accounts::write_private(path, &serde_json::to_value(&state.records)?)
            })
            .transpose();
        self.changed.notify_all();
        persisted?;
        result
    }
}

#[cfg(test)]
#[path = "quota_cache_tests.rs"]
mod tests;
