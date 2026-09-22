//! Bounded, process-local metrics and request bodies; excludes HTTP headers.
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

pub(crate) static METRICS: LazyLock<Arc<Metrics>> = LazyLock::new(|| Arc::new(Metrics::default()));
const RECENT_LIMIT: usize = 100;

#[derive(Serialize)]
struct Sample {
    id: u64,
    #[serde(skip)]
    body: Option<String>,
    body_truncated: bool,
    #[serde(skip)]
    client_headers: Vec<serde_json::Value>,
    #[serde(skip)]
    upstream: Option<serde_json::Value>,
    completed_at: u64,
    status: u16,
    duration_ms: f64,
    transport_error: bool,
}

#[derive(Default, Serialize)]
struct State {
    total: u64,
    completed: u64,
    transport_errors: u64,
    status_counts: BTreeMap<u16, u64>,
    model_counts: BTreeMap<String, u64>,
    total_duration_ms: f64,
    max_duration_ms: f64,
    recent: VecDeque<Sample>,
}

pub(crate) struct Metrics {
    started_at: u64,
    state: Mutex<State>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            started_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            state: Mutex::new(State::default()),
        }
    }
}

impl Metrics {
    pub(crate) fn start(self: &Arc<Self>) -> Completion {
        let id = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.total += 1;
            state.total
        };
        Completion {
            id,
            body: None,
            body_truncated: false,
            client_headers: Vec::new(),
            upstream: None,
            metrics: Arc::clone(self),
            started: Instant::now(),
            status: 500,
            delivered: false,
            model: None,
        }
    }

    pub(crate) fn detail(&self, id: u64) -> Option<serde_json::Value> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .recent
            .iter()
            .find(|sample| sample.id == id)
            .and_then(|sample| {
                let mut value = serde_json::to_value(sample).ok()?;
                value["body"] = serde_json::json!(sample.body);
                value["client_headers"] = serde_json::json!(sample.client_headers);
                value["upstream"] = serde_json::json!(sample.upstream);
                Some(value)
            })
    }

    pub(crate) fn snapshot(&self) -> serde_json::Value {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        serde_json::json!({
            "started_at": self.started_at,
            "total": state.total,
            "completed": state.completed,
            "in_flight": state.total - state.completed,
            "transport_errors": state.transport_errors,
            "status_counts": state.status_counts,
            "model_counts": state.model_counts,
            "average_duration_ms": if state.completed == 0 { 0.0 } else { state.total_duration_ms / state.completed as f64 },
            "max_duration_ms": state.max_duration_ms,
            "recent": state.recent,
        })
    }
}

/// Owns the request lifetime through the final response byte, including queue time.
pub(crate) struct Completion {
    id: u64,
    body: Option<String>,
    body_truncated: bool,
    pub(crate) client_headers: Vec<serde_json::Value>,
    pub(crate) upstream: Option<serde_json::Value>,
    metrics: Arc<Metrics>,
    started: Instant,
    pub(crate) status: u16,
    pub(crate) delivered: bool,
    model: Option<String>,
}

impl Completion {
    pub(crate) fn capture(&mut self, body: &[u8]) {
        // At most 64 KiB per request and 100 retained requests; never retain headers.
        const LIMIT: usize = 64 * 1024;
        self.body_truncated = body.len() > LIMIT;
        self.body = Some(String::from_utf8_lossy(&body[..body.len().min(LIMIT)]).into_owned());
        self.model = model_from_json(body);
    }

    pub(crate) fn capture_upstream_model(&mut self, upstream: &serde_json::Value) {
        if let Some(body) = upstream.get("body").and_then(serde_json::Value::as_str)
            && let Some(model) = model_from_json(body.as_bytes())
        {
            self.model = Some(model);
        }
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        let duration_ms = self.started.elapsed().as_secs_f64() * 1000.0;
        let mut state = self
            .metrics
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.completed += 1;
        state.transport_errors += u64::from(!self.delivered);
        *state.status_counts.entry(self.status).or_default() += 1;
        if let Some(model) = self.model.take() {
            *state.model_counts.entry(model).or_default() += 1;
        }
        state.total_duration_ms += duration_ms;
        state.max_duration_ms = state.max_duration_ms.max(duration_ms);
        if state.recent.len() == RECENT_LIMIT {
            state.recent.pop_back();
        }
        state.recent.push_front(Sample {
            id: self.id,
            body: self.body.take(),
            body_truncated: self.body_truncated,
            client_headers: std::mem::take(&mut self.client_headers),
            upstream: self.upstream.take(),
            completed_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            status: self.status,
            duration_ms,
            transport_error: !self.delivered,
        });
    }
}

fn model_from_json(body: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let model = value.get("model")?.as_str()?.trim();
    (1..=128).contains(&model.len()).then(|| model.to_owned())
}

#[cfg(test)]
#[path = "request_metrics_tests.rs"]
mod tests;
