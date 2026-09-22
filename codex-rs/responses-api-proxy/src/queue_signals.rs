//! Bounded observation only: the original request and response bytes stay untouched.
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;
use std::collections::VecDeque;
use std::time::Duration;
use std::time::Instant;

#[derive(Clone, Default)]
pub(super) struct Evidence {
    prefixes: VecDeque<(usize, String)>,
    items: VecDeque<(usize, Option<String>, bool)>,
    pub length: usize,
    pub hash: String,
    previous: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Kind {
    User,
    Tool,
    Unknown,
}

#[derive(Default)]
pub(super) struct Signals {
    pub input: Evidence,
    pub output: Observation,
    pub finished: Option<Instant>,
}

impl Evidence {
    pub(super) fn read(body: &Value) -> Self {
        let mut evidence = Self {
            previous: bounded_string(&body["previous_response_id"]),
            ..Self::default()
        };
        let Some(items) = body["input"].as_array() else {
            return evidence;
        };
        let mut hash = Sha256::new();
        for (index, item) in items.iter().enumerate() {
            evidence
                .prefixes
                .push_back((index, format!("{:x}", hash.clone().finalize())));
            let bytes = serde_json::to_vec(item).unwrap_or_default();
            hash.update((bytes.len() as u64).to_le_bytes());
            hash.update(bytes);
            let output = matches!(
                item["type"].as_str(),
                Some("function_call_output" | "custom_tool_call_output")
            );
            evidence.items.push_back((
                index,
                output.then(|| bounded_string(&item["call_id"])).flatten(),
                item["role"] == "user",
            ));
            if evidence.prefixes.len() > 64 {
                evidence.prefixes.pop_front();
                evidence.items.pop_front();
            }
        }
        evidence.length = items.len();
        evidence.hash = format!("{:x}", hash.finalize());
        evidence
    }

    pub(super) fn kind(&self, prior: &Signals, now: Instant) -> Kind {
        let Some(finished) = prior.finished else {
            return Kind::Unknown;
        };
        if !prior.output.valid || now.duration_since(finished) > Duration::from_secs(300) {
            return Kind::Unknown;
        }
        let start = if self.previous.is_some() && self.previous == prior.output.response_id {
            0
        } else if self
            .prefixes
            .iter()
            .any(|(index, hash)| *index == prior.input.length && hash == &prior.input.hash)
        {
            prior.input.length
        } else {
            return Kind::Unknown;
        };
        if self.length.saturating_sub(start) > 64 {
            return Kind::Unknown;
        }
        let added: Vec<_> = self
            .items
            .iter()
            .filter(|(index, _, _)| *index >= start)
            .collect();
        if added.last().is_some_and(|(_, _, user)| *user) {
            return Kind::User;
        }
        let outputs: Vec<_> = added.iter().filter_map(|(_, id, _)| id.as_ref()).collect();
        if !outputs.is_empty()
            && !added.iter().any(|(_, _, user)| *user)
            && added.last().is_some_and(|(_, id, _)| id.is_some())
            && outputs.iter().all(|id| prior.output.calls.contains(id))
        {
            Kind::Tool
        } else {
            Kind::Unknown
        }
    }
}

#[derive(Default)]
pub(super) struct Observation {
    pub valid: bool,
    pub terminal: bool,
    pub successful: bool,
    pub rate_limited: bool,
    pub quota_exhausted: bool,
    pub response_id: Option<String>,
    pub calls: Vec<String>,
}

fn bounded_string(value: &Value) -> Option<String> {
    value
        .as_str()
        .filter(|value| !value.is_empty() && value.len() <= 512)
        .map(str::to_string)
}

pub(super) struct Observer {
    pub(crate) recorder: Option<crate::continuation_index::Recorder>,
    streaming: bool,
    line: Vec<u8>,
    data: Vec<u8>,
    overflow: bool,
    named_terminal: bool,
    observation: Observation,
}

const MAX_EVENT: usize = 256 * 1024;

impl Observer {
    pub(super) fn new(streaming: bool) -> Self {
        Self {
            recorder: None,
            streaming,
            line: Vec::new(),
            data: Vec::new(),
            overflow: false,
            named_terminal: false,
            observation: Observation {
                valid: true,
                ..Observation::default()
            },
        }
    }

    pub(super) fn bytes(&mut self, bytes: &[u8]) {
        let max_event = if self.recorder.is_some() {
            4 * 1024 * 1024
        } else {
            MAX_EVENT
        };
        if !self.streaming {
            if self.data.len() + bytes.len() <= max_event {
                self.data.extend_from_slice(bytes);
            } else {
                self.observation.valid = false;
                self.overflow = true;
                self.data.clear();
            }
            return;
        }
        for byte in bytes {
            if *byte == b'\n' {
                if self.line.last() == Some(&b'\r') {
                    self.line.pop();
                }
                if self.line.is_empty() {
                    if !self.overflow {
                        self.parse();
                    }
                    // A fully delimited named terminal event remains a terminal
                    // signal even when its large JSON payload is not observed.
                    self.observation.terminal |=
                        self.named_terminal && (self.overflow || !self.data.is_empty());
                    self.named_terminal = false;
                    self.data.clear();
                    self.overflow = false;
                } else if self.line.starts_with(b"event:") {
                    self.named_terminal = std::str::from_utf8(&self.line[6..]).is_ok_and(|name| {
                        matches!(
                            name.trim(),
                            "response.completed" | "response.failed" | "response.incomplete"
                        )
                    });
                } else if self.line.starts_with(b"data:") && !self.overflow {
                    if self.data.len() + self.line.len() > max_event {
                        self.overflow = true;
                        self.observation.valid = false;
                    } else {
                        self.data.extend_from_slice(&self.line[5..]);
                        self.data.push(b'\n');
                    }
                }
                self.line.clear();
            } else if self.line.len() < max_event {
                self.line.push(*byte);
            } else {
                self.overflow = true;
                self.observation.valid = false;
            }
        }
    }

    fn parse(&mut self) {
        let Ok(event) = serde_json::from_slice::<Value>(&self.data) else {
            if !std::str::from_utf8(&self.data)
                .is_ok_and(|text| text.trim().is_empty() || text.trim() == "[DONE]")
            {
                self.observation.valid = false;
            }
            return;
        };
        if let Some(recorder) = &self.recorder {
            recorder.observe(&event);
        }
        let kind = event["type"].as_str().unwrap_or_default();
        let response = if self.streaming {
            &event["response"]
        } else {
            &event
        };
        self.observation.quota_exhausted |= [
            response["error"]["code"].as_str(),
            response["error"]["type"].as_str(),
        ]
        .into_iter()
        .flatten()
        .any(|code| {
            matches!(
                code,
                "insufficient_quota" | "billing_hard_limit_reached" | "usage_limit_reached"
            )
        });
        if let Some(id) = bounded_string(&response["id"]) {
            self.observation.response_id = Some(id);
        }
        if matches!(
            kind,
            "response.completed" | "response.failed" | "response.incomplete"
        ) || (!self.streaming
            && matches!(
                response["status"].as_str(),
                Some("completed" | "failed" | "incomplete")
            ))
        {
            self.observation.terminal = true;
            self.observation.successful = kind == "response.completed"
                || (!self.streaming && response["status"] == "completed");
            self.observation.rate_limited = matches!(
                response["error"]["code"].as_str(),
                Some("rate_limit_exceeded" | "rate_limit_error")
            );
        }
        let mut items = Vec::new();
        if kind == "response.output_item.done" {
            items.push(&event["item"]);
        }
        if let Some(output) = response["output"].as_array() {
            items.extend(output);
        }
        for item in items {
            if matches!(
                item["type"].as_str(),
                Some("function_call" | "custom_tool_call")
            ) {
                if let Some(id) = bounded_string(&item["call_id"]) {
                    if !self.observation.calls.contains(&id) {
                        self.observation.calls.push(id);
                    }
                    if self.observation.calls.len() > 64 {
                        self.observation.valid = false;
                        self.observation.calls.clear();
                        break;
                    }
                } else {
                    self.observation.valid = false;
                }
            }
        }
    }

    pub(super) fn finish(mut self) -> Observation {
        if !self.streaming && !self.overflow {
            self.parse();
        }
        if !self.observation.terminal {
            self.observation.valid = false;
        }
        self.observation
    }
}

#[cfg(test)]
#[path = "queue_signals_tests.rs"]
mod tests;
