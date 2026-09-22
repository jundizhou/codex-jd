//! HTTP SDK compatibility at the worker boundary, after strict ingress capture.
use anyhow::Context;
use anyhow::Result;
use anyhow::ensure;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use tiny_http::Header;
use tiny_http::Response;
use tiny_http::StatusCode;

use crate::monitored_request::Request;

const MAX_EVENT_BYTES: usize = 4 * 1024 * 1024;
const MAX_OUTPUT_ITEMS: u64 = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Delivery {
    Stream,
    Json,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Compatibility {
    delivery: Delivery,
    ignored_output_limit: bool,
}

impl Compatibility {
    pub(crate) fn prepare(body: &mut Value) -> Result<Self> {
        let object = body.as_object_mut().context("request must be an object")?;
        let delivery = match object.get("stream") {
            Some(Value::Bool(true)) => Delivery::Stream,
            None | Some(Value::Null | Value::Bool(false)) => Delivery::Json,
            Some(_) => anyhow::bail!("stream must be a boolean or null"),
        };
        if let Some(value) = object.get("max_output_tokens") {
            ensure!(
                value.is_null() || value.as_u64().is_some_and(|limit| limit > 0),
                "max_output_tokens must be a positive integer or null"
            );
        }
        let ignored_output_limit = object
            .remove("max_output_tokens")
            .is_some_and(|value| !value.is_null());
        object.insert("stream".into(), Value::Bool(true));
        Ok(Self {
            delivery,
            ignored_output_limit,
        })
    }

    pub(crate) fn respond(
        &self,
        req: Request,
        status: StatusCode,
        mut headers: Vec<Header>,
        body: impl Read,
    ) -> io::Result<()> {
        if self.ignored_output_limit {
            headers.push(
                Header::from_bytes("x-codex-ignored-parameters", "max_output_tokens")
                    .map_err(|()| io::Error::other("invalid compatibility header"))?,
            );
            headers.push(
                Header::from_bytes("x-codex-output-token-limit", "not-enforced")
                    .map_err(|()| io::Error::other("invalid compatibility header"))?,
            );
        }
        let event_stream = headers.iter().any(|header| {
            header.field.equiv("content-type")
                && header
                    .value
                    .as_str()
                    .split(';')
                    .next()
                    .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/event-stream"))
        });
        if self.delivery == Delivery::Stream || !(200..300).contains(&status.0) || !event_stream {
            return crate::stream_http::respond(req, status, &headers, body);
        }

        // Preserve terminal status/usage and complete output items. Codex may
        // send an empty terminal output after delivering output_item.done.
        let (status, payload) = match collect_response(body) {
            Ok(response) => (status, response),
            Err(error) => (
                StatusCode(502),
                json!({"error": {
                    "type": "upstream_error", "code": "invalid_upstream_stream",
                    "message": format!("Cannot assemble Responses JSON: {error}")
                }}),
            ),
        };
        headers.retain(|header| !header.field.equiv("content-type"));
        headers.push(
            Header::from_bytes("content-type", "application/json")
                .map_err(|()| io::Error::other("invalid JSON content type"))?,
        );
        let mut response =
            Response::from_data(serde_json::to_vec(&payload)?).with_status_code(status);
        for header in headers {
            response.add_header(header);
        }
        req.respond(response)
    }
}

fn collect_response(body: impl Read) -> Result<Value> {
    let mut reader = BufReader::new(body);
    let mut line = Vec::new();
    let mut data = Vec::new();
    let mut terminal = None;
    let mut output = BTreeMap::new();
    let mut started = BTreeSet::new();
    let mut output_bytes = 0;
    loop {
        line.clear();
        let read = reader
            .by_ref()
            .take((MAX_EVENT_BYTES + 1) as u64)
            .read_until(b'\n', &mut line);
        let size = match read {
            Ok(size) => size,
            // A complete model terminal remains usable if the trailing RPC fails.
            Err(_) if terminal.is_some() => break,
            Err(error) => return Err(error.into()),
        };
        if size == 0 {
            break;
        }
        ensure!(line.len() <= MAX_EVENT_BYTES, "SSE line exceeds 4 MiB");
        if terminal.is_some() {
            continue;
        }
        // An unterminated final event is not proof of completion.
        ensure!(line.last() == Some(&b'\n'), "truncated SSE line");
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        if let Some(value) = line.strip_prefix(b"data:") {
            ensure!(
                data.len() + value.len() < MAX_EVENT_BYTES,
                "SSE event exceeds 4 MiB"
            );
            data.extend_from_slice(value.strip_prefix(b" ").unwrap_or(value));
            data.push(b'\n');
        } else if line.is_empty() && !data.is_empty() {
            if data == b"[DONE]\n" {
                anyhow::bail!("stream ended before a terminal Response");
            }
            let mut event: Value = serde_json::from_slice(&data).context("invalid SSE JSON")?;
            match event.get("type").and_then(Value::as_str) {
                Some("response.output_item.added" | "response.output_item.done") => {
                    let index = event["output_index"]
                        .as_u64()
                        .context("output item index missing")?;
                    ensure!(index < MAX_OUTPUT_ITEMS, "too many output items");
                    started.insert(index);
                    if event["type"] == "response.output_item.done" {
                        ensure!(!output.contains_key(&index), "duplicate output item");
                        output_bytes += data.len();
                        ensure!(output_bytes <= MAX_EVENT_BYTES, "output exceeds 4 MiB");
                        let item = event.get_mut("item").context("output item missing")?;
                        ensure!(item.is_object(), "output item must be an object");
                        output.insert(index, item.take());
                    }
                }
                Some("response.completed" | "response.failed" | "response.incomplete") => {
                    let response = event
                        .get_mut("response")
                        .context("terminal Response missing")?;
                    ensure!(response.is_object(), "terminal Response must be an object");
                    ensure!(
                        matches!(
                            response["status"].as_str(),
                            Some("completed" | "failed" | "incomplete")
                        ),
                        "terminal Response has no final status"
                    );
                    if response
                        .get("output")
                        .is_none_or(|items| items.as_array().is_some_and(Vec::is_empty))
                        && !started.is_empty()
                    {
                        if response["status"] == "completed" {
                            ensure!(
                                started.iter().copied().eq(0..started.len() as u64)
                                    && output.len() == started.len(),
                                "completed Response has unfinished output items"
                            );
                        }
                        ensure!(
                            data.len() + output_bytes <= MAX_EVENT_BYTES,
                            "assembled Response exceeds 4 MiB"
                        );
                        response["output"] = Value::Array(output.into_values().collect());
                        output = BTreeMap::new();
                    }
                    terminal = Some(response.take());
                }
                _ => {}
            }
            data.clear();
        }
    }
    terminal.context("upstream ended without a terminal Response")
}

#[cfg(test)]
#[path = "sdk_compat_tests.rs"]
mod tests;
