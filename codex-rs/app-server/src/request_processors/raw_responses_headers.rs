//! Narrow header boundary for the raw Responses adapter.

use std::collections::HashMap;

use axum::http::HeaderMap;
use axum::http::HeaderName;
use axum::http::HeaderValue;
use codex_app_server_protocol::JSONRPCErrorError;
use uuid::Uuid;

use super::invalid_request;

pub(super) fn request_headers(
    supplied: HashMap<String, String>,
) -> Result<HeaderMap, JSONRPCErrorError> {
    if supplied.len() > 4 {
        return Err(invalid_request("too many raw Responses headers"));
    }
    let mut headers = HeaderMap::new();
    for (name, value) in supplied {
        let name = name.to_ascii_lowercase();
        if !matches!(
            name.as_str(),
            "x-codex-turn-state" | "x-codex-inference-call-id" | "traceparent" | "tracestate"
        ) || value.len() > 8192
            || headers.contains_key(&name)
        {
            return Err(invalid_request(
                "unsupported, oversized or duplicate raw Responses header",
            ));
        }
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| invalid_request("invalid raw Responses header name"))?;
        let value = HeaderValue::from_str(&value)
            .map_err(|_| invalid_request("invalid raw Responses header value"))?;
        headers.insert(name, value);
    }
    if !headers.contains_key("x-codex-inference-call-id") {
        let call_id = HeaderValue::from_str(&Uuid::new_v4().to_string())
            .map_err(|_| super::internal_error("failed to encode inference call ID"))?;
        headers.insert("x-codex-inference-call-id", call_id);
    }
    Ok(headers)
}

pub(super) fn response_headers(headers: &HeaderMap) -> HashMap<String, String> {
    ["x-codex-turn-state", "retry-after", "content-type"]
        .into_iter()
        .filter_map(|name| {
            let values: Vec<_> = headers.get_all(name).iter().collect();
            if values.len() != 1 {
                return None;
            }
            let value = values[0].to_str().ok()?;
            let limit = if name == "x-codex-turn-state" {
                8192
            } else {
                1024
            };
            (value.len() <= limit).then(|| (name.to_string(), value.to_string()))
        })
        .collect()
}

#[cfg(test)]
#[path = "raw_responses_headers_tests.rs"]
mod tests;
