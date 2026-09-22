//! Extracts a stable client conversation key before Codex identity rewriting.

use serde_json::Map;
use serde_json::Value;

pub(crate) fn key_for_http_request(
    body: &[u8],
    headers: &[tiny_http::Header],
) -> anyhow::Result<Option<String>> {
    let mut values = headers
        .iter()
        .filter(|header| header.field.equiv("x-codex-turn-metadata"));
    let metadata = values.next().map(|header| header.value.as_str());
    anyhow::ensure!(values.next().is_none(), "duplicate turn metadata header");
    let header_key = metadata
        .map(|metadata| -> anyhow::Result<Option<String>> {
            anyhow::ensure!(metadata.len() <= 8192, "turn metadata header too large");
            let object: Map<String, Value> = serde_json::from_str(metadata)?;
            anyhow::ensure!(
                !object.contains_key("x-codex-turn-metadata"),
                "recursive turn metadata"
            );
            Ok(metadata_value(&object))
        })
        .transpose()?
        .flatten();
    let mut direct_key = None;
    for name in ["session_id", "thread_id"] {
        let mut values = headers.iter().filter(|header| header.field.equiv(name));
        if let Some(header) = values.next() {
            anyhow::ensure!(values.next().is_none(), "duplicate {name} header");
            let value = header.value.as_str().trim();
            anyhow::ensure!(
                !value.is_empty() && value.len() <= 512,
                "invalid {name} header"
            );
            direct_key.get_or_insert_with(|| format!("{name}:{value}"));
        }
    }
    let body_key = key_for_request(body);
    // SDKs can carry the stable session solely in a request header while
    // retaining other metadata (such as a rotating thread ID) in the body.
    // Use the same session_id namespace as native Codex metadata, and reject
    // contradictory session identities instead of silently choosing one.
    let key = if let Some(session_key) = direct_key
        .as_ref()
        .filter(|key| key.starts_with("session_id:"))
    {
        for candidate in [&body_key, &header_key].into_iter().flatten() {
            anyhow::ensure!(
                !candidate.starts_with("session_id:") || candidate == session_key,
                "conflicting session_id values"
            );
        }
        Some(session_key.clone())
    } else {
        body_key.or(header_key).or(direct_key)
    };
    anyhow::ensure!(
        key.as_ref().is_none_or(|key| key.len() <= 512),
        "conversation key too long"
    );
    Ok(key)
}

#[cfg(test)]
#[path = "affinity_header_tests.rs"]
mod header_tests;

pub(crate) fn key_for_request(body: &[u8]) -> Option<String> {
    let value = serde_json::from_slice::<Value>(body).ok()?;
    let object = value.as_object()?;

    if let Some(metadata) = object.get("client_metadata").and_then(Value::as_object)
        && let Some(key) = metadata_value(metadata)
    {
        return Some(key);
    }

    object
        .get("metadata")
        .and_then(Value::as_object)
        .and_then(metadata_value)
}

fn metadata_value(metadata: &Map<String, Value>) -> Option<String> {
    for key in ["session_id", "thread_id", "conversation_id"] {
        if let Some(value) = metadata.get(key).and_then(Value::as_str) {
            let value = value.trim();
            if !value.is_empty() {
                return Some(format!("{key}:{value}"));
            }
        }
    }

    metadata
        .get("x-codex-turn-metadata")
        .and_then(Value::as_str)
        .and_then(|nested| serde_json::from_str::<Value>(nested).ok())
        .and_then(|nested| nested.as_object().and_then(metadata_value))
}

#[cfg(test)]
mod tests {
    use super::key_for_request;

    #[test]
    fn prefers_client_session_id_for_affinity() {
        let body =
            br#"{"client_metadata":{"session_id":"client-session","thread_id":"client-thread"}}"#;
        assert_eq!(
            key_for_request(body),
            Some("session_id:client-session".to_string())
        );
    }

    #[test]
    fn reads_nested_session_id() {
        let body = br#"{"client_metadata":{"x-codex-turn-metadata":"{\"session_id\":\"nested-session\"}"}}"#;
        assert_eq!(
            key_for_request(body),
            Some("session_id:nested-session".to_string())
        );
    }

    #[test]
    fn falls_back_to_top_level_conversation_metadata() {
        let body = br#"{"metadata":{"conversation_id":"conversation-1"}}"#;
        assert_eq!(
            key_for_request(body),
            Some("conversation_id:conversation-1".to_string())
        );
    }

    #[test]
    fn ignores_missing_or_non_string_keys() {
        assert_eq!(key_for_request(br#"{"model":"gpt-5.6"}"#), None);
        assert_eq!(
            key_for_request(br#"{"client_metadata":{"session_id":42}}"#),
            None
        );
    }
}
