//! Extracts a stable client conversation key before Codex identity rewriting.

use serde_json::Map;
use serde_json::Value;

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
