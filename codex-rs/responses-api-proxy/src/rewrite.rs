//! Rewrite Codex model-request identity fields without changing request shape.

use anyhow::Result;
use serde_json::Map;
use serde_json::Value;

use super::identity::SessionIdentity;

pub(crate) struct RewrittenRequest {
    pub(crate) body: Vec<u8>,
}

fn identity_value<'a>(identity: &'a SessionIdentity, key: &str) -> Option<&'a str> {
    match key {
        "installation_id" | "x-codex-installation-id" => Some(&identity.installation_id),
        "session_id" => Some(&identity.session_id),
        "thread_id" => Some(&identity.thread_id),
        "turn_id" => identity.turn_id.as_deref(),
        "window_id" | "x-codex-window-id" | "context_window_id" => Some(&identity.window_id),
        "root_turn_id" => identity.root_turn_id.as_deref(),
        "parent_turn_id" => identity.parent_turn_id.as_deref(),
        _ => None,
    }
}

fn replace_existing(map: &mut Map<String, Value>, identity: &SessionIdentity) {
    let keys = map.keys().cloned().collect::<Vec<_>>();
    for key in keys {
        if let Some(value) = identity_value(identity, &key) {
            map.insert(key, Value::String(value.to_string()));
        }
    }
}

fn rewrite_nested_metadata(value: &mut Value, identity: &SessionIdentity) -> Result<()> {
    let Some(json) = value.as_str() else {
        anyhow::bail!("x-codex-turn-metadata must be a JSON string")
    };
    let mut nested = serde_json::from_str::<Value>(json)
        .map_err(|error| anyhow::anyhow!("invalid x-codex-turn-metadata JSON: {error}"))?;
    let Some(object) = nested.as_object_mut() else {
        anyhow::bail!("x-codex-turn-metadata must be a JSON object")
    };
    replace_existing(object, identity);
    *value = Value::String(serde_json::to_string(&nested)?);
    Ok(())
}

fn rewrite_metadata_map(
    metadata: &mut Map<String, Value>,
    identity: &SessionIdentity,
) -> Result<()> {
    replace_existing(metadata, identity);
    if let Some(nested) = metadata.get_mut("x-codex-turn-metadata") {
        rewrite_nested_metadata(nested, identity)?;
    }
    Ok(())
}

/// Rewrites only identity fields already present in the JSON body.
pub(crate) fn rewrite_body(body: &[u8], identity: &SessionIdentity) -> Result<RewrittenRequest> {
    let mut body_value: Value = serde_json::from_slice(body)?;
    if let Some(client_metadata) = body_value
        .as_object_mut()
        .and_then(|object| object.get_mut("client_metadata"))
    {
        let metadata = client_metadata
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("request client_metadata must be a JSON object"))?;
        rewrite_metadata_map(metadata, identity)?;
    }
    Ok(RewrittenRequest {
        body: serde_json::to_vec(&body_value)?,
    })
}

/// Rewrites only identity fields already present in a request header.
pub(crate) fn rewrite_header(
    name: &str,
    value: &str,
    identity: &SessionIdentity,
) -> Result<String> {
    if name.eq_ignore_ascii_case("x-codex-turn-metadata") {
        let mut nested = serde_json::from_str::<Value>(value)
            .map_err(|error| anyhow::anyhow!("invalid x-codex-turn-metadata JSON: {error}"))?;
        let object = nested
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("x-codex-turn-metadata must be a JSON object"))?;
        replace_existing(object, identity);
        return Ok(serde_json::to_string(&nested)?);
    }
    Ok(identity_value(identity, name).map_or_else(|| value.to_string(), str::to_string))
}

#[cfg(test)]
mod tests {
    use super::rewrite_body;
    use super::rewrite_header;
    use crate::identity::SessionIdentity;
    use serde_json::Value;

    fn identity() -> SessionIdentity {
        SessionIdentity {
            installation_id: "install-a".to_string(),
            session_id: "session-a".to_string(),
            thread_id: "thread-a".to_string(),
            window_id: "window-a".to_string(),
            parent_thread_id: None,
            turn_id: Some("turn-a".to_string()),
            root_turn_id: Some("root-a".to_string()),
            parent_turn_id: Some("parent-a".to_string()),
        }
    }

    #[test]
    fn rewrites_only_existing_body_fields() {
        let original = serde_json::json!({
            "model": "gpt-5",
            "input": [{"type": "message", "role": "user"}],
            "client_metadata": {
                "session_id": "old-session",
                "x-codex-turn-metadata": serde_json::to_string(&serde_json::json!({
                    "thread_id": "old-thread",
                    "workspaces": {"/repo": {"has_changes": true}}
                })).unwrap()
            }
        });
        let rewritten = rewrite_body(&serde_json::to_vec(&original).unwrap(), &identity()).unwrap();
        let actual: Value = serde_json::from_slice(&rewritten.body).unwrap();
        assert_eq!(actual["client_metadata"]["session_id"], "session-a");
        let nested: Value = serde_json::from_str(
            actual["client_metadata"]["x-codex-turn-metadata"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(nested["thread_id"], "thread-a");
        assert!(nested["installation_id"].is_null());
        assert!(nested["workspaces"]["/repo"].is_object());
    }

    #[test]
    fn does_not_create_client_metadata_or_identity_fields() {
        let original = br#"{"model":"gpt-5","input":[]}"#;
        let rewritten = rewrite_body(original, &identity()).unwrap();
        let actual: Value = serde_json::from_slice(&rewritten.body).unwrap();
        let expected: Value = serde_json::from_slice(original).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn rewrites_header_in_place() {
        let value = r#"{"session_id":"old","workspaces":{"/repo":{}}}"#;
        let rewritten = rewrite_header("X-Codex-Turn-Metadata", value, &identity()).unwrap();
        let actual: Value = serde_json::from_str(&rewritten).unwrap();
        assert_eq!(actual["session_id"], "session-a");
        assert!(actual["installation_id"].is_null());
        assert!(actual["workspaces"]["/repo"].is_object());
    }
}
