//! Rewrite Codex model-request identity fields before upstream forwarding.

use anyhow::Result;
use serde_json::Map;
use serde_json::Value;

use super::identity::SessionIdentity;

const WORKSPACE_SESSION_COPIES: usize = 5;

pub(crate) struct RewrittenRequest {
    pub(crate) body: Vec<u8>,
}

fn insert_str(map: &mut Map<String, Value>, key: &str, value: &str) {
    map.insert(key.to_string(), Value::String(value.to_string()));
}

fn insert_optional(map: &mut Map<String, Value>, key: &str, value: Option<&str>) {
    match value {
        Some(value) => insert_str(map, key, value),
        None => {
            map.remove(key);
        }
    }
}

fn identity_fields(identity: &SessionIdentity) -> Vec<(&'static str, Option<&str>)> {
    vec![
        ("installation_id", Some(identity.installation_id.as_str())),
        ("session_id", Some(identity.session_id.as_str())),
        ("thread_id", Some(identity.thread_id.as_str())),
        ("root_turn_id", identity.root_turn_id.as_deref()),
        ("parent_turn_id", identity.parent_turn_id.as_deref()),
    ]
}

fn update_turn_metadata(
    client_metadata: &mut Map<String, Value>,
    identity: &SessionIdentity,
) -> Result<()> {
    let mut metadata = match client_metadata.get("x-codex-turn-metadata") {
        None => Map::new(),
        Some(Value::String(json)) => serde_json::from_str::<Value>(json)
            .map_err(|error| anyhow::anyhow!("invalid x-codex-turn-metadata JSON: {error}"))?
            .as_object()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("x-codex-turn-metadata must be a JSON object"))?,
        Some(_) => anyhow::bail!("x-codex-turn-metadata must be a JSON string"),
    };
    for (key, value) in identity_fields(identity) {
        insert_optional(&mut metadata, key, value);
    }
    replicate_workspaces(&mut metadata);
    let metadata = Value::Object(metadata);
    let json = serde_json::to_string(&metadata)?;
    insert_str(client_metadata, "x-codex-turn-metadata", &json);
    Ok(())
}

/// Expands each real workspace record into one stable record per proxy session.
/// The Git metadata is copied unchanged; only the map key is made session-specific.
fn replicate_workspaces(metadata: &mut Map<String, Value>) {
    let Some(Value::Object(workspaces)) = metadata.get("workspaces") else {
        return;
    };
    let mut replicas = Map::new();
    for (path, value) in workspaces {
        for session in 1..=WORKSPACE_SESSION_COPIES {
            replicas.insert(format!("{path}#session-{session}"), value.clone());
        }
    }
    metadata.insert("workspaces".to_string(), Value::Object(replicas));
}

/// Rewrites request body identity metadata and returns the compatible header payload.
pub(crate) fn rewrite_body(body: &[u8], identity: &SessionIdentity) -> Result<RewrittenRequest> {
    let mut body_value: Value = serde_json::from_slice(body)?;
    let body_object = body_value
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("request body must be a JSON object"))?;
    if !body_object.contains_key("client_metadata") {
        body_object.insert("client_metadata".to_string(), Value::Object(Map::new()));
    }
    let client_metadata = body_object
        .get_mut("client_metadata")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| anyhow::anyhow!("request client_metadata must be a JSON object"))?;

    insert_str(
        client_metadata,
        "x-codex-installation-id",
        &identity.installation_id,
    );
    insert_str(client_metadata, "session_id", &identity.session_id);
    insert_str(client_metadata, "thread_id", &identity.thread_id);
    insert_optional(
        client_metadata,
        "root_turn_id",
        identity.root_turn_id.as_deref(),
    );
    insert_optional(
        client_metadata,
        "parent_turn_id",
        identity.parent_turn_id.as_deref(),
    );
    update_turn_metadata(client_metadata, identity)?;

    Ok(RewrittenRequest {
        body: serde_json::to_vec(&body_value)?,
    })
}

#[cfg(test)]
mod tests {
    use super::rewrite_body;
    use crate::identity::SessionIdentity;
    use serde_json::Value;

    fn identity() -> SessionIdentity {
        SessionIdentity {
            installation_id: "install-a".to_string(),
            session_id: "session-a".to_string(),
            thread_id: "thread-a".to_string(),
            window_id: "thread-a:0".to_string(),
            parent_thread_id: None,
            turn_id: Some("turn-a".to_string()),
            root_turn_id: Some("root-a".to_string()),
            parent_turn_id: None,
        }
    }

    #[test]
    fn rewrites_flat_and_nested_identity_fields() {
        let original = br#"{"client_metadata":{"x-codex-installation-id":"old","session_id":"old-session","thread_id":"old-thread","x-codex-turn-metadata":"{\"installation_id\":\"old\",\"session_id\":\"old-session\",\"thread_id\":\"old-thread\",\"turn_id\":\"old-turn\"}"}}"#;
        let rewritten = rewrite_body(original, &identity()).expect("rewrite body");
        let body: Value = serde_json::from_slice(&rewritten.body).expect("parse body");
        let mut expected = serde_json::from_slice::<Value>(original).expect("parse original");
        let metadata = expected["client_metadata"].as_object_mut().unwrap();
        metadata["x-codex-installation-id"] = "install-a".into();
        metadata["session_id"] = "session-a".into();
        metadata["thread_id"] = "thread-a".into();
        metadata.insert("root_turn_id".to_string(), "root-a".into());
        let nested: Value =
            serde_json::from_str(metadata["x-codex-turn-metadata"].as_str().unwrap())
                .expect("nested metadata");
        let mut nested = nested;
        nested["installation_id"] = "install-a".into();
        nested["session_id"] = "session-a".into();
        nested["thread_id"] = "thread-a".into();
        nested["root_turn_id"] = "root-a".into();
        metadata["x-codex-turn-metadata"] = serde_json::to_string(&nested).unwrap().into();
        assert_eq!(body, expected);
    }

    #[test]
    fn creates_client_metadata_when_missing() {
        let rewritten =
            rewrite_body(br#"{"model":"gpt-test"}"#, &identity()).expect("rewrite body");
        let body: Value = serde_json::from_slice(&rewritten.body).expect("parse body");
        assert_eq!(body["client_metadata"]["session_id"], "session-a");
    }

    #[test]
    fn preserves_non_identity_request_fields() {
        let original = serde_json::json!({
            "model": "gpt-5",
            "input": [
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]}
            ],
            "instructions": "keep this",
            "tools": [{"type": "function", "name": "tool", "parameters": {"type": "object"}}],
            "stream": true,
            "previous_response_id": "resp-123",
            "metadata": {"opaque": "value"},
            "unknown": {"nested": [1, true, null]}
        });
        let rewritten = rewrite_body(
            &serde_json::to_vec(&original).expect("serialize request"),
            &identity(),
        )
        .expect("rewrite body");
        let mut actual: Value = serde_json::from_slice(&rewritten.body).expect("parse body");
        let object = actual.as_object_mut().expect("object request");
        object.remove("client_metadata");
        assert_eq!(actual, original);
    }

    #[test]
    fn rejects_malformed_nested_metadata_instead_of_dropping_it() {
        let result = rewrite_body(
            br#"{"client_metadata":{"x-codex-turn-metadata":"not-json"}}"#,
            &identity(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn replicates_real_workspaces_into_five_session_records() {
        let original = serde_json::json!({
            "client_metadata": {
                "x-codex-turn-metadata": serde_json::to_string(&serde_json::json!({
                    "workspaces": {
                        "/Users/keting/workspace/hermes-sre": {
                            "associated_remote_urls": {"origin": "git@github.com:example/hermes-sre.git"},
                            "latest_git_commit_hash": "e70a0e59",
                            "has_changes": true
                        }
                    }
                })).unwrap()
            }
        });
        let rewritten = rewrite_body(&serde_json::to_vec(&original).unwrap(), &identity()).unwrap();
        let body: Value = serde_json::from_slice(&rewritten.body).unwrap();
        let metadata: Value = serde_json::from_str(
            body["client_metadata"]["x-codex-turn-metadata"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(metadata["workspaces"].as_object().unwrap().len(), 5);
        for session in 1..=super::WORKSPACE_SESSION_COPIES {
            assert_eq!(
                metadata["workspaces"]
                    [format!("/Users/keting/workspace/hermes-sre#session-{session}")]["latest_git_commit_hash"],
                "e70a0e59"
            );
        }
    }
}
