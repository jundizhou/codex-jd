use super::rewrite_raw_responses_identity;
use codex_core::ModelRequestIdentity;
use pretty_assertions::assert_eq;
use serde_json::json;

fn identity() -> ModelRequestIdentity {
    ModelRequestIdentity {
        installation_id: "installation-local".to_string(),
        session_id: "session-local".to_string(),
        thread_id: "thread-local".to_string(),
        window_id: "window-local".to_string(),
        parent_thread_id: Some("parent-thread-local".to_string()),
        turn_id: Some("turn-local".to_string()),
        root_turn_id: Some("root-local".to_string()),
        parent_turn_id: Some("parent-turn-local".to_string()),
    }
}

#[test]
fn rewrites_identity_fields_and_preserves_other_request_fields() {
    let original = json!({
        "model": "gpt-5",
        "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]}],
        "instructions": "keep",
        "stream": true,
        "unknown": {"nested": [1, true, null]},
        "client_metadata": {
            "x-codex-installation-id": "old-installation",
            "session_id": "old-session",
            "thread_id": "old-thread",
            "root_turn_id": "old-root",
            "parent_turn_id": "old-parent",
            "turn_id": "old-turn",
            "x-codex-window-id": "old-window",
            "x-codex-turn-metadata": serde_json::to_string(&json!({
                "installation_id": "old-installation",
                "session_id": "old-session",
                "thread_id": "old-thread",
                "root_turn_id": "old-root",
                "parent_turn_id": "old-parent",
                "turn_id": "old-turn",
                "window_id": "old-window",
                "opaque": "preserve"
            })).expect("serialize nested metadata")
        }
    });
    let rewritten = rewrite_raw_responses_identity(original.clone(), &identity())
        .expect("rewrite raw Responses identity");

    let mut expected = original;
    let metadata = expected["client_metadata"].as_object_mut().unwrap();
    metadata["x-codex-installation-id"] = "installation-local".into();
    metadata["session_id"] = "session-local".into();
    metadata["thread_id"] = "thread-local".into();
    metadata["x-codex-window-id"] = "window-local".into();
    metadata["root_turn_id"] = "root-local".into();
    metadata["parent_turn_id"] = "parent-turn-local".into();
    metadata["turn_id"] = "turn-local".into();
    let mut nested: serde_json::Value =
        serde_json::from_str(metadata["x-codex-turn-metadata"].as_str().unwrap())
            .expect("parse nested metadata");
    nested["installation_id"] = "installation-local".into();
    nested["session_id"] = "session-local".into();
    nested["thread_id"] = "thread-local".into();
    nested["window_id"] = "window-local".into();
    nested["root_turn_id"] = "root-local".into();
    nested["parent_turn_id"] = "parent-turn-local".into();
    nested["turn_id"] = "turn-local".into();
    metadata["x-codex-turn-metadata"] = serde_json::to_string(&nested).unwrap().into();
    assert_eq!(rewritten, expected);
}
