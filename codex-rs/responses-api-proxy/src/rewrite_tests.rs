use std::collections::HashMap;

use super::rewrite_request;
use crate::affinity::key_for_http_request;
use crate::identity::SessionIdentity;
use crate::metadata_profiles::Profiles;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;

fn identity(thread: &str) -> SessionIdentity {
    SessionIdentity {
        installation_id: "server-installation".into(),
        session_id: thread.into(),
        thread_id: thread.into(),
        window_id: "server-window".into(),
        parent_thread_id: None,
        turn_id: None,
        root_turn_id: None,
        parent_turn_id: None,
    }
}

fn profiles() -> (tempfile::TempDir, Profiles) {
    let directory = tempfile::tempdir().unwrap();
    let document = json!({
        "version": 1, "threads": {},
        "profiles": (0..5).map(|index| json!({
            "path": format!("/workspace/project-{index}"),
            "remote_url": "https://github.com/example/project.git",
            "commit": "1234567890123456789012345678901234567890",
            "has_changes": false
        })).collect::<Vec<_>>()
    });
    let path = directory.path().join("profiles.json");
    std::fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
    let profiles = Profiles::open(&path).unwrap();
    (directory, profiles)
}

fn headers(metadata: &Value) -> HashMap<String, String> {
    HashMap::from([("x-codex-turn-metadata".into(), metadata.to_string())])
}

#[test]
fn replaces_workspace_and_identity_in_both_carriers_and_survives_restart() {
    let (directory, profiles) = profiles();
    let metadata = json!({
        "installation_id": "client-installation", "session_id": "client-thread", "thread_id": "client-thread",
        "window_id": "client-window", "context_window_id": "client-context", "turn_id": "client-turn",
        "root_turn_id": "client-turn", "parent_turn_id": null, "agent_name": "/root",
        "workspaces": {"/Users/client/project": {
            "associated_remote_urls": {"origin": "https://private.example/original", "upstream": null},
            "latest_git_commit_hash": "old-commit", "has_changes": true, "unknown": [1, null]
        }},
        "sandbox": "workspace-write"
    });
    let body = json!({"model": "mock", "input": [{"role": "user", "content": "/Users/client/project"}],
        "tools": [{"type": "function", "name": "read", "parameters": {}}],
        "client_metadata": {"session_id": "client-thread", "x-codex-turn-metadata": metadata.to_string()}});
    let bytes = serde_json::to_vec(&body).unwrap();
    let rewritten = rewrite_request(
        &bytes,
        headers(&metadata),
        &identity("server-thread"),
        Some(&profiles),
    )
    .unwrap();
    let actual: Value = serde_json::from_str(&rewritten.headers["x-codex-turn-metadata"]).unwrap();
    let workspace = actual["workspaces"].as_object().unwrap();
    assert_eq!(workspace.len(), 1);
    let (path, _) = workspace.iter().next().unwrap();
    assert!(path.starts_with("/workspace/project-") && path.contains("/worktrees/"));
    for field in ["context_window_id", "turn_id"] {
        assert_ne!(actual[field], metadata[field]);
        assert_eq!(actual[field].as_str().unwrap().len(), 36);
    }
    let mut expected = metadata.clone();
    expected["installation_id"] = "server-installation".into();
    expected["session_id"] = "server-thread".into();
    expected["thread_id"] = "server-thread".into();
    expected["window_id"] = "server-window".into();
    expected["context_window_id"] = actual["context_window_id"].clone();
    expected["turn_id"] = actual["turn_id"].clone();
    expected["root_turn_id"] = actual["turn_id"].clone();
    expected["workspaces"] = json!({path: {
        "associated_remote_urls": {"origin": "https://github.com/example/project.git", "upstream": null},
        "latest_git_commit_hash": "1234567890123456789012345678901234567890", "has_changes": false,
        "unknown": [1, null]
    }});
    assert_eq!(actual, expected);
    let mut expected_body = body;
    expected_body["store"] = false.into();
    expected_body["client_metadata"]["session_id"] = "server-thread".into();
    expected_body["client_metadata"]["x-codex-turn-metadata"] = actual.to_string().into();
    assert_eq!(rewritten.body, expected_body);
    drop(profiles);
    let reopened = Profiles::open(&directory.path().join("profiles.json")).unwrap();
    let after = rewrite_request(
        &bytes,
        headers(&metadata),
        &identity("server-thread"),
        Some(&reopened),
    )
    .unwrap();
    assert_eq!(
        (after.body, after.headers),
        (rewritten.body, rewritten.headers)
    );
}

#[test]
fn preserves_shape_multiple_workspaces_nulls_and_header_only_body() {
    let (_directory, profiles) = profiles();
    let body = br#"{"model":"mock","input":[],"store":false,"unknown":[null,true]}"#;
    let metadata = json!({"session_id": "client", "turn_id": null, "workspaces": {
        "C:\\repo": {"has_changes": null, "other": 7}, "/other": {}
    }});
    let rewritten = rewrite_request(
        body,
        headers(&metadata),
        &identity("server"),
        Some(&profiles),
    )
    .unwrap();
    assert_eq!(
        rewritten.body,
        serde_json::from_slice::<Value>(body).unwrap()
    );
    let actual: Value = serde_json::from_str(&rewritten.headers["x-codex-turn-metadata"]).unwrap();
    let workspaces = actual["workspaces"].as_object().unwrap();
    assert_eq!(workspaces.len(), 2);
    assert!(
        workspaces
            .keys()
            .all(|path| path.starts_with("/workspace/project-"))
    );
    let mut values = workspaces.values().cloned().collect::<Vec<_>>();
    values.sort_by_key(Value::to_string);
    let mut expected = vec![json!({"has_changes": null, "other": 7}), json!({})];
    expected.sort_by_key(Value::to_string);
    assert_eq!(values, expected);
    assert_eq!(actual["turn_id"], Value::Null);
    let empty =
        rewrite_request(body, HashMap::new(), &identity("server"), Some(&profiles)).unwrap();
    assert_eq!(
        (empty.body, empty.headers),
        (
            serde_json::from_slice::<Value>(body).unwrap(),
            HashMap::new()
        )
    );
}

#[test]
fn preserves_parent_thread_and_turn_links_across_restart_and_new_turns() {
    let (directory, profiles) = profiles();
    let parent = json!({"thread_id": "parent", "turn_id": "root-turn"});
    let parent_request = rewrite_request(
        b"{}",
        headers(&parent),
        &identity("mapped-parent"),
        Some(&profiles),
    )
    .unwrap();
    let parent_result: Value =
        serde_json::from_str(&parent_request.headers["x-codex-turn-metadata"]).unwrap();
    drop(profiles);
    let profiles = Profiles::open(&directory.path().join("profiles.json")).unwrap();
    let child = json!({"thread_id": "child", "parent_thread_id": "parent", "turn_id": "child-turn", "root_turn_id": "root-turn", "parent_turn_id": "root-turn"});
    let mut supplied = headers(&child);
    supplied.insert("x-codex-parent-thread-id".into(), "parent".into());
    let request =
        rewrite_request(b"{}", supplied, &identity("mapped-child"), Some(&profiles)).unwrap();
    let actual: Value = serde_json::from_str(&request.headers["x-codex-turn-metadata"]).unwrap();
    assert_ne!(actual["turn_id"], parent_result["turn_id"]);
    assert_ne!(actual["turn_id"], child["turn_id"]);
    assert_eq!(
        actual,
        json!({"thread_id":"mapped-child", "parent_thread_id":"mapped-parent", "turn_id": actual["turn_id"], "root_turn_id": parent_result["turn_id"], "parent_turn_id": parent_result["turn_id"]})
    );
    assert_eq!(request.headers["x-codex-parent-thread-id"], "mapped-parent");
}

#[test]
fn rejects_unbound_parent_conflicts_and_headers_that_expand_past_limit() {
    let (_directory, profiles) = profiles();
    for metadata in [
        json!({"parent_thread_id":"unseen"}),
        json!({"turn_id":42}),
        json!({"workspaces":[]}),
    ] {
        assert!(
            rewrite_request(
                b"{}",
                headers(&metadata),
                &identity("server"),
                Some(&profiles)
            )
            .is_err()
        );
    }
    let body = br#"{"client_metadata":{"thread_id":"one"}}"#;
    assert!(
        rewrite_request(
            body,
            headers(&json!({"thread_id":"two"})),
            &identity("server"),
            Some(&profiles)
        )
        .is_err()
    );
    let metadata = json!({"padding": "x".repeat(8110), "workspaces": {"/a":{"associated_remote_urls":{"origin":"x"}}}});
    assert!(metadata.to_string().len() <= 8192);
    assert!(
        rewrite_request(
            b"{}",
            headers(&metadata),
            &identity("server"),
            Some(&profiles)
        )
        .is_err()
    );
}

#[test]
fn header_affinity_matches_body_affinity_without_modifying_input() {
    let metadata = json!({"session_id":"client-session", "thread_id":"client-thread"});
    let header =
        tiny_http::Header::from_bytes("x-codex-turn-metadata", metadata.to_string()).unwrap();
    let from_header = key_for_http_request(b"{}", std::slice::from_ref(&header)).unwrap();
    let body = json!({"client_metadata":metadata}).to_string();
    assert_eq!(
        from_header,
        key_for_http_request(body.as_bytes(), &[]).unwrap()
    );
    assert_eq!(from_header, Some("session_id:client-session".into()));
    let direct = tiny_http::Header::from_bytes("session_id", "client-session").unwrap();
    assert_eq!(key_for_http_request(b"{}", &[direct]).unwrap(), from_header);
    assert!(key_for_http_request(b"{}", &[header.clone(), header]).is_err());
}

#[test]
fn defaults_omitted_storage_without_changing_explicit_values_or_sdk_parameters() {
    let base = json!({
        "model": "mock", "input": [{"role":"user", "content":"hello"}],
        "stream": true, "max_output_tokens": 1024,
        "reasoning": {"effort":"low"}, "text": {"verbosity":"low"}
    });
    for supplied_store in [
        None,
        Some(json!(false)),
        Some(json!(true)),
        Some(Value::Null),
    ] {
        let mut supplied = base.clone();
        if let Some(value) = &supplied_store {
            supplied["store"] = value.clone();
        }
        let rewritten = rewrite_request(
            &serde_json::to_vec(&supplied).unwrap(),
            HashMap::new(),
            &identity("server"),
            /*profiles*/ None,
        )
        .unwrap();
        let mut expected = base.clone();
        expected["store"] = supplied_store.unwrap_or(json!(false));
        assert_eq!(rewritten.body, expected);
    }
}

#[test]
fn uses_all_five_profiles_and_retries_failed_persistence_without_losing_aliases() {
    let (directory, profiles) = profiles();
    let mut chosen = std::collections::HashSet::new();
    for index in 0..100 {
        let binding = profiles
            .bind(&identity(&format!("thread-{index}")), &[], &[])
            .unwrap();
        chosen.insert(binding.workspace.path);
    }
    assert_eq!(chosen.len(), 5);
    let source = vec!["client-parent".to_owned()];
    let temporary = directory.path().join("profiles.tmp");
    std::fs::create_dir(&temporary).unwrap();
    assert!(
        profiles
            .bind(&identity("server-parent"), &source, &[])
            .is_err()
    );
    std::fs::remove_dir(temporary).unwrap();
    profiles
        .bind(&identity("server-parent"), &source, &[])
        .unwrap();
    drop(profiles);
    let profiles = Profiles::open(&directory.path().join("profiles.json")).unwrap();
    let binding = profiles
        .bind(&identity("server-child"), &[], &source)
        .unwrap();
    assert_eq!(
        binding.parents,
        HashMap::from([("client-parent".into(), "server-parent".into())])
    );
}
