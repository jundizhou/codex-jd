use super::Continuation;
use pretty_assertions::assert_eq;
use serde_json::json;

#[test]
fn full_history_preserves_tool_pairs_but_dangling_dependencies_require_identity() {
    let call = json!({"type": "function_call", "call_id": "one", "arguments": "{}"});
    let output = json!({"type": "function_call_output", "call_id": "one", "output": "original"});
    for (input, expected) in [
        (
            json!([call.clone(), output.clone()]),
            Continuation::SelfContained,
        ),
        (json!([output.clone()]), Continuation::RequiresIdentity),
        (
            json!([{"type":"reasoning","encrypted_content":"account-bound"}]),
            Continuation::RequiresIdentity,
        ),
        (
            json!([{"type":"function_call","encrypted_function_args":"account-bound"}]),
            Continuation::RequiresIdentity,
        ),
        (json!([output, call]), Continuation::RequiresIdentity),
        (
            json!([{"type":"item_reference", "id":"opaque"}]),
            Continuation::RequiresIdentity,
        ),
        (
            json!([{"role":"user", "content":"hello"}]),
            Continuation::SelfContained,
        ),
    ] {
        assert_eq!(
            Continuation::from_request(&json!({"input": input}), /*routing*/ None),
            expected
        );
    }
}

#[test]
fn routing_and_server_side_history_require_a_known_identity() {
    for body in [
        json!({"previous_response_id":"old"}),
        json!({"conversation":{"id":"old"}}),
    ] {
        assert_eq!(
            Continuation::from_request(&body, /*routing*/ None),
            Continuation::RequiresIdentity
        );
    }
    assert_eq!(
        Continuation::from_request(&json!({"input":"hi"}), Some("opaque")),
        Continuation::RequiresIdentity
    );
    assert_eq!(
        Continuation::from_request(
            &json!({"input":"hi", "previous_response_id":null}),
            /*routing*/ None
        ),
        Continuation::SelfContained
    );
}

#[test]
fn durable_index_locks_and_preserves_invalidation_across_restart() {
    use super::*;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("sessions.json");
    let auth = root.path().join("auth.json");
    std::fs::write(&auth, r#"{"tokens":{"account_id":"test"}}"#).unwrap();
    let mut index = Conversations::open(
        path.clone(),
        auth.clone(),
        /*capacity*/ 2,
        Duration::from_secs(1),
        &[],
    )
    .unwrap();
    assert!(
        Conversations::open(
            path.clone(),
            auth.clone(),
            /*capacity*/ 2,
            Duration::ZERO,
            &[]
        )
        .is_err()
    );
    let identity = SessionIdentity {
        installation_id: "installation".into(),
        session_id: "session".into(),
        thread_id: "thread".into(),
        window_id: "thread:0".into(),
        parent_thread_id: None,
        turn_id: None,
        root_turn_id: None,
        parent_turn_id: None,
    };
    index.records.insert(
        "logical".into(),
        Record {
            account: index.account.clone().unwrap(),
            identity: identity.clone(),
            used_at: 10,
            invalid: false,
            recoverable: false,
        },
    );
    index.save().unwrap();
    drop(index);
    let index = Conversations::open(
        path.clone(),
        auth.clone(),
        /*capacity*/ 2,
        Duration::ZERO,
        &["logical".into()],
    )
    .unwrap();
    assert_eq!(&index.records["logical"].identity, &identity);
    assert!(index.records["logical"].invalid);
    drop(index);
    let index = Conversations::open(
        path.clone(),
        auth.clone(),
        /*capacity*/ 2,
        Duration::ZERO,
        &[],
    )
    .unwrap();
    assert!(index.records["logical"].invalid);
    drop(index);
    std::fs::write(&path, b"{broken").unwrap();
    assert!(Conversations::open(path, auth, /*capacity*/ 2, Duration::ZERO, &[]).is_err());
}

#[test]
fn failed_index_write_does_not_replace_the_last_durable_mapping() {
    use super::*;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("sessions.json");
    let auth = root.path().join("auth.json");
    std::fs::write(&auth, r#"{"OPENAI_API_KEY":"fixture"}"#).unwrap();
    let index =
        Conversations::open(path.clone(), auth, /*capacity*/ 2, Duration::ZERO, &[]).unwrap();
    let before = std::fs::read(&path).unwrap();
    std::fs::create_dir(path.with_extension("tmp")).unwrap();
    assert!(index.save().is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
}

#[tokio::test]
async fn empty_deployment_accepts_first_account_without_restarting() {
    use super::*;
    use tokio_tungstenite::tungstenite::protocol::Role;

    let root = tempfile::tempdir().unwrap();
    let auth = root.path().join("auth.json");
    let mut index = Conversations::open(
        root.path().join("sessions.json"),
        auth.clone(),
        /*capacity*/ 2,
        Duration::ZERO,
        &[],
    )
    .unwrap();
    let (client, _server) = tokio::io::duplex(1024);
    let mut stream = WebSocketStream::from_raw_socket(client, Role::Client, /*config*/ None).await;
    assert!(
        index
            .acquire(
                &mut stream,
                "missing".into(),
                Continuation::RequiresIdentity
            )
            .await
            .is_err()
    );
    assert_eq!(index.account, None);

    std::fs::write(&auth, r#"{"OPENAI_API_KEY":"fixture"}"#).unwrap();
    let error = index
        .acquire(
            &mut stream,
            "missing".into(),
            Continuation::RequiresIdentity,
        )
        .await
        .unwrap_err();
    assert!(error.downcast_ref::<Rejection>().is_some());
    assert_eq!(index.account, Some(account(&auth).unwrap()));
}
