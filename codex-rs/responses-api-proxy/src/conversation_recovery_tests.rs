use super::*;
use crate::app_server_reader::recv_message;
use crate::app_server_reader::send_message;
use pretty_assertions::assert_eq;
use tokio_tungstenite::tungstenite::protocol::Role;

#[tokio::test]
async fn terminated_rpc_restores_original_identity_after_restart_and_continues_tools() {
    for restart in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let auth = root.path().join("auth.json");
        std::fs::write(&auth, r#"{"tokens":{"account_id":"account"}}"#).unwrap();
        let path = root.path().join("conversations.json");
        let mut index = Conversations::open(
            path.clone(),
            auth.clone(),
            /*capacity*/ 2,
            Duration::ZERO,
            &[],
        )
        .unwrap();
        let identity = SessionIdentity {
            installation_id: "installation".into(),
            session_id: "session".into(),
            thread_id: "original".into(),
            window_id: "original:0".into(),
            parent_thread_id: None,
            turn_id: None,
            root_turn_id: None,
            parent_turn_id: None,
        };
        index.records.insert(
            "logical".into(),
            Record {
                account: account(&auth).unwrap(),
                identity: identity.clone(),
                used_at: crate::queue_store::now(),
                invalid: false,
                recoverable: false,
            },
        );
        index.loaded.insert(identity.thread_id.clone(), None);
        index
            .release(&identity.thread_id, Outcome::Recoverable)
            .unwrap();
        if restart {
            drop(index);
            index = Conversations::open(
                path.clone(),
                auth.clone(),
                /*capacity*/ 2,
                Duration::ZERO,
                &["logical".into()],
            )
            .unwrap();
        }
        let (client, server) = tokio::io::duplex(8192);
        let mut client =
            WebSocketStream::from_raw_socket(client, Role::Client, /*config*/ None).await;
        let mut server =
            WebSocketStream::from_raw_socket(server, Role::Server, /*config*/ None).await;
        let peer = async {
            let mut methods = Vec::new();
            // Recovery may resume after restart, then verifies identity. Repeated recovery
            // and a tool continuation must both reuse that same identity.
            for _ in 0..(3 + usize::from(restart)) {
                let request = recv_message(&mut server).await.unwrap();
                let method = request["method"].as_str().unwrap().to_owned();
                let result = match method.as_str() {
                    "thread/resume" => {
                        assert_eq!(request["params"]["threadId"], "original");
                        json!({})
                    }
                    "thread/modelIdentity/list" => json!({"data":[{
                        "installationId":"installation", "sessionId":"session",
                        "threadId":"original", "windowId":"original:0"
                    }], "nextCursor":null}),
                    _ => panic!("unexpected mutation: {method}"),
                };
                methods.push(method);
                send_message(&mut server, &json!({"id":request["id"],"result":result}))
                    .await
                    .unwrap();
            }
            methods
        };
        let work = async {
            index.restore_binding(&mut client, "logical").await.unwrap();
            index.restore_binding(&mut client, "logical").await.unwrap();
            let body = json!({"input":[{"type":"function_call_output","call_id":"call-1","output":"done"}]});
            let continuation = Continuation::from_request(&body, Some("original-route"));
            let restored = index
                .acquire(&mut client, "logical".into(), continuation)
                .await
                .unwrap();
            assert_eq!(restored, identity);
            assert!(!index.records["logical"].recoverable);
            assert!(!index.records["logical"].invalid);
            index
                .release(&restored.thread_id, Outcome::Released)
                .unwrap();
        };
        let ((), methods) = tokio::join!(work, peer);
        assert_eq!(
            methods.iter().filter(|m| *m == "thread/resume").count(),
            usize::from(restart)
        );
    }
}

#[tokio::test]
async fn unknown_execution_or_changed_account_cannot_restore_binding() {
    for outcome in [Outcome::Unknown, Outcome::Recoverable] {
        let root = tempfile::tempdir().unwrap();
        let auth = root.path().join("auth.json");
        std::fs::write(&auth, r#"{"tokens":{"account_id":"account"}}"#).unwrap();
        let mut index = Conversations::open(
            root.path().join("state.json"),
            auth.clone(),
            /*capacity*/ 2,
            Duration::ZERO,
            &[],
        )
        .unwrap();
        let identity = SessionIdentity {
            installation_id: "i".into(),
            session_id: "s".into(),
            thread_id: "t".into(),
            window_id: "w".into(),
            parent_thread_id: None,
            turn_id: None,
            root_turn_id: None,
            parent_turn_id: None,
        };
        index.records.insert(
            "logical".into(),
            Record {
                account: account(&auth).unwrap(),
                identity,
                used_at: crate::queue_store::now(),
                invalid: false,
                recoverable: false,
            },
        );
        index.release("t", outcome).unwrap();
        if matches!(outcome, Outcome::Recoverable) {
            std::fs::write(&auth, r#"{"tokens":{"account_id":"different"}}"#).unwrap();
        }
        let (client, _server) = tokio::io::duplex(1024);
        let mut client =
            WebSocketStream::from_raw_socket(client, Role::Client, /*config*/ None).await;
        assert!(index.restore_binding(&mut client, "logical").await.is_err());
        assert!(index.records["logical"].invalid);
    }
}
