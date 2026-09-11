use super::IdentityItem;
use pretty_assertions::assert_eq;
use serde_json::json;

#[tokio::test]
async fn follows_identity_pages_and_rejects_cursor_cycles() {
    for cycle in [false, true] {
        let (client, server) = tokio::io::duplex(16_384);
        let mut client = tokio_tungstenite::WebSocketStream::from_raw_socket(
            client,
            tokio_tungstenite::tungstenite::protocol::Role::Client,
            /*config*/ None,
        )
        .await;
        let mut server = tokio_tungstenite::WebSocketStream::from_raw_socket(
            server,
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            /*config*/ None,
        )
        .await;
        let serve = async {
            for cursor in [None, Some("page-2")] {
                let request = super::recv_message(&mut server).await.expect("request");
                assert_eq!(request["params"], json!({"cursor": cursor, "limit": 100}));
                super::send_message(
                    &mut server,
                    &json!({
                        "id": request["id"],
                        "result": {
                            "data": [],
                            "nextCursor": if cursor.is_none() || cycle { Some("page-2") } else { None }
                        }
                    }),
                )
                .await
                .expect("response");
            }
        };
        let (result, ()) = tokio::join!(super::load_identities(&mut client), serve);
        if cycle {
            assert!(
                result
                    .expect_err("cursor cycle")
                    .to_string()
                    .contains("repeated a cursor")
            );
        } else {
            assert_eq!(result.expect("all pages"), Vec::new());
        }
    }
}

#[test]
fn reads_app_server_camel_case_identity() {
    let identity: IdentityItem = serde_json::from_value(json!({
        "threadId": "thread-1",
        "sessionId": "session-1",
        "installationId": "installation-1",
        "windowId": "thread-1:0",
        "parentThreadId": null,
        "turnId": null,
        "rootTurnId": null,
        "parentTurnId": null
    }))
    .expect("decode app-server identity");

    assert_eq!(
        (
            identity.thread_id,
            identity.session_id,
            identity.installation_id,
            identity.window_id,
            identity.parent_thread_id,
            identity.turn_id,
            identity.root_turn_id,
            identity.parent_turn_id,
        ),
        (
            "thread-1".to_string(),
            "session-1".to_string(),
            "installation-1".to_string(),
            "thread-1:0".to_string(),
            None,
            None,
            None,
            None,
        )
    );
}
