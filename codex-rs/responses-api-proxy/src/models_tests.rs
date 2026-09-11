use super::load_models;
use super::model_response;
use super::read_catalog_file;
use futures::SinkExt;
use futures::StreamExt;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::Role;

async fn recv_message(
    stream: &mut WebSocketStream<tokio::io::DuplexStream>,
) -> anyhow::Result<Value> {
    let message = stream.next().await.expect("message")?;
    Ok(serde_json::from_str(message.to_text()?)?)
}

async fn send_message(
    stream: &mut WebSocketStream<tokio::io::DuplexStream>,
    value: &Value,
) -> anyhow::Result<()> {
    stream
        .send(Message::Text(serde_json::to_string(value)?.into()))
        .await?;
    Ok(())
}

#[tokio::test]
async fn lists_all_pages_including_hidden_models_using_callable_ids() {
    let (client, server) = tokio::io::duplex(16_384);
    let mut client = WebSocketStream::from_raw_socket(client, Role::Client, /*config*/ None).await;
    let mut server = WebSocketStream::from_raw_socket(server, Role::Server, /*config*/ None).await;
    let serve = async {
        for (cursor, data, next) in [
            (
                None,
                json!([{"id": "picker-id", "model": "visible", "hidden": false}]),
                Some("next"),
            ),
            (
                Some("next"),
                json!([{"model": "hidden", "hidden": true}]),
                None,
            ),
        ] {
            let request = recv_message(&mut server).await.expect("request");
            assert_eq!(request["method"], "model/list");
            assert_eq!(
                request["params"],
                json!({"cursor": cursor, "limit": 100, "includeHidden": true})
            );
            send_message(
                &mut server,
                &json!({
                    "id": request["id"], "result": {"data": data, "nextCursor": next}
                }),
            )
            .await
            .expect("response");
        }
    };
    let (result, ()) = tokio::join!(load_models(&mut client), serve);
    let models = result.expect("catalog");
    let visible = json!({"id": "visible", "object": "model", "created": 0, "owned_by": "codex", "shutdown_date": null});
    let hidden = json!({"id": "hidden", "object": "model", "created": 0, "owned_by": "codex", "shutdown_date": null});
    assert_eq!(models, vec![visible.clone(), hidden.clone()]);
    assert_eq!(
        model_response("/v1/models", models.clone()),
        (200, json!({"object": "list", "data": [visible, hidden]}))
    );
    assert_eq!(
        model_response("/v1/models/hidden", models.clone()),
        (200, hidden)
    );
    assert_eq!(
        model_response("/v1/models/missing", models),
        (
            404,
            json!({"error": {
                "message": "Model not found in the app-server catalog",
                "type": "invalid_request_error", "param": "model", "code": "model_not_found"
            }})
        )
    );
}

#[tokio::test]
async fn rejects_invalid_catalogs_and_rpc_errors() {
    for reply in [
        json!({"result": {"data": [{"id": "missing-callable-model"}], "nextCursor": null}}),
        json!({"error": {"code": -32603, "message": "catalog unavailable"}}),
    ] {
        let (client, server) = tokio::io::duplex(16_384);
        let mut client =
            WebSocketStream::from_raw_socket(client, Role::Client, /*config*/ None).await;
        let mut server =
            WebSocketStream::from_raw_socket(server, Role::Server, /*config*/ None).await;
        let serve = async {
            let request = recv_message(&mut server).await.expect("request");
            let mut reply = reply;
            reply["id"] = request["id"].clone();
            send_message(&mut server, &reply).await.expect("response");
        };
        let (result, ()) = tokio::join!(load_models(&mut client), serve);
        assert!(result.is_err());
    }
}

#[tokio::test]
async fn rejects_repeated_pagination_cursors() {
    let (client, server) = tokio::io::duplex(16_384);
    let mut client = WebSocketStream::from_raw_socket(client, Role::Client, /*config*/ None).await;
    let mut server = WebSocketStream::from_raw_socket(server, Role::Server, /*config*/ None).await;
    let serve = async {
        for _ in 0..2 {
            let request = recv_message(&mut server).await.expect("request");
            send_message(
                &mut server,
                &json!({
                    "id": request["id"], "result": {"data": [], "nextCursor": "repeated"}
                }),
            )
            .await
            .expect("response");
        }
    };
    let (result, ()) = tokio::join!(load_models(&mut client), serve);
    assert!(
        result
            .expect_err("cursor cycle")
            .to_string()
            .contains("repeated a cursor")
    );
}

#[test]
fn serves_catalog_cache_payload_with_etag() {
    let cache_path = std::env::temp_dir().join(format!(
        "codex-proxy-models-cache-test-{}.json",
        std::process::id()
    ));
    let entries = json!([
        {
            "slug": "gpt-5.6-terra",
            "display_name": "GPT-5.6 Terra",
            "model_messages": {"instructionsTemplate": "instructions"},
            "supported_reasoning_levels": [],
            "context_window": 400000
        }
    ]);
    std::fs::write(
        &cache_path,
        json!({
            "fetched_at": "2026-09-10T12:00:00Z",
            "etag": "\"catalog-etag\"",
            "client_version": "0.153.4",
            "models": entries
        })
        .to_string(),
    )
    .expect("write cache");

    let (body, etag) = read_catalog_file(&cache_path).expect("catalog");
    assert_eq!(
        body,
        json!({"models": [{"slug": "gpt-5.6-terra", "display_name": "GPT-5.6 Terra", "model_messages": {"instructionsTemplate": "instructions"}, "supported_reasoning_levels": [], "context_window": 400000}]})
    );
    assert_eq!(etag.as_deref(), Some("\"catalog-etag\""));
    std::fs::remove_file(cache_path).ok();
}

#[test]
fn rejects_catalog_cache_without_models_key() {
    let cache_path = std::env::temp_dir().join(format!(
        "codex-proxy-models-cache-invalid-{}.json",
        std::process::id()
    ));
    std::fs::write(&cache_path, json!({"fetched_at": "x"}).to_string()).expect("write cache");
    let error = read_catalog_file(&cache_path).expect_err("missing models key");
    assert!(error.to_string().contains("missing the models key"));
    std::fs::remove_file(cache_path).ok();
}
