use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadModelIdentityListParams;
use codex_app_server_protocol::ThreadModelIdentityListResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use core_test_support::responses;
use core_test_support::skip_if_no_network;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::start_streaming_sse_server;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;
use tempfile::TempDir;
use wiremock::Mock;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;

#[tokio::test]
async fn raw_turn_start_uses_provider_timeout_without_retrying() -> Result<()> {
    skip_if_no_network!(Ok(()));
    let server = responses::start_mock_server().await;
    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(3)))
        .expect(1)
        .mount(&server)
        .await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_provider_config("supports_websockets = false\nstream_idle_timeout_ms = 1000")
        .write(codex_home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let ThreadStartResponse { thread, .. } = app.start_thread(ThreadStartParams::default()).await?;
    let request_id = app
        .send_turn_start_request(TurnStartParams {
            thread_id: thread.id,
            raw_responses: Some(json!({"model":"mock-model", "input":[], "stream":true})),
            ..Default::default()
        })
        .await?;
    let error = app
        .read_stream_until_error_message(RequestId::Integer(request_id))
        .await?;
    assert_eq!(
        error.error,
        JSONRPCErrorError {
            code: -32603,
            message: "raw Responses request timed out".into(),
            data: None,
        }
    );
    Ok(())
}

#[tokio::test]
async fn raw_stream_uses_provider_timeout_between_chunks() -> Result<()> {
    skip_if_no_network!(Ok(()));
    let (gate, blocked) = tokio::sync::oneshot::channel();
    let (server, _) = start_streaming_sse_server(vec![vec![
        StreamingSseChunk {
            gate: None,
            body: responses::sse(vec![responses::ev_response_created("stalled")]),
        },
        StreamingSseChunk {
            gate: Some(blocked),
            body: responses::sse(vec![responses::ev_completed("stalled")]),
        },
    ]])
    .await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(server.uri())
        .with_provider_config("supports_websockets = false\nstream_idle_timeout_ms = 500")
        .write(codex_home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let ThreadStartResponse { thread, .. } = app.start_thread(ThreadStartParams::default()).await?;
    let request_id = app
        .send_turn_start_request(TurnStartParams {
            thread_id: thread.id,
            raw_responses: Some(json!({"model":"mock-model", "input":[], "stream":true})),
            raw_responses_stream: true,
            ..Default::default()
        })
        .await?;
    let error = app
        .read_stream_until_error_message(RequestId::Integer(request_id))
        .await?;
    assert_eq!(
        error.error,
        JSONRPCErrorError {
            code: -32603,
            message: "raw Responses stream idle timeout".into(),
            data: None,
        }
    );
    assert_eq!(server.requests().await.len(), 1);
    drop(gate);
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn raw_turn_start_preserves_non_identity_request_fields() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = responses::start_mock_server().await;
    let upstream_body = responses::sse(vec![
        responses::ev_response_created("resp-raw"),
        responses::ev_completed("resp-raw"),
    ]);
    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .insert_header("x-codex-turn-state", "upstream-next-token")
                .insert_header("set-cookie", "must-not-leak")
                .insert_header("retry-after", "120")
                .set_body_raw(upstream_body.as_bytes().to_vec(), "text/event-stream"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_provider_config("supports_websockets = false")
        .write(codex_home.path())?;

    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let ThreadStartResponse { thread, .. } = app
        .start_thread(ThreadStartParams {
            model: Some("mock-model".to_string()),
            ..Default::default()
        })
        .await?;

    let identities: ThreadModelIdentityListResponse = app
        .request(|request_id| ClientRequest::ThreadModelIdentityList {
            request_id,
            params: ThreadModelIdentityListParams::default(),
        })
        .await?;
    let identity = identities
        .data
        .iter()
        .find(|item| item.thread_id == thread.id.as_str())
        .unwrap();
    let raw_request = json!({
        "model": "mock-model",
        "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]}],
        "instructions": "preserve instructions",
        "tools": [{"type": "function", "name": "keep_tool", "parameters": {"type": "object"}}],
        "stream": true,
        "previous_response_id": "resp-previous",
        "unknown": {"nested": [1, true, null]},
        "client_metadata": {
            "x-codex-installation-id": "old-installation",
            "session_id": "old-session",
            "thread_id": "old-thread",
            "root_turn_id": "old-root",
            "parent_turn_id": "old-parent",
            "turn_id": "preserve-turn",
            "x-codex-window-id": "preserve-window",
            "window_id": "old-window",
            "context_window_id": "old-context-window",
            "x-codex-turn-metadata": serde_json::to_string(&json!({
                "installation_id": "old-installation",
                "session_id": "old-session",
                "thread_id": "old-thread",
                "root_turn_id": "old-root",
                "parent_turn_id": "old-parent",
                "turn_id": "preserve-turn",
                "window_id": "preserve-window",
                "context_window_id": "old-context-window",
                "opaque": "preserve"
            }))?
        }
    });
    let supplied_headers = HashMap::from([
        ("x-codex-turn-state".to_string(), "caller-token".to_string()),
        (
            "x-codex-inference-call-id".to_string(),
            "caller-call-id".to_string(),
        ),
        (
            "traceparent".to_string(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
        ),
        ("tracestate".to_string(), "vendor=value".to_string()),
    ]);
    let response: TurnStartResponse = app
        .request(|request_id| ClientRequest::TurnStart {
            request_id,
            params: TurnStartParams {
                thread_id: thread.id,
                input: vec![],
                raw_responses: Some(raw_request.clone()),
                raw_responses_headers: Some(supplied_headers.clone()),
                ..Default::default()
            },
        })
        .await?;

    assert_eq!(response.raw_response_status, Some(200));
    assert_eq!(response.raw_response_body, Some(upstream_body));
    assert_eq!(
        response.raw_response_headers,
        Some(HashMap::from([
            (
                "x-codex-turn-state".to_string(),
                "upstream-next-token".to_string()
            ),
            ("retry-after".to_string(), "120".to_string()),
            ("content-type".to_string(), "text/event-stream".to_string()),
        ]))
    );

    let mut expected = raw_request;
    let metadata = expected["client_metadata"].as_object_mut().unwrap();
    metadata.insert(
        "x-codex-installation-id".to_string(),
        json!(identity.installation_id),
    );
    metadata.insert("session_id".to_string(), json!(identity.session_id));
    metadata.insert("thread_id".to_string(), json!(identity.thread_id));
    for key in ["x-codex-window-id", "window_id", "context_window_id"] {
        metadata.insert(key.to_string(), json!(identity.window_id));
    }
    metadata.remove("root_turn_id");
    metadata.remove("parent_turn_id");
    let mut nested: serde_json::Value =
        serde_json::from_str(metadata["x-codex-turn-metadata"].as_str().unwrap())?;
    nested["installation_id"] = json!(identity.installation_id);
    nested["session_id"] = json!(identity.session_id);
    nested["thread_id"] = json!(identity.thread_id);
    nested["window_id"] = json!(identity.window_id);
    nested["context_window_id"] = json!(identity.window_id);
    nested.as_object_mut().unwrap().remove("root_turn_id");
    nested.as_object_mut().unwrap().remove("parent_turn_id");
    metadata.insert(
        "x-codex-turn-metadata".to_string(),
        json!(serde_json::to_string(&nested)?),
    );
    let requests = server.received_requests().await.unwrap();
    let outbound = requests
        .iter()
        .filter(|request| request.url.path() == "/v1/responses")
        .collect::<Vec<_>>();
    assert_eq!(outbound.len(), 1);
    let request = outbound[0];
    assert_eq!(request.body_json::<serde_json::Value>()?, expected);
    for (name, value) in supplied_headers {
        assert_eq!(request.headers.get(&name).unwrap().to_str()?, value);
    }
    assert_eq!(
        request.headers["x-codex-window-id"].to_str()?,
        identity.window_id
    );
    assert!(
        request.headers["user-agent"]
            .to_str()?
            .contains(env!("CARGO_PKG_VERSION"))
    );

    Ok(())
}

#[tokio::test]
async fn raw_rate_limit_preserves_retry_hint_and_never_retries() -> Result<()> {
    skip_if_no_network!(Ok(()));
    let server = responses::start_mock_server().await;
    let body = r#"{"error":{"type":"rate_limit_exceeded"}}"#;
    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .respond_with(
            ResponseTemplate::new(429)
                .set_body_raw(body.as_bytes().to_vec(), "application/json")
                .insert_header("retry-after", "3600")
                .insert_header("set-cookie", "private"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_provider_config("supports_websockets = false")
        .write(codex_home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let ThreadStartResponse { thread, .. } = app
        .start_thread(ThreadStartParams {
            model: Some("mock-model".into()),
            ephemeral: Some(true),
            ..Default::default()
        })
        .await?;
    let response: TurnStartResponse = app
        .request(|request_id| ClientRequest::TurnStart {
            request_id,
            params: TurnStartParams {
                thread_id: thread.id.to_string(),
                raw_responses: Some(json!({"model":"mock-model", "input":[], "stream":false})),
                ..Default::default()
            },
        })
        .await?;
    assert_eq!(
        (
            response.raw_response_status,
            response.raw_response_body,
            response.raw_response_headers
        ),
        (
            Some(429),
            Some(body.to_string()),
            Some(HashMap::from([
                ("retry-after".into(), "3600".into()),
                ("content-type".into(), "application/json".into()),
            ]))
        )
    );
    Ok(())
}
