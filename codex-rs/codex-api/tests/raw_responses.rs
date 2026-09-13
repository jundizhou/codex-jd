use std::sync::Arc;
use std::time::Duration;

use codex_api::AuthProvider;
use codex_api::Compression;
use codex_api::Provider;
use codex_api::ResponsesClient;
use codex_api::RetryConfig;
use codex_client::ReqwestTransport;
use codex_http_client::HttpClientBuilder;
use futures::TryStreamExt;
use http::HeaderMap;
use pretty_assertions::assert_eq;
use serde_json::json;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;

struct NoAuth;

impl AuthProvider for NoAuth {
    fn add_auth_headers(&self, _headers: &mut HeaderMap) {}
}

#[tokio::test]
async fn raw_forwarding_preserves_http_results_and_sends_once() -> anyhow::Result<()> {
    for status in [200, 400, 429, 500, 503] {
        let server = MockServer::start().await;
        let upstream_body = if status == 200 {
            r#"{"output":[{"type":"function_call","name":"shell","call_id":"call-1","arguments":"{}"}]}"#
        } else {
            r#"{"error":{"message":"upstream failure","code":"test_error"}}"#
        };
        Mock::given(method("POST"))
            .and(path("/responses"))
            .respond_with(
                ResponseTemplate::new(status)
                    .set_body_string(upstream_body)
                    .insert_header("x-codex-turn-state", "opaque-state"),
            )
            .expect(1)
            .mount(&server)
            .await;

        let provider = Provider {
            name: "test".to_string(),
            base_url: server.uri(),
            query_params: None,
            headers: HeaderMap::new(),
            retry: RetryConfig {
                max_attempts: 3,
                base_delay: Duration::from_millis(1),
                retry_429: true,
                retry_5xx: true,
                retry_transport: true,
            },
            stream_idle_timeout: Duration::from_secs(1),
        };
        let http_client = HttpClientBuilder::new().build_direct()?;
        let client = ResponsesClient::new(
            ReqwestTransport::from_http_client(http_client),
            provider,
            Arc::new(NoAuth),
        );
        let body = json!({
            "model": "gpt-test",
            "instructions": "unchanged",
            "tools": [{"type": "function", "name": "shell"}],
            "input": [{"role": "user", "content": "hello"}],
            "prompt_cache_key": "unchanged-key",
            "metadata": {"conversation_id": "client-conversation"}
        });
        let response = client
            .stream_raw(body.clone(), HeaderMap::new(), Compression::None)
            .await?;
        assert_eq!(response.status.as_u16(), status);
        assert_eq!(response.headers["x-codex-turn-state"], "opaque-state");
        let chunks: Vec<_> = response.bytes.try_collect().await?;
        assert_eq!(chunks.concat(), upstream_body.as_bytes());
        let requests = server.received_requests().await.expect("received requests");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].body_json::<serde_json::Value>()?, body);
    }
    Ok(())
}
