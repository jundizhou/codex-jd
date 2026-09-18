use super::*;
use crate::server::ServerOptions;
use codex_config::types::AuthCredentialsStoreMode;
use pretty_assertions::assert_eq;

fn options() -> ServerOptions {
    ServerOptions::new(
        std::env::temp_dir(),
        "test-client".to_string(),
        /*forced_chatgpt_workspace_id*/ None,
        AuthCredentialsStoreMode::File,
        crate::AuthKeyringBackendKind::default(),
        crate::AuthRouteConfig::from_http_client_factory(
            codex_http_client::HttpClientFactory::new(
                codex_http_client::OutboundProxyPolicy::ReqwestDefault,
            ),
        ),
    )
}

fn callback_url(code: &str, state: &str) -> String {
    format!("http://localhost:1455/auth/callback?code={code}&state={state}")
}

#[test]
fn authorize_url_carries_pkce_and_state() {
    let login = CallbackLogin::begin(&options());
    assert!(
        login
            .authorize_url
            .starts_with("https://auth.openai.com/oauth/authorize?")
    );
    let query = login.authorize_url.split_once('?').unwrap().1;
    for key in [
        "response_type=code",
        "client_id=test-client",
        "redirect_uri=http%3A%2F%2Flocalhost%3A1455%2Fauth%2Fcallback",
        "code_challenge_method=S256",
    ] {
        assert!(query.contains(key), "missing {key} in {query}");
    }
    let state = query_param(query, "state").unwrap();
    assert_eq!(state, login.state);
    assert_eq!(login.pkce.code_challenge.len(), 43);
}

#[test]
fn complete_rejects_malformed_and_mismatched_callbacks() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let login = CallbackLogin::begin(&options());
    for bad in [
        "http://localhost:1455/auth/callback",
        "not-a-url",
        &callback_url("c", "wrong"),
    ] {
        let error = runtime
            .block_on(login.complete(&options(), bad))
            .unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("state") || message.contains("授权码") || message.contains("查询参数"),
            "unexpected error for {bad}: {message}"
        );
    }
}

#[test]
fn query_param_decodes_values() {
    assert_eq!(
        query_param("a=1&code=x%20y&state=s", "code"),
        Some("x y".to_string())
    );
    assert_eq!(query_param("a=1", "missing"), None);
    assert_eq!(query_param("code=", "code"), Some(String::new()));
}
