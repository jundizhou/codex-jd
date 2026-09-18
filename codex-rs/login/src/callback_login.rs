//! Browser login for hosts that cannot bind a localhost callback server: the
//! operator opens the authorize URL anywhere, the browser lands on the
//! standard `http://localhost:1455/auth/callback` redirect, and the pasted
//! callback URL supplies the authorization code exchanged here.
use crate::callback_params::login_callback_result_from_state;
use crate::pkce::PkceCodes;
use crate::server::ServerOptions;
use crate::server::build_authorize_url;
use crate::server::ensure_workspace_allowed;
use crate::server::exchange_code_for_tokens;
use crate::server::generate_state;
use crate::server::persist_tokens_async;

/// A pending authorization awaiting the pasted callback URL.
pub struct CallbackLogin {
    authorize_url: String,
    redirect_uri: String,
    state: String,
    pkce: PkceCodes,
}

impl CallbackLogin {
    /// Builds the authorize URL; no network or local port is involved.
    pub fn begin(opts: &ServerOptions) -> Self {
        let pkce = crate::pkce::generate_pkce();
        let state = generate_state();
        let redirect_uri = format!(
            "http://localhost:{}/auth/callback",
            crate::server::DEFAULT_PORT
        );
        let authorize_url = build_authorize_url(
            &opts.issuer,
            &opts.client_id,
            &redirect_uri,
            &pkce,
            &state,
            opts.forced_chatgpt_workspace_id.as_deref(),
        );
        Self {
            authorize_url,
            redirect_uri,
            state,
            pkce,
        }
    }

    /// The URL the operator opens in a browser.
    pub fn authorize_url(&self) -> &str {
        &self.authorize_url
    }

    /// Validates the pasted callback URL, exchanges the code and persists the
    /// credentials into the options' codex home.
    pub async fn complete(&self, opts: &ServerOptions, callback_url: &str) -> std::io::Result<()> {
        let Some(query) = callback_url.split_once('?').map(|(_, query)| query) else {
            return Err(std::io::Error::other("回调链接中没有查询参数"));
        };
        let code = query_param(query, "code")
            .ok_or_else(|| std::io::Error::other("回调链接中没有授权码"))?;
        let state = query_param(query, "state")
            .ok_or_else(|| std::io::Error::other("回调链接中没有 state"))?;
        if login_callback_result_from_state(&state, &self.state).is_none() {
            return Err(std::io::Error::other(
                "回调链接的 state 校验失败，请重新发起登录",
            ));
        }
        let tokens = exchange_code_for_tokens(
            &opts.issuer,
            &opts.client_id,
            &self.redirect_uri,
            &self.pkce,
            &code,
            &opts.auth_route_config,
        )
        .await?;
        if let Err(message) = ensure_workspace_allowed(
            opts.forced_chatgpt_workspace_id.as_deref(),
            &tokens.id_token,
        ) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                message,
            ));
        }
        persist_tokens_async(
            &opts.codex_home,
            /*api_key*/ None,
            tokens.id_token,
            tokens.access_token,
            tokens.refresh_token,
            opts.cli_auth_credentials_store_mode,
            opts.auth_keyring_backend_kind,
        )
        .await
    }
}

fn query_param(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (name, value) = pair.split_once('=')?;
        if name != key {
            return None;
        }
        urlencoding::decode(value)
            .ok()
            .map(std::borrow::Cow::into_owned)
    })
}

#[cfg(test)]
#[path = "callback_login_tests.rs"]
mod tests;
