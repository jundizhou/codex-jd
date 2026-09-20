//! Paste-back account onboarding: the operator opens the authorize URL in any
//! browser and returns the redirected localhost callback URL; this server
//! exchanges it for credentials and stores them as a named profile.
use crate::admin_accounts::read_auth;
use crate::admin_accounts::valid_name;
use anyhow::Context;
use serde_json::Value;
use serde_json::json;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

struct Pending {
    id: u64,
    profile: String,
    login: codex_login::CallbackLogin,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
static PENDING: OnceLock<Mutex<Option<Pending>>> = OnceLock::new();

fn pending() -> &'static Mutex<Option<Pending>> {
    PENDING.get_or_init(|| Mutex::new(None))
}

// The leading dot keeps staging homes out of the listed profile names, and
// the session id isolates concurrent generations of the login flow.
fn staging_dir(root: &Path, id: u64) -> PathBuf {
    root.join(format!(".login-{id}"))
}

// Removes leftover staging homes from previous server runs.
fn clear_stale_staging(root: &Path) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        if entry.file_name().to_string_lossy().starts_with(".login-") {
            let _ = fs::remove_dir_all(entry.path());
        }
    }
}

fn login_options(codex_home: PathBuf) -> codex_login::ServerOptions {
    codex_login::ServerOptions::new(
        codex_home,
        codex_login::oauth_client_id(),
        /*forced_chatgpt_workspace_id*/ None,
        codex_login::AuthCredentialsStoreMode::File,
        codex_login::AuthKeyringBackendKind::default(),
        codex_login::AuthRouteConfig::from_http_client_factory(
            codex_http_client::HttpClientFactory::new(
                codex_http_client::OutboundProxyPolicy::RespectSystemProxy,
            ),
        ),
    )
}

pub(crate) fn status() -> Value {
    let guard = pending()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match guard.as_ref() {
        Some(session) => json!({
            "active": true,
            "profile": session.profile,
            "url": session.login.authorize_url(),
        }),
        None => json!({"active": false}),
    }
}

/// Starts a login: no network is involved, the URL is returned immediately.
pub(crate) fn start(root: &Path, profile: &str) -> anyhow::Result<Value> {
    anyhow::ensure!(valid_name(profile), "账号名称格式无效");
    // Starting a new login supersedes the previous one via the generation id.
    let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
    clear_stale_staging(root);
    let staging = staging_dir(root, id);
    fs::create_dir_all(&staging).context("无法创建登录暂存目录")?;
    let login = codex_login::CallbackLogin::begin(&login_options(staging));
    let url = login.authorize_url().to_owned();
    *pending()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Pending {
        id,
        profile: profile.to_owned(),
        login,
    });
    Ok(json!({"url": url}))
}

/// Completes a login with the pasted callback URL; blocks for one token
/// exchange round trip and then stores the credentials as the named profile.
/// Credentials remain staged after a save failure so a retry does not reuse
/// the single-use authorization code.
pub(crate) fn complete(
    root: &Path,
    profile: &str,
    callback_url: &str,
    save: impl FnOnce(&Value) -> anyhow::Result<Value>,
) -> anyhow::Result<Value> {
    anyhow::ensure!(
        !callback_url.is_empty() && callback_url.len() <= 2048,
        "回调链接无效"
    );
    let mut guard = pending()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(session) = guard.as_ref() else {
        anyhow::bail!("没有进行中的登录，请先生成登录链接");
    };
    anyhow::ensure!(
        session.profile == profile,
        "登录会话对应账号「{}」，与请求的「{profile}」不一致，请重新生成登录链接",
        session.profile
    );
    let staging = staging_dir(root, session.id);
    if !staging.join("auth.json").exists() {
        let options = login_options(staging.clone());
        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("无法启动登录运行时")?
            .block_on(session.login.complete(&options, callback_url));
        if let Err(error) = result {
            anyhow::bail!("登录未完成：{error}");
        }
    }
    let outcome = read_auth(&staging.join("auth.json"))
        .and_then(|value| save(&value))
        .map_err(|error| anyhow::anyhow!("登录完成但保存失败：{error:#}"))?;
    *guard = None;
    let _ = fs::remove_dir_all(staging);
    Ok(outcome)
}

#[cfg(test)]
#[path = "admin_login_tests.rs"]
mod tests;
