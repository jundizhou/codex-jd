//! Authenticated account storage and browser administration. Credentials never leave the server.
use crate::monitored_request::Request;
use crate::scheduler::Scheduler;
use anyhow::Context;
use serde_json::Value;
use serde_json::json;
use std::fs;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use tiny_http::Header;
use tiny_http::Method;
use tiny_http::Response;
use tiny_http::StatusCode;
const MAX_AUTH: u64 = 64 * 1024;
const MAX_PROFILES: usize = 64;

pub(crate) struct Store<'a> {
    root: &'a Path,
    auth: &'a Path,
    capacity: usize,
}

impl<'a> Store<'a> {
    pub(crate) fn new(root: &'a Path, auth: &'a Path, capacity: usize) -> Self {
        Self {
            root,
            auth,
            capacity: capacity.min(32),
        }
    }

    fn names(&self) -> anyhow::Result<Vec<String>> {
        if !self.root.exists() {
            return Ok(Vec::new());
        }
        anyhow::ensure!(
            fs::symlink_metadata(self.root)?.is_dir(),
            "账号目录不可为符号链接"
        );
        let mut names = Vec::new();
        for entry in fs::read_dir(self.root)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if entry.file_type()?.is_dir() && valid_name(&name) {
                names.push(name);
                anyhow::ensure!(names.len() <= MAX_PROFILES, "最多保存 64 个账号");
            }
        }
        names.sort();
        Ok(names)
    }

    fn profile(&self, name: &str) -> anyhow::Result<std::path::PathBuf> {
        anyhow::ensure!(
            valid_name(name),
            "名称必须为 1–64 个英文字母、数字、短横线或下划线"
        );
        anyhow::ensure!(
            fs::symlink_metadata(self.root)?.is_dir(),
            "账号目录不可为符号链接"
        );
        let dir = self.root.join(name);
        anyhow::ensure!(fs::symlink_metadata(&dir)?.is_dir(), "账号不存在或路径无效");
        Ok(dir.join("auth.json"))
    }

    pub(crate) fn limit(&self, fallback: usize) -> anyhow::Result<usize> {
        let path = self.auth.with_file_name("admin-concurrency.json");
        match fs::read(&path) {
            Ok(bytes) => {
                let limit: usize = serde_json::from_slice(&bytes)?;
                anyhow::ensure!(
                    (1..=self.capacity).contains(&limit),
                    "保存的并发上限超过当前会话容量"
                );
                Ok(limit)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(fallback),
            Err(error) => Err(error.into()),
        }
    }

    fn list(&self) -> anyhow::Result<Value> {
        let active = read_auth(self.auth).ok();
        let current = active.as_ref().map(login_label);
        let mut result = Vec::new();
        for name in self.names()? {
            let auth = self.profile(&name).and_then(|path| read_auth(&path));
            let account = auth.as_ref().ok().map(login_label);
            let is_active = auth
                .as_ref()
                .ok()
                .zip(active.as_ref())
                .is_some_and(|(a, b)| identity(a) == identity(b));
            result.push(
                json!({"name": name, "account": account, "active": is_active, "valid": auth.is_ok()}),
            );
        }
        Ok(json!({"profiles": result, "current": current}))
    }

    pub(crate) fn add(&self, name: &str, value: &Value) -> anyhow::Result<()> {
        anyhow::ensure!(valid_name(name), "账号名称格式无效");
        validate_auth(value)?;
        anyhow::ensure!(self.names()?.len() < MAX_PROFILES, "最多保存 64 个账号");
        fs::create_dir_all(self.root)?;
        for existing in self.names()? {
            if let Ok(auth) = read_auth(&self.profile(&existing)?) {
                anyhow::ensure!(identity(&auth) != identity(value), "该账号已保存");
            }
        }
        let dir = self.root.join(name);
        fs::create_dir(&dir).context("该名称已存在，或目录不可写")?;
        if let Err(error) = write_private(&dir.join("auth.json"), value) {
            let _ = fs::remove_dir(&dir);
            return Err(error);
        }
        Ok(())
    }

    /// Saves fresh credentials under the existing identity's name. The caller
    /// serializes active credential replacement with request admission.
    pub(crate) fn save_login(
        &self,
        name: &str,
        value: &Value,
        activate: impl FnOnce(&Value) -> anyhow::Result<()>,
    ) -> anyhow::Result<Value> {
        anyhow::ensure!(valid_name(name), "账号名称格式无效");
        validate_auth(value)?;
        let existing = self.names()?.into_iter().find(|existing| {
            self.profile(existing)
                .and_then(|path| read_auth(&path))
                .is_ok_and(|saved| identity(&saved) == identity(value))
        });
        let active = read_auth(self.auth).is_ok_and(|saved| identity(&saved) == identity(value));
        let profile = existing.as_deref().unwrap_or(name);
        if existing.is_some() {
            write_private(&self.profile(profile)?, value)?;
        } else {
            self.add(profile, value)?;
        }
        if active {
            activate(value)
                .context("新认证已保存，但当前认证尚未更新；请等待请求结束后再次提交")?;
        }
        Ok(json!({"ok": true, "profile": profile, "updated": existing.is_some(), "active": active}))
    }

    fn delete(&self, name: &str) -> anyhow::Result<()> {
        let path = self.profile(name)?;
        let value = read_auth(&path).ok();
        let active = read_auth(self.auth).ok();
        anyhow::ensure!(
            value
                .as_ref()
                .zip(active.as_ref())
                .is_none_or(|(value, active)| identity(value) != identity(active)),
            "当前使用的账号不能删除，请先切换账号"
        );
        fs::remove_file(&path)?;
        fs::remove_dir(path.parent().context("无效账号目录")?)?;
        Ok(())
    }

    fn activate(&self, name: &str) -> anyhow::Result<()> {
        let next = read_auth(&self.profile(name)?)?;
        let previous = match read_auth(self.auth) {
            Ok(previous) => previous,
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
            {
                return write_private(self.auth, &next);
            }
            Err(error) => return Err(error),
        };
        if identity(&next) == identity(&previous) {
            return Ok(());
        }
        let mut saved = false;
        for existing in self.names()? {
            let path = self.profile(&existing)?;
            if read_auth(&path).is_ok_and(|a| identity(&a) == identity(&previous)) {
                write_private(&path, &previous)?;
                saved = true;
                break;
            }
        }
        // Preserve the current login (including refreshed tokens) before its first switch.
        if !saved {
            let backup = format!(
                "saved-{}",
                &crate::queue_store::digest(&[identity(&previous).as_bytes()])[..12]
            );
            self.add(&backup, &previous)?;
        }
        write_private(self.auth, &next)
    }
}

pub(crate) fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
}

fn identity(value: &Value) -> String {
    value
        .pointer("/tokens/account_id")
        .or_else(|| value.get("OPENAI_API_KEY"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// Display label for a stored credential: the masked id_token email when the
/// JWT carries one, otherwise the masked account identifier. The JWT payload
/// is read for display only and never signature-verified; a malformed token
/// just falls back to the identifier.
fn login_label(value: &Value) -> String {
    id_token_email(value).map_or_else(
        || masked_account(&identity(value)),
        |email| masked_account(&email),
    )
}

fn id_token_email(value: &Value) -> Option<String> {
    use base64::Engine;
    let token = value.pointer("/tokens/id_token")?.as_str()?;
    // Padding never appears inside base64url payloads, so stripping it keeps
    // both padded and unpadded encodings decodable.
    let payload = token.split('.').nth(1)?.trim_end_matches('=');
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    let claims: Value = serde_json::from_slice(&decoded).ok()?;
    claims
        .get("email")
        .and_then(Value::as_str)
        .filter(|email| email.contains('@'))
        .map(str::to_owned)
}

/// Masks an account identifier for display: emails keep their first character
/// and domain, opaque identifiers keep only four trailing characters, and
/// anything shorter would be revealed by a suffix so it is fully hidden.
pub(crate) fn masked_account(account: &str) -> String {
    if let Some((local, domain)) = account.split_once('@') {
        let prefix = local.chars().next().unwrap_or('*');
        return format!("{prefix}***@{domain}");
    }
    let suffix: String = if account.len() < 8 {
        String::new()
    } else {
        account
            .chars()
            .rev()
            .take(4)
            .collect::<String>()
            .chars()
            .rev()
            .collect()
    };
    format!("account-***{suffix}")
}

fn validate_auth(value: &Value) -> anyhow::Result<()> {
    let nonempty = |key| {
        value
            .pointer(key)
            .and_then(Value::as_str)
            .is_some_and(|s| !s.trim().is_empty())
    };
    anyhow::ensure!(
        value.is_object()
            && (nonempty("/OPENAI_API_KEY")
                || (nonempty("/tokens/account_id")
                    && nonempty("/tokens/access_token")
                    && nonempty("/tokens/refresh_token")
                    && nonempty("/tokens/id_token"))),
        "请选择完整的 Codex auth.json（文件格式有效不代表登录仍有效）"
    );
    anyhow::ensure!(
        serde_json::to_vec(value)?.len() <= MAX_AUTH as usize,
        "认证文件超过 64 KiB"
    );
    Ok(())
}

pub(crate) fn read_auth(path: &Path) -> anyhow::Result<Value> {
    let metadata = fs::symlink_metadata(path)?;
    anyhow::ensure!(
        metadata.is_file() && metadata.len() <= MAX_AUTH,
        "认证文件无效"
    );
    let value = serde_json::from_reader(fs::File::open(path)?.take(MAX_AUTH + 1))?;
    validate_auth(&value)?;
    Ok(value)
}

fn write_private(path: &Path, value: &Value) -> anyhow::Result<()> {
    let temp = path.with_extension("pending");
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let result = (|| -> anyhow::Result<()> {
        let mut file = options.open(&temp)?;
        file.write_all(&serde_json::to_vec(value)?)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        Ok(())
    })();
    if result.is_err() && temp.is_file() {
        let _ = fs::remove_file(temp);
    }
    result
}

pub(crate) fn control(
    queue: &Scheduler,
    mut req: Request,
    root: &Path,
    auth: &Path,
    capacity: usize,
) -> Option<Request> {
    let store = Store::new(root, auth, capacity);
    if req.method() == &Method::Get && req.url() == "/admin/accounts" {
        let mut response = Response::from_string(include_str!("admin_accounts.html"));
        if let Ok(header) = Header::from_bytes(b"Content-Type", b"text/html; charset=utf-8") {
            response = response.with_header(header);
        }
        if let Ok(header) = Header::from_bytes(b"Cache-Control", b"no-store") {
            response = response.with_header(header);
        }
        let _ = req.respond(response);
        return None;
    }
    let result = (|| -> anyhow::Result<Value> {
        if req.method() == &Method::Get
            && let Some(id) = req.url().strip_prefix("/admin/api/request?id=")
        {
            let id = id.parse::<u64>().context("无效请求编号")?;
            return crate::request_metrics::METRICS
                .detail(id)
                .context("请求记录已过期或不存在");
        }
        if req.method() == &Method::Get && req.url() == "/admin/api/accounts" {
            let mut body = store.list()?;
            body["queue"] = queue.status();
            body["capacity"] = json!(store.capacity);
            body["metrics"] = crate::request_metrics::METRICS.snapshot();
            return Ok(body);
        }
        if req.method() == &Method::Get && req.url() == "/admin/api/login-status" {
            return Ok(crate::admin_login::status());
        }
        anyhow::ensure!(req.method() == &Method::Post, "不支持的操作");
        let mut bytes = Vec::new();
        req.as_reader()
            .take(MAX_AUTH * 2 + 1)
            .read_to_end(&mut bytes)?;
        anyhow::ensure!(bytes.len() <= (MAX_AUTH * 2) as usize, "请求过大");
        let body: Value = serde_json::from_slice(&bytes).context("请求必须是 JSON")?;
        let name = body["profile"].as_str().unwrap_or_default();
        match req.url() {
            "/admin/api/add" => {
                return store.save_login(name, &body["auth"], |value| {
                    queue.switch_idle(|| write_private(auth, value))
                });
            }
            "/admin/api/delete" => store.delete(name)?,
            "/admin/api/switch" => queue.switch_idle(|| store.activate(name))?,
            "/admin/api/login-start" => return crate::admin_login::start(root, name),
            "/admin/api/login-callback" => {
                let url = body["url"].as_str().context("缺少回调链接")?;
                return crate::admin_login::complete(root, name, url, |value| {
                    store.save_login(name, value, |value| {
                        queue.switch_idle(|| write_private(auth, value))
                    })
                });
            }
            "/admin/api/concurrency" => {
                let limit = body["limit"].as_u64().context("并发必须为整数")?;
                anyhow::ensure!(
                    (1..=store.capacity as u64).contains(&limit),
                    "并发必须介于 1 和 {}",
                    store.capacity
                );
                queue.set_limit(limit as usize, || {
                    write_private(
                        &auth.with_file_name("admin-concurrency.json"),
                        &json!(limit),
                    )
                })?;
            }
            _ => anyhow::bail!("未知管理接口"),
        }
        Ok(json!({"ok": true}))
    })();
    let (status, body) = match result {
        Ok(body) => (200, body),
        Err(error) => (400, json!({"error": error.to_string()})),
    };
    let mut response = Response::from_string(body.to_string()).with_status_code(StatusCode(status));
    if let Ok(header) = Header::from_bytes(b"Content-Type", b"application/json") {
        response = response.with_header(header);
    }
    if let Ok(header) = Header::from_bytes(b"Cache-Control", b"no-store") {
        response = response.with_header(header);
    }
    let _ = req.respond(response);
    None
}

#[cfg(test)]
#[path = "admin_accounts_tests.rs"]
mod tests;
