//! Opt-in, bounded snapshots at the application HTTP send boundary.
use serde_json::json;
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

static LOCK: Mutex<()> = Mutex::new(());

pub(crate) fn capture(request: &reqwest::Request, directory: &Path) -> std::io::Result<()> {
    // Only the raw Responses adapter's correlated requests are eligible.
    if !request.url().path().ends_with("/responses") {
        return Ok(());
    }
    let Some(id) = request
        .headers()
        .get("x-client-request-id")
        .and_then(|v| v.to_str().ok())
    else {
        return Ok(());
    };
    if id.is_empty() || id.len() > 64 || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Ok(());
    }
    let Some(body) = request.body().and_then(reqwest::Body::as_bytes) else {
        return Ok(());
    };
    let headers: Vec<_> = request
        .headers()
        .iter()
        .take(128)
        .map(|(name, value)| {
            let secret = value.is_sensitive()
                || name.as_str().contains("authorization")
                || name.as_str().contains("cookie")
                || name.as_str().contains("token")
                || name.as_str().contains("api-key");
            let value = if secret {
                "[REDACTED]".to_owned()
            } else {
                value
                    .to_str()
                    .unwrap_or("[binary]")
                    .chars()
                    .take(2048)
                    .collect()
            };
            json!({"name":name.as_str(),"value":value})
        })
        .collect();
    let mut url = request.url().clone();
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    let snapshot = json!({
        "method": request.method().as_str(), "url": url.as_str(), "headers": headers,
        "body": String::from_utf8_lossy(&body[..body.len().min(64 * 1024)]),
        "body_truncated": body.len() > 64 * 1024,
        "headers_truncated": request.headers().len() > 128 || request.headers().values().any(|v| v.len() > 2048),
        "boundary": "application_http_send"
    });
    let _lock = LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    std::fs::create_dir_all(directory)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))?;
    }
    let mut files: Vec<_> = std::fs::read_dir(directory)?
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|s| s == "json"))
        .collect();
    files.sort_by_key(|entry| entry.metadata().and_then(|m| m.modified()).ok());
    for entry in files.iter().take(files.len().saturating_sub(99)) {
        let _ = std::fs::remove_file(entry.path());
    }
    let temporary = directory.join(format!("{id}.tmp"));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    let result = (|| {
        file.write_all(snapshot.to_string().as_bytes())?;
        std::fs::rename(&temporary, directory.join(format!("{id}.json")))
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temporary);
    }
    result
}

#[cfg(test)]
#[path = "request_capture_tests.rs"]
mod tests;
