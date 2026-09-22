//! Read-only per-profile quota queries; never activates an account or runs inference.
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Context;
use anyhow::Result;
use anyhow::ensure;
use serde_json::Value;
use serde_json::json;
use tiny_http::Header;
use tiny_http::Response;
use tiny_http::StatusCode;

use crate::monitored_request::Request;

const USAGE_URL: &str = "https://chatgpt.com/backend-api/wham/usage";
static WORKERS: AtomicUsize = AtomicUsize::new(0);
struct QueryPermit;
impl Drop for QueryPermit {
    fn drop(&mut self) {
        WORKERS.fetch_sub(1, Ordering::AcqRel);
    }
}
pub(crate) fn respond(req: Request, profile: String, credentials: Result<Value>) {
    let auth = match credentials {
        Ok(auth) => auth,
        Err(error) => {
            reply(req, /*status*/ 400, json!({"error":error.to_string()}));
            return;
        }
    };
    if WORKERS
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
            (n < 4).then_some(n + 1)
        })
        .is_err()
    {
        reply(
            req,
            /*status*/ 429,
            json!({"error":"额度查询繁忙，请稍后重试"}),
        );
        return;
    }
    std::thread::spawn(move || {
        let _permit = QueryPermit;
        match cached(&auth, crate::quota_cache::Freshness::Manual) {
            Ok(mut value) => {
                value["profile"] = json!(profile);
                reply(req, /*status*/ 200, value);
            }
            Err(error) => reply(req, /*status*/ 502, json!({"error":error.to_string()})),
        }
    });
}
pub(crate) fn cached(auth: &Value, mode: crate::quota_cache::Freshness) -> Result<Value> {
    crate::quota_cache::CACHE.get(
        crate::quota_cache::key(auth),
        mode,
        crate::queue_store::now(),
        || query(auth, USAGE_URL),
    )
}

fn reply(req: Request, status: u16, value: Value) {
    let mut response =
        Response::from_string(value.to_string()).with_status_code(StatusCode(status));
    for (name, value) in [
        ("Content-Type", "application/json"),
        ("Cache-Control", "no-store"),
    ] {
        if let Ok(header) = Header::from_bytes(name, value) {
            response = response.with_header(header);
        }
    }
    let _ = req.respond(response);
}

fn query(auth: &Value, url: &str) -> Result<Value> {
    let token = auth
        .pointer("/tokens/access_token")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .context("仅支持 ChatGPT 登录账号查询额度；API Key 账号暂不支持")?;
    let account = auth
        .pointer("/tokens/account_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .context("账号缺少身份信息，请重新授权")?;
    let mut bearer = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
        .context("账号认证格式无效")?;
    bearer.set_sensitive(true);
    let mut account =
        reqwest::header::HeaderValue::from_str(account).context("账号身份格式无效")?;
    account.set_sensitive(true);
    let factory = codex_http_client::HttpClientFactory::new(
        codex_http_client::OutboundProxyPolicy::RespectSystemProxy,
    );
    let client = codex_http_client::HttpClientBuilder::new()
        .without_redirects()
        .without_request_logging()
        .connect_timeout(Duration::from_secs(/*secs*/ 5))
        .build_respecting_outbound_proxy_policy(
            &factory,
            url,
            codex_http_client::ClientRouteClass::Api,
        )
        .context("无法创建额度查询连接")?;
    let payload = tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
        let mut response = client.get(url).header("Authorization", bearer)
            .header("ChatGPT-Account-Id", account).header("Accept", "application/json")
            .header("OpenAI-Beta", "codex-1").header("originator", "Codex Desktop")
            .header("User-Agent", "Codex Desktop/0.155.0-alpha.9.2 (Mac OS 13.5.0; arm64) unknown (Codex Desktop; 26.915.31945)")
            .timeout(Duration::from_secs(/*secs*/ 15)).send().await.context("额度查询连接失败或超时，请稍后重试")?;
        let status = response.status().as_u16();
        if status != 200 {
            let message = match status {
                401 => "账号认证已失效，请重新授权后查询".to_owned(),
                403 => "上游拒绝额度查询，请检查账号权限或网络出口".to_owned(),
                429 => "上游额度查询过于频繁，请稍后重试".to_owned(),
                _ => format!("上游额度查询失败（HTTP {status}）"),
            };
            let retry_after = response.headers().get("retry-after").and_then(|v| v.to_str().ok())
                .and_then(|v| crate::queue_throttle::retry_after(v, SystemTime::now())).map(|d| d.as_secs());
            return Err(crate::quota_cache::Failure {message,retry_after,auth_invalid: status == 401}.into());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.context("读取额度数据失败")? {
            ensure!(body.len() + chunk.len() <= 1024 * 1024, "额度响应过大");
            body.extend_from_slice(&chunk);
        }
        serde_json::from_slice::<Value>(&body).context("上游未返回有效额度数据")
    })?;
    normalize(
        &payload,
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
    )
}

fn text(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(|value| value.chars().take(/*n*/ 128).collect())
}

fn limit(name: &str, data: &Value, now: u64) -> Value {
    let windows = ["primary_window", "secondary_window"].into_iter().filter_map(|slot| {
        let value = data.get(slot)?.as_object()?;
        let used = value.get("used_percent").and_then(Value::as_f64).filter(|value| value.is_finite() && *value >= 0.0);
        let reset = value.get("reset_at").and_then(Value::as_u64).filter(|value| *value > 0)
            .or_else(|| value.get("reset_after_seconds").and_then(Value::as_u64).and_then(|seconds| now.checked_add(seconds)));
        Some(json!({"seconds":value.get("limit_window_seconds").and_then(Value::as_u64),
            "used_percent":used,"remaining_percent":used.map(|used| (100.0-used).clamp(/*min*/ 0.0, /*max*/ 100.0)),"reset_at":reset}))
    }).collect::<Vec<_>>();
    json!({"name":name,"allowed":data["allowed"].as_bool(),"limit_reached":data["limit_reached"].as_bool(),"windows":windows})
}

fn normalize(payload: &Value, now: u64) -> Result<Value> {
    ensure!(
        payload.is_object()
            && ["plan_type", "rate_limit", "credits"]
                .iter()
                .any(|key| payload.get(key).is_some()),
        "上游未提供可识别的额度信息"
    );
    let mut limits = Vec::new();
    for (key, name) in [
        ("rate_limit", "Codex"),
        ("code_review_rate_limit", "代码审查"),
    ] {
        if payload[key].is_object() {
            limits.push(limit(name, &payload[key], now));
        }
    }
    if let Some(additional) = payload["additional_rate_limits"].as_array() {
        for item in additional.iter().take(/*n*/ 16) {
            if item["rate_limit"].is_object() {
                let name = text(&item["limit_name"])
                    .or_else(|| text(&item["metered_feature"]))
                    .unwrap_or_else(|| "其他额度".into());
                limits.push(limit(&name, &item["rate_limit"], now));
            }
        }
    }
    let credits = payload["credits"].as_object().map(|_| json!({
        "has_credits":payload["credits"]["has_credits"].as_bool(),
        "unlimited":payload["credits"]["unlimited"].as_bool(),"balance":text(&payload["credits"]["balance"])}));
    Ok(
        json!({"plan_type":text(&payload["plan_type"]),"fetched_at":now,"limits":limits,"credits":credits,
        "reset_credits":payload["rate_limit_reset_credits"]["available_count"].as_u64(),
        "spend_control_reached":payload["spend_control"]["reached"].as_bool()}),
    )
}

#[cfg(test)]
#[path = "admin_usage_tests.rs"]
mod tests;
