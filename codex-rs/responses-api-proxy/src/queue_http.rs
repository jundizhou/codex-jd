//! The queue control plane never enters the model request or its response stream.
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tiny_http::Header;
use tiny_http::Method;
use tiny_http::Request;
use tiny_http::Response;
use tiny_http::StatusCode;

use crate::scheduler::Key;
use crate::scheduler::Pending;
use crate::scheduler::Rejection;
use crate::scheduler::Scheduler;

pub(crate) fn error(req: Request, rejection: Rejection) {
    let (status, code) = rejection.response();
    let body = serde_json::json!({"error":{"type":code,"message":code}});
    let mut response = Response::from_string(body.to_string()).with_status_code(StatusCode(status));
    if let Ok(header) = Header::from_bytes(b"Content-Type", b"application/json") {
        response = response.with_header(header);
    }
    if let Rejection::Cooldown(seconds) = rejection
        && let Ok(header) = Header::from_bytes(b"Retry-After", seconds.to_string())
    {
        response = response.with_header(header);
    }
    let _ = req.respond(response);
}

fn respond(req: Request, status: u16, body: serde_json::Value) {
    let mut response = Response::from_string(body.to_string()).with_status_code(StatusCode(status));
    if let Ok(header) = Header::from_bytes(b"Content-Type", b"application/json") {
        response = response.with_header(header);
    }
    let _ = req.respond(response);
}

pub(crate) fn header(req: &Request, name: &'static str) -> anyhow::Result<Option<String>> {
    let mut headers = req.headers().iter().filter(|h| h.field.equiv(name));
    let value = headers.next().map(|h| h.value.as_str());
    anyhow::ensure!(headers.next().is_none(), "duplicate queue header");
    if let Some(value) = value {
        anyhow::ensure!(
            !value.is_empty()
                && value.len() <= 128
                && value
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"-_:".contains(&c)),
            "invalid queue header"
        );
    }
    Ok(value.map(str::to_string))
}

pub(crate) fn control(
    queue: &Scheduler,
    req: Request,
    _account_label: &str,
    profile_dir: Option<&Path>,
    auth_path: &Path,
    capacity: usize,
) -> Option<Request> {
    let req = if req.url().starts_with("/admin/") {
        let root = profile_dir.unwrap_or_else(|| auth_path.parent().unwrap_or(Path::new(".")));
        return crate::admin_accounts::control(queue, req, root, auth_path, capacity);
    } else {
        req
    };
    if req.method() == &Method::Get && req.url() == "/readyz" {
        let status = queue.status();
        let ready = status["paused"] == false
            && status["worker_fault"] == false
            && status["quarantined"].as_u64() < status["effective_max_running"].as_u64()
            && status["account_unavailable"] == false
            && status["cooldown_seconds"] == 0;
        respond(req, if ready { 200 } else { 503 }, status);
        return None;
    }
    if req.method() == &Method::Post
        && matches!(
            req.url(),
            "/internal/queue/pause" | "/internal/queue/resume" | "/internal/queue/recover"
        )
    {
        let result = match req.url() {
            "/internal/queue/pause" => {
                queue.pause();
                Ok(())
            }
            "/internal/queue/resume" => queue.resume(),
            _ if header(&req, "x-codex-queue-confirm-stopped")
                .ok()
                .flatten()
                .as_deref()
                == Some("true") =>
            {
                queue.recover()
            }
            _ => Err(Rejection::Unavailable),
        };
        match result {
            Ok(()) => respond(req, 200, queue.status()),
            Err(error) => self::error(req, error),
        }
        return None;
    }
    if req.method() == &Method::Get && req.url() == "/internal/queue/status" {
        respond(req, 200, queue.status());
        return None;
    }
    if req.method() == &Method::Post
        && let Some(id) = req.url().strip_prefix("/internal/queue/cancel/")
    {
        let id = id.to_string();
        let principal = header(&req, "x-codex-queue-principal");
        if id.is_empty()
            || id.len() > 128
            || !id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"-_:".contains(&c))
            || principal.is_err()
        {
            respond(
                req,
                400,
                serde_json::json!({"error":{"type":"invalid_queue_metadata"}}),
            );
            return None;
        }
        match queue.cancel(
            principal.ok().flatten().unwrap_or_else(|| "direct".into()),
            id,
        ) {
            Ok(()) => respond(req, 200, serde_json::json!({"cancellation_requested":true})),
            Err(rejection) => error(req, rejection),
        }
        return None;
    }
    Some(req)
}

pub(crate) fn pending(
    req: &Request,
    body: &[u8],
    received: Instant,
    fallback: String,
) -> anyhow::Result<Pending> {
    let object: serde_json::Map<String, serde_json::Value> = serde_json::from_slice(body)?;
    anyhow::ensure!(
        object
            .get("background")
            .and_then(serde_json::Value::as_bool)
            != Some(true),
        "background responses are not supported by the queue"
    );
    anyhow::ensure!(
        object
            .get("model")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|model| !model.is_empty()),
        "missing model"
    );
    if let Some(metadata) = object.get("client_metadata") {
        let metadata = metadata
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("invalid client_metadata"))?;
        if let Some(nested) = metadata.get("x-codex-turn-metadata") {
            let nested = nested
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("invalid turn metadata"))?;
            let _: serde_json::Map<String, serde_json::Value> = serde_json::from_str(nested)?;
        }
    }
    let id = header(req, "x-codex-queue-request-id")?.unwrap_or(fallback);
    let principal = header(req, "x-codex-queue-principal")?.unwrap_or_else(|| "direct".into());
    let budget = header(req, "x-codex-queue-budget-ms")?
        .map(|value| value.parse::<u64>())
        .transpose()?
        .map(Duration::from_millis)
        .unwrap_or(crate::scheduler::MAX_QUEUE_WAIT)
        .min(crate::scheduler::MAX_QUEUE_WAIT);
    let affinity = crate::affinity::key_for_request(body);
    anyhow::ensure!(
        affinity.as_ref().is_none_or(|key| key.len() <= 512),
        "conversation key too long"
    );
    let evidence = crate::queue_signals::Evidence::read(&serde_json::Value::Object(object));
    let routing = req
        .headers()
        .iter()
        .find(|header| header.field.equiv("x-codex-turn-state"))
        .map(|header| header.value.as_str())
        .unwrap_or_default();
    // Bind the stable request ID to the exact bytes and continuation routing state.
    let fingerprint = crate::queue_store::digest(&[
        body,
        routing.as_bytes(),
        affinity.as_deref().unwrap_or_default().as_bytes(),
    ]);
    Ok(Pending {
        id: id.clone(),
        key: Key(principal, affinity.clone().unwrap_or(id)),
        sticky: affinity.is_some(),
        fingerprint,
        evidence,
        bytes: body.len(),
        deadline: received + budget,
    })
}

pub(crate) fn admission(
    queue: &Arc<Scheduler>,
    req: &Request,
) -> Result<crate::scheduler::Admission, Rejection> {
    queue.admit(req.body_length().unwrap_or(crate::scheduler::MAX_BODY))
}
