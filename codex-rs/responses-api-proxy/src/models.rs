//! OpenAI-compatible access to the complete app-server model catalog.

use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use crate::monitored_request::Request;
use anyhow::Context;
use anyhow::Result;
use codex_utils_home_dir::find_codex_home;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;
use tiny_http::Header;
use tiny_http::Response;
use tokio_tungstenite::WebSocketStream;

use crate::app_server_reader::connect;
use crate::app_server_reader::initialize;
use crate::app_server_reader::send_and_wait_for_response;

#[derive(Deserialize)]
struct CatalogModel {
    model: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CatalogPage {
    data: Vec<CatalogModel>,
    next_cursor: Option<String>,
}

pub(super) fn respond(socket: &Path, req: Request) -> Result<()> {
    let url = req.url();
    let (path, query) = url.split_once('?').unwrap_or((url, ""));
    if path == "/v1/models" && query.contains("client_version=") {
        return respond_catalog(req);
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("creating model catalog runtime")?;
    let result = runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(30), async {
            let mut stream = connect(socket).await?;
            initialize(&mut stream).await?;
            load_models(&mut stream).await
        })
        .await
        .context("model catalog request timed out")?
    });
    let (status, body) = match result {
        Ok(models) => model_response(path, models),
        Err(error) => {
            eprintln!("model catalog error: {error:#}");
            (
                502,
                json!({"error": {
                    "message": "Unable to load the app-server model catalog",
                    "type": "server_error",
                    "param": null,
                    "code": "model_catalog_unavailable"
                }}),
            )
        }
    };
    let content_type = Header::from_bytes(b"content-type", b"application/json")
        .map_err(|_| anyhow::anyhow!("invalid content-type header"))?;
    req.respond(
        Response::from_string(serde_json::to_string(&body)?)
            .with_status_code(status)
            .with_header(content_type),
    )?;
    Ok(())
}

/// Serves the complete app-server model catalog to Codex clients.
///
/// Codex refreshes its model list by requesting `/v1/models?client_version=...`
/// and expects the full `ModelInfo` payload (including per-model instructions)
/// that the OpenAI-compatible summary cannot express. The app-server keeps the
/// authoritative catalog in `$CODEX_HOME/models_cache.json`, so serve it
/// directly and let clients fall back to their local catalog on failure.
fn respond_catalog(req: Request) -> Result<()> {
    let content_type = Header::from_bytes(b"content-type", b"application/json")
        .map_err(|_| anyhow::anyhow!("invalid content-type header"))?;
    match read_catalog_from_cache() {
        Ok((body, etag)) => {
            let mut response =
                Response::from_string(serde_json::to_string(&body)?).with_status_code(200);
            response = response.with_header(content_type);
            if let Some(etag) = etag {
                let header = Header::from_bytes(b"ETag", etag.as_bytes())
                    .map_err(|_| anyhow::anyhow!("invalid etag header"))?;
                response = response.with_header(header);
            }
            req.respond(response)?;
            Ok(())
        }
        Err(error) => {
            eprintln!("model catalog cache error: {error:#}");
            req.respond(
                Response::from_string(serde_json::to_string(&json!({"error": {
                    "message": "Unable to load the model catalog cache",
                    "type": "server_error",
                    "param": null,
                    "code": "model_catalog_unavailable"
                }}))?)
                .with_status_code(502)
                .with_header(content_type),
            )?;
            Ok(())
        }
    }
}

fn read_catalog_from_cache() -> Result<(Value, Option<String>)> {
    let codex_home = find_codex_home().context("failed to resolve CODEX_HOME")?;
    read_catalog_file(&codex_home.as_path().join("models_cache.json"))
}

fn read_catalog_file(cache_path: &Path) -> Result<(Value, Option<String>)> {
    let text = std::fs::read_to_string(cache_path)
        .with_context(|| format!("failed to read {}", cache_path.display()))?;
    let value: Value = serde_json::from_str(&text).context("invalid models cache JSON")?;
    let models = value
        .get("models")
        .cloned()
        .context("models cache is missing the models key")?;
    anyhow::ensure!(models.is_array(), "models cache models key is not an array");
    let etag = value
        .get("etag")
        .and_then(Value::as_str)
        .map(str::to_string);
    Ok((json!({"models": models}), etag))
}

async fn load_models<S>(stream: &mut WebSocketStream<S>) -> Result<Vec<Value>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut models = Vec::new();
    let mut cursor = None;
    let mut seen_cursors = HashSet::new();
    loop {
        let result = send_and_wait_for_response(
            stream,
            "model/list",
            json!({"cursor": cursor, "limit": 100, "includeHidden": true}),
        )
        .await?;
        let page: CatalogPage =
            serde_json::from_value(result).context("invalid app-server model/list response")?;
        // The catalog has no creation timestamp or ownership field.
        models.extend(page.data.into_iter().map(|item| {
            json!({
                "id": item.model,
                "object": "model",
                "created": 0,
                "owned_by": "codex",
                "shutdown_date": null
            })
        }));
        let Some(next_cursor) = page.next_cursor else {
            return Ok(models);
        };
        anyhow::ensure!(
            seen_cursors.insert(next_cursor.clone()) && seen_cursors.len() < 100,
            "model catalog pagination repeated a cursor or exceeded 100 pages"
        );
        cursor = Some(next_cursor);
    }
}

fn model_response(path: &str, models: Vec<Value>) -> (u16, Value) {
    if path == "/v1/models" {
        return (200, json!({"object": "list", "data": models}));
    }
    let id = path.strip_prefix("/v1/models/").unwrap_or_default();
    match models.into_iter().find(|model| model["id"] == id) {
        Some(model) => (200, model),
        None => (
            404,
            json!({"error": {
                "message": "Model not found in the app-server catalog",
                "type": "invalid_request_error",
                "param": "model",
                "code": "model_not_found"
            }}),
        ),
    }
}

#[cfg(test)]
#[path = "models_tests.rs"]
mod tests;
