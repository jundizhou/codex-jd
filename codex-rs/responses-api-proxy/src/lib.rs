use std::fs::File;
use std::fs::{self};
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use clap::Parser;
use serde::Serialize;
use serde_json::Value;
use tiny_http::Header;
use tiny_http::Method;
use tiny_http::Request;
use tiny_http::Response;
use tiny_http::Server;
use tiny_http::StatusCode;

mod app_server_reader;
mod dump;
mod identity;
mod models;
mod rewrite;
mod session_pool;
use app_server_reader::AppServerIdentityClient;
use app_server_reader::resolve_socket_arg;
use dump::ExchangeDumper;
use identity::SessionIdentity;
use rewrite::rewrite_body;
use session_pool::SessionPool;

/// CLI arguments for the proxy.
#[derive(Debug, Clone, Parser)]
#[command(name = "responses-api-proxy", about = "Minimal OpenAI responses proxy")]
pub struct Args {
    /// Port to listen on. If not set, an ephemeral port is used.
    #[arg(long)]
    pub port: Option<u16>,

    /// Path to a JSON file to write startup info (single line). Includes {"port": <u16>}.
    #[arg(long, value_name = "FILE")]
    pub server_info: Option<PathBuf>,

    /// Enable HTTP shutdown endpoint at GET /shutdown
    #[arg(long)]
    pub http_shutdown: bool,

    /// Directory where request/response dumps should be written as JSON.
    #[arg(long, value_name = "DIR")]
    pub dump_dir: Option<PathBuf>,

    /// Absolute path to the local app-server control socket.
    #[arg(long, value_name = "PATH")]
    pub app_server_socket: Option<PathBuf>,
}

#[derive(Serialize)]
struct ServerInfo {
    port: u16,
    pid: u32,
}

struct ForwardConfig {
    identity_client: Arc<AppServerIdentityClient>,
}

/// Entry point for the library main, for parity with other crates.
pub fn run_main(args: Args) -> Result<()> {
    let app_server_socket = resolve_socket_arg(args.app_server_socket.clone())?;
    let (identity_client, identities) = AppServerIdentityClient::start(app_server_socket.clone())
        .with_context(|| {
        format!(
            "failed to create proxy sessions through {}",
            app_server_socket.display()
        )
    })?;
    let session_pool = Arc::new(SessionPool::new(identities)?);
    let identity_client = Arc::new(identity_client);

    let forward_config = Arc::new(ForwardConfig { identity_client });
    let dump_dir = args
        .dump_dir
        .map(ExchangeDumper::new)
        .transpose()
        .context("creating --dump-dir")?
        .map(Arc::new);

    let (listener, bound_addr) = bind_listener(args.port)?;
    if let Some(path) = args.server_info.as_ref() {
        write_server_info(path, bound_addr.port())?;
    }
    let server = Server::from_listener(listener, None)
        .map_err(|err| anyhow!("creating HTTP server: {err}"))?;
    eprintln!("responses-api-proxy listening on {bound_addr}");

    let http_shutdown = args.http_shutdown;
    for request in server.incoming_requests() {
        let forward_config = forward_config.clone();
        let dump_dir = dump_dir.clone();
        let session_pool = session_pool.clone();
        std::thread::spawn(move || {
            if http_shutdown && request.method() == &Method::Get && request.url() == "/shutdown" {
                let _ = request.respond(Response::new_empty(StatusCode(200)));
                std::process::exit(0);
            }

            if let Err(e) =
                forward_request(&forward_config, dump_dir.as_deref(), &session_pool, request)
            {
                eprintln!("forwarding error: {e}");
            }
        });
    }

    Err(anyhow!("server stopped unexpectedly"))
}

fn bind_listener(port: Option<u16>) -> Result<(TcpListener, SocketAddr)> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port.unwrap_or(0)));
    let listener = TcpListener::bind(addr).with_context(|| format!("failed to bind {addr}"))?;
    let bound = listener.local_addr().context("failed to read local_addr")?;
    Ok((listener, bound))
}

fn write_server_info(path: &Path, port: u16) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }

    let info = ServerInfo {
        port,
        pid: std::process::id(),
    };
    let mut data = serde_json::to_string(&info)?;
    data.push('\n');
    let mut f = File::create(path)?;
    f.write_all(data.as_bytes())?;
    Ok(())
}

fn forward_request(
    config: &ForwardConfig,
    dump_dir: Option<&ExchangeDumper>,
    session_pool: &SessionPool,
    req: Request,
) -> Result<()> {
    let method = req.method().clone();
    let url = req.url();
    let url_path = url.split_once('?').map_or(url, |(path, _)| path);
    let owned_url_path;
    let url_path = if url_path == url {
        owned_url_path = url.to_string();
        owned_url_path.as_str()
    } else {
        url_path
    };
    if method == Method::Get && (url_path == "/v1/models" || url_path.starts_with("/v1/models/")) {
        return config.identity_client.respond_models(req);
    }
    if method != Method::Post || url_path != "/v1/responses" {
        let _ = req.respond(Response::new_empty(StatusCode(403)));
        return Ok(());
    }

    let identity = session_pool.acquire();
    let result = forward_request_with_identity(config, dump_dir, &identity, req);
    session_pool.release(identity);
    result
}

fn forward_request_with_identity(
    config: &ForwardConfig,
    dump_dir: Option<&ExchangeDumper>,
    identity: &SessionIdentity,
    mut req: Request,
) -> Result<()> {
    let effective_identity = match config.identity_client.read_identity(&identity.thread_id) {
        Ok(Some(identity)) => identity,
        Ok(None) => {
            req.respond(Response::new_empty(StatusCode(503)))?;
            anyhow::bail!("assigned app-server thread is no longer loaded");
        }
        Err(error) => {
            req.respond(Response::new_empty(StatusCode(503)))?;
            return Err(error.context("refreshing assigned app-server thread identity"));
        }
    };
    let method = req.method().clone();
    let url_path = req.url().to_string();
    let mut body = Vec::new();
    req.as_reader().read_to_end(&mut body)?;
    let rewritten = match rewrite_body(&body, &effective_identity) {
        Ok(rewritten) => rewritten,
        Err(error) => {
            req.respond(Response::new_empty(StatusCode(400)))?;
            return Err(error.context("rewriting request identity metadata"));
        }
    };
    let exchange_dump = dump_dir.and_then(|dump_dir| {
        dump_dir
            .dump_request(&method, &url_path, req.headers(), &body)
            .map_err(|err| {
                eprintln!("responses-api-proxy failed to dump request: {err}");
                err
            })
            .ok()
    });
    let request_value: Value = serde_json::from_slice(&rewritten.body)?;
    let mut headers = std::collections::HashMap::new();
    for header in req.headers() {
        let name = header.field.as_str().to_ascii_lowercase().to_string();
        if matches!(
            name.as_str(),
            "x-codex-turn-state" | "x-codex-inference-call-id" | "traceparent" | "tracestate"
        ) && (header.value.len() > 8192
            || headers.insert(name, header.value.to_string()).is_some())
        {
            req.respond(Response::new_empty(StatusCode(400)))?;
            anyhow::bail!("oversized or duplicate routing/tracing header");
        }
    }
    let result = match config.identity_client.run_raw_response(
        &effective_identity.thread_id,
        request_value,
        headers,
    ) {
        Ok(result) => result,
        Err(error) => {
            req.respond(Response::new_empty(StatusCode(502)))?;
            return Err(error.context("calling app-server raw Responses"));
        }
    };
    let status_code = result.status;
    let status = StatusCode(status_code);
    let content_type = if body_contains_stream(&rewritten.body) {
        "text/event-stream"
    } else {
        "application/json"
    };
    let mut response_headers = vec![
        Header::from_bytes(b"content-type", content_type.as_bytes())
            .map_err(|_| anyhow!("invalid content-type header"))?,
    ];
    if let Some(state) = result.headers.get("x-codex-turn-state") {
        response_headers.push(
            Header::from_bytes(b"x-codex-turn-state", state.as_bytes())
                .map_err(|_| anyhow!("invalid upstream routing header"))?,
        );
    }
    let response_body = Cursor::new(result.body.into_bytes());
    let response_body: Box<dyn Read + Send> = if let Some(exchange_dump) = exchange_dump {
        let headers = reqwest::header::HeaderMap::new();
        Box::new(exchange_dump.tee_response_body(status_code, &headers, response_body))
    } else {
        Box::new(response_body)
    };
    let response = Response::new(
        status,
        std::mem::take(&mut response_headers),
        response_body,
        None,
        None,
    );
    req.respond(response)?;
    Ok(())
}

fn body_contains_stream(body: &[u8]) -> bool {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| value.get("stream").and_then(Value::as_bool))
        .unwrap_or(false)
}
