use std::fs::File;
use std::fs::{self};
use std::io::Read;
use std::io::Write;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::monitored_request::Request;
use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use serde::Serialize;
use serde_json::Value;
use tiny_http::Header;
use tiny_http::Method;
use tiny_http::Response;
use tiny_http::Server;
use tiny_http::StatusCode;

mod affinity;
mod app_server_reader;
mod args;
pub use args::Args;
mod account_rotation;
mod account_switch;
mod admin_accounts;
mod admin_login;
mod admin_usage;
mod auth;
mod continuation_index;
mod conversations;
mod dump;
mod identity;
mod metadata_profiles;
mod models;
mod monitored_request;
mod queue_http;
mod queue_signals;
mod queue_store;
mod queue_throttle;
mod quota_cache;
mod raw_response_stream;
mod request_metrics;
mod rewrite;
mod scheduler;
mod sdk_compat;
mod session_pool;
mod stream_http;
use affinity::key_for_http_request;
use app_server_reader::AppServerIdentityClient;
use app_server_reader::IdentityMode;
use app_server_reader::resolve_socket_arg;
use dump::ExchangeDumper;
use identity::SessionIdentity;
use rewrite::rewrite_request;
use session_pool::SessionPool;

#[derive(Serialize)]
struct ServerInfo {
    port: u16,
    pid: u32,
}

struct ForwardConfig {
    continuations: Option<Arc<std::sync::Mutex<continuation_index::Index>>>,
    metadata_profiles: Option<metadata_profiles::Profiles>,
    identity_client: Arc<AppServerIdentityClient>,
    worker_api_key: Option<String>,
    queue: Option<Arc<scheduler::Scheduler>>,
    account_label: String,
    admin_capacity: usize,
    profile_dir: Option<std::path::PathBuf>,
    auth_path: std::path::PathBuf,
}

static PROXY_REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn resolve_session_pool_size(cli_value: Option<usize>, default_size: usize) -> Result<usize> {
    let size = match cli_value {
        Some(size) => size,
        None => match std::env::var("CODEX_SESSION_POOL_SIZE") {
            Ok(raw) => raw
                .trim()
                .parse::<usize>()
                .with_context(|| format!("invalid CODEX_SESSION_POOL_SIZE: {raw}"))?,
            Err(_) => default_size,
        },
    };
    anyhow::ensure!(size >= 1, "session pool size must be at least 1");
    Ok(size)
}

fn configured_account_label() -> String {
    let Ok(home) = codex_utils_home_dir::find_codex_home() else {
        return "configured account".to_string();
    };
    let path = home.as_path().join("auth.json");
    let Ok(contents) = fs::read_to_string(path) else {
        return "configured account".to_string();
    };
    let Ok(value) = serde_json::from_str::<Value>(&contents) else {
        return "configured account".to_string();
    };
    let Some(account) = value
        .pointer("/tokens/account_id")
        .and_then(Value::as_str)
        .filter(|account| !account.is_empty())
    else {
        return "configured account".to_string();
    };
    admin_accounts::masked_account(account)
}

/// Entry point for the library main, for parity with other crates.
pub fn run_main(args: Args) -> Result<()> {
    std::sync::LazyLock::force(&request_metrics::METRICS);
    let default_size = if args.queue {
        32
    } else {
        session_pool::DEFAULT_SESSION_POOL_SIZE
    };
    let session_pool_size = resolve_session_pool_size(args.session_pool_size, default_size)?;
    anyhow::ensure!(
        !args.queue || session_pool_size <= 256,
        "queue mode supports at most 256 identities"
    );
    anyhow::ensure!(
        !args.queue || usize::from(args.queue_max_running) <= session_pool_size,
        "queue concurrency exceeds available session identities"
    );
    let auth_path = codex_utils_home_dir::find_codex_home()?
        .as_path()
        .join("auth.json");
    let admin_dir = std::env::var_os("CODEX_ACCOUNT_PROFILES_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| auth_path.with_file_name("accounts"));
    let admin = admin_accounts::Store::new(&admin_dir, &auth_path, session_pool_size);
    let maximum = admin.limit(usize::from(args.queue_max_running))?;
    let worker_api_key = args
        .worker_api_key
        .or_else(|| std::env::var("CODEX_WORKER_API_KEY").ok());
    anyhow::ensure!(
        !args.queue || worker_api_key.as_ref().is_some_and(|key| !key.is_empty()),
        "queue mode requires a worker API key"
    );
    let queue_path = if args.queue {
        Some(
            args.queue_state.clone().unwrap_or(
                codex_utils_home_dir::find_codex_home()?
                    .as_path()
                    .join("responses-queue.json"),
            ),
        )
    } else {
        None
    };
    let journal = queue_store::Journal::open(queue_path.as_deref())?;
    let mode = match &queue_path {
        Some(path) => IdentityMode::Durable(conversations::Conversations::open(
            path.with_extension("conversations.json"),
            codex_utils_home_dir::find_codex_home()?
                .as_path()
                .join("auth.json"),
            session_pool_size,
            std::time::Duration::from_secs(args.queue_idle_ttl_secs),
            &journal.unknown_conversations(),
        )?),
        None => IdentityMode::Pool(session_pool_size),
    };
    let continuations = match &mode {
        IdentityMode::Durable(conversations) => Some(Arc::clone(&conversations.continuations)),
        IdentityMode::Pool(_) => None,
    };
    let app_server_socket = resolve_socket_arg(args.app_server_socket.clone())?;
    let (identity_client, identities) = AppServerIdentityClient::start(app_server_socket, mode)
        .context("starting proxy identity manager")?;
    let queue = if args.queue {
        Some(scheduler::Scheduler::new(
            scheduler::Config {
                automatic_recovery: args.queue_auto_recover,
                max_running: maximum,
                gap: std::time::Duration::from_millis(args.queue_conversation_gap_ms),
                user_gap: std::time::Duration::from_millis(args.queue_user_gap_ms),
                tool_gap: std::time::Duration::from_millis(args.queue_tool_gap_ms),
                idle_ttl: std::time::Duration::from_secs(args.queue_idle_ttl_secs),
                start_gap: std::time::Duration::from_millis(args.queue_start_gap_ms),
                timeout: scheduler::MAX_QUEUE_WAIT,
            },
            journal,
        ))
    } else {
        None
    };
    let session_pool = if args.queue {
        None
    } else {
        Some(Arc::new(SessionPool::new(identities)?))
    };
    let identity_client = Arc::new(identity_client);
    if args.queue_auto_recover
        && let Some(queue) = &queue
    {
        queue.start_recovery(&identity_client)?;
    }

    if let Some(queue) = &queue {
        account_rotation::start(
            Arc::clone(queue),
            Arc::clone(&identity_client),
            admin_dir.clone(),
            auth_path.clone(),
            session_pool_size,
        )?;
    }
    let metadata_profiles = std::env::var_os("CODEX_METADATA_PROFILES")
        .map(|path| metadata_profiles::Profiles::open(Path::new(&path)))
        .transpose()
        .context("loading persistent metadata profiles")?;
    let forward_config = Arc::new(ForwardConfig {
        continuations,
        metadata_profiles,
        identity_client,
        worker_api_key,
        queue,
        account_label: configured_account_label(),
        profile_dir: Some(admin_dir),
        auth_path,
        admin_capacity: session_pool_size,
    });
    let dump_dir = args
        .dump_dir
        .map(ExchangeDumper::new)
        .transpose()
        .context("creating --dump-dir")?
        .map(Arc::new);

    let (listener, bound_addr) = bind_listener(args.listen_address, args.port)?;
    if let Some(path) = args.server_info.as_ref() {
        write_server_info(path, bound_addr.port())?;
    }
    let server = Server::from_listener(listener, None)
        .map_err(|err| anyhow!("creating HTTP server: {err}"))?;
    eprintln!("responses-api-proxy listening on {bound_addr}");

    let http_shutdown = args.http_shutdown;
    for request in server.incoming_requests() {
        let request = Request::from(request);
        let received = Instant::now();
        let (request, admission) = if let Some(queue) = &forward_config.queue {
            if request.method() == &Method::Get && request.url() == "/healthz" {
                let _ = request.respond(Response::from_string("ok\n"));
                continue;
            }
            if request.method() == &Method::Get && request.url() == "/admin/accounts" {
                let _ = queue_http::control(
                    queue,
                    &forward_config.identity_client,
                    request,
                    &forward_config.account_label,
                    forward_config.profile_dir.as_deref(),
                    &forward_config.auth_path,
                    forward_config.admin_capacity,
                );
                continue;
            }
            if !auth::is_authorized(request.headers(), forward_config.worker_api_key.as_deref()) {
                let _ = request.respond(Response::new_empty(StatusCode(401)));
                continue;
            }
            if request.url() == "/readyz" && !forward_config.identity_client.is_available() {
                queue_http::error(request, scheduler::Rejection::IdentityUnavailable);
                continue;
            }
            if request.method() == &Method::Post && request.url().starts_with("/admin/") {
                let config = Arc::clone(&forward_config);
                std::thread::spawn(move || {
                    if let Some(queue) = &config.queue {
                        queue_http::control(
                            queue,
                            &config.identity_client,
                            request,
                            &config.account_label,
                            config.profile_dir.as_deref(),
                            &config.auth_path,
                            config.admin_capacity,
                        );
                    }
                });
                continue;
            }
            let Some(request) = queue_http::control(
                queue,
                &forward_config.identity_client,
                request,
                &forward_config.account_label,
                forward_config.profile_dir.as_deref(),
                &forward_config.auth_path,
                forward_config.admin_capacity,
            ) else {
                continue;
            };
            match queue_http::admission(queue, &request) {
                Ok(admission) => (request, Some(admission)),
                Err(error) => {
                    queue_http::error(request, error);
                    continue;
                }
            }
        } else {
            (request, None)
        };
        let forward_config = forward_config.clone();
        let dump_dir = dump_dir.clone();
        let session_pool = session_pool.clone();
        std::thread::spawn(move || {
            let _admission = admission;
            if http_shutdown && request.method() == &Method::Get && request.url() == "/shutdown" {
                if auth::is_authorized(request.headers(), forward_config.worker_api_key.as_deref())
                {
                    let _ = request.respond(Response::new_empty(StatusCode(200)));
                    std::process::exit(0);
                }
                let response = Response::from_string("unauthorized\n")
                    .with_status_code(StatusCode(401))
                    .with_header(
                        Header::from_bytes(b"www-authenticate", b"Bearer").unwrap_or_else(|_| {
                            unreachable!("static WWW-Authenticate header is valid")
                        }),
                    );
                let _ = request.respond(response);
                return;
            }

            if let Err(e) = forward_request(
                &forward_config,
                dump_dir.as_deref(),
                session_pool.as_deref(),
                request,
                received,
            ) {
                eprintln!("forwarding error: {e:#}");
            }
        });
    }

    Err(anyhow!("server stopped unexpectedly"))
}

fn bind_listener(listen_address: IpAddr, port: Option<u16>) -> Result<(TcpListener, SocketAddr)> {
    let addr = SocketAddr::new(listen_address, port.unwrap_or(0));
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
    session_pool: Option<&SessionPool>,
    mut req: Request,
    received: Instant,
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
    if method == Method::Get && url_path == "/healthz" {
        req.respond(Response::from_string("ok\n").with_status_code(StatusCode(200)))?;
        return Ok(());
    }
    if !auth::is_authorized(req.headers(), config.worker_api_key.as_deref()) {
        let response = Response::from_string("unauthorized\n")
            .with_status_code(StatusCode(401))
            .with_header(
                Header::from_bytes(b"www-authenticate", b"Bearer")
                    .map_err(|_| anyhow!("invalid WWW-Authenticate header"))?,
            );
        req.respond(response)?;
        return Ok(());
    }
    if method == Method::Get && (url_path == "/v1/models" || url_path.starts_with("/v1/models/")) {
        return config.identity_client.respond_models(req);
    }
    if method != Method::Post || url_path != "/v1/responses" {
        let _ = req.respond(Response::new_empty(StatusCode(403)));
        return Ok(());
    }

    let mut body = Vec::new();
    if let Some(queue) = &config.queue {
        let reserved = req.body_length().unwrap_or(scheduler::MAX_BODY);
        req.as_reader()
            .take((reserved + 1) as u64)
            .read_to_end(&mut body)?;
        req.capture_body(&body);
        if body.len() > reserved {
            queue_http::error(req, scheduler::Rejection::TooLarge);
            return Ok(());
        }
        let fallback = format!(
            "{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos(),
            PROXY_REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        let mut pending = match queue_http::pending(&req, &body, received, fallback) {
            Ok(pending) => pending,
            Err(error) => {
                req.respond(Response::new_empty(StatusCode(400)))?;
                return Err(error);
            }
        };
        let request_body: Value = serde_json::from_slice(&body)?;
        let account = match conversations::account(&config.auth_path) {
            Ok(account) => account,
            Err(error) => {
                queue_http::error(req, scheduler::Rejection::IdentityUnavailable);
                return Err(error);
            }
        };
        let mut matched = false;
        if !pending.sticky
            && let Some(index) = &config.continuations
        {
            let resolved = index
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .resolve(&pending.key.0, &account, &request_body);
            match resolved {
                Ok(Some(conversation)) => {
                    pending.key.1 = conversation;
                    pending.sticky = true;
                    matched = true;
                }
                Ok(None) => {}
                Err(error) => {
                    queue_http::error(req, error);
                    return Ok(());
                }
            }
            // Generated conversation keys also retain scheduling signals for
            // the first automatically matched continuation.
            pending.sticky = true;
        }
        let key = pending.key.digest();
        let routing = req
            .headers()
            .iter()
            .find(|header| header.field.equiv("x-codex-turn-state"))
            .map(|header| header.value.as_str());
        let continuation = if matched {
            conversations::Continuation::RequiresIdentity
        } else {
            conversations::Continuation::from_request(&request_body, routing)
        };
        let lease = match queue.acquire(pending) {
            Ok(lease) => lease,
            Err(error) => {
                queue_http::error(req, error);
                return Ok(());
            }
        };
        if matched && conversations::account(&config.auth_path)? != account {
            queue_http::error(req, scheduler::Rejection::BindingLost);
            return Ok(());
        }
        let identity = match config
            .identity_client
            .acquire_conversation(key, continuation)
        {
            Ok(identity) => identity,
            Err(error) => {
                let rejection = error
                    .downcast_ref::<scheduler::Rejection>()
                    .copied()
                    .unwrap_or(scheduler::Rejection::IdentityUnavailable);
                queue_http::error(req, rejection);
                return Err(error.context("acquiring durable conversation"));
            }
        };
        let result =
            forward_request_with_identity(config, dump_dir, &identity, req, body, Some(&lease));
        lease.settle();
        config
            .identity_client
            .release_conversation(identity, lease.identity_outcome())?;
        return result;
    }
    req.as_reader().read_to_end(&mut body)?;
    req.capture_body(&body);
    let affinity_key = match key_for_http_request(&body, req.headers()) {
        Ok(key) => key,
        Err(error) => {
            req.respond(Response::new_empty(StatusCode(400)))?;
            return Err(error);
        }
    };
    let session_pool = session_pool.context("nonqueue session pool missing")?;
    let identity = session_pool.acquire(affinity_key.as_deref());
    let result =
        forward_request_with_identity(config, dump_dir, &identity, req, body, /*lease*/ None);
    session_pool.release(identity);
    result
}

fn forward_request_with_identity(
    config: &ForwardConfig,
    dump_dir: Option<&ExchangeDumper>,
    identity: &SessionIdentity,
    mut req: Request,
    body: Vec<u8>,
    lease: Option<&scheduler::Lease>,
) -> Result<()> {
    let request_id = PROXY_REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let started_at = Instant::now();
    let effective_identity = match config.identity_client.read_identity(&identity.thread_id) {
        Ok(Some(identity)) => identity,
        Ok(None) => {
            if lease.is_some() {
                queue_http::error(req, scheduler::Rejection::IdentityUnavailable);
            } else {
                req.respond(Response::new_empty(StatusCode(503)))?;
            }
            anyhow::bail!("assigned app-server thread is no longer loaded");
        }
        Err(error) => {
            if lease.is_some() {
                queue_http::error(req, scheduler::Rejection::IdentityUnavailable);
            } else {
                req.respond(Response::new_empty(StatusCode(503)))?;
            }
            return Err(error.context("refreshing assigned app-server thread identity"));
        }
    };
    let method = req.method().clone();
    let url_path = req.url().to_string();
    eprintln!(
        "responses-proxy request_start id={request_id} thread_id={} body_bytes={}",
        effective_identity.thread_id,
        body.len()
    );
    let exchange_dump = dump_dir.and_then(|dump_dir| {
        dump_dir
            .dump_request(&method, &url_path, req.headers(), &body)
            .map_err(|err| {
                eprintln!("responses-api-proxy failed to dump request: {err}");
                err
            })
            .ok()
    });
    let headers = match codex_http_client::raw_responses_headers(
        req.headers()
            .iter()
            .map(|header| (header.field.as_str().as_str(), header.value.as_str())),
    ) {
        Ok(headers) => headers
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_owned(),
                    value.to_str().unwrap_or_default().to_owned(),
                )
            })
            .collect(),
        Err(error) => {
            req.respond(Response::new_empty(StatusCode(400)))?;
            anyhow::bail!(error);
        }
    };
    let mut rewritten = match rewrite_request(
        &body,
        headers,
        &effective_identity,
        config.metadata_profiles.as_ref(),
    ) {
        Ok(rewritten) => rewritten,
        Err(error) => {
            req.respond(
                Response::from_string(format!("invalid metadata: {error}\n"))
                    .with_status_code(StatusCode(400)),
            )?;
            return Err(error.context("rewriting request metadata"));
        }
    };
    let compatibility = match sdk_compat::Compatibility::prepare(&mut rewritten.body) {
        Ok(compatibility) => compatibility,
        Err(error) => {
            req.respond(
                Response::from_string(
                    serde_json::json!({"error": {
                        "type": "invalid_request_error", "message": error.to_string()
                    }})
                    .to_string(),
                )
                .with_header(
                    Header::from_bytes("content-type", "application/json")
                        .map_err(|()| std::io::Error::other("invalid JSON content type"))?,
                )
                .with_status_code(StatusCode(400)),
            )?;
            return Err(error.context("preparing SDK request"));
        }
    };
    let capture_path = std::env::var_os("CODEX_HTTP_CAPTURE_DIR")
        .filter(|_| {
            effective_identity.thread_id.len() <= 64
                && effective_identity
                    .thread_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
        .map(|root| {
            std::path::PathBuf::from(root).join(format!("{}.json", effective_identity.thread_id))
        });
    if let Some(path) = &capture_path {
        let _ = std::fs::remove_file(path);
    }
    let quota_key = admin_accounts::read_auth(&config.auth_path)
        .ok()
        .map(|auth| quota_cache::key(&auth));
    let recorder = match (&config.continuations, lease) {
        (Some(index), Some(lease)) => Some(continuation_index::Recorder {
            index: Arc::clone(index),
            account: conversations::account(&config.auth_path)?,
            key: lease.key.clone(),
        }),
        _ => None,
    };
    let raw_result = config.identity_client.run_raw_response(
        &effective_identity.thread_id,
        rewritten.body,
        rewritten.headers,
        lease.map(|lease| lease.dispatch.clone()),
        recorder,
    );
    if let Some(path) = &capture_path {
        req.capture_upstream(path);
    }
    let result = match raw_result {
        Ok(result) => result,
        Err(error) => {
            if let Some(rejection) = error.downcast_ref::<scheduler::Rejection>() {
                queue_http::error(req, *rejection);
                return Err(error);
            }
            req.respond(Response::new_empty(StatusCode(502)))?;
            return Err(error.context("calling app-server raw Responses"));
        }
    };
    let status_code = result.status;
    if let Some(key) = quota_key {
        quota_cache::CACHE.observe(&key, &result.headers, queue_store::now());
    }
    let status = StatusCode(status_code);
    let content_type = result
        .headers
        .get("content-type")
        .map(String::as_str)
        .unwrap_or_else(|| {
            if status_code < 400 {
                "text/event-stream"
            } else {
                "application/json"
            }
        });
    let mut response_headers = vec![
        Header::from_bytes(b"content-type", content_type.as_bytes())
            .map_err(|_| anyhow!("invalid content-type header"))?,
    ];
    for name in ["x-codex-turn-state", "retry-after"] {
        if let Some(state) = result.headers.get(name) {
            response_headers.push(
                Header::from_bytes(name.as_bytes(), state.as_bytes())
                    .map_err(|_| anyhow!("invalid upstream routing header"))?,
            );
        }
    }
    let response_body = result.body;
    let response_body: Box<dyn Read + Send> = if let Some(exchange_dump) = exchange_dump {
        let headers = reqwest::header::HeaderMap::new();
        Box::new(exchange_dump.tee_response_body(status_code, &headers, response_body))
    } else {
        Box::new(response_body)
    };
    compatibility.respond(req, status, response_headers, response_body)?;
    eprintln!(
        "responses-proxy request_end id={request_id} thread_id={} raw_calls=1 status={} elapsed_ms={}",
        effective_identity.thread_id,
        status_code,
        started_at.elapsed().as_millis()
    );
    Ok(())
}
