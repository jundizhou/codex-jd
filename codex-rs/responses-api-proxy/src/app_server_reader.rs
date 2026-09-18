//! Creates and reads model-request identities through the local app-server.
//!
//! This deliberately avoids taking a dependency on the full app-server client
//! crate. It only speaks the small JSON-RPC over a local WebSocket subset
//! needed by this proxy.

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use codex_uds::UnixStream;
use codex_utils_home_dir::find_codex_home;
use futures::SinkExt;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::mpsc as tokio_mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::conversations::Continuation;
use crate::conversations::Conversations;
use crate::identity::SessionIdentity;

#[derive(Debug, Deserialize)]
pub(super) struct RpcError {
    pub code: i64,
    pub message: String,
}
impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for RpcError {}

static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

const HANDSHAKE_URL: &str = "ws://localhost/rpc";
// Raw model responses can legitimately take several minutes for large contexts.
// The stream itself still has the app-server idle timeout; this control timeout
// only bounds a connection that stops producing a terminal RPC response.
pub(super) const RAW_RESPONSE_CONTROL_TIMEOUT: Duration = Duration::from_secs(3600);

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IdentityItem {
    thread_id: String,
    session_id: String,
    installation_id: String,
    window_id: String,
    parent_thread_id: Option<String>,
    turn_id: Option<String>,
    root_turn_id: Option<String>,
    parent_turn_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IdentityPage {
    data: Vec<IdentityItem>,
    next_cursor: Option<String>,
}

pub(crate) enum IdentityMode {
    Pool(usize),
    Durable(Conversations),
}

enum Command {
    RecoverConversation {
        key: String,
        response: mpsc::Sender<Result<()>>,
    },
    AcquireConversation {
        key: String,
        continuation: Continuation,
        response: mpsc::Sender<Result<SessionIdentity>>,
    },
    ReleaseConversation {
        identity: SessionIdentity,
        outcome: crate::conversations::Outcome,
        response: mpsc::Sender<Result<()>>,
    },
    ReadIdentity {
        thread_id: String,
        response: mpsc::Sender<Result<Option<SessionIdentity>, String>>,
    },
}

/// Owns the app-server connection and the currently loaded identity threads.
pub(crate) struct AppServerIdentityClient {
    command_tx: tokio_mpsc::UnboundedSender<Command>,
    socket: PathBuf,
    alive: Arc<AtomicBool>,
}

impl AppServerIdentityClient {
    pub(crate) fn recover_conversation(&self, key: String) -> Result<()> {
        let (response, receiver) = mpsc::channel();
        self.command_tx
            .send(Command::RecoverConversation { key, response })
            .context("identity worker stopped")?;
        receiver
            .recv()
            .context("identity worker stopped during recovery")?
    }

    pub(crate) fn is_available(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    pub(crate) fn respond_models(&self, req: tiny_http::Request) -> Result<()> {
        crate::models::respond(&self.socket, req)
    }

    pub(crate) fn start(
        socket: PathBuf,
        mode: IdentityMode,
    ) -> Result<(Self, Vec<SessionIdentity>)> {
        let (command_tx, command_rx) = tokio_mpsc::unbounded_channel();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let worker_socket = socket.clone();
        let alive = Arc::new(AtomicBool::new(true));
        let worker_alive = Arc::clone(&alive);
        std::thread::Builder::new()
            .name("responses-proxy-app-server".to_string())
            .spawn(move || {
                run_worker(worker_socket, command_rx, ready_tx, mode);
                worker_alive.store(false, Ordering::Release);
            })
            .context("failed to start app-server identity worker")?;

        let identities = ready_rx
            .recv()
            .context("app-server identity worker stopped during startup")?
            .map_err(anyhow::Error::msg)?;
        Ok((
            Self {
                command_tx,
                socket,
                alive,
            },
            identities,
        ))
    }

    pub(crate) fn acquire_conversation(
        &self,
        key: String,
        continuation: Continuation,
    ) -> Result<SessionIdentity> {
        let (response, receiver) = mpsc::channel();
        self.command_tx
            .send(Command::AcquireConversation {
                key,
                continuation,
                response,
            })
            .context("identity worker stopped")?;
        receiver
            .recv()
            .context("identity worker stopped before replying")?
    }

    pub(crate) fn release_conversation(
        &self,
        identity: SessionIdentity,
        outcome: crate::conversations::Outcome,
    ) -> Result<()> {
        let (response, receiver) = mpsc::channel();
        self.command_tx
            .send(Command::ReleaseConversation {
                identity,
                outcome,
                response,
            })
            .context("identity worker stopped")?;
        receiver
            .recv()
            .context("identity worker stopped before releasing conversation")?
    }

    pub(crate) fn read_identity(&self, thread_id: &str) -> Result<Option<SessionIdentity>> {
        let (response_tx, response_rx) = mpsc::channel();
        self.command_tx
            .send(Command::ReadIdentity {
                thread_id: thread_id.to_string(),
                response: response_tx,
            })
            .context("app-server identity worker is not running")?;
        response_rx
            .recv()
            .context("app-server identity worker stopped before replying")?
            .map_err(anyhow::Error::msg)
    }

    pub(crate) fn run_raw_response(
        &self,
        thread_id: &str,
        body: Value,
        headers: HashMap<String, String>,
        dispatch: Option<crate::scheduler::Dispatch>,
    ) -> Result<crate::raw_response_stream::RawResponseStream> {
        crate::raw_response_stream::start(&self.socket, thread_id, body, headers, dispatch)
    }
}

pub(crate) fn default_app_server_socket() -> Result<PathBuf> {
    let codex_home = find_codex_home().context("failed to resolve CODEX_HOME")?;
    Ok(codex_home
        .as_path()
        .join("app-server-control")
        .join("app-server-control.sock"))
}

pub(crate) fn resolve_socket_arg(value: Option<PathBuf>) -> Result<PathBuf> {
    match value {
        Some(path) => Ok(path),
        None => default_app_server_socket(),
    }
}

pub(super) fn request_id(prefix: &str) -> String {
    format!(
        "responses-proxy-{prefix}-{}-{}",
        std::process::id(),
        REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}

pub(super) async fn connect(socket: &Path) -> Result<WebSocketStream<UnixStream>> {
    let stream = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        UnixStream::connect(socket),
    )
    .await
    .with_context(|| {
        format!(
            "timed out connecting to app-server socket {}",
            socket.display()
        )
    })?
    .with_context(|| {
        format!(
            "failed to connect to app-server socket {}",
            socket.display()
        )
    })?;
    let request = HANDSHAKE_URL
        .into_client_request()
        .context("failed to build app-server websocket handshake")?;
    let (stream, _) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        tokio_tungstenite::client_async(request, stream),
    )
    .await
    .context("timed out upgrading app-server websocket")?
    .context("failed to upgrade app-server websocket")?;
    Ok(stream)
}

pub(super) async fn send_message<S>(stream: &mut WebSocketStream<S>, message: &Value) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    stream
        .send(Message::Text(serde_json::to_string(message)?.into()))
        .await
        .context("failed to write app-server JSON-RPC message")
}

pub(super) async fn recv_message<S>(stream: &mut WebSocketStream<S>) -> Result<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        match stream.next().await {
            Some(Ok(Message::Text(text))) => {
                return serde_json::from_str(&text).context("invalid app-server JSON-RPC message");
            }
            Some(Ok(Message::Ping(payload))) => {
                stream
                    .send(Message::Pong(payload))
                    .await
                    .context("failed to reply to app-server websocket ping")?;
            }
            Some(Ok(Message::Binary(_))) => {
                return Err(anyhow::anyhow!(
                    "unexpected binary app-server websocket message"
                ));
            }
            Some(Ok(Message::Close(_))) => {
                return Err(anyhow::anyhow!("app-server websocket closed"));
            }
            Some(Ok(_)) => continue,
            Some(Err(err)) => return Err(anyhow::anyhow!("app-server websocket error: {err}")),
            None => return Err(anyhow::anyhow!("app-server websocket closed")),
        }
    }
}

pub(super) async fn send_and_wait_for_response<S>(
    stream: &mut WebSocketStream<S>,
    method: &str,
    params: Value,
) -> Result<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    send_and_wait_for_response_with_timeout(stream, method, params, Duration::from_secs(30)).await
}

async fn send_and_wait_for_response_with_timeout<S>(
    stream: &mut WebSocketStream<S>,
    method: &str,
    params: Value,
    timeout: Duration,
) -> Result<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    tokio::time::timeout(timeout, async {
        let id = request_id(method);
        send_message(
            stream,
            &serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params,
            }),
        )
        .await?;
        loop {
            let message = recv_message(stream).await?;
            if message.get("id").and_then(Value::as_str) != Some(id.as_str()) {
                continue;
            }
            if let Some(error) = message.get("error") {
                return Err(serde_json::from_value::<RpcError>(error.clone())?.into());
            }
            return message
                .get("result")
                .cloned()
                .with_context(|| format!("app-server method {method} returned no result"));
        }
    })
    .await
    .with_context(|| format!("app-server method {method} timed out"))?
}

pub(super) async fn initialize<S>(stream: &mut WebSocketStream<S>) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let params = serde_json::json!({
        "clientInfo": {
            "name": "codex-responses-api-proxy",
            "version": env!("CARGO_PKG_VERSION"),
        },
        "capabilities": {
            "experimentalApi": true,
            "requestAttestation": false,
            "mcpServerOpenaiFormElicitation": false,
            "optOutNotificationMethods": [],
        },
    });
    send_and_wait_for_response(stream, "initialize", params).await?;
    send_message(
        stream,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "method": "initialized",
        }),
    )
    .await
}

async fn create_pool_threads<S>(
    stream: &mut WebSocketStream<S>,
    pool_size: usize,
) -> Result<Vec<String>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut thread_ids = Vec::with_capacity(pool_size);
    for _ in 0..pool_size {
        let result = send_and_wait_for_response(
            stream,
            "thread/start",
            serde_json::json!({
                "ephemeral": true,
                "environments": [],
            }),
        )
        .await?;
        let thread_id = result
            .pointer("/thread/id")
            .and_then(Value::as_str)
            .context("app-server thread/start returned no thread id")?;
        thread_ids.push(thread_id.to_string());
    }
    Ok(thread_ids)
}

pub(super) async fn load_identities<S>(
    stream: &mut WebSocketStream<S>,
) -> Result<Vec<SessionIdentity>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut items = Vec::new();
    let mut cursor = None;
    let mut seen_cursors = std::collections::HashSet::new();
    loop {
        let result = send_and_wait_for_response(
            stream,
            "thread/modelIdentity/list",
            serde_json::json!({ "cursor": cursor, "limit": 100 }),
        )
        .await?;
        let page: IdentityPage = serde_json::from_value(result)
            .context("app-server thread/modelIdentity/list returned an unexpected response")?;
        items.extend(page.data);
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        anyhow::ensure!(
            seen_cursors.insert(next_cursor.clone()) && seen_cursors.len() < 100,
            "app-server identity pagination repeated a cursor or exceeded 100 pages"
        );
        cursor = Some(next_cursor);
    }
    Ok(items
        .into_iter()
        .map(|item| SessionIdentity {
            installation_id: item.installation_id,
            session_id: item.session_id,
            thread_id: item.thread_id,
            window_id: item.window_id,
            parent_thread_id: item.parent_thread_id,
            turn_id: item.turn_id,
            root_turn_id: item.root_turn_id,
            parent_turn_id: item.parent_turn_id,
        })
        .collect())
}

fn select_pool_identities(
    thread_ids: &[String],
    identities: Vec<SessionIdentity>,
) -> Result<Vec<SessionIdentity>> {
    let mut by_thread_id: HashMap<_, _> = identities
        .into_iter()
        .map(|identity| (identity.thread_id.clone(), identity))
        .collect();
    thread_ids
        .iter()
        .map(|thread_id| {
            by_thread_id.remove(thread_id).with_context(|| {
                format!("app-server did not return identity for thread {thread_id}")
            })
        })
        .collect()
}

fn run_worker(
    socket: PathBuf,
    mut command_rx: tokio_mpsc::UnboundedReceiver<Command>,
    ready_tx: mpsc::SyncSender<Result<Vec<SessionIdentity>, String>>,
    mut mode: IdentityMode,
) {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = ready_tx.send(Err(format!("failed to start tokio runtime: {error}")));
            return;
        }
    };
    runtime.block_on(async move {
        let mut stream = match connect(&socket).await {
            Ok(stream) => stream,
            Err(error) => {
                let _ = ready_tx.send(Err(format!("{error:#}")));
                return;
            }
        };
        let setup = async {
            initialize(&mut stream).await?;
            match &mode {
                IdentityMode::Pool(size) => {
                    let thread_ids = create_pool_threads(&mut stream, *size).await?;
                    select_pool_identities(&thread_ids, load_identities(&mut stream).await?)
                }
                IdentityMode::Durable(_) => Ok(Vec::new()),
            }
        }
        .await;
        let identities = match setup {
            Ok(identities) => identities,
            Err(error) => {
                let _ = ready_tx.send(Err(format!("{error:#}")));
                return;
            }
        };
        if ready_tx.send(Ok(identities)).is_err() {
            return;
        }

        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            let command = tokio::select! {
                command = command_rx.recv() => match command { Some(command) => command, None => break },
                _ = tick.tick() => {
                    if let IdentityMode::Durable(conversations) = &mut mode
                        && let Err(error) = conversations.retire(&mut stream).await {
                        eprintln!("conversation retirement failed: {error:#}");
                        break;
                    }
                    continue;
                }
            };
            match command {
                Command::RecoverConversation { key, response } => {
                    let result = match &mut mode {
                        IdentityMode::Durable(conversations) => conversations.recover(&socket, &mut stream, &key).await,
                        IdentityMode::Pool(_) => Err(anyhow::anyhow!("durable conversations disabled")),
                    };
                    let _ = response.send(result);
                }
                Command::AcquireConversation { key, continuation, response } => {
                    let result = match &mut mode {
                        IdentityMode::Durable(conversations) => conversations.acquire(&mut stream, key, continuation).await,
                        IdentityMode::Pool(_) => Err(anyhow::anyhow!("durable conversations disabled")),
                    };
                    let fatal = result.as_ref().is_err_and(|error| error.downcast_ref::<crate::scheduler::Rejection>().is_none());
                    let _ = response.send(result);
                    if fatal { break; }
                }
                Command::ReleaseConversation { identity, outcome, response } => {
                    let result = match &mut mode {
                        IdentityMode::Durable(conversations) => conversations.release(&identity.thread_id, outcome),
                        IdentityMode::Pool(_) => Err(anyhow::anyhow!("durable conversations disabled")),
                    };
                    let failed = result.is_err();
                    let _ = response.send(result);
                    if failed { break; }
                }
                Command::ReadIdentity {
                    thread_id,
                    response,
                } => {
                    let result = load_identities(&mut stream)
                        .await
                        .map(|identities| {
                            identities
                                .into_iter()
                                .find(|identity| identity.thread_id == thread_id)
                        })
                        .map_err(|error| format!("{error:#}"));
                    let failed = result.is_err();
                    let _ = response.send(result);
                    if failed {
                        break;
                    }
                }
            }
        }
    });
}

#[cfg(test)]
#[path = "app_server_reader_tests.rs"]
mod wire_tests;

#[cfg(test)]
mod tests {
    use super::select_pool_identities;
    use crate::identity::SessionIdentity;

    use pretty_assertions::assert_eq;

    fn identity(thread_id: &str) -> SessionIdentity {
        SessionIdentity {
            installation_id: "install".to_string(),
            session_id: thread_id.to_string(),
            thread_id: thread_id.to_string(),
            window_id: format!("{thread_id}:0"),
            parent_thread_id: None,
            turn_id: None,
            root_turn_id: None,
            parent_turn_id: None,
        }
    }

    #[test]
    fn selects_created_threads_in_creation_order() {
        let selected = select_pool_identities(
            &["thread-b".to_string(), "thread-a".to_string()],
            vec![
                identity("thread-a"),
                identity("unrelated"),
                identity("thread-b"),
            ],
        )
        .expect("select identities");

        assert_eq!(selected, vec![identity("thread-b"), identity("thread-a")]);
    }
}
