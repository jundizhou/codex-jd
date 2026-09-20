//! Bounded byte transport from app-server notifications to the HTTP response.
use crate::app_server_reader;
use anyhow::Context;
use anyhow::Result;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::io;
use std::io::Cursor;
use std::io::Read;
use std::path::Path;
use std::sync::mpsc;
use tokio_tungstenite::WebSocketStream;

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum Event {
    Started {
        status: u16,
        headers: HashMap<String, String>,
    },
    Chunk {
        data: Vec<u8>,
    },
    Completed,
}

#[derive(Debug)]
enum Failure {
    Queue(crate::scheduler::Rejection),
    Transport(String),
}

pub(crate) struct RawResponseStream {
    pub(crate) status: u16,
    pub(crate) headers: HashMap<String, String>,
    pub(crate) body: StreamBody,
}

pub(crate) struct StreamBody {
    receiver: mpsc::Receiver<Result<Event, Failure>>,
    pending: Cursor<Vec<u8>>,
    complete: bool,
}

impl Read for StreamBody {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            let n = self.pending.read(buf)?;
            if n > 0 || self.complete {
                return Ok(n);
            }
            match self.receiver.recv() {
                Ok(Ok(Event::Chunk { data })) => self.pending = Cursor::new(data),
                Ok(Ok(Event::Completed)) => self.complete = true,
                Ok(Ok(Event::Started { .. })) => {
                    return Err(io::Error::other("duplicate raw response headers"));
                }
                Ok(Err(error)) => return Err(io::Error::other(format!("{error:?}"))),
                Err(error) => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, error)),
            }
        }
    }
}

pub(crate) fn start(
    socket: &Path,
    thread_id: &str,
    body: Value,
    headers: HashMap<String, String>,
    dispatch: Option<crate::scheduler::Dispatch>,
) -> Result<RawResponseStream> {
    let socket = socket.to_path_buf();
    let thread_id = thread_id.to_string();
    // At most four 16 KiB chunks can be queued; detached readers still allow draining.
    let (sender, receiver) = mpsc::sync_channel(4);
    std::thread::Builder::new()
        .name("responses-proxy-stream".to_string())
        .spawn(move || {
            let result = (|| -> Result<()> {
                let runtime = tokio::runtime::Runtime::new()?;
                runtime.block_on(async {
                    let mut stream = app_server_reader::connect(&socket).await?;
                    app_server_reader::initialize(&mut stream).await?;
                    relay(
                        &mut stream,
                        serde_json::json!({
                            "threadId": thread_id, "input": [], "rawResponses": body,
                            "rawResponsesHeaders": headers, "rawResponsesStream": true,
                        }),
                        &sender,
                        dispatch.as_ref(),
                    )
                    .await
                })
            })();
            if let Err(error) = result {
                if let Some(dispatch) = &dispatch {
                    dispatch.unconfirmed();
                }
                let failure = error
                    .downcast_ref::<crate::scheduler::Rejection>()
                    .copied()
                    .map(Failure::Queue)
                    .unwrap_or_else(|| Failure::Transport(format!("{error:#}")));
                let _ = sender.try_send(Err(failure));
            }
        })?;
    match receiver
        .recv()
        .context("raw response worker stopped")?
        .map_err(|failure| match failure {
            Failure::Queue(error) => anyhow::Error::new(error),
            Failure::Transport(message) => anyhow::Error::msg(message),
        })? {
        Event::Started { status, headers } => Ok(RawResponseStream {
            status,
            headers,
            body: StreamBody {
                receiver,
                pending: Cursor::new(Vec::new()),
                complete: false,
            },
        }),
        Event::Chunk { .. } | Event::Completed => anyhow::bail!("raw response missing headers"),
    }
}

async fn relay<S>(
    stream: &mut WebSocketStream<S>,
    params: Value,
    sender: &mpsc::SyncSender<Result<Event, Failure>>,
    dispatch: Option<&crate::scheduler::Dispatch>,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let id = app_server_reader::request_id("turn/start");
    let streaming = params["rawResponses"]["stream"].as_bool().unwrap_or(false);
    let mut observer = crate::queue_signals::Observer::new(streaming);
    let mut status = 0;
    if let Some(dispatch) = dispatch {
        dispatch.start()?;
    }
    app_server_reader::send_message(
        stream,
        &serde_json::json!({
            "jsonrpc": "2.0", "id": id, "method": "turn/start", "params": params,
        }),
    )
    .await?;
    let result = tokio::time::timeout(app_server_reader::RAW_RESPONSE_CONTROL_TIMEOUT, async {
        loop {
            if dispatch
                .and_then(crate::scheduler::Dispatch::finalizing_deadline)
                .is_some_and(|deadline| std::time::Instant::now() >= deadline)
            {
                anyhow::bail!(
                    "raw response finalization deadline exceeded; upstream outcome unknown"
                );
            }
            let message = match tokio::time::timeout(
                std::time::Duration::from_millis(100),
                app_server_reader::recv_message(stream),
            )
            .await
            {
                Ok(message) => message?,
                Err(_) => continue,
            };
            if message.get("id").and_then(Value::as_str) == Some(&id) {
                if (message.get("error").is_some() || message.get("result").is_some())
                    && let Some(dispatch) = dispatch
                {
                    dispatch.local_finished();
                }
                if let Some(error) = message.get("error") {
                    anyhow::bail!("app-server raw Responses failed: {error}");
                }
                anyhow::ensure!(
                    message.get("result").is_some(),
                    "raw response missing result"
                );
                return Ok(());
            }
            if message.get("method").and_then(Value::as_str) == Some("rawResponse/stream")
                && message.pointer("/params/requestId").and_then(Value::as_str) == Some(&id)
            {
                let event: Event = serde_json::from_value(message["params"]["event"].clone())?;
                match &event {
                    Event::Started {
                        status: code,
                        headers,
                    } => {
                        status = *code;
                        let is_stream = headers
                            .get("content-type")
                            .map_or(streaming && status < 400, |value| {
                                value.contains("text/event-stream")
                            });
                        observer = crate::queue_signals::Observer::new(is_stream);
                        if let Some(dispatch) = dispatch
                            && status >= 400
                        {
                            dispatch.feedback(status, headers);
                        }
                    }
                    Event::Chunk { data } => observer.bytes(data),
                    Event::Completed => {}
                }
                emit(sender, event, dispatch).await?;
            }
        }
    })
    .await
    .context("app-server raw Responses control timeout")
    .and_then(std::convert::identity);
    if let Some(dispatch) = dispatch {
        let mut observation = observer.finish();
        // A model terminal remains authoritative even if the trailing RPC fails.
        // Non-success HTTP responses require a complete body, not just headers.
        if observation.terminal || (result.is_ok() && status >= 300) {
            if !(200..300).contains(&status) {
                observation.successful = false;
                observation.rate_limited = false;
            }
            dispatch.confirmed(observation);
        } else if result.is_ok() {
            anyhow::bail!("raw response ended without a model terminal event; outcome unknown");
        }
    }
    result?;
    emit(sender, Event::Completed, dispatch).await
}

// After downstream loss, drain within the original execution deadline. The HTTP
// handler retains its lease until this observer confirms completion or fails.
async fn emit(
    sender: &mpsc::SyncSender<Result<Event, Failure>>,
    event: Event,
    dispatch: Option<&crate::scheduler::Dispatch>,
) -> Result<()> {
    let mut pending = Ok(event);
    loop {
        match sender.try_send(pending) {
            Ok(()) => return Ok(()),
            Err(mpsc::TrySendError::Disconnected(_)) => {
                if let Some(dispatch) = dispatch {
                    dispatch.finalize();
                    return Ok(());
                }
                anyhow::bail!("raw response reader disconnected");
            }
            Err(mpsc::TrySendError::Full(event)) => pending = event,
        }
        if dispatch
            .and_then(crate::scheduler::Dispatch::finalizing_deadline)
            .is_some_and(|deadline| std::time::Instant::now() >= deadline)
        {
            anyhow::bail!("raw response finalization deadline exceeded");
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

#[cfg(test)]
#[path = "raw_response_stream_tests.rs"]
mod tests;
