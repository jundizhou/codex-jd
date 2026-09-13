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

pub(crate) struct RawResponseStream {
    pub(crate) status: u16,
    pub(crate) headers: HashMap<String, String>,
    pub(crate) body: StreamBody,
}

pub(crate) struct StreamBody {
    receiver: mpsc::Receiver<Result<Event, String>>,
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
                Ok(Err(error)) => return Err(io::Error::other(error)),
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
) -> Result<RawResponseStream> {
    let socket = socket.to_path_buf();
    let thread_id = thread_id.to_string();
    // At most four 16 KiB chunks can be queued; a disconnected reader cancels the RPC.
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
                    )
                    .await
                })
            })();
            if let Err(error) = result {
                let _ = sender.send(Err(format!("{error:#}")));
            }
        })?;
    match receiver
        .recv()
        .context("raw response worker stopped")?
        .map_err(anyhow::Error::msg)?
    {
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
    sender: &mpsc::SyncSender<Result<Event, String>>,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let id = app_server_reader::request_id("turn/start");
    app_server_reader::send_message(
        stream,
        &serde_json::json!({
            "jsonrpc": "2.0", "id": id, "method": "turn/start", "params": params,
        }),
    )
    .await?;
    tokio::time::timeout(app_server_reader::RAW_RESPONSE_CONTROL_TIMEOUT, async {
        loop {
            let message = app_server_reader::recv_message(stream).await?;
            if message.get("id").and_then(Value::as_str) == Some(&id) {
                if let Some(error) = message.get("error") {
                    anyhow::bail!("app-server raw Responses failed: {error}");
                }
                anyhow::ensure!(
                    message.get("result").is_some(),
                    "raw response missing result"
                );
                sender.send(Ok(Event::Completed))?;
                return Ok(());
            }
            if message.get("method").and_then(Value::as_str) == Some("rawResponse/stream")
                && message.pointer("/params/requestId").and_then(Value::as_str) == Some(&id)
            {
                let event: Event = serde_json::from_value(message["params"]["event"].clone())?;
                sender.send(Ok(event))?;
            }
        }
    })
    .await
    .context("app-server raw Responses control timeout")?
}

#[cfg(test)]
#[path = "raw_response_stream_tests.rs"]
mod tests;
