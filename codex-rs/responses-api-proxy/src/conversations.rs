//! Durable logical identities, independently bounded from loaded threads and queue slots.
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Result;
use anyhow::ensure;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use tokio_tungstenite::WebSocketStream;

use crate::app_server_reader::RpcError;
use crate::app_server_reader::load_identities;
use crate::app_server_reader::send_and_wait_for_response;
use crate::identity::SessionIdentity;
use crate::queue_store::digest;
use crate::scheduler::Rejection;

const MAX_RECORDS: usize = 4096;
const RETENTION: u64 = 30 * 24 * 3600;

#[path = "conversation_recovery.rs"]
mod recovery;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Continuation {
    SelfContained,
    RequiresIdentity,
}

impl Continuation {
    pub(crate) fn from_request(body: &Value, routing: Option<&str>) -> Self {
        if routing.is_some_and(|value| !value.is_empty())
            || !body["previous_response_id"].is_null()
            || !body["conversation"].is_null()
        {
            return Self::RequiresIdentity;
        }
        let mut calls = HashSet::new();
        if let Some(items) = body["input"].as_array() {
            for item in items {
                match item["type"].as_str() {
                    Some("function_call" | "custom_tool_call") => {
                        if let Some(id) = item["call_id"].as_str() {
                            calls.insert(id);
                        }
                    }
                    Some("function_call_output" | "custom_tool_call_output")
                        if !item["call_id"]
                            .as_str()
                            .is_some_and(|id| calls.contains(id)) =>
                    {
                        return Self::RequiresIdentity;
                    }
                    Some("item_reference") => return Self::RequiresIdentity,
                    _ => {}
                }
            }
        }
        Self::SelfContained
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Outcome {
    Released,
    Recoverable,
    Unknown,
}

#[derive(Deserialize, Serialize)]
struct Record {
    account: String,
    identity: SessionIdentity,
    used_at: u64,
    #[serde(default)]
    invalid: bool,
    // Persist proof that the local RPC ended independently of upstream outcome.
    #[serde(default)]
    recoverable: bool,
}

#[derive(Deserialize, Serialize)]
struct Snapshot {
    version: u8,
    records: HashMap<String, Record>,
}

pub(crate) struct Conversations {
    path: PathBuf,
    _lock: File,
    account: Option<String>,
    auth_path: PathBuf,
    records: HashMap<String, Record>,
    // None denotes a lease; only completed leases may be unloaded or evicted.
    loaded: HashMap<String, Option<Instant>>,
    capacity: usize,
    idle_ttl: Duration,
}

fn account(path: &Path) -> Result<String> {
    let mut data = Vec::new();
    File::open(path)?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut data)?;
    ensure!(data.len() <= 1024 * 1024, "auth file too large");
    let auth: Value = serde_json::from_slice(&data)?;
    if let Some(id) = auth["tokens"]["account_id"]
        .as_str()
        .filter(|id| !id.is_empty())
    {
        return Ok(digest(&[b"chatgpt", id.as_bytes()]));
    }
    if let Some(key) = auth["OPENAI_API_KEY"]
        .as_str()
        .filter(|key| !key.is_empty())
    {
        return Ok(digest(&[b"api-key", key.as_bytes()]));
    }
    anyhow::bail!("durable queue sessions require file-backed Codex authentication")
}

impl Conversations {
    pub(crate) fn open(
        path: PathBuf,
        auth_path: PathBuf,
        capacity: usize,
        idle_ttl: Duration,
        unknown: &[String],
    ) -> Result<Self> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let lock = options.open(path.with_extension("lock"))?;
        lock.try_lock()?;
        let mut records = match File::open(&path) {
            Ok(file) => {
                let mut data = Vec::new();
                file.take(4 * 1024 * 1024 + 1).read_to_end(&mut data)?;
                ensure!(
                    data.len() <= 4 * 1024 * 1024,
                    "conversation index too large"
                );
                let snapshot: Snapshot = serde_json::from_slice(&data)?;
                ensure!(
                    snapshot.version == 1 && snapshot.records.len() <= MAX_RECORDS,
                    "unsupported conversation index"
                );
                snapshot.records
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(error) => return Err(error.into()),
        };
        for key in unknown {
            if let Some(record) = records.get_mut(key) {
                record.invalid = true;
            }
        }
        let conversations = Self {
            path,
            _lock: lock,
            account: match account(&auth_path) {
                Ok(account) => Some(account),
                Err(error)
                    if error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
                {
                    None
                }
                Err(error) => return Err(error),
            },
            auth_path,
            records,
            loaded: HashMap::new(),
            capacity,
            idle_ttl,
        };
        conversations.save()?;
        Ok(conversations)
    }

    pub(crate) async fn acquire<S>(
        &mut self,
        stream: &mut WebSocketStream<S>,
        key: String,
        continuation: Continuation,
    ) -> Result<SessionIdentity>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // An empty deployment can serve administration before its first account is added.
        let account = account(&self.auth_path)?;
        let active = self.account.get_or_insert_with(|| account.clone());
        // Do not let an in-place account replacement reuse a live account's threads.
        ensure!(
            &account == active,
            "account changed; restart the Worker after draining"
        );
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let previous = self.records.get(&key).filter(|record| {
            record.account == account
                && !record.invalid
                && now.saturating_sub(record.used_at) < RETENTION
        });
        if previous.is_none() && matches!(continuation, Continuation::RequiresIdentity) {
            return Err(Rejection::BindingLost.into());
        }
        let prior_identity = previous.map(|record| record.identity.clone());
        if let Some(identity) = &prior_identity {
            ensure!(
                self.loaded.get(&identity.thread_id) != Some(&None),
                "conversation is already leased"
            );
        }
        if prior_identity.is_none()
            && self
                .records
                .get(&key)
                .is_some_and(|record| self.loaded.contains_key(&record.identity.thread_id))
        {
            return Err(Rejection::BindingLost.into());
        }
        let already_loaded = prior_identity
            .as_ref()
            .is_some_and(|identity| self.loaded.contains_key(&identity.thread_id));
        if !already_loaded && self.loaded.len() >= self.capacity {
            let oldest = self
                .loaded
                .iter()
                .filter_map(|(id, idle)| idle.map(|at| (id.clone(), at)))
                .min_by_key(|(_, at)| *at)
                .map(|(id, _)| id)
                .ok_or(Rejection::Full)?;
            send_and_wait_for_response(stream, "thread/unsubscribe", json!({"threadId": oldest}))
                .await?;
            self.loaded.remove(&oldest);
        }
        // Delete only this registry's idle threads, retaining records until deletion succeeds.
        let obsolete = self
            .records
            .iter()
            .filter(|(candidate, record)| {
                !self.loaded.contains_key(&record.identity.thread_id)
                    && ((candidate.as_str() == key && prior_identity.is_none())
                        || now.saturating_sub(record.used_at) >= RETENTION
                        || (!self.records.contains_key(&key) && self.records.len() >= MAX_RECORDS))
            })
            .min_by_key(|(candidate, record)| (candidate.as_str() != key, record.used_at))
            .map(|(key, record)| (key.clone(), record.identity.thread_id.clone()));
        if let Some((obsolete, thread)) = obsolete {
            if let Err(error) =
                send_and_wait_for_response(stream, "thread/delete", json!({"threadId": thread}))
                    .await
                && !error.downcast_ref::<RpcError>().is_some_and(|rpc| {
                    rpc.code == -32600 && rpc.message == format!("thread not found: {thread}")
                })
            {
                return Err(error);
            }
            self.records.remove(&obsolete);
            self.save()?;
        }
        if !self.records.contains_key(&key) && self.records.len() >= MAX_RECORDS {
            return Err(Rejection::Full.into());
        }
        let thread_id = match &prior_identity {
            Some(identity) => {
                if !already_loaded {
                    let deadline = Instant::now() + Duration::from_secs(5);
                    loop {
                        match send_and_wait_for_response(
                            stream,
                            "thread/resume",
                            json!({"threadId": identity.thread_id, "excludeTurns": true}),
                        )
                        .await
                        {
                            Ok(_) => break,
                            Err(error)
                                if Instant::now() < deadline
                                    && error.downcast_ref::<RpcError>().is_some_and(|rpc| {
                                        rpc.code == -32600
                                            && rpc.message.contains("is closing; retry")
                                    }) =>
                            {
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }
                            Err(error) => return Err(error),
                        }
                    }
                }
                identity.thread_id.clone()
            }
            None => {
                let result = send_and_wait_for_response(
                    stream,
                    "thread/start",
                    json!({"ephemeral": false, "historyMode": "legacy", "environments": []}),
                )
                .await?;
                let id = result
                    .pointer("/thread/id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("missing persistent thread id"))?
                    .to_owned();
                self.loaded.insert(id.clone(), Some(Instant::now()));
                // Naming materializes an otherwise empty rollout without adding model history.
                send_and_wait_for_response(
                    stream,
                    "thread/name/set",
                    json!({"threadId": id, "name": "Worker conversation"}),
                )
                .await?;
                id
            }
        };
        // A failed lookup/save may be retried, but must not leave an unbounded loaded thread.
        self.loaded.insert(thread_id.clone(), Some(Instant::now()));
        let identity = load_identities(stream)
            .await?
            .into_iter()
            .find(|identity| identity.thread_id == thread_id)
            .ok_or_else(|| anyhow::anyhow!("persistent thread identity not loaded"))?;
        if let Some(prior) = prior_identity {
            ensure!(
                identity == prior,
                "restored thread identity changed; refusing continuation"
            );
        }
        self.records.insert(
            key,
            Record {
                account,
                identity: identity.clone(),
                used_at: now,
                invalid: false,
                recoverable: false,
            },
        );
        self.save()?;
        self.loaded.insert(thread_id, None);
        Ok(identity)
    }

    pub(crate) fn release(&mut self, thread_id: &str, outcome: Outcome) -> Result<()> {
        match outcome {
            Outcome::Released => {
                if let Some(idle) = self.loaded.get_mut(thread_id) {
                    *idle = Some(Instant::now());
                }
            }
            Outcome::Unknown | Outcome::Recoverable => {
                for record in self
                    .records
                    .values_mut()
                    .filter(|record| record.identity.thread_id == thread_id)
                {
                    record.invalid = true;
                    record.recoverable = matches!(outcome, Outcome::Recoverable);
                }
                self.save()?;
                if matches!(outcome, Outcome::Recoverable)
                    && let Some(idle) = self.loaded.get_mut(thread_id)
                {
                    *idle = Some(Instant::now());
                }
            }
        }
        Ok(())
    }

    fn save(&self) -> Result<()> {
        let data = serde_json::to_vec(&json!({"version": 1, "records": self.records}))?;
        ensure!(
            data.len() <= 4 * 1024 * 1024,
            "conversation index too large"
        );
        let temporary = self.path.with_extension("tmp");
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(&data)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(temporary, &self.path)?;
        #[cfg(unix)]
        if let Some(parent) = self.path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    }

    pub(crate) async fn retire<S>(&mut self, stream: &mut WebSocketStream<S>) -> Result<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let expired: Vec<_> = self
            .loaded
            .iter()
            .filter(|(_, idle)| idle.is_some_and(|at| at.elapsed() >= self.idle_ttl))
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            send_and_wait_for_response(stream, "thread/unsubscribe", json!({"threadId": id}))
                .await?;
            self.loaded.remove(&id);
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "conversations_tests.rs"]
mod tests;
