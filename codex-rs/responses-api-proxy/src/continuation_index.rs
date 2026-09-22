//! Account- and caller-scoped ownership of references observed in upstream output.
//! Only hashes are retained; request contents never establish ownership.
use anyhow::Result;
use anyhow::ensure;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use crate::queue_store::digest;
use crate::queue_store::now;
use crate::scheduler::Key;
use crate::scheduler::Rejection;

const MAX_RECORDS: usize = 16384;
const MAX_BYTES: u64 = 16 * 1024 * 1024;
const MAX_REFERENCES: usize = 4096;
const RETENTION: u64 = 30 * 24 * 3600;

#[derive(Deserialize, Serialize)]
struct Record {
    conversation: String,
    binding: String,
    observed_at: u64,
    ambiguous: bool,
}

#[derive(Default, Deserialize, Serialize)]
struct Snapshot {
    records: HashMap<String, Record>,
}

pub(crate) struct Index {
    path: PathBuf,
    state: Snapshot,
    dirty: bool,
}

#[derive(Clone)]
pub(crate) struct Recorder {
    pub(crate) index: Arc<Mutex<Index>>,
    pub(crate) account: String,
    pub(crate) key: Key,
}

fn reference(kind: &str, value: &Value) -> Option<String> {
    value
        .as_str()
        .filter(|value| !value.is_empty())
        .map(|value| digest(&[kind.as_bytes(), value.as_bytes()]))
}

fn item_references(item: &Value) -> impl Iterator<Item = String> + '_ {
    [
        "id",
        "encrypted_content",
        "encrypted_function_args",
        "call_id",
    ]
    .into_iter()
    .filter(|_| !matches!(item["role"].as_str(), Some("user" | "system" | "developer")))
    .filter_map(|name| reference(name, &item[name]))
}

impl Index {
    pub(crate) fn open(path: PathBuf) -> Result<Self> {
        // The owning conversation registry holds the process lock. A crash may
        // leave an incomplete atomic-write temporary, never a committed index.
        match std::fs::remove_file(path.with_extension("pending")) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let state = match File::open(&path) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
                ensure!(
                    bytes.len() as u64 <= MAX_BYTES,
                    "continuation index too large"
                );
                serde_json::from_slice::<Snapshot>(&bytes)?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Snapshot::default(),
            Err(error) => return Err(error.into()),
        };
        ensure!(
            state.records.len() <= MAX_RECORDS,
            "too many continuation references"
        );
        Ok(Self {
            path,
            state,
            dirty: false,
        })
    }

    pub(crate) fn resolve(
        &self,
        principal: &str,
        account: &str,
        body: &Value,
    ) -> Result<Option<String>, Rejection> {
        let mut references = HashSet::new();
        for reference in reference("response", &body["previous_response_id"])
            .into_iter()
            .chain(
                body["input"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .flat_map(item_references),
            )
        {
            references.insert(reference);
            if references.len() > MAX_REFERENCES {
                return Err(Rejection::TooLarge);
            }
        }
        let mut conversation = None;
        for reference in references {
            let scoped = digest(&[
                principal.as_bytes(),
                account.as_bytes(),
                reference.as_bytes(),
            ]);
            let record = self
                .state
                .records
                .get(&scoped)
                .filter(|record| now().saturating_sub(record.observed_at) < RETENTION)
                .ok_or(Rejection::BindingLost)?;
            if record.ambiguous
                || conversation
                    .as_ref()
                    .is_some_and(|key| key != &record.conversation)
            {
                return Err(Rejection::ContinuationConflict);
            }
            conversation = Some(record.conversation.clone());
        }
        Ok(conversation)
    }

    pub(crate) fn retain_bindings(&mut self, bindings: &HashSet<String>) -> Result<()> {
        self.state.records.retain(|_, record| {
            (record.ambiguous || bindings.contains(&record.binding))
                && now().saturating_sub(record.observed_at) < RETENTION
        });
        self.dirty = true;
        self.save()
    }

    fn save(&mut self) -> Result<()> {
        if self.dirty {
            let value = serde_json::to_value(&self.state)?;
            ensure!(
                serde_json::to_vec(&value)?.len() as u64 <= MAX_BYTES,
                "continuation index too large"
            );
            crate::admin_accounts::write_private(&self.path, &value)?;
            #[cfg(unix)]
            if let Some(parent) = self.path.parent() {
                File::open(parent)?.sync_all()?;
            }
            self.dirty = false;
        }
        Ok(())
    }
}

impl Recorder {
    pub(crate) fn observe(&self, event: &Value) {
        let response = event.get("response").unwrap_or(event);
        let item = matches!(
            event["type"].as_str(),
            Some("response.output_item.added" | "response.output_item.done")
        )
        .then_some(&event["item"]);
        let references = reference("response", &response["id"])
            .into_iter()
            .chain(item.into_iter().flat_map(item_references))
            .chain(
                response["output"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .flat_map(item_references),
            );
        let mut index = self
            .index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for reference in references.take(MAX_REFERENCES) {
            let scoped = digest(&[
                self.key.0.as_bytes(),
                self.account.as_bytes(),
                reference.as_bytes(),
            ]);
            if let Some(record) = index.state.records.get_mut(&scoped) {
                if record.conversation != self.key.1 && !record.ambiguous {
                    record.ambiguous = true;
                    record.observed_at = now();
                    index.dirty = true;
                }
                continue;
            }
            if index.state.records.len() == MAX_RECORDS
                && let Some(oldest) = index
                    .state
                    .records
                    .iter()
                    .min_by_key(|(_, record)| record.observed_at)
                    .map(|(key, _)| key.clone())
            {
                index.state.records.remove(&oldest);
            }
            index.state.records.insert(
                scoped,
                Record {
                    conversation: self.key.1.clone(),
                    binding: self.key.digest(),
                    observed_at: now(),
                    ambiguous: false,
                },
            );
            index.dirty = true;
        }
    }

    pub(crate) fn flush(&self) -> Result<()> {
        self.index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .save()
    }
}

#[cfg(test)]
#[path = "continuation_index_tests.rs"]
mod tests;
