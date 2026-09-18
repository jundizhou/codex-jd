//! Bounded dispatch journal. No prompts, routing tokens or response bytes are stored.
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::path::Path;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;

use crate::scheduler::Rejection;

pub(super) fn digest(parts: &[&[u8]]) -> String {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update((part.len() as u64).to_le_bytes());
        hash.update(part);
    }
    format!("{:x}", hash.finalize())
}

pub(super) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Deserialize, Serialize)]
struct Record {
    fingerprint: String,
    #[serde(default)]
    conversation: Option<String>,
    #[serde(default)]
    quarantined_at: Option<u64>,
    #[serde(default)]
    next_check_at: Option<u64>,
    #[serde(default)]
    recovery_step: u8,
    // Releasing a local reservation does not establish an upstream terminal outcome.
    #[serde(default)]
    released_at: Option<u64>,
    // None means committed to dispatch, without a durable completion acknowledgement.
    finished_at: Option<u64>,
}

#[derive(Default, Deserialize, Serialize)]
struct Snapshot {
    records: HashMap<String, Record>,
    #[serde(default)]
    cooldown_until: Option<u64>,
    #[serde(default)]
    account_blocked: bool,
    #[serde(default)]
    concurrency: usize,
}

#[derive(Default)]
pub(super) struct Journal {
    state: Snapshot,
    file: Option<File>,
}

impl Journal {
    pub(super) fn open(path: Option<&Path>) -> anyhow::Result<Self> {
        let Some(path) = path else {
            return Ok(Self::default());
        };
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let (mut file, created) = match options.open(path) {
            Ok(file) => (file, true),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                (OpenOptions::new().read(true).write(true).open(path)?, false)
            }
            Err(error) => return Err(error.into()),
        };
        file.try_lock()?;
        let mut state = if created {
            Snapshot::default()
        } else {
            anyhow::ensure!(
                file.metadata()?.len() <= 4 * 1024 * 1024,
                "queue journal too large"
            );
            let mut data = Vec::new();
            file.read_to_end(&mut data)?;
            serde_json::from_slice::<Snapshot>(&data)?
        };
        anyhow::ensure!(state.records.len() <= 4096, "queue journal limits exceeded");
        let mut migrated = false;
        for record in state.records.values_mut() {
            if record.finished_at.is_none()
                && record.released_at.is_none()
                && record.next_check_at.is_none()
            {
                record.quarantined_at.get_or_insert_with(now);
                record.next_check_at = Some(now().saturating_add(10));
                migrated = true;
            }
        }
        let mut journal = Self {
            state,
            file: Some(file),
        };
        if created || migrated {
            journal.save()?;
        }
        Ok(journal)
    }

    pub(super) fn unknown_conversations(&self) -> Vec<String> {
        self.state
            .records
            .values()
            .filter(|record| record.finished_at.is_none() && record.released_at.is_none())
            .filter_map(|record| record.conversation.clone())
            .collect()
    }

    pub(super) fn unfinished(&self) -> HashMap<String, Option<String>> {
        self.state
            .records
            .iter()
            .filter(|(_, record)| record.finished_at.is_none() && record.released_at.is_none())
            .map(|(id, record)| (id.clone(), record.conversation.clone()))
            .collect()
    }

    pub(super) fn account(&self) -> (Option<std::time::Duration>, bool, usize) {
        (
            self.state
                .cooldown_until
                .filter(|at| *at > now())
                .map(|at| std::time::Duration::from_secs(at.saturating_sub(now()))),
            self.state.account_blocked,
            self.state.concurrency,
        )
    }

    pub(super) fn save_account(
        &mut self,
        throttle: &crate::queue_throttle::Throttle,
    ) -> anyhow::Result<()> {
        self.state.cooldown_until = throttle.until.map(|at| {
            now()
                .saturating_add(
                    at.saturating_duration_since(std::time::Instant::now())
                        .as_secs(),
                )
                .saturating_add(1)
        });
        self.state.account_blocked = throttle.blocked;
        self.state.concurrency = throttle.limit;
        self.save()
    }

    pub(super) fn check(&mut self, id: &str, fingerprint: &str) -> Result<(), Rejection> {
        self.state.records.retain(|_, record| {
            record
                .finished_at
                .is_none_or(|at| now().saturating_sub(at) < 600)
        });
        if let Some(record) = self.state.records.get(id) {
            return Err(if record.fingerprint != fingerprint {
                Rejection::Conflict
            } else if record.finished_at.is_some() {
                Rejection::Completed
            } else if record.released_at.is_some() {
                Rejection::OutcomeUnknown
            } else {
                Rejection::Duplicate
            });
        }
        if self.state.records.len() >= 4096 {
            return Err(Rejection::Full);
        }
        Ok(())
    }

    pub(super) fn start(
        &mut self,
        id: &str,
        fingerprint: &str,
        conversation: Option<&str>,
    ) -> Result<(), Rejection> {
        self.check(id, fingerprint)?;
        self.state.records.insert(
            id.to_string(),
            Record {
                fingerprint: fingerprint.to_string(),
                conversation: conversation.map(str::to_owned),
                quarantined_at: None,
                next_check_at: None,
                recovery_step: 0,
                released_at: None,
                finished_at: None,
            },
        );
        self.save().map_err(|_| Rejection::Unavailable)
    }

    pub(super) fn finish(&mut self, id: &str) -> anyhow::Result<()> {
        if let Some(record) = self.state.records.get_mut(id) {
            record.finished_at = Some(now());
        }
        self.save()
    }

    pub(super) fn quarantine(&mut self, id: &str) -> anyhow::Result<()> {
        if let Some(record) = self.state.records.get_mut(id) {
            record.quarantined_at.get_or_insert_with(now);
            record
                .next_check_at
                .get_or_insert_with(|| now().saturating_add(10));
        }
        self.save()
    }

    pub(super) fn acknowledge_unknown(&mut self) -> anyhow::Result<()> {
        for record in self.state.records.values_mut() {
            if record.finished_at.is_none() {
                record.finished_at = Some(now());
            }
        }
        self.save()
    }

    pub(super) fn recovery_due(&self, at: u64) -> Vec<(String, String)> {
        self.state
            .records
            .iter()
            .filter_map(|(id, record)| {
                (record.finished_at.is_none()
                    && record.released_at.is_none()
                    && record.next_check_at.is_some_and(|next| next <= at))
                .then(|| record.conversation.clone().map(|key| (id.clone(), key)))
                .flatten()
            })
            .collect()
    }

    pub(super) fn recovery_result(
        &mut self,
        id: &str,
        result: &anyhow::Result<()>,
        at: u64,
    ) -> anyhow::Result<()> {
        if let Some(record) = self.state.records.get_mut(id) {
            if result.is_ok() {
                record.released_at = Some(at);
                record.next_check_at = None;
            } else {
                record.recovery_step = (record.recovery_step % 3 + 1) % 3;
                record.next_check_at =
                    Some(at.saturating_add([10, 20, 30][usize::from(record.recovery_step)]));
            }
        }
        self.save()
    }

    pub(super) fn recovery_status(&self) -> serde_json::Value {
        serde_json::json!({
            "released_unknown": self.state.records.values().filter(|r| r.finished_at.is_none() && r.released_at.is_some()).count(),
            "next_check_at": self.state.records.values().filter(|r| r.finished_at.is_none() && r.released_at.is_none()).filter_map(|r| r.next_check_at).min(),
        })
    }

    fn save(&mut self) -> anyhow::Result<()> {
        if let Some(file) = &mut self.file {
            let data = serde_json::to_vec(&self.state)?;
            file.rewind()?;
            file.write_all(&data)?;
            file.set_len(data.len() as u64)?;
            file.sync_all()?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "queue_store_tests.rs"]
mod tests;
