//! Persisted workspace snapshots and bounded client-to-server thread aliases.
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::Context;
use anyhow::Result;
use anyhow::ensure;
use serde::Deserialize;
use serde::Serialize;

use crate::identity::SessionIdentity;
use crate::queue_store::digest;

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct WorkspaceProfile {
    pub(crate) path: String,
    pub(crate) remote_url: String,
    pub(crate) commit: String,
    pub(crate) has_changes: bool,
}

#[derive(Clone, Deserialize, Serialize)]
struct Snapshot {
    version: u8,
    profiles: Vec<WorkspaceProfile>,
    threads: HashMap<String, String>,
}

pub(crate) struct Profiles {
    path: PathBuf,
    _lock: File,
    snapshot: Mutex<Snapshot>,
}

pub(crate) struct Binding {
    pub(crate) workspace: WorkspaceProfile,
    pub(crate) parents: HashMap<String, String>,
}

impl Profiles {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let lock = options.open(path.with_extension("lock"))?;
        lock.try_lock()?;
        let mut data = Vec::new();
        File::open(path)?
            .take(4 * 1024 * 1024 + 1)
            .read_to_end(&mut data)?;
        ensure!(
            data.len() <= 4 * 1024 * 1024,
            "metadata profiles file too large"
        );
        let snapshot: Snapshot = serde_json::from_slice(&data)?;
        ensure!(
            snapshot.version == 1 && snapshot.profiles.len() == 5,
            "expected metadata profiles version 1 with five profiles"
        );
        ensure!(
            snapshot.threads.len() <= 4096,
            "too many metadata thread aliases"
        );
        for profile in &snapshot.profiles {
            ensure!(
                profile.path.starts_with('/')
                    && profile.path.len() <= 256
                    && !profile.path.chars().any(char::is_control),
                "invalid workspace path"
            );
            ensure!(
                profile.remote_url.starts_with("https://")
                    && profile.remote_url.len() <= 512
                    && !profile.remote_url.chars().any(char::is_control),
                "invalid workspace remote URL"
            );
            ensure!(
                [40, 64].contains(&profile.commit.len())
                    && profile.commit.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "invalid workspace commit"
            );
        }
        Ok(Self {
            path: path.to_owned(),
            _lock: lock,
            snapshot: Mutex::new(snapshot),
        })
    }

    pub(crate) fn bind(
        &self,
        identity: &SessionIdentity,
        sources: &[String],
        parents: &[String],
    ) -> Result<Binding> {
        let mut guard = self
            .snapshot
            .lock()
            .map_err(|_| anyhow::anyhow!("metadata profiles lock poisoned"))?;
        let mut snapshot = guard.clone();
        let mut changed = false;
        for source in sources {
            let key = digest(&[identity.installation_id.as_bytes(), source.as_bytes()]);
            ensure!(
                snapshot.threads.contains_key(&key) || snapshot.threads.len() < 4096,
                "metadata thread alias capacity reached"
            );
            if snapshot.threads.get(&key) != Some(&identity.thread_id) {
                snapshot.threads.insert(key, identity.thread_id.clone());
                changed = true;
            }
        }
        if changed {
            let temporary = self.path.with_extension("tmp");
            let mut options = OpenOptions::new();
            options.write(true).create(true).truncate(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temporary)?;
            file.write_all(&serde_json::to_vec(&snapshot)?)?;
            file.sync_all()?;
            drop(file);
            std::fs::rename(temporary, &self.path)?;
        }
        *guard = snapshot.clone();
        let parents = parents
            .iter()
            .map(|source| {
                let key = digest(&[identity.installation_id.as_bytes(), source.as_bytes()]);
                let target = snapshot
                    .threads
                    .get(&key)
                    .or(identity.parent_thread_id.as_ref())
                    .context("parent thread has no recorded proxy binding")?;
                Ok((source.clone(), target.clone()))
            })
            .collect::<Result<_>>()?;
        let key = digest(&[identity.thread_id.as_bytes()]);
        let slot = u32::from_str_radix(&key[..8], 16)? as usize % snapshot.profiles.len();
        Ok(Binding {
            workspace: snapshot.profiles[slot].clone(),
            parents,
        })
    }
}
