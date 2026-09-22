//! One account switch coordinator for background rotation and administrator changes.
use crate::admin_accounts::Store;
use crate::admin_accounts::identity;
use crate::admin_accounts::read_auth;
use crate::admin_accounts::valid_name;
use crate::admin_accounts::write_private;
use crate::app_server_reader::AppServerIdentityClient;
use crate::quota_cache::CACHE;
use crate::quota_cache::Freshness;
use crate::quota_cache::eligible;
use crate::quota_cache::key;
use crate::quota_cache::remaining;
use crate::scheduler::Scheduler;
use anyhow::Context;
use anyhow::Result;
use anyhow::ensure;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

static MANAGER: OnceLock<Arc<Manager>> = OnceLock::new();
fn bounded_error(error: &anyhow::Error) -> String {
    error.to_string().chars().take(/*n*/ 512).collect()
}
#[derive(Clone, Default, Deserialize, Serialize)]
struct Settings {
    enabled: bool,
    priority: Vec<String>,
}
#[derive(Default, Deserialize, Serialize)]
struct Saved {
    settings: Settings,
    events: VecDeque<Value>,
    backoff: HashMap<String, (String, u64)>,
}
pub(crate) struct Manager {
    queue: Arc<Scheduler>,
    client: Arc<AppServerIdentityClient>,
    root: PathBuf,
    auth: PathBuf,
    path: PathBuf,
    capacity: usize,
    saved: Mutex<Saved>,
    display: Mutex<Value>,
    next_scan: AtomicU64,
}
pub(crate) fn start(
    queue: Arc<Scheduler>,
    client: Arc<AppServerIdentityClient>,
    root: PathBuf,
    auth: PathBuf,
    capacity: usize,
) -> Result<()> {
    let path = auth.with_file_name("account-rotation.json");
    let saved: Saved = match std::fs::File::open(&path) {
        Ok(file) => {
            let mut bytes = Vec::new();
            file.take(1024 * 1024 + 1).read_to_end(&mut bytes)?;
            ensure!(bytes.len() <= 1024 * 1024, "rotation state too large");
            serde_json::from_slice(&bytes)?
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Saved::default(),
        Err(e) => return Err(e.into()),
    };
    ensure!(
        saved.settings.priority.len() <= 64
            && saved.events.len() <= 50
            && saved.backoff.len() <= 64,
        "rotation state exceeds limits"
    );
    CACHE.initialize(auth.with_file_name("account-quota-cache.json"))?;
    queue.rotation_enabled(saved.settings.enabled);
    let manager = Arc::new(Manager {
        queue,
        client,
        root,
        auth,
        path,
        capacity,
        saved: Mutex::new(saved),
        display: Mutex::new(json!({"phase":"idle"})),
        next_scan: AtomicU64::new(0),
    });
    ensure!(
        MANAGER.set(Arc::clone(&manager)).is_ok(),
        "rotation already initialized"
    );
    std::thread::Builder::new()
        .name("account-rotation".into())
        .spawn(move || {
            let mut last_traffic = 0;
            let mut last_block = String::new();
            loop {
                std::thread::sleep(Duration::from_secs(1));
                let Ok(_guard) = crate::account_switch::LOCK.try_lock() else {
                    continue;
                };
                if let Err(error) = manager.tick(&mut last_traffic, &mut last_block) {
                    *manager
                        .display
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) =
                        json!({"phase":"error","message":bounded_error(&error)});
                }
            }
        })?;
    Ok(())
}

pub(crate) fn status() -> Value {
    let Some(m) = MANAGER.get() else {
        return json!({"settings":{"enabled":false,"priority":[]},"phase":"disabled","events":[],"profiles":{}});
    };
    let saved = m
        .saved
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut result = m
        .display
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    result["settings"] = json!(saved.settings);
    result["events"] = json!(saved.events);
    drop(saved);
    let store = Store::new(&m.root, &m.auth, m.capacity);
    let mut profiles = serde_json::Map::new();
    for name in store.names().unwrap_or_default() {
        if let Ok(auth) = crate::account_switch::credentials(&store, &m.auth, &name) {
            profiles.insert(name, CACHE.status(&key(&auth), Freshness::Active));
        }
    }
    result["profiles"] = Value::Object(profiles);
    result
}

pub(crate) fn configure(value: Value) -> Result<()> {
    let m = MANAGER.get().context("当前模式不支持自动换号")?;
    let settings: Settings = serde_json::from_value(value)?;
    ensure!(
        settings.priority.len() <= 64 && settings.priority.iter().all(|n| valid_name(n)),
        "账号优先级格式无效"
    );
    let store = Store::new(&m.root, &m.auth, m.capacity);
    for name in &settings.priority {
        store.profile(name)?;
    }
    let mut saved = m
        .saved
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let previous = std::mem::replace(&mut saved.settings, settings);
    if let Err(error) = write_private(&m.path, &serde_json::to_value(&*saved)?) {
        saved.settings = previous;
        return Err(error);
    }
    m.queue.rotation_enabled(saved.settings.enabled);
    *m.display
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) =
        json!({"phase":if saved.settings.enabled {"monitoring"} else {"disabled"}});
    Ok(())
}

impl Manager {
    fn tick(&self, last_traffic: &mut u64, last_block: &mut String) -> Result<()> {
        let settings = self
            .saved
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .settings
            .clone();
        if !settings.enabled {
            return Ok(());
        }
        let q = self.queue.status();
        if q["paused"] == true || q["outcome_unknown"] == true || !self.client.is_available() {
            return Ok(());
        }
        let traffic = q["traffic_version"].as_u64().unwrap_or(0);
        let hard_block = matches!(
            q["account_unavailable_reason"].as_str(),
            Some("quota_exhausted" | "auth_invalid")
        );
        if traffic == *last_traffic
            && q["running"] == 0
            && q["pending"] == 0
            && q["rotation_hold"] != true
            && !hard_block
        {
            return Ok(());
        }
        let now = crate::queue_store::now();
        let auth = read_auth(&self.auth)?;
        let fingerprint = key(&auth);
        if hard_block && *last_block != fingerprint {
            CACHE.block(
                fingerprint.clone(),
                q["account_unavailable_reason"] == "auth_invalid",
                now,
            );
            *last_block = fingerprint.clone();
        }
        let quota = crate::admin_usage::cached(&auth, Freshness::Active);
        *last_traffic = traffic;
        if q["rotation_hold"] == true && quota.as_ref().is_ok_and(eligible) {
            crate::account_switch::apply(&self.queue, &self.client, &self.auth, || Ok(()))?;
            *last_block = String::new();
            *self
                .display
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                json!({"phase":"monitoring","message":"当前账号额度已恢复"});
            return Ok(());
        }
        let low = hard_block
            || q["rotation_hold"] == true
            || quota
                .as_ref()
                .is_ok_and(|v| remaining(v).is_some_and(|n| n <= 2.0))
            || quota.as_ref().is_err_and(|e| {
                e.downcast_ref::<crate::quota_cache::Failure>()
                    .is_some_and(|e| e.auth_invalid)
            });
        if !low {
            quota?;
            *self
                .display
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = json!({"phase":"monitoring","next_check_at":CACHE.status(&fingerprint,Freshness::Active)["next_check_at"]});
            return Ok(());
        }
        self.queue.hold_for_rotation();
        let store = Store::new(&self.root, &self.auth, self.capacity);
        let mut names = settings.priority;
        for name in store.names()? {
            if !names.contains(&name) {
                names.push(name);
            }
        }
        let active = store.list()?["profiles"]
            .as_array()
            .and_then(|rows| rows.iter().find(|r| r["active"] == true))
            .and_then(|r| r["name"].as_str())
            .unwrap_or("当前账号")
            .to_owned();
        let mut next_check = now.saturating_add(900);
        for name in names {
            let Ok(candidate) = crate::account_switch::credentials(&store, &self.auth, &name)
            else {
                continue;
            };
            if identity(&candidate) == identity(&auth) {
                continue;
            }
            let candidate_key = key(&candidate);
            if self
                .saved
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .backoff
                .get(&name)
                .is_some_and(|(key, until)| *key == candidate_key && *until > now)
            {
                continue;
            }
            let status = CACHE.status(&candidate_key, Freshness::Candidate);
            if status["auth_invalid"] == true {
                continue;
            }
            let due = status["next_check_at"].as_u64().unwrap_or(u64::MAX);
            next_check = next_check.min(due.max(self.next_scan.load(Ordering::Acquire)));
            if due > now {
                if status["error"].is_null() && eligible(&status["quota"]) {
                    return self.activate(&store, &active, &name, candidate_key, now);
                }
                continue;
            }
            if now < self.next_scan.load(Ordering::Acquire) {
                continue;
            }
            self.next_scan
                .store(now.saturating_add(30), Ordering::Release);
            *self
                .display
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                json!({"phase":"checking","candidate":name});
            if let Ok(value) = crate::admin_usage::cached(&candidate, Freshness::Candidate)
                && eligible(&value)
            {
                return self.activate(&store, &active, &name, candidate_key, now);
            }
        }
        *self
            .display
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = json!({"phase":"waiting","message":"没有已确认可用的备用账号","next_check_at":next_check.max(now+30)});
        Ok(())
    }

    fn activate(
        &self,
        store: &Store<'_>,
        from: &str,
        to: &str,
        candidate_key: String,
        now: u64,
    ) -> Result<()> {
        *self
            .display
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            json!({"phase":"draining","from":from,"candidate":to});
        let result = crate::account_switch::apply(&self.queue, &self.client, &self.auth, || {
            *self
                .display
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                json!({"phase":"switching","from":from,"candidate":to});
            let credentials = crate::account_switch::credentials(store, &self.auth, to)?;
            ensure!(
                eligible(&crate::admin_usage::cached(
                    &credentials,
                    Freshness::Candidate
                )?),
                "备用账号在等待期间已不可用"
            );
            store.activate(to)
        });
        let mut saved = self
            .saved
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if result.is_err() {
            if saved.backoff.len() >= 64 {
                saved.backoff.clear();
            }
            saved
                .backoff
                .insert(to.into(), (candidate_key, now.saturating_add(900)));
        } else {
            saved.backoff.remove(to);
        }
        saved.events.push_front(json!({"at":now,"from":from,"to":to,"ok":result.is_ok(),"reason":"额度 ≤2% 或账号不可用","error":result.as_ref().err().map(bounded_error)}));
        saved.events.truncate(50);
        write_private(&self.path, &serde_json::to_value(&*saved)?)?;
        *self
            .display
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = json!({"phase":if result.is_ok() {"monitoring"} else {"error"},"message":result.as_ref().err().map(bounded_error)});
        result
    }
}
