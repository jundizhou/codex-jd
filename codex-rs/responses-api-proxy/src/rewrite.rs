//! Replace existing metadata values without moving fields between headers and JSON.
use std::collections::HashMap;

use anyhow::Result;
use anyhow::ensure;
use serde_json::Map;
use serde_json::Value;

use crate::identity::SessionIdentity;
use crate::metadata_profiles::Binding;
use crate::metadata_profiles::Profiles;
use crate::queue_store::digest;

pub(crate) struct RewrittenRequest {
    pub(crate) body: Value,
    pub(crate) headers: HashMap<String, String>,
}

struct Rewriter<'a> {
    identity: &'a SessionIdentity,
    binding: Option<Binding>,
}

impl Rewriter<'_> {
    fn field(&self, key: &str, value: &mut Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        let identity = self.identity;
        let (target, namespace) = match key {
            "installation_id" | "x-codex-installation-id" => (Some(&identity.installation_id), ""),
            "session_id" => (Some(&identity.session_id), ""),
            "thread_id" => (Some(&identity.thread_id), ""),
            "window_id" | "x-codex-window-id" => (Some(&identity.window_id), ""),
            "context_window_id" => (None, "context-window"),
            "turn_id" | "root_turn_id" | "parent_turn_id" => (None, "turn"),
            "parent_thread_id" | "x-codex-parent-thread-id" => {
                let source = value
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("invalid parent thread ID"))?;
                let parent = self
                    .binding
                    .as_ref()
                    .and_then(|binding| binding.parents.get(source))
                    .or(identity.parent_thread_id.as_ref())
                    .ok_or_else(|| anyhow::anyhow!("parent thread has no proxy binding"))?;
                *value = Value::String(parent.clone());
                return Ok(());
            }
            _ => return Ok(()),
        };
        let source = value
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("{key} must be a string or null"))?;
        ensure!(!source.is_empty() && source.len() <= 512, "invalid {key}");
        let replacement = match target {
            Some(target) => target.clone(),
            None => {
                // All turn roles share a namespace so parent/root links retain equality.
                let hash = digest(&[
                    identity.installation_id.as_bytes(),
                    namespace.as_bytes(),
                    source.as_bytes(),
                ]);
                format!(
                    "{}-{}-8{}-a{}-{}",
                    &hash[..8],
                    &hash[8..12],
                    &hash[13..16],
                    &hash[17..20],
                    &hash[20..32]
                )
            }
        };
        *value = Value::String(replacement);
        Ok(())
    }

    fn metadata(&self, metadata: &mut Map<String, Value>) -> Result<()> {
        for (key, value) in metadata.iter_mut() {
            self.field(key, value)?;
        }
        if let Some(value) = metadata.get_mut("x-codex-turn-metadata") {
            let mut nested: Map<String, Value> = serde_json::from_str(
                value
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("turn metadata must be a JSON string"))?,
            )?;
            ensure!(
                !nested.contains_key("x-codex-turn-metadata"),
                "recursive turn metadata"
            );
            self.metadata(&mut nested)?;
            *value = Value::String(serde_json::to_string(&nested)?);
        }
        if let Some(workspaces) = metadata
            .get_mut("workspaces")
            .filter(|value| !value.is_null())
            && let Some(binding) = &self.binding
        {
            let original = workspaces
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("workspaces must be an object"))?;
            ensure!(original.len() <= 64, "too many workspaces");
            let profile = &binding.workspace;
            let mut rewritten = Map::new();
            for (path, workspace) in original {
                let mut workspace = workspace.clone();
                if let Some(fields) = workspace.as_object_mut() {
                    if let Some(remotes) = fields
                        .get_mut("associated_remote_urls")
                        .filter(|value| !value.is_null())
                    {
                        let remotes = remotes.as_object_mut().ok_or_else(|| {
                            anyhow::anyhow!("workspace remotes must be an object")
                        })?;
                        for remote in remotes.values_mut().filter(|value| !value.is_null()) {
                            ensure!(remote.is_string(), "workspace remote must be a string");
                            *remote = Value::String(profile.remote_url.clone());
                        }
                    }
                    for (name, replacement) in [
                        (
                            "latest_git_commit_hash",
                            Value::String(profile.commit.clone()),
                        ),
                        ("has_changes", Value::Bool(profile.has_changes)),
                    ] {
                        if let Some(value) = fields.get_mut(name).filter(|value| !value.is_null()) {
                            ensure!(
                                std::mem::discriminant(value)
                                    == std::mem::discriminant(&replacement),
                                "invalid workspace {name}"
                            );
                            *value = replacement;
                        }
                    }
                } else {
                    ensure!(workspace.is_null(), "workspace must be an object or null");
                }
                let key = digest(&[path.as_bytes()]);
                let path = format!("{}/worktrees/{}", profile.path, &key[..16]);
                ensure!(
                    rewritten.insert(path, workspace).is_none(),
                    "workspace path collision"
                );
            }
            *workspaces = Value::Object(rewritten);
        }
        Ok(())
    }
}

pub(crate) fn rewrite_request(
    body: &[u8],
    mut headers: HashMap<String, String>,
    identity: &SessionIdentity,
    profiles: Option<&Profiles>,
) -> Result<RewrittenRequest> {
    let mut body: Value = serde_json::from_slice(body)?;
    ensure!(body.is_object(), "request must be an object");
    let mut metadata = Vec::new();
    if let Some(flat) = body.get("client_metadata") {
        ensure!(flat.is_object(), "client_metadata must be an object");
        metadata.push(flat.clone());
        if let Some(nested) = flat.get("x-codex-turn-metadata") {
            metadata.push(serde_json::from_str(
                nested
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("invalid turn metadata"))?,
            )?);
        }
    }
    if let Some(nested) = headers.get("x-codex-turn-metadata") {
        metadata.push(serde_json::from_str(nested)?);
    }
    let mut sources = Vec::new();
    let mut parents = Vec::new();
    for map in &metadata {
        ensure!(map.is_object(), "turn metadata must be an object");
        for (key, values) in [
            ("thread_id", &mut sources),
            ("parent_thread_id", &mut parents),
        ] {
            if let Some(value) = map.get(key).filter(|value| !value.is_null()) {
                let value = value
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("invalid {key}"))?;
                ensure!(!value.is_empty() && value.len() <= 512, "invalid {key}");
                values.push(value.to_owned());
            }
        }
    }
    for map in &metadata {
        if let Some(parent) = map
            .get("x-codex-parent-thread-id")
            .filter(|value| !value.is_null())
        {
            parents.push(
                parent
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("invalid parent thread ID"))?
                    .to_owned(),
            );
        }
    }
    if let Some(parent) = headers.get("x-codex-parent-thread-id") {
        parents.push(parent.clone());
    }
    sources.sort();
    sources.dedup();
    ensure!(sources.len() <= 1, "conflicting metadata thread IDs");
    let binding = profiles
        .map(|profiles| profiles.bind(identity, &sources, &parents))
        .transpose()?;
    let rewriter = Rewriter { identity, binding };
    if let Some(metadata) = body
        .get_mut("client_metadata")
        .and_then(Value::as_object_mut)
    {
        rewriter.metadata(metadata)?;
    }
    for (name, value) in &mut headers {
        if name == "x-codex-turn-metadata" {
            let mut metadata = serde_json::from_str(value)?;
            rewriter.metadata(&mut metadata)?;
            *value = serde_json::to_string(&metadata)?;
        } else {
            let mut replacement = Value::String(value.clone());
            rewriter.field(name, &mut replacement)?;
            let Value::String(replacement) = replacement else {
                anyhow::bail!("rewritten header {name} must be a string");
            };
            *value = replacement;
        }
        ensure!(
            value.len() <= 8192,
            "rewritten header {name} exceeds 8192 bytes"
        );
    }
    Ok(RewrittenRequest { body, headers })
}

#[cfg(test)]
#[path = "rewrite_tests.rs"]
mod tests;
