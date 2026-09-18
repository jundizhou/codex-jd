//! Reloads file-backed authentication when an operator activates another profile.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use codex_login::AuthManager;

use crate::config_manager::ConfigManager;

/// Starts the conservative file watcher used by profile-based account switching.
///
/// The watcher only observes metadata and delegates validation to `AuthManager`.
/// It never reads or logs credential contents. Keyring and external auth sources
/// are unaffected; their normal refresh paths remain authoritative.
pub(crate) fn spawn(
    auth_path: PathBuf,
    auth_manager: Arc<AuthManager>,
    config_manager: Arc<ConfigManager>,
    chatgpt_base_url: String,
    http_client_factory: codex_http_client::HttpClientFactory,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut signature = metadata_signature(&auth_path).await;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {
                    let next = metadata_signature(&auth_path).await;
                    if next == signature {
                        continue;
                    }
                    signature = next;
                    if auth_manager.reload().await {
                        config_manager.replace_cloud_config_bundle_loader(
                            Arc::clone(&auth_manager),
                            chatgpt_base_url.clone(),
                            http_client_factory.clone(),
                        );
                        config_manager.sync_default_client_residency_requirement().await;
                        tracing::info!(path = %auth_path.display(), "reloaded authentication profile");
                    } else {
                        tracing::warn!(path = %auth_path.display(), "authentication profile changed but reload produced no new credentials");
                    }
                }
            }
        }
    });
}

async fn metadata_signature(path: &PathBuf) -> Option<(u64, Option<SystemTime>)> {
    tokio::fs::metadata(path)
        .await
        .ok()
        .map(|metadata| (metadata.len(), metadata.modified().ok()))
}
