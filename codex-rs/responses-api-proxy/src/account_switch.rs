//! Atomic account activation with acknowledgement and verified rollback.
use crate::admin_accounts::Store;
use crate::admin_accounts::identity;
use crate::admin_accounts::read_auth;
use crate::admin_accounts::write_private;
use crate::app_server_reader::AppServerIdentityClient;
use crate::app_server_reader::connect;
use crate::app_server_reader::initialize;
use crate::app_server_reader::send_and_wait_for_response;
use crate::scheduler::Activation;
use crate::scheduler::Scheduler;
use anyhow::Context;
use anyhow::Result;
use anyhow::ensure;
use serde_json::Value;
use serde_json::json;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

pub(crate) static LOCK: Mutex<()> = Mutex::new(());

pub(crate) fn apply(
    queue: &Scheduler,
    client: &AppServerIdentityClient,
    auth_path: &Path,
    change: impl FnOnce() -> Result<()>,
) -> Result<()> {
    let mut failed = None;
    let outcome = queue.switch_account(|| {
        let previous = match read_auth(auth_path) {
            Ok(value) => Some(value),
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
            {
                None
            }
            Err(error) => return Err(error),
        };
        let result = change().and_then(|()| confirm(client, auth_path));
        if let Err(error) = result {
            failed = Some(error);
            match previous {
                Some(value) => {
                    write_private(auth_path, &value)?;
                    confirm(client, auth_path)?;
                }
                None => {
                    std::fs::remove_file(auth_path)?;
                    anyhow::bail!("首次认证未能确认，请重新加载服务");
                }
            }
            Ok(Activation::Restored)
        } else {
            Ok(Activation::Changed)
        }
    })?;
    ensure!(
        outcome == Activation::Changed,
        "切换失败，已恢复原账号：{}",
        failed.context("缺少切换结果")?
    );
    Ok(())
}

fn confirm(client: &AppServerIdentityClient, path: &Path) -> Result<()> {
    let expected_account = identity(&read_auth(path)?);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(15), async {
            let mut stream = connect(client.socket()).await?;
            initialize(&mut stream).await?;
            loop {
                let disk = read_auth(path)?;
                ensure!(
                    identity(&disk) == expected_account,
                    "认证文件被其他操作修改"
                );
                let expected = disk
                    .pointer("/tokens/access_token")
                    .or_else(|| disk.get("OPENAI_API_KEY"))
                    .and_then(Value::as_str)
                    .context("认证缺少 access token")?;
                let status = send_and_wait_for_response(
                    &mut stream,
                    "getAuthStatus",
                    json!({"includeToken":true,"refreshToken":false}),
                )
                .await?;
                // Compare only in memory; credential values never enter logs/status.
                if status["authToken"].as_str() == Some(expected) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            // OAuth tokens can be shared by several workspaces. A single usage
            // read confirms the loaded account, without polling the quota endpoint.
            let disk = read_auth(path)?;
            if let Some(account) = disk.pointer("/tokens/account_id").and_then(Value::as_str) {
                let loaded =
                    send_and_wait_for_response(&mut stream, "account/rateLimits/read", Value::Null)
                        .await?;
                ensure!(
                    loaded["accountId"].as_str() == Some(account),
                    "app-server 尚未确认目标账号身份"
                );
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("等待 app-server 加载认证超时")?
    })?;
    client.reload()
}

pub(crate) fn credentials(store: &Store<'_>, auth_path: &Path, name: &str) -> Result<Value> {
    let saved = read_auth(&store.profile(name)?)?;
    match read_auth(auth_path) {
        Ok(active) if identity(&active) == identity(&saved) => Ok(active),
        _ => Ok(saved),
    }
}
