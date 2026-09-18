//! Check live account availability without generating a response, then retire the invalid binding.
use super::*;
use crate::app_server_reader::connect;
use crate::app_server_reader::initialize;

impl Conversations {
    pub(crate) async fn recover<S>(
        &mut self,
        socket: &Path,
        stream: &mut WebSocketStream<S>,
        key: &str,
    ) -> Result<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        ensure!(account(&self.auth_path)? == self.account, "account changed");
        // A dedicated connection bounds probe failure without poisoning the identity connection.
        let limits = tokio::time::timeout(Duration::from_secs(10), async {
            let mut probe = connect(socket).await?;
            initialize(&mut probe).await?;
            send_and_wait_for_response(&mut probe, "account/rateLimits/read", Value::Null).await
        })
        .await??;
        ensure!(
            account(&self.auth_path)? == self.account,
            "account changed during probe"
        );
        if let Some(id) = limits["accountId"].as_str() {
            ensure!(
                digest(&[b"chatgpt", id.as_bytes()]) == self.account,
                "probe account mismatch"
            );
        }
        let snapshot = &limits["rateLimits"];
        let windows: Vec<_> = ["primary", "secondary"]
            .into_iter()
            .filter_map(|name| snapshot[name].as_object())
            .collect();
        ensure!(
            !windows.is_empty()
                && windows.iter().all(|window| {
                    window
                        .get("usedPercent")
                        .and_then(Value::as_f64)
                        .is_some_and(|used| (0.0..100.0).contains(&used))
                })
                && snapshot["spendControlReached"] != true
                && snapshot["rateLimitReachedType"].is_null()
                && snapshot["individualLimit"]["remainingPercent"]
                    .as_i64()
                    .is_none_or(|remaining| remaining > 0),
            "account limits unavailable or exhausted"
        );
        if let Some(record) = self.records.get_mut(key) {
            ensure!(record.invalid, "refusing to retire a valid binding");
            let thread = record.identity.thread_id.clone();
            // Persist invalidation before releasing the scheduler reservation. Recovery
            // is idempotent across a crash between this write and the journal write.
            self.save()?;
            if self.loaded.contains_key(&thread) {
                send_and_wait_for_response(
                    stream,
                    "thread/unsubscribe",
                    json!({"threadId": thread}),
                )
                .await?;
                self.loaded.remove(&thread);
            }
        } else {
            self.save()?;
        }
        Ok(())
    }
}
