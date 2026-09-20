//! Restore the original binding only after local termination and identity verification.
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
        ensure!(
            Some(account(&self.auth_path)?) == self.account,
            "account changed"
        );
        // A dedicated connection bounds probe failure without poisoning the identity connection.
        let limits = tokio::time::timeout(Duration::from_secs(10), async {
            let mut probe = connect(socket).await?;
            initialize(&mut probe).await?;
            send_and_wait_for_response(&mut probe, "account/rateLimits/read", Value::Null).await
        })
        .await??;
        ensure!(
            Some(account(&self.auth_path)?) == self.account,
            "account changed during probe"
        );
        if let Some(id) = limits["accountId"].as_str() {
            ensure!(
                Some(digest(&[b"chatgpt", id.as_bytes()])) == self.account,
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
        self.restore_binding(stream, key).await
    }

    async fn restore_binding<S>(&mut self, stream: &mut WebSocketStream<S>, key: &str) -> Result<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        ensure!(
            Some(account(&self.auth_path)?) == self.account,
            "account changed"
        );
        let record = self.records.get(key).ok_or(Rejection::BindingLost)?;
        ensure!(
            record.recoverable,
            "local request termination has not been confirmed"
        );
        let thread = record.identity.thread_id.clone();
        ensure!(
            self.loaded.get(&thread) != Some(&None),
            "original thread is still leased"
        );
        if !self.loaded.contains_key(&thread) {
            send_and_wait_for_response(
                stream,
                "thread/resume",
                json!({"threadId": thread, "excludeTurns": true}),
            )
            .await?;
            self.loaded.insert(thread.clone(), Some(Instant::now()));
        }
        let expected = &self.records[key].identity;
        ensure!(
            load_identities(stream)
                .await?
                .iter()
                .any(|identity| identity == expected),
            "restored thread identity changed; refusing continuation"
        );
        // Keep termination proof until the next acquire, making recovery idempotent
        // if queue persistence fails or the process stops before releasing the slot.
        self.records
            .get_mut(key)
            .ok_or(Rejection::BindingLost)?
            .invalid = false;
        self.save()?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "conversation_recovery_tests.rs"]
mod tests;
