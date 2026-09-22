//! Account feedback affects future dispatch only; it never retries a model call.
use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

#[derive(Default)]
pub(super) struct Throttle {
    pub until: Option<Instant>,
    pub blocked: bool,
    pub blocked_reason: Option<&'static str>,
    pub limit: usize,
    pub(super) limited: u32,
    pub(super) failures: u32,
    pub(super) healthy_since: Option<Instant>,
    pub(super) successes: u32,
}

impl Throttle {
    pub(super) fn observe(
        &mut self,
        status: u16,
        headers: &HashMap<String, String>,
        now: Instant,
        maximum: usize,
    ) {
        if matches!(status, 401 | 402) {
            self.blocked = true;
            self.blocked_reason = Some(if status == 402 {
                "quota_exhausted"
            } else {
                "auth_invalid"
            });
        }
        if status < 500 {
            self.failures = 0;
        }
        if status == 429 || status >= 500 {
            self.successes = 0;
            self.healthy_since = None;
            self.failures = if status >= 500 {
                self.failures.saturating_add(1)
            } else {
                0
            };
            if status == 429 {
                self.limited = self.limited.saturating_add(1);
                self.limit = (self.limit / 2).max(1);
            }
            if status == 429 || self.failures >= 3 {
                self.limit = 1;
                let delay = headers
                    .get("retry-after")
                    .and_then(|value| retry_after(value, SystemTime::now()))
                    .unwrap_or_else(|| {
                        Duration::from_secs(
                            (5_u64 << self.limited.saturating_sub(1).min(5)).min(120),
                        )
                    });
                // Add bounded jitter to recovery, never shorten the upstream's advice.
                let jitter = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .subsec_millis()
                    % 251;
                if let Some(until) = now
                    .checked_add(delay)
                    .and_then(|t| t.checked_add(Duration::from_millis(u64::from(jitter))))
                {
                    self.until = Some(self.until.map_or(until, |old| old.max(until)));
                } else {
                    self.blocked = true;
                }
            }
        } else if (200..300).contains(&status) && self.until.is_none_or(|until| now >= until) {
            self.failures = 0;
            self.successes += 1;
            let since = self.healthy_since.get_or_insert(now);
            if self.successes >= 20 && now.duration_since(*since) >= Duration::from_secs(60) {
                self.limit = (self.limit + 1).min(maximum);
                self.limited = 0;
                self.successes = 0;
                self.healthy_since = Some(now);
            }
        }
    }
}

pub(crate) fn retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    let value = value.trim();
    if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        // An unrepresentably long cooldown requires manual recovery.
        return Some(Duration::from_secs(value.parse().unwrap_or(u64::MAX)));
    }
    httpdate::parse_http_date(value)
        .ok()
        .map(|at| at.duration_since(now).unwrap_or_default())
}

#[cfg(test)]
#[path = "queue_throttle_tests.rs"]
mod tests;
