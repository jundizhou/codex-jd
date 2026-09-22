use super::*;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

fn quota(left: f64, at: u64) -> Value {
    json!({"fetched_at":at,"limits":[{"name":"Codex","windows":[{"seconds":18000,"remaining_percent":left,"reset_at":5000}]}]})
}
#[test]
fn schedules_active_candidates_and_unknown_windows_without_polling_standby() {
    assert_eq!(
        [
            due(&quota(80.0, 100), Freshness::Active),
            due(&quota(8.0, 100), Freshness::Active),
            due(&quota(4.0, 100), Freshness::Active),
            due(&quota(4.0, 100), Freshness::Candidate),
            due(&quota(2.0, 100), Freshness::Active),
            due(&quota(80.0, 100), Freshness::Manual)
        ],
        [700, 220, 220, 5000, 5000, 130]
    );
    assert!(!eligible(&quota(5.0, 100)));
    assert!(eligible(&quota(6.0, 100)));
    let mut missing = quota(50.0, 100);
    missing["limits"][0]["windows"]
        .as_array_mut()
        .unwrap()
        .push(json!({"remaining_percent":null}));
    assert!(!eligible(&missing));
    missing["limits"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"Spark","limit_reached":true,"windows":[]}));
    assert_eq!(remaining(&missing), Some(50.0));
}

#[test]
fn failures_back_off_and_changed_credentials_escape_invalid_auth_cache() {
    let cache = Cache::default();
    let failure = || {
        Err(Failure {
            message: "invalid".into(),
            auth_invalid: true,
            retry_after: None,
        }
        .into())
    };
    assert!(
        cache
            .get("old".into(), Freshness::Active, 100, failure)
            .is_err()
    );
    assert!(
        cache
            .get("old".into(), Freshness::Manual, 100000, || panic!(
                "must not poll invalid auth"
            ))
            .is_err()
    );
    assert!(
        cache
            .get("new".into(), Freshness::Active, 100, || Ok(quota(
                80.0, 100
            )))
            .is_ok()
    );
    for (at, next) in [(100, 160), (160, 460), (460, 1360)] {
        assert!(
            cache
                .get("network".into(), Freshness::Active, at, || anyhow::bail!(
                    "network"
                ))
                .is_err()
        );
        assert_eq!(
            cache.status("network", Freshness::Active)["next_check_at"],
            next
        );
        assert!(
            cache
                .get("network".into(), Freshness::Manual, next - 1, || panic!(
                    "backoff"
                ))
                .is_err()
        );
    }
    assert!(
        cache
            .get("limited".into(), Freshness::Manual, 100, || Err(Failure {
                message: "429".into(),
                auth_invalid: false,
                retry_after: Some(1000)
            }
            .into()))
            .is_err()
    );
    assert_eq!(
        cache.status("limited", Freshness::Manual)["next_check_at"],
        1100
    );
}

#[test]
fn concurrent_reads_share_one_fetch_and_persist_without_credentials() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("quota.json");
    let cache = Arc::new(Cache::default());
    cache.initialize(path.clone())?;
    let count = Arc::new(AtomicUsize::new(0));
    let mut joins = Vec::new();
    for _ in 0..4 {
        let cache = Arc::clone(&cache);
        let count = Arc::clone(&count);
        joins.push(std::thread::spawn(move || {
            cache
                .get("fingerprint".into(), Freshness::Manual, 100, || {
                    count.fetch_add(1, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(30));
                    Ok(quota(80.0, 100))
                })
                .unwrap()
        }));
    }
    let values = joins
        .into_iter()
        .map(|join| join.join().unwrap()["limits"].clone())
        .collect::<Vec<_>>();
    assert_eq!(values, vec![quota(80.0, 100)["limits"].clone(); 4]);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let reopened = Cache::default();
    reopened.initialize(path)?;
    assert_eq!(
        reopened.get("fingerprint".into(), Freshness::Active, 200, || panic!(
            "persisted cache"
        ))?["cached"],
        true
    );
    Ok(())
}

#[test]
fn response_observations_are_scoped_to_the_original_credentials() -> Result<()> {
    let cache = Cache::default();
    for name in ["old", "new"] {
        cache.get(name.into(), Freshness::Manual, 100, || Ok(quota(80.0, 100)))?;
    }
    let headers = HashMap::from([
        ("x-codex-primary-used-percent".into(), "99".into()),
        ("x-codex-primary-window-minutes".into(), "300".into()),
        ("x-codex-primary-reset-at".into(), "5000".into()),
    ]);
    cache.observe("old", &headers, 101);
    assert_eq!(
        (
            remaining(&cache.status("old", Freshness::Active)["quota"]),
            remaining(&cache.status("new", Freshness::Active)["quota"])
        ),
        (Some(1.0), Some(80.0))
    );
    Ok(())
}
