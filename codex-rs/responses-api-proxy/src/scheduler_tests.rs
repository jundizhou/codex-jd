use super::*;
use pretty_assertions::assert_eq;

fn config() -> Config {
    Config {
        automatic_recovery: false,
        max_running: 2,
        gap: Duration::ZERO,
        user_gap: Duration::ZERO,
        tool_gap: Duration::ZERO,
        idle_ttl: Duration::from_secs(900),
        start_gap: Duration::ZERO,
        timeout: Duration::from_secs(5),
    }
}

fn pending(id: &str, tenant: &str, conversation: &str) -> Pending {
    Pending {
        id: id.into(),
        key: Key(tenant.into(), conversation.into()),
        sticky: true,
        fingerprint: id.into(),
        evidence: Evidence::default(),
        bytes: 1,
        deadline: Instant::now() + Duration::from_secs(5),
    }
}

#[test]
fn recovery_rechecks_gates_and_only_releases_the_checked_conversation() {
    for gate in ["none", "paused", "account", "cooldown", "disk"] {
        let scheduler = Scheduler::new(config(), Journal::default());
        let lease = scheduler
            .acquire(pending("lost", "user", "conversation"))
            .unwrap();
        let (id, key) = (lease.dispatch.id.clone(), lease.key.digest());
        lease.dispatch.start().unwrap();
        drop(lease);
        {
            let mut state = scheduler.state.lock().unwrap();
            state.paused = gate == "paused";
            state.throttle.blocked = gate == "account";
            state.uncertain = gate == "disk";
            state.throttle.until =
                (gate == "cooldown").then(|| Instant::now() + Duration::from_secs(30));
        }
        scheduler.complete_recovery(&id, "wrong-conversation", Ok(()));
        assert_eq!(scheduler.status()["quarantined"], 1);
        scheduler.complete_recovery(&id, &key, Ok(()));
        assert_eq!(
            scheduler.status()["quarantined"],
            usize::from(gate != "none")
        );
        if gate == "none" {
            assert_eq!(
                scheduler
                    .acquire(pending("lost", "user", "conversation"))
                    .err(),
                Some(Rejection::OutcomeUnknown)
            );
            assert!(
                scheduler
                    .acquire(pending("new", "user", "conversation"))
                    .is_ok()
            );
        } else {
            assert_eq!(scheduler.status()["recovery"]["released_unknown"], 0);
        }
    }
}

#[test]
fn admission_bounds_memory_and_releases_every_reservation() {
    let scheduler = Scheduler::new(config(), Journal::default());
    let admissions: Vec<_> = (0..4).map(|_| scheduler.admit(MAX_BODY).unwrap()).collect();
    assert_eq!(scheduler.admit(MAX_BODY).err(), Some(Rejection::Full));
    drop(admissions);
    let state = scheduler.state.lock().unwrap();
    assert_eq!((state.inbound, state.bytes), (0, 0));
}

#[test]
fn completed_conversations_do_not_exhaust_execution_slots() {
    let scheduler = Scheduler::new(config(), Journal::default());
    for i in 0..40 {
        let lease = scheduler
            .acquire(pending(
                &format!("req-{i}"),
                &format!("tenant-{i}"),
                "same-id",
            ))
            .unwrap();
        lease.dispatch.start().unwrap();
        lease.dispatch.confirmed(Observation::default());
    }
    let _lease = scheduler
        .acquire(pending("follow-up", "tenant-0", "same-id"))
        .unwrap();
    assert_eq!(scheduler.status()["running"], 1);
    assert_eq!(scheduler.status()["conversations"], 40);
}

#[test]
fn round_robin_skips_cooling_conversations_and_preserves_fifo() {
    let now = Instant::now();
    let a = Key("tenant-a".into(), "a".into());
    let b = Key("tenant-a".into(), "b".into());
    let c = Key("tenant-b".into(), "c".into());
    let mut state = State {
        pending: vec![
            pending("a1", "tenant-a", "a"),
            pending("a2", "tenant-a", "a"),
            pending("b1", "tenant-a", "b"),
            pending("c1", "tenant-b", "c"),
        ],
        tenants: VecDeque::from([a.0.clone(), c.0.clone()]),
        conversations: VecDeque::from([a.clone(), b, c]),
        bindings: HashMap::from([(
            a,
            Binding {
                finished: Some(now),
                signals: Signals::default(),
            },
        )]),
        ..State::default()
    };
    let config = Config {
        gap: Duration::from_millis(800),
        ..config()
    };
    assert_eq!(state.selected(now, &config), Some("b1"));
    let _ = state.remove("b1");
    assert_eq!(state.selected(now, &config), Some("c1"));
    let _ = state.remove("c1");
    assert_eq!(
        state.selected(now + Duration::from_millis(799), &config),
        None
    );
    assert_eq!(
        state.selected(now + Duration::from_millis(800), &config),
        Some("a1")
    );
    let _ = state.remove("a1");
    assert_eq!(
        state.selected(now + Duration::from_secs(1), &config),
        Some("a2")
    );
}

#[test]
fn preparing_and_running_requests_cannot_overlap_the_same_conversation() {
    let scheduler = Scheduler::new(config(), Journal::default());
    let first = scheduler
        .acquire(pending("first", "user", "conversation"))
        .unwrap();
    {
        let state = scheduler.state.lock().unwrap();
        assert_eq!(
            state.dispatching.as_deref(),
            Some(digest(&[b"user", b"first"]).as_str())
        );
        assert_eq!(state.next_start, None);
    }
    first.dispatch.start().unwrap();
    let follower = {
        let scheduler = Arc::clone(&scheduler);
        std::thread::spawn(move || scheduler.acquire(pending("second", "user", "conversation")))
    };
    let mut state = scheduler.state.lock().unwrap();
    while state.pending.is_empty() {
        let (next, timed) = scheduler
            .changed
            .wait_timeout(state, Duration::from_secs(5))
            .unwrap();
        state = next;
        assert!(!timed.timed_out(), "follower did not enqueue");
    }
    assert_eq!(state.selected(Instant::now(), &config()), None);
    drop(state);
    scheduler.cancel("user".into(), "second".into()).unwrap();
    assert_eq!(follower.join().unwrap().err(), Some(Rejection::Cancelled));
    first.dispatch.confirmed(Observation::default());
    drop(first);
    assert_eq!(scheduler.status()["running"], 0);
    assert_eq!(scheduler.status()["pending"], 0);
}

#[test]
fn cancellation_before_arrival_and_expiration_never_allocate_identities() {
    let scheduler = Scheduler::new(config(), Journal::default());
    scheduler.cancel("user".into(), "cancelled".into()).unwrap();
    assert_eq!(
        scheduler.acquire(pending("cancelled", "user", "a")).err(),
        Some(Rejection::Cancelled)
    );
    let mut expired = pending("expired", "user", "b");
    expired.deadline = Instant::now();
    assert_eq!(scheduler.acquire(expired).err(), Some(Rejection::Expired));
    assert_eq!(scheduler.status()["conversations"], 0);
}

#[test]
fn unknown_outcome_isolates_its_conversation_and_retains_one_slot() {
    let scheduler = Scheduler::new(config(), Journal::default());
    let lease = scheduler.acquire(pending("first", "user", "a")).unwrap();
    lease.dispatch.start().unwrap();
    let preparing = scheduler
        .acquire(pending("preparing", "user", "b"))
        .unwrap();
    drop(lease);
    preparing.dispatch.start().unwrap();
    assert_eq!(scheduler.status()["running"], 2);
    preparing.dispatch.confirmed(Observation::default());
    drop(preparing);
    assert_eq!(
        scheduler
            .acquire(pending("same-conversation", "user", "a"))
            .err(),
        Some(Rejection::Unavailable)
    );
    assert_eq!(scheduler.status()["outcome_unknown"], true);
    assert_eq!(scheduler.status()["running"], 1);
    let other = scheduler.acquire(pending("second", "user", "b")).unwrap();
    other.dispatch.start().unwrap();
    drop(other);
    assert_eq!(scheduler.status()["quarantined"], 2);
    assert_eq!(
        scheduler
            .acquire(pending("no-capacity", "other-user", "c"))
            .err(),
        Some(Rejection::Unavailable)
    );
    scheduler.pause();
    scheduler.recover().unwrap();
    scheduler.resume().unwrap();
    assert!(scheduler.acquire(pending("recovered", "user", "a")).is_ok());
}

#[test]
fn restart_restores_quarantined_capacity_and_conversation_isolation() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state.json");
    let scheduler = Scheduler::new(config(), Journal::open(Some(&path)).unwrap());
    let lease = scheduler.acquire(pending("first", "user", "a")).unwrap();
    lease.dispatch.start().unwrap();
    drop(lease);
    drop(scheduler);
    let scheduler = Scheduler::new(config(), Journal::open(Some(&path)).unwrap());
    assert_eq!(
        scheduler
            .acquire(pending("same-conversation", "user", "a"))
            .err(),
        Some(Rejection::Unavailable)
    );
    let other = scheduler.acquire(pending("second", "user", "b")).unwrap();
    other.dispatch.start().unwrap();
    assert_eq!(scheduler.status()["running"], 2);
    scheduler.pause();
    assert_eq!(scheduler.recover(), Err(Rejection::Duplicate));
    other.dispatch.confirmed(Observation::default());
    drop(other);
    scheduler.recover().unwrap();
    scheduler.resume().unwrap();
    drop(scheduler);
    assert!(Journal::open(Some(&path)).unwrap().unfinished().is_empty());
}

#[test]
fn global_start_gap_and_capacity_are_hard_gates() {
    let now = Instant::now();
    let mut state = State {
        pending: vec![pending("p", "tenant", "conversation")],
        tenants: VecDeque::from(["tenant".into()]),
        conversations: VecDeque::from([Key("tenant".into(), "conversation".into())]),
        next_start: Some(now + Duration::from_millis(300)),
        ..State::default()
    };
    assert_eq!(state.selected(now, &config()), None);
    assert_eq!(
        state.selected(now + Duration::from_millis(300), &config()),
        Some("p")
    );
    state
        .running
        .insert(Key("other".into(), "a".into()), "r1".into());
    state
        .running
        .insert(Key("other".into(), "b".into()), "r2".into());
    assert_eq!(
        state.selected(now + Duration::from_secs(1), &config()),
        None
    );
}

#[test]
fn cancelling_during_connection_setup_prevents_rpc_and_returns_permit() {
    let scheduler = Scheduler::new(config(), Journal::default());
    let lease = scheduler
        .acquire(pending("preparing", "tenant", "a"))
        .unwrap();
    scheduler
        .cancel("tenant".into(), "preparing".into())
        .unwrap();
    assert!(lease.dispatch.start().is_err());
    drop(lease);
    let _lease = scheduler.acquire(pending("next", "tenant", "b")).unwrap();
    assert_eq!(scheduler.status()["running"], 1);
    assert_eq!(scheduler.status()["outcome_unknown"], false);
}

#[test]
fn idle_timing_cache_eviction_allows_same_conversation_to_queue() {
    let scheduler = Scheduler::new(config(), Journal::default());
    let first = scheduler.acquire(pending("first", "user", "old")).unwrap();
    first.dispatch.start().unwrap();
    first.dispatch.confirmed(Observation::default());
    drop(first);
    scheduler
        .state
        .lock()
        .unwrap()
        .bindings
        .get_mut(&Key("user".into(), "old".into()))
        .unwrap()
        .finished = Some(Instant::now() - Duration::from_secs(901));
    let next = scheduler.acquire(pending("next", "user", "new")).unwrap();
    assert_eq!(scheduler.status()["running"], 1);
    drop(next);
    let _resumed = scheduler
        .acquire(pending("old-again", "user", "old"))
        .unwrap();
    assert_eq!(scheduler.status()["running"], 1);
}

#[test]
fn completed_ids_and_changed_bodies_are_not_dispatched_again() {
    let scheduler = Scheduler::new(config(), Journal::default());
    let lease = scheduler
        .acquire(pending("request", "user", "conversation"))
        .unwrap();
    lease.dispatch.start().unwrap();
    lease.dispatch.confirmed(Observation::default());
    drop(lease);
    assert_eq!(
        scheduler
            .acquire(pending("request", "user", "conversation"))
            .err(),
        Some(Rejection::Completed)
    );
    let mut changed = pending("request", "user", "conversation");
    changed.fingerprint = "changed".into();
    assert_eq!(scheduler.acquire(changed).err(), Some(Rejection::Conflict));
}

#[test]
fn running_cancellation_waits_for_confirmed_completion() {
    let scheduler = Scheduler::new(config(), Journal::default());
    let lease = scheduler
        .acquire(pending("request", "user", "conversation"))
        .unwrap();
    lease.dispatch.start().unwrap();
    scheduler.cancel("user".into(), "request".into()).unwrap();
    assert!(lease.dispatch.finalizing_deadline().is_some());
    assert_eq!(scheduler.status()["running"], 1);
    lease.dispatch.confirmed(Observation::default());
    lease.settle();
    drop(lease);
    assert_eq!(scheduler.status()["running"], 0);
    assert_eq!(scheduler.status()["outcome_unknown"], false);
}

#[test]
fn paused_and_cooling_accounts_cannot_dispatch_preparing_requests() {
    let scheduler = Scheduler::new(config(), Journal::default());
    let lease = scheduler
        .acquire(pending("preparing", "user", "a"))
        .unwrap();
    scheduler.pause();
    assert!(lease.dispatch.start().is_err());
    drop(lease);
    assert_eq!(
        scheduler.acquire(pending("paused", "user", "b")).err(),
        Some(Rejection::Paused)
    );
    scheduler.resume().unwrap();
    let lease = scheduler
        .acquire(pending("throttled", "user", "c"))
        .unwrap();
    lease
        .dispatch
        .feedback(429, &HashMap::from([("retry-after".into(), "3600".into())]));
    assert!(lease.dispatch.start().is_err());
    drop(lease);
    assert!(matches!(
        scheduler.acquire(pending("cooling", "user", "d")).err(),
        Some(Rejection::Cooldown(_))
    ));
}

#[test]
fn tool_priority_yields_after_two_starts_and_near_deadlines_win() {
    let now = Instant::now();
    let tool_key = Key("tenant".into(), "tool".into());
    let mut tool = pending("tool", "tenant", "tool");
    tool.evidence = Evidence::read(
        &serde_json::json!({"previous_response_id":"response", "input":[{"type":"function_call_output", "call_id":"call", "output":"done"}]}),
    );
    let normal = pending("normal", "tenant", "normal");
    let mut state = State {
        conversations: VecDeque::from([normal.key.clone(), tool.key.clone()]),
        pending: vec![normal, tool],
        tenants: VecDeque::from(["tenant".into()]),
        bindings: HashMap::from([(
            tool_key.clone(),
            Binding {
                finished: Some(now),
                signals: Signals {
                    input: Evidence::default(),
                    finished: Some(now),
                    output: Observation {
                        valid: true,
                        response_id: Some("response".into()),
                        calls: vec!["call".into()],
                        ..Observation::default()
                    },
                },
            },
        )]),
        ..State::default()
    };
    assert_eq!(state.selected(now, &config()), Some("tool"));
    state.tool_streak = Some((tool_key, 2));
    assert_eq!(state.selected(now, &config()), Some("normal"));
    state.tool_streak = None;
    state.pending[0].deadline = now + Duration::from_millis(500);
    assert_eq!(state.selected(now, &config()), Some("normal"));
}

#[test]
fn confirmed_quota_exhaustion_disables_dispatch_until_explicit_recovery() {
    let scheduler = Scheduler::new(config(), Journal::default());
    let lease = scheduler
        .acquire(pending("quota", "user", "conversation"))
        .unwrap();
    lease.dispatch.start().unwrap();
    lease.dispatch.confirmed(Observation {
        quota_exhausted: true,
        ..Observation::default()
    });
    drop(lease);
    assert_eq!(
        scheduler
            .acquire(pending("next", "user", "conversation"))
            .err(),
        Some(Rejection::Paused)
    );
    assert_eq!(scheduler.resume(), Err(Rejection::Unavailable));
    scheduler.pause();
    scheduler.recover().unwrap();
    scheduler.resume().unwrap();
    assert!(
        scheduler
            .acquire(pending("after-recovery", "user", "conversation"))
            .is_ok()
    );
}

#[test]
fn admin_limit_applies_without_cancelling_running_work() -> anyhow::Result<()> {
    let scheduler = Scheduler::new(config(), Journal::default());
    let lease = scheduler.acquire(pending("active", "user", "conversation"))?;
    assert!(scheduler.switch_idle(|| Ok(())).is_err());
    scheduler.set_limit(1, || Ok(()))?;
    {
        let state = scheduler.state.lock().unwrap();
        assert_eq!(
            (state.max_running, state.throttle.limit, state.running.len()),
            (1, 1, 1)
        );
    }
    assert!(
        scheduler
            .set_limit(3, || anyhow::bail!("disk full"))
            .is_err()
    );
    assert_eq!(scheduler.state.lock().unwrap().max_running, 1);
    drop(lease);
    Ok(())
}
