use super::tests::config;
use super::tests::pending;
use super::*;
use pretty_assertions::assert_eq;
use std::sync::mpsc;

#[test]
fn switching_drains_the_real_lease_and_preserves_queued_work() -> anyhow::Result<()> {
    let scheduler = Scheduler::new(config(), Journal::default());
    let lease = scheduler.acquire(pending("old", "tenant", "a"))?;
    lease.dispatch.start()?;
    let old_feedback = lease.dispatch.clone();
    let (entered, observed) = mpsc::channel();
    let switch = Arc::clone(&scheduler);
    let handle = std::thread::spawn(move || {
        switch.switch_account(|| {
            entered.send(()).unwrap();
            Ok(Activation::Changed)
        })
    });
    let deadline = Instant::now() + Duration::from_secs(2);
    while scheduler.status()["switching"] != true && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(scheduler.status()["switching"], true);
    let pending_scheduler = Arc::clone(&scheduler);
    let waiting =
        std::thread::spawn(move || pending_scheduler.acquire(pending("new", "tenant", "b")));
    assert!(observed.recv_timeout(Duration::from_millis(50)).is_err());
    lease.dispatch.confirmed(Observation {
        terminal: true,
        successful: true,
        ..Observation::default()
    });
    drop(lease);
    observed.recv_timeout(Duration::from_secs(2))?;
    assert_eq!(handle.join().unwrap()?, Activation::Changed);
    let next = waiting.join().unwrap()?;
    old_feedback.feedback(402, &HashMap::new());
    assert_eq!(scheduler.status()["account_unavailable"], false);
    drop(next);
    Ok(())
}

#[test]
fn unknown_outcomes_block_switching_and_failed_confirmation_stops_dispatch() -> anyhow::Result<()> {
    let scheduler = Scheduler::new(config(), Journal::default());
    let lease = scheduler.acquire(pending("unknown", "tenant", "a"))?;
    lease.dispatch.start()?;
    drop(lease);
    assert!(
        scheduler
            .switch_account(|| panic!("unknown outcome must not activate"))
            .is_err()
    );
    let scheduler = Scheduler::new(config(), Journal::default());
    assert!(
        scheduler
            .switch_account(|| anyhow::bail!("rollback acknowledgement failed"))
            .is_err()
    );
    assert_eq!(scheduler.status()["worker_fault"], true);
    Ok(())
}

#[test]
fn outcome_becoming_unknown_during_drain_never_activates_credentials() -> anyhow::Result<()> {
    let scheduler = Scheduler::new(config(), Journal::default());
    let lease = scheduler.acquire(pending("unknown", "tenant", "a"))?;
    lease.dispatch.start()?;
    let switch = Arc::clone(&scheduler);
    let handle = std::thread::spawn(move || {
        switch.switch_account(|| panic!("drain must recheck unknown outcomes"))
    });
    let deadline = Instant::now() + Duration::from_secs(2);
    while scheduler.status()["switching"] != true && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(scheduler.status()["switching"], true);
    drop(lease);
    assert!(handle.join().unwrap().is_err());
    assert_eq!(scheduler.status()["quarantined"], 1);
    Ok(())
}
