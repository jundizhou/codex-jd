use super::*;
use pretty_assertions::assert_eq;

#[test]
fn journal_locks_owner_and_preserves_dispatch_across_restart() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state.json");
    let mut journal = Journal::open(Some(&path)).unwrap();
    assert!(Journal::open(Some(&path)).is_err());
    journal
        .start("request", "fingerprint", Some("binding"))
        .unwrap();
    drop(journal);
    let mut journal = Journal::open(Some(&path)).unwrap();
    assert_eq!(
        journal.unfinished(),
        HashMap::from([("request".into(), Some("binding".into()))])
    );
    assert_eq!(journal.unknown_conversations(), vec!["binding"]);
    assert_eq!(
        journal.check("request", "changed"),
        Err(Rejection::Conflict)
    );
    assert_eq!(
        journal.check("request", "fingerprint"),
        Err(Rejection::Duplicate)
    );
    journal.acknowledge_unknown().unwrap();
    assert!(journal.unfinished().is_empty());
    assert_eq!(
        journal.check("request", "fingerprint"),
        Err(Rejection::Completed)
    );
    drop(journal);
    assert!(Journal::open(Some(&path)).unwrap().unfinished().is_empty());
}

#[test]
fn corrupt_journal_fails_closed_and_finished_records_expire() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state.json");
    std::fs::write(&path, b"{\"records\":").unwrap();
    assert!(Journal::open(Some(&path)).is_err());
    let mut journal = Journal::default();
    journal.start("request", "body", None).unwrap();
    journal
        .state
        .records
        .get_mut("request")
        .unwrap()
        .finished_at = Some(now() - 601);
    assert_eq!(journal.check("request", "body"), Ok(()));
    assert_eq!(journal.state.records.len(), 0);
}

#[test]
fn restarting_does_not_erase_account_cooldown() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state.json");
    let mut journal = Journal::open(Some(&path)).unwrap();
    journal
        .save_account(&crate::queue_throttle::Throttle {
            until: Some(std::time::Instant::now() + std::time::Duration::from_secs(3600)),
            limit: 1,
            ..crate::queue_throttle::Throttle::default()
        })
        .unwrap();
    drop(journal);
    let journal = Journal::open(Some(&path)).unwrap();
    let (cooldown, blocked, limit) = journal.account();
    assert!(cooldown.unwrap() >= std::time::Duration::from_secs(3599));
    assert_eq!((blocked, limit), (false, 1));
}

#[test]
fn recovery_cycles_survive_restart_and_release_never_acknowledges_completion() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("queue.json");
    let mut journal = Journal::open(Some(&path)).unwrap();
    journal.start("old", "body", Some("conversation")).unwrap();
    journal.quarantine("old").unwrap();
    let first = journal.state.records["old"].next_check_at.unwrap();
    assert!(journal.recovery_due(first - 1).is_empty());
    for offset in [0, 20, 50, 60, 80, 110] {
        let at = first + offset;
        assert_eq!(journal.state.records["old"].next_check_at, Some(at));
        assert_eq!(
            journal.recovery_due(at),
            vec![("old".into(), "conversation".into())]
        );
        journal
            .recovery_result("old", &Err(anyhow::anyhow!("probe failed")), at)
            .unwrap();
        drop(journal);
        journal = Journal::open(Some(&path)).unwrap();
    }
    journal
        .recovery_result("old", &Ok(()), first + 120)
        .unwrap();
    drop(journal);
    let mut journal = Journal::open(Some(&path)).unwrap();
    assert!(journal.unfinished().is_empty());
    assert!(journal.unknown_conversations().is_empty());
    assert_eq!(journal.state.records["old"].finished_at, None);
    assert_eq!(journal.check("old", "body"), Err(Rejection::OutcomeUnknown));
    assert_eq!(journal.check("old", "changed"), Err(Rejection::Conflict));
    assert_eq!(journal.check("next", "next"), Ok(()));
    assert_eq!(
        journal.recovery_status(),
        serde_json::json!({"released_unknown": 1, "next_check_at": null})
    );
}
