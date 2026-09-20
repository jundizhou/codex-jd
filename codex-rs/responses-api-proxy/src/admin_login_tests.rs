use super::*;
use pretty_assertions::assert_eq;
use std::fs;

#[test]
fn status_tracks_the_pending_session_and_start_supersedes_it() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let root = tmp.path().join("accounts");
    assert_eq!(status(), json!({"active": false}));
    let first = start(&root, "account-a")?;
    assert!(first["url"].as_str().unwrap().contains("oauth/authorize"));
    assert_eq!(status()["active"], json!(true));
    assert_eq!(status()["profile"], json!("account-a"));
    let second = start(&root, "account-b")?;
    assert_ne!(first["url"], second["url"]);
    assert_eq!(status()["profile"], json!("account-b"));
    *pending().lock().unwrap() = None;
    assert_eq!(status(), json!({"active": false}));
    Ok(())
}

#[test]
fn staging_homes_are_isolated_and_stale_ones_removed() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let root = tmp.path().join("accounts");
    let reply = start(&root, "account-a")?;
    assert!(reply["url"].as_str().is_some());
    let id = pending().lock().unwrap().as_ref().unwrap().id;
    assert!(root.join(format!(".login-{id}")).is_dir());
    // Profile directories must never collide with staging homes.
    fs::create_dir_all(root.join("account-a"))?;
    start(&root, "account-b")?;
    assert!(root.join("account-a").is_dir());
    let leftovers: Vec<_> = fs::read_dir(&root)?
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(".login-"))
        .collect();
    assert_eq!(
        leftovers.len(),
        1,
        "start must clear previous staging homes"
    );
    *pending().lock().unwrap() = None;
    Ok(())
}

#[test]
fn complete_requires_an_active_matching_session() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let root = tmp.path().join("accounts");
    assert!(
        complete(
            &root,
            "account-a",
            "http://localhost:1455/auth/callback?code=x&state=y",
            |_| Ok(json!({"ok": true}))
        )
        .is_err()
    );
    start(&root, "account-a")?;
    // A different profile name must not consume someone else's session.
    assert!(
        complete(
            &root,
            "account-b",
            "http://localhost:1455/auth/callback?code=x&state=y",
            |_| Ok(json!({"ok": true}))
        )
        .is_err()
    );
    assert_eq!(status()["profile"], json!("account-a"));
    assert!(complete(&root, "account-a", "garbage", |_| Ok(json!({"ok": true}))).is_err());
    *pending().lock().unwrap() = None;
    Ok(())
}

#[test]
fn failed_save_retains_credentials_and_retry_skips_token_exchange() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let root = tmp.path().join("accounts");
    start(&root, "plus-5")?;
    let staging = staging_dir(&root, pending().lock().unwrap().as_ref().unwrap().id);
    let credentials = json!({"OPENAI_API_KEY":"fresh-test-credential"});
    fs::write(staging.join("auth.json"), serde_json::to_vec(&credentials)?)?;
    assert!(
        complete(&root, "plus-5", "already-consumed", |_| anyhow::bail!(
            "busy"
        ))
        .is_err()
    );
    assert_eq!(status()["profile"], "plus-5");
    assert_eq!(read_auth(&staging.join("auth.json"))?, credentials);
    let result = complete(&root, "plus-5", "already-consumed", |value| {
        assert_eq!(value, &credentials);
        Ok(json!({"profile":"plus-1","updated":true}))
    })?;
    assert_eq!(result, json!({"profile":"plus-1","updated":true}));
    assert_eq!(status(), json!({"active":false}));
    assert!(!staging.exists());
    Ok(())
}
