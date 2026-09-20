use super::*;
use pretty_assertions::assert_eq;

#[test]
fn account_console_snapshot() {
    insta::assert_snapshot!("account_console", include_str!("admin_accounts.html"));
}

#[test]
fn accounts_preserve_current_credentials_and_protect_active_deletion() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let auth = tmp.path().join("auth.json");
    let root = tmp.path().join("accounts");
    let first = json!({"OPENAI_API_KEY":"first-login-identity-0001"});
    let second = json!({"tokens":{"account_id":"user@example.com","access_token":"a","refresh_token":"r","id_token":"i"}});
    write_private(&auth, &first)?;
    let store = Store::new(&root, &auth, 6);
    store.add("a", &first)?;
    store.add("b", &second)?;
    assert!(store.add("duplicate", &first).is_err());
    assert!(store.add("../escape", &second).is_err());
    assert!(store.add("invalid", &json!({})).is_err());
    assert!(store.delete("a").is_err());
    assert_eq!(
        store.list()?,
        json!({
            "profiles": [
                {"name":"a","account":"account-***0001","active":true,"valid":true},
                {"name":"b","account":"u***@example.com","active":false,"valid":true}
            ],
            "current": "account-***0001"
        })
    );
    store.activate("b")?;
    assert_eq!(read_auth(&auth)?, second);
    assert_eq!(
        store.list()?,
        json!({
            "profiles": [
                {"name":"a","account":"account-***0001","active":false,"valid":true},
                {"name":"b","account":"u***@example.com","active":true,"valid":true}
            ],
            "current": "u***@example.com"
        })
    );
    store.delete("a")?;
    assert_eq!(store.names()?, vec!["b"]);
    assert!(store.delete("b").is_err());
    Ok(())
}

#[test]
fn short_identifiers_reveal_no_suffix() -> anyhow::Result<()> {
    assert_eq!(masked_account("short"), "account-***");
    assert_eq!(masked_account(""), "account-***");
    Ok(())
}

#[test]
fn login_label_prefers_masked_id_token_email() -> anyhow::Result<()> {
    use base64::Engine;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(br#"{"email":"alice@example.com"}"#);
    let with_jwt = json!({"tokens":{
        "account_id":"opaque-account-identifier-123",
        "access_token":"a",
        "refresh_token":"r",
        "id_token":format!("header.{payload}.signature")}});
    assert_eq!(login_label(&with_jwt), "a***@example.com");
    let without_jwt = json!({"OPENAI_API_KEY":"first-login-identity-0001"});
    assert_eq!(login_label(&without_jwt), "account-***0001");
    Ok(())
}

#[test]
fn settings_survive_reload_and_reject_excess_capacity() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let auth = tmp.path().join("auth.json");
    let store = Store::new(tmp.path(), &auth, 6);
    write_private(&auth.with_file_name("admin-concurrency.json"), &json!(4))?;
    assert_eq!(store.limit(2)?, 4);
    write_private(&auth.with_file_name("admin-concurrency.json"), &json!(7))?;
    assert!(store.limit(2).is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn rejects_symlink_profiles_and_writes_private_files() -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::fs::symlink;
    let tmp = tempfile::tempdir()?;
    let auth = tmp.path().join("auth.json");
    let root = tmp.path().join("accounts");
    fs::create_dir(&root)?;
    write_private(&auth, &json!({"OPENAI_API_KEY":"first"}))?;
    symlink(tmp.path(), root.join("linked"))?;
    let store = Store::new(&root, &auth, 6);
    assert!(store.profile("linked").is_err());
    assert_eq!(fs::metadata(&auth)?.permissions().mode() & 0o777, 0o600);
    Ok(())
}

#[test]
fn first_account_can_be_activated_without_an_existing_login() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let auth = tmp.path().join("auth.json");
    let root = tmp.path().join("accounts");
    let credentials = json!({"OPENAI_API_KEY":"first-account-0001"});
    let store = Store::new(&root, &auth, /*capacity*/ 6);
    store.add("first", &credentials)?;
    store.activate("first")?;
    assert_eq!(read_auth(&auth)?, credentials);
    assert_eq!(
        store.list()?,
        json!({
            "profiles": [{"name":"first","account":"account-***0001","active":true,"valid":true}],
            "current": "account-***0001"
        })
    );
    Ok(())
}

#[test]
fn malformed_saved_profile_can_be_deleted_without_removing_active_credentials() -> anyhow::Result<()>
{
    let root = tempfile::tempdir()?;
    let auth = root.path().join("auth.json");
    let profiles = root.path().join("accounts");
    let active = json!({"OPENAI_API_KEY":"active-credential"});
    write_private(&auth, &active)?;
    fs::create_dir_all(profiles.join("broken"))?;
    fs::write(profiles.join("broken/auth.json"), b"{invalid")?;
    let store = Store::new(&profiles, &auth, /*capacity*/ 2);
    store.delete("broken")?;
    assert_eq!(store.names()?, Vec::<String>::new());
    assert_eq!(read_auth(&auth)?, active);
    Ok(())
}

#[test]
fn reauthorization_replaces_revoked_tokens_and_preserves_profile_name() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let auth = tmp.path().join("auth.json");
    let root = tmp.path().join("accounts");
    let old = json!({"tokens":{"account_id":"same-account","access_token":"revoked","refresh_token":"old-refresh","id_token":"old-id"}});
    let fresh = json!({"tokens":{"account_id":"same-account","access_token":"fresh","refresh_token":"new-refresh","id_token":"new-id"}});
    write_private(&auth, &old)?;
    let store = Store::new(&root, &auth, /*capacity*/ 2);
    store.add("plus-1", &old)?;
    let result = store.save_login("plus-5", &fresh, |value| write_private(&auth, value))?;
    assert_eq!(
        result,
        json!({"ok":true,"profile":"plus-1","updated":true,"active":true})
    );
    assert_eq!(store.names()?, vec!["plus-1"]);
    assert_eq!(
        (read_auth(&auth)?, read_auth(&store.profile("plus-1")?)?),
        (fresh.clone(), fresh)
    );
    Ok(())
}

#[test]
fn reauthorization_of_inactive_account_does_not_replace_current_login() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let auth = tmp.path().join("auth.json");
    let root = tmp.path().join("accounts");
    let active = json!({"OPENAI_API_KEY":"another-account"});
    let fresh = json!({"tokens":{"account_id":"saved-account","access_token":"fresh","refresh_token":"refresh","id_token":"id"}});
    write_private(&auth, &active)?;
    let store = Store::new(&root, &auth, /*capacity*/ 2);
    store.add("saved", &fresh)?;
    let result = store.save_login("new-label", &fresh, |_| panic!("must not activate"))?;
    assert_eq!(
        result,
        json!({"ok":true,"profile":"saved","updated":true,"active":false})
    );
    assert_eq!(read_auth(&auth)?, active);
    assert_eq!(store.names()?, vec!["saved"]);
    Ok(())
}

#[test]
fn reauthorization_keeps_fresh_profile_when_active_replacement_is_busy() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let auth = tmp.path().join("auth.json");
    let root = tmp.path().join("accounts");
    let old = json!({"tokens":{"account_id":"same","access_token":"old","refresh_token":"old","id_token":"old"}});
    let fresh = json!({"tokens":{"account_id":"same","access_token":"fresh","refresh_token":"fresh","id_token":"fresh"}});
    write_private(&auth, &old)?;
    let store = Store::new(&root, &auth, /*capacity*/ 2);
    store.add("saved", &old)?;
    assert!(
        store
            .save_login("saved", &fresh, |_| anyhow::bail!("busy"))
            .is_err()
    );
    assert_eq!(
        (read_auth(&auth)?, read_auth(&store.profile("saved")?)?),
        (old, fresh)
    );
    Ok(())
}

#[test]
fn first_saved_profile_uses_fresh_login_instead_of_stale_active_tokens() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let auth = tmp.path().join("auth.json");
    let root = tmp.path().join("accounts");
    let old = json!({"tokens":{"account_id":"same","access_token":"old","refresh_token":"old","id_token":"old"}});
    let fresh = json!({"tokens":{"account_id":"same","access_token":"fresh","refresh_token":"fresh","id_token":"fresh"}});
    write_private(&auth, &old)?;
    let store = Store::new(&root, &auth, /*capacity*/ 2);
    assert_eq!(
        store.save_login("first", &fresh, |value| write_private(&auth, value))?,
        json!({"ok":true,"profile":"first","updated":false,"active":true})
    );
    assert_eq!(
        (read_auth(&auth)?, read_auth(&store.profile("first")?)?),
        (fresh.clone(), fresh)
    );
    Ok(())
}
