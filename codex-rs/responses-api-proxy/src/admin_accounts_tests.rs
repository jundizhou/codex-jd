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
