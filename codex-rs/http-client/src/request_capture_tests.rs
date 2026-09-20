use super::*;
use pretty_assertions::assert_eq;

#[test]
fn captures_actual_prepared_body_and_redacts_credentials() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let request = reqwest::Client::new()
        .post("https://chatgpt.com/backend-api/codex/responses?token=secret")
        .header("x-client-request-id", "test-thread")
        .header("authorization", "Bearer private")
        .header("cookie", "session=private")
        .header("originator", "codex")
        .body(r#"{"input":[],"metadata":{"session":"real"}}"#)
        .build()?;
    capture(&request, dir.path())?;
    let data: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("test-thread.json"))?)?;
    assert_eq!(
        data["body"],
        r#"{"input":[],"metadata":{"session":"real"}}"#
    );
    assert_eq!(
        data["url"],
        "https://chatgpt.com/backend-api/codex/responses"
    );
    let headers: std::collections::BTreeMap<_, _> = data["headers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| (h["name"].as_str().unwrap(), h["value"].as_str().unwrap()))
        .collect();
    assert_eq!(
        headers,
        std::collections::BTreeMap::from([
            ("authorization", "[REDACTED]"),
            ("cookie", "[REDACTED]"),
            ("originator", "codex"),
            ("x-client-request-id", "test-thread")
        ])
    );
    Ok(())
}
