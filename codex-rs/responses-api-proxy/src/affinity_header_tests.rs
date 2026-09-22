use super::key_for_http_request;
use pretty_assertions::assert_eq;
use serde_json::json;
use tiny_http::Header;

#[test]
fn sdk_session_header_shares_native_codex_metadata_binding() {
    let metadata = json!({"session_id": "desktop-task", "thread_id": "runtime-segment"});
    let expected = Some("session_id:desktop-task".to_string());
    for (body, headers) in [
        (
            json!({}),
            vec![Header::from_bytes("Session_Id", "desktop-task").unwrap()],
        ),
        (json!({"client_metadata": metadata}), vec![]),
        (json!({"metadata": metadata}), vec![]),
        (
            json!({"client_metadata": {"x-codex-turn-metadata": metadata.to_string()}}),
            vec![],
        ),
        (
            json!({}),
            vec![Header::from_bytes("x-codex-turn-metadata", metadata.to_string()).unwrap()],
        ),
    ] {
        assert_eq!(
            key_for_http_request(&serde_json::to_vec(&body).unwrap(), &headers).unwrap(),
            expected
        );
    }
}

#[test]
fn explicit_session_survives_thread_rotation_and_request_id_changes() {
    for segment in ["before-compression", "after-compression"] {
        let body = json!({"client_metadata": {"thread_id": segment}});
        let headers = vec![
            Header::from_bytes("session_id", "desktop-task").unwrap(),
            Header::from_bytes("thread_id", segment).unwrap(),
            Header::from_bytes("x-client-request-id", segment).unwrap(),
            Header::from_bytes("x-codex-queue-request-id", segment).unwrap(),
        ];
        assert_eq!(
            key_for_http_request(&serde_json::to_vec(&body).unwrap(), &headers).unwrap(),
            Some("session_id:desktop-task".to_string())
        );
    }
}

#[test]
fn conflicting_session_headers_and_metadata_are_rejected() {
    for body in [
        json!({"client_metadata": {"session_id": "other-task"}}),
        json!({"metadata": {"session_id": "other-task"}}),
        json!({"client_metadata": {"x-codex-turn-metadata": "{\"session_id\":\"other-task\"}"}}),
    ] {
        let headers = vec![Header::from_bytes("session_id", "desktop-task").unwrap()];
        assert_eq!(
            key_for_http_request(&serde_json::to_vec(&body).unwrap(), &headers)
                .unwrap_err()
                .to_string(),
            "conflicting session_id values"
        );
    }
    let headers = vec![
        Header::from_bytes("session_id", "desktop-task").unwrap(),
        Header::from_bytes("x-codex-turn-metadata", "{\"session_id\":\"other-task\"}").unwrap(),
    ];
    assert_eq!(
        key_for_http_request(b"{}", &headers)
            .unwrap_err()
            .to_string(),
        "conflicting session_id values"
    );
}

#[test]
fn matching_session_headers_and_metadata_are_accepted() {
    let body = json!({"client_metadata": {"session_id": "desktop-task"}});
    let headers = vec![
        Header::from_bytes("session_id", "desktop-task").unwrap(),
        Header::from_bytes("x-codex-turn-metadata", "{\"session_id\":\"desktop-task\"}").unwrap(),
    ];
    assert_eq!(
        key_for_http_request(&serde_json::to_vec(&body).unwrap(), &headers).unwrap(),
        Some("session_id:desktop-task".to_string())
    );
}

#[test]
fn duplicate_case_insensitive_session_headers_are_rejected() {
    let headers = vec![
        Header::from_bytes("session_id", "desktop-task").unwrap(),
        Header::from_bytes("Session_Id", "desktop-task").unwrap(),
    ];
    assert_eq!(
        key_for_http_request(b"{}", &headers)
            .unwrap_err()
            .to_string(),
        "duplicate session_id header"
    );
}
