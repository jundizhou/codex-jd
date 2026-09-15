use super::*;

fn header(name: &str, value: &str) -> Header {
    Header::from_bytes(name.as_bytes(), value.as_bytes()).expect("valid test header")
}

#[test]
fn allows_requests_when_authentication_is_disabled() {
    assert!(is_authorized(&[], None));
}

#[test]
fn requires_matching_bearer_token() {
    let headers = vec![header("Authorization", "Bearer worker-secret")];
    assert!(is_authorized(&headers, Some("worker-secret")));
    assert!(!is_authorized(&headers, Some("other-secret")));

    let headers = vec![header("Authorization", "bearer worker-secret")];
    assert!(is_authorized(&headers, Some("worker-secret")));
}

#[test]
fn rejects_missing_malformed_and_duplicate_headers() {
    assert!(!is_authorized(&[], Some("worker-secret")));
    assert!(!is_authorized(
        &[header("Authorization", "worker-secret")],
        Some("worker-secret")
    ));
    assert!(!is_authorized(
        &[
            header("Authorization", "Bearer worker-secret"),
            header("authorization", "Bearer worker-secret"),
        ],
        Some("worker-secret")
    ));
}
