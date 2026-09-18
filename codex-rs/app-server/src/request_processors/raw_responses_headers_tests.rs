use super::*;
use pretty_assertions::assert_eq;

#[test]
fn preserves_allowed_headers_and_normalizes_names() {
    let supplied = HashMap::from([
        ("X-Codex-Turn-State".to_string(), "opaque-token".to_string()),
        (
            "x-codex-inference-call-id".to_string(),
            "caller-id".to_string(),
        ),
        ("traceparent".to_string(), "00-abc-def-01".to_string()),
        ("tracestate".to_string(), "vendor=value".to_string()),
    ]);
    let expected = supplied
        .iter()
        .map(|(name, value)| {
            (
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            )
        })
        .collect::<HeaderMap>();
    assert_eq!(request_headers(supplied).unwrap(), expected);
}

#[test]
fn generates_distinct_ids_without_reusing_routing_or_trace_state() {
    let first = request_headers(HashMap::new()).unwrap();
    let second = request_headers(HashMap::new()).unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    for headers in [&first, &second] {
        let id = Uuid::parse_str(headers["x-codex-inference-call-id"].to_str().unwrap()).unwrap();
        assert_eq!(id.get_version(), Some(uuid::Version::Random));
    }
    assert_ne!(first, second);
}

#[test]
fn rejects_unsafe_headers() {
    for supplied in [
        HashMap::from([("authorization".to_string(), "secret".to_string())]),
        HashMap::from([("traceparent".to_string(), "a\r\nb".to_string())]),
        HashMap::from([("tracestate".to_string(), "x".repeat(8193))]),
        HashMap::from([
            ("Traceparent".to_string(), "one".to_string()),
            ("traceparent".to_string(), "two".to_string()),
        ]),
    ] {
        assert!(request_headers(supplied).is_err());
    }
}

#[test]
fn returns_only_bounded_upstream_routing_state() {
    let mut upstream = HeaderMap::new();
    upstream.insert("set-cookie", HeaderValue::from_static("secret"));
    assert_eq!(response_headers(&upstream), HashMap::new());
    upstream.insert("x-codex-turn-state", HeaderValue::from_static("next-token"));
    assert_eq!(
        response_headers(&upstream),
        HashMap::from([("x-codex-turn-state".to_string(), "next-token".to_string()),])
    );
    upstream.insert(
        "x-codex-turn-state",
        HeaderValue::from_str(&"x".repeat(8193)).unwrap(),
    );
    assert_eq!(response_headers(&upstream), HashMap::new());
}

#[test]
fn preserves_retry_hint_and_media_type_but_rejects_ambiguous_values() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "retry-after",
        HeaderValue::from_static("Wed, 21 Oct 2037 07:28:00 GMT"),
    );
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    headers.insert("authorization", HeaderValue::from_static("secret"));
    assert_eq!(
        response_headers(&headers),
        HashMap::from([
            ("retry-after".into(), "Wed, 21 Oct 2037 07:28:00 GMT".into()),
            ("content-type".into(), "application/json".into()),
        ])
    );
    headers.append("retry-after", HeaderValue::from_static("5"));
    headers.insert(
        "content-type",
        HeaderValue::from_str(&"a".repeat(1025)).unwrap(),
    );
    assert_eq!(response_headers(&headers), HashMap::new());
}
