use super::raw_responses_headers;
use http::HeaderMap;
use http::HeaderName;
use http::HeaderValue;
use pretty_assertions::assert_eq;

#[test]
fn preserves_application_headers_and_excludes_caller_transport_state() {
    let preserved = [
        ("X-OpenAI-Internal-Codex-Responses-Lite", "true"),
        ("X-Codex-Beta-Features", "feature-a,feature-b"),
        ("X-Future-Application-Header", "opaque value"),
        ("Content-Type", "application/json"),
        ("X-Codex-Turn-Metadata", "{}"),
        ("Session_Id", "client-session"),
        ("X-Stainless-Lang", "python"),
        ("Traceparent", "application-trace"),
        ("Tracestate", "vendor=value"),
        ("Baggage", "request-kind=smoke"),
        ("X-Request-ID", "application-request"),
    ];
    let supplied = preserved.into_iter().chain([
        ("Authorization", "Bearer client"),
        ("X-Api-Key", "client-key"),
        ("Cookie", "client-cookie"),
        ("ChatGPT-Account-Id", "client-account"),
        ("OpenAI-Project", "client-project"),
        ("X-Oai-Attestation", "client-signature"),
        ("User-Agent", "client-agent"),
        ("Originator", "client-originator"),
        ("Session-Id", "client-session"),
        ("Thread-Id", "client-thread"),
        ("X-Client-Request-Id", "client-request"),
        ("Host", "client-host"),
        ("Content-Length", "999"),
        ("Content-Encoding", "gzip"),
        ("Accept-Encoding", "br"),
        ("Transfer-Encoding", "chunked"),
        ("X-Codex-Queue-Principal", "internal"),
        ("X-Forwarded-For", "client-ip"),
        ("X-Hop-Only", "private"),
        ("Connection", "keep-alive, X-Hop-Only"),
        ("Keep-Alive", "timeout=60"),
        ("CDN-Loop", "cloudflare; loops=1"),
        ("CF-Ray", "edge-ray"),
        ("CF-EW-Via", "15"),
        ("CF-Worker", "ingress.example"),
        ("CF-Connecting-IPv6", "2001:db8::1"),
        ("CF-Pseudo-IPv4", "240.0.0.1"),
        ("CF-Access-Jwt-Assertion", "ingress-jwt"),
        ("CF-Access-Client-Id", "ingress-id"),
        ("CF-Access-Client-Secret", "ingress-secret"),
        ("True-Client-IP", "203.0.113.1"),
        ("Fastly-Client-IP", "203.0.113.1"),
        ("X-Envoy-External-Address", "203.0.113.1"),
        ("Origin", "https://ingress.example"),
        ("Referer", "https://ingress.example/private"),
        ("Sec-Fetch-Site", "same-origin"),
        ("Sec-CH-UA", "browser"),
        ("Sec-CH-UA-Platform", "platform"),
    ]);
    let expected = preserved
        .into_iter()
        .map(|(name, value)| {
            (
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            )
        })
        .collect::<HeaderMap>();
    assert_eq!(raw_responses_headers(supplied).unwrap(), expected);
}

#[test]
fn rejects_duplicate_invalid_and_oversized_headers_before_filtering() {
    for supplied in [
        vec![("X-Custom", "one"), ("x-custom", "two")],
        vec![("Authorization", "one"), ("authorization", "two")],
        vec![("bad name", "value")],
        vec![("x-custom", "a\r\nb")],
        vec![("connection", "invalid token")],
    ] {
        assert!(raw_responses_headers(supplied).is_err());
    }
    assert!(raw_responses_headers([("x-custom", "x".repeat(8193).as_str())]).is_err());
    let many = (0..129)
        .map(|i| (format!("x-custom-{i}"), "v".to_owned()))
        .collect::<Vec<_>>();
    assert!(raw_responses_headers(many.iter().map(|(k, v)| (k.as_str(), v.as_str()))).is_err());
    let large = (0..9)
        .map(|i| (format!("x-custom-{i}"), "x".repeat(8192)))
        .collect::<Vec<_>>();
    assert!(raw_responses_headers(large.iter().map(|(k, v)| (k.as_str(), v.as_str()))).is_err());
}
