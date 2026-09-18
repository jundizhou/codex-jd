use super::*;
use pretty_assertions::assert_eq;
use serde_json::json;

#[test]
fn matches_only_new_tool_outputs_against_observed_calls() {
    let now = Instant::now();
    let initial = json!({"input":[{"role":"user","content":"hello"}]});
    let signals = Signals {
        input: Evidence::read(&initial),
        finished: Some(now),
        output: Observation {
            valid: true,
            response_id: Some("response-1".into()),
            calls: vec!["call-1".into()],
            ..Observation::default()
        },
    };
    let tool = json!({"input":[{"role":"user","content":"hello"},{"type":"function_call_output","call_id":"call-1","output":"ok"}]});
    assert_eq!(Evidence::read(&tool).kind(&signals, now), Kind::Tool);
    let unknown =
        json!({"input":[{"type":"function_call_output","call_id":"call-1","output":"old"}]});
    assert_eq!(Evidence::read(&unknown).kind(&signals, now), Kind::Unknown);
    let user =
        json!({"input":[{"role":"user","content":"hello"},{"role":"user","content":"new turn"}]});
    assert_eq!(Evidence::read(&user).kind(&signals, now), Kind::User);
    assert_eq!(
        Evidence::read(&tool).kind(&signals, now + Duration::from_secs(301)),
        Kind::Unknown
    );
}

#[test]
fn observes_arbitrary_sse_chunk_boundaries_and_failed_terminal() {
    let body = b"data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call-1\"}}\r\n\r\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\"}}\r\n\r\n";
    let mut observer = Observer::new(true);
    for byte in body {
        observer.bytes(&[*byte]);
    }
    let result = observer.finish();
    assert_eq!(
        (
            result.valid,
            result.terminal,
            result.successful,
            result.calls,
            result.response_id
        ),
        (
            true,
            true,
            true,
            vec!["call-1".into()],
            Some("resp-1".into())
        )
    );
    let mut observer = Observer::new(true);
    observer.bytes(b"data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"rate_limit_exceeded\"}}}\n\n");
    let result = observer.finish();
    assert_eq!(
        (result.terminal, result.successful, result.rate_limited),
        (true, false, true)
    );
}

#[test]
fn oversized_or_truncated_stream_never_grants_continuation_priority() {
    let mut observer = Observer::new(true);
    observer.bytes(&vec![b'x'; MAX_EVENT + 1]);
    observer.bytes(b"\n\ndata: {\"type\":\"response.completed\"}\n\n");
    assert!(!observer.finish().valid);
    let mut observer = Observer::new(true);
    observer.bytes(b"data: {\"type\":\"response.created\"}\n\n");
    assert!(!observer.finish().terminal);
    let mut observer = Observer::new(false);
    observer.bytes(br#"{"error":{"code":"insufficient_quota"}}"#);
    assert!(observer.finish().quota_exhausted);
    let mut observer = Observer::new(true);
    observer.bytes(b"event: response.completed\ndata: ");
    observer.bytes(&vec![b'x'; MAX_EVENT + 1]);
    observer.bytes(b"\n\n");
    let observation = observer.finish();
    assert_eq!((observation.terminal, observation.valid), (true, false));
    let mut observer = Observer::new(true);
    observer.bytes(b"event: response.completed\n\n");
    assert!(!observer.finish().terminal);
}
