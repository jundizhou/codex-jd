use super::*;
use pretty_assertions::assert_eq;

#[test]
fn counts_completed_statuses_and_keeps_only_bounded_recent_samples() {
    let metrics = Arc::new(Metrics::default());
    for _ in 0..105 {
        let mut request = metrics.start();
        request.status = 429;
        request.delivered = true;
    }
    let pending = metrics.start();
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot["total"], 106);
    assert_eq!(snapshot["completed"], 105);
    assert_eq!(snapshot["in_flight"], 1);
    assert_eq!(snapshot["status_counts"], serde_json::json!({"429": 105}));
    assert_eq!(snapshot["recent"].as_array().unwrap().len(), RECENT_LIMIT);
    assert_eq!(snapshot["transport_errors"], 0);
    drop(pending);
    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot["status_counts"],
        serde_json::json!({"429":105,"500":1})
    );
    assert_eq!(snapshot["transport_errors"], 1);
    assert_eq!(snapshot["in_flight"], 0);
}

#[test]
fn latency_tracks_the_entire_request_lifetime() {
    let metrics = Arc::new(Metrics::default());
    let mut request = metrics.start();
    request.started -= std::time::Duration::from_secs(2);
    request.status = 200;
    request.delivered = true;
    drop(request);
    let snapshot = metrics.snapshot();
    let duration = snapshot["recent"][0]["duration_ms"].as_f64().unwrap();
    assert!(duration >= 2000.0);
    assert_eq!(
        snapshot["average_duration_ms"],
        snapshot["recent"][0]["duration_ms"]
    );
    assert_eq!(
        snapshot["max_duration_ms"],
        snapshot["recent"][0]["duration_ms"]
    );
}

#[test]
fn request_detail_is_bounded_evicted_and_excluded_from_summary() {
    let metrics = Arc::new(Metrics::default());
    let mut request = metrics.start();
    request.capture(br#"{"model":"example","input":"hello"}"#);
    drop(request);
    assert_eq!(
        metrics.detail(1).unwrap()["body"],
        r#"{"model":"example","input":"hello"}"#
    );
    assert!(metrics.snapshot()["recent"][0].get("body").is_none());
    let mut request = metrics.start();
    request.capture(&vec![b'a'; 70 * 1024]);
    drop(request);
    let detail = metrics.detail(2).unwrap();
    assert_eq!(detail["body"].as_str().unwrap().len(), 64 * 1024);
    assert_eq!(detail["body_truncated"], true);
    for _ in 0..100 {
        drop(metrics.start());
    }
    assert_eq!(metrics.detail(1), None);
    assert_eq!(metrics.detail(2), None);
}

#[test]
fn counts_models_from_request_body() {
    let metrics = Arc::new(Metrics::default());
    let mut first = metrics.start();
    first.capture(br#"{"model":"gpt-6-astra","input":[]}"#);
    first.status = 200;
    first.delivered = true;
    let mut second = metrics.start();
    second.capture(br#"{"model":"gpt-5.5","input":[]}"#);
    second.status = 200;
    second.delivered = true;
    drop(first);
    drop(second);
    assert_eq!(
        metrics.snapshot()["model_counts"],
        serde_json::json!({"gpt-5.5": 1, "gpt-6-astra": 1})
    );
}

#[test]
fn truncated_upstream_snapshot_does_not_erase_model() {
    let metrics = Arc::new(Metrics::default());
    let mut request = metrics.start();
    request.capture(br#"{"model":"gpt-6-astra","input":[]}"#);
    request.capture_upstream_model(&serde_json::json!({
        "body": "{\"client_metadata\":{\"large\":"
    }));
    request.status = 200;
    request.delivered = true;
    drop(request);
    assert_eq!(
        metrics.snapshot()["model_counts"],
        serde_json::json!({"gpt-6-astra": 1})
    );
}
