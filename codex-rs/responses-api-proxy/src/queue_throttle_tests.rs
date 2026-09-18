use super::*;
use pretty_assertions::assert_eq;

#[test]
fn cooldown_respects_long_hints_and_successes_cannot_erase_it() {
    let now = Instant::now();
    let mut throttle = Throttle {
        limit: 4,
        ..Throttle::default()
    };
    throttle.observe(
        429,
        &HashMap::from([("retry-after".into(), "3600".into())]),
        now,
        4,
    );
    assert!(throttle.until.unwrap() >= now + Duration::from_secs(3600));
    let until = throttle.until;
    for _ in 0..30 {
        throttle.observe(200, &HashMap::new(), now + Duration::from_secs(60), 4);
    }
    assert_eq!((throttle.until, throttle.limit), (until, 1));
    for _ in 0..19 {
        throttle.observe(200, &HashMap::new(), now + Duration::from_secs(3601), 4);
    }
    throttle.observe(200, &HashMap::new(), now + Duration::from_secs(3662), 4);
    assert_eq!(throttle.limit, 2);
}

#[test]
fn dates_invalid_hints_and_auth_failures_have_distinct_behavior() {
    let time = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
    assert_eq!(
        retry_after(
            &httpdate::fmt_http_date(time + Duration::from_secs(90)),
            time
        ),
        Some(Duration::from_secs(90))
    );
    assert_eq!(retry_after("-10", time), None);
    let mut throttle = Throttle {
        limit: 2,
        ..Throttle::default()
    };
    let now = Instant::now();
    throttle.observe(403, &HashMap::new(), now, 2);
    assert!(!throttle.blocked);
    throttle.observe(429, &HashMap::new(), now, 2);
    assert!(throttle.until.unwrap() >= now + Duration::from_secs(5));
    throttle.observe(401, &HashMap::new(), now, 2);
    assert!(throttle.blocked);
}
