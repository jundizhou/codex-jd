use super::*;
use crate::request_metrics::Metrics;
use pretty_assertions::assert_eq;
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;
use tiny_http::Server;

#[test]
fn records_http_errors_and_stream_completion_without_changing_responses() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::default());
    let server = Server::http("127.0.0.1:0").map_err(|e| anyhow::anyhow!(e))?;
    let address = server.server_addr().to_ip().unwrap();
    let worker_metrics = Arc::clone(&metrics);
    let worker = std::thread::spawn(move || -> io::Result<()> {
        let request = Request {
            inner: server.recv()?,
            completion: Some(worker_metrics.start()),
        };
        crate::queue_http::error(request, crate::scheduler::Rejection::Cooldown(3));
        let request = Request {
            inner: server.recv()?,
            completion: Some(worker_metrics.start()),
        };
        crate::stream_http::respond(request, StatusCode(200), &[], &b"data: OK\n\n"[..])
    });
    for expected in ["429", "200"] {
        let mut client = TcpStream::connect(address)?;
        client.set_read_timeout(Some(Duration::from_secs(5)))?;
        client.write_all(b"POST /v1/responses HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")?;
        let mut response = String::new();
        client.read_to_string(&mut response)?;
        assert!(response.starts_with(&format!("HTTP/1.1 {expected}")));
        if expected == "200" {
            assert!(response.contains("data: OK"));
        }
    }
    worker.join().unwrap()?;
    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot["status_counts"],
        serde_json::json!({"200":1,"429":1})
    );
    assert_eq!(snapshot["total"], 2);
    assert_eq!(snapshot["completed"], 2);
    assert_eq!(snapshot["transport_errors"], 0);
    Ok(())
}
