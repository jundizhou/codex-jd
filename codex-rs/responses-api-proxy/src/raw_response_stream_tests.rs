use super::Event;
use super::StreamBody;
use crate::app_server_reader;
use crate::queue_signals::Evidence;
use crate::queue_store::Journal;
use crate::scheduler::Config;
use crate::scheduler::Key;
use crate::scheduler::Pending;
use crate::scheduler::Scheduler;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use std::io::Cursor;
use std::io::Read;
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Role;

#[test]
fn stream_body_preserves_exact_chunk_order_and_bytes() {
    let (sender, receiver) = mpsc::sync_channel(4);
    sender
        .send(Ok(Event::Chunk {
            data: vec![0, 1, b'\n'],
        }))
        .unwrap();
    sender
        .send(Ok(Event::Chunk {
            data: vec![0xff, b'\r'],
        }))
        .unwrap();
    sender.send(Ok(Event::Completed)).unwrap();
    drop(sender);
    let mut body = StreamBody {
        receiver,
        pending: Cursor::new(Vec::new()),
        complete: false,
    };
    let mut bytes = Vec::new();
    body.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, vec![0, 1, b'\n', 0xff, b'\r']);
}

async fn disconnected_relay(
    payload: &[u8],
    reply: Value,
    delay: Duration,
) -> (anyhow::Result<()>, Value) {
    let scheduler = Scheduler::new(
        Config {
            automatic_recovery: false,
            max_running: 2,
            gap: Duration::ZERO,
            user_gap: Duration::ZERO,
            tool_gap: Duration::ZERO,
            idle_ttl: Duration::from_secs(900),
            start_gap: Duration::ZERO,
            timeout: Duration::from_secs(5),
        },
        Journal::default(),
    );
    let lease = scheduler
        .acquire(Pending {
            id: "request".into(),
            key: Key("user".into(), "conversation".into()),
            sticky: true,
            fingerprint: "body".into(),
            evidence: Evidence::default(),
            bytes: 1,
            deadline: Instant::now() + Duration::from_secs(5),
        })
        .unwrap();
    let (client, server) = tokio::io::duplex(8192);
    let mut client = WebSocketStream::from_raw_socket(client, Role::Client, /*config*/ None).await;
    let mut server = WebSocketStream::from_raw_socket(server, Role::Server, /*config*/ None).await;
    let (sender, receiver) = mpsc::sync_channel(4);
    drop(receiver);
    let peer = async {
        let request = app_server_reader::recv_message(&mut server).await.unwrap();
        let id = &request["id"];
        app_server_reader::send_message(&mut server, &json!({
            "method":"rawResponse/stream", "params":{"requestId":id,
                "event":{"type":"started", "status":200, "headers":{"content-type":"text/event-stream"}}}
        })).await.unwrap();
        tokio::time::sleep(delay).await;
        // Split a terminal across notifications to exercise the bounded SSE parser.
        for data in payload.chunks(17) {
            app_server_reader::send_message(
                &mut server,
                &json!({
                    "method":"rawResponse/stream", "params":{"requestId":id,
                        "event":{"type":"chunk", "data":data}}
                }),
            )
            .await
            .unwrap();
        }
        let mut reply = reply;
        reply["id"] = id.clone();
        app_server_reader::send_message(&mut server, &reply)
            .await
            .unwrap();
    };
    let (result, ()) = tokio::join!(
        super::relay(
            &mut client,
            json!({"rawResponses":{"stream":true}}),
            &sender,
            Some(&lease.dispatch)
        ),
        peer,
    );
    if result.is_err() {
        lease.dispatch.unconfirmed();
    }
    lease.settle();
    drop(lease);
    (result, scheduler.status())
}

#[tokio::test]
async fn model_terminal_survives_trailing_rpc_error() {
    for kind in [
        "response.completed",
        "response.failed",
        "response.incomplete",
    ] {
        let payload = format!("data: {{\"type\":\"{kind}\"}}\n\n");
        let (result, status) = disconnected_relay(
            payload.as_bytes(),
            json!({"error":{"code":-32603,"message":"connection lost after terminal"}}),
            Duration::ZERO,
        )
        .await;
        assert!(result.is_err());
        assert_eq!(
            (status["running"].clone(), status["outcome_unknown"].clone()),
            (json!(0), json!(false))
        );
    }
}

#[tokio::test]
async fn disconnected_reader_drains_terminal_after_fifteen_seconds() {
    let (result, status) = tokio::time::timeout(
        Duration::from_secs(25),
        disconnected_relay(
            b"data: {\"type\":\"response.completed\"}\n\n",
            json!({"result":{}}),
            Duration::from_secs(16),
        ),
    )
    .await
    .unwrap();
    result.unwrap();
    assert_eq!(
        (status["running"].clone(), status["outcome_unknown"].clone()),
        (json!(0), json!(false))
    );
}

#[tokio::test]
async fn truncated_stream_retains_capacity_even_with_successful_rpc() {
    for reply in [
        json!({"result":{}}),
        json!({"error":{"code":-32603,"message":"idle timeout"}}),
    ] {
        let (result, status) = disconnected_relay(
            b"data: {\"type\":\"response.created\"}\n\n",
            reply,
            Duration::ZERO,
        )
        .await;
        assert!(result.is_err());
        assert_eq!(
            (
                status["running"].clone(),
                status["quarantined"].clone(),
                status["worker_fault"].clone()
            ),
            (json!(1), json!(1), json!(false))
        );
    }
}
