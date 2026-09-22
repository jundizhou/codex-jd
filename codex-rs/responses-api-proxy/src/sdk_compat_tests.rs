use super::Compatibility;
use super::Delivery;
use super::MAX_EVENT_BYTES;
use super::collect_response;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use std::io;
use std::io::Read;
use tiny_http::Header;
use tiny_http::Server;
use tiny_http::StatusCode;

fn terminal(status: &str) -> Value {
    json!({
        "id":"resp-sdk", "object":"response", "status":status,
        "output":[
            {"type":"reasoning", "encrypted_content":"opaque", "summary":[]},
            {"type":"function_call", "call_id":"call-one", "name":"lookup", "arguments":"{}"},
            {"type":"message", "role":"assistant", "content":[{"type":"output_text", "text":"你好"}]}
        ],
        "usage":{"input_tokens":23,"output_tokens":7,"total_tokens":30},
        "incomplete_details":if status == "incomplete" {json!({"reason":"max_output_tokens"})} else {Value::Null},
        "error":if status == "failed" {json!({"code":"server_error","message":"failed"})} else {Value::Null}
    })
}

fn sse(response: &Value) -> Vec<u8> {
    let event = json!({"type":format!("response.{}", response["status"].as_str().unwrap()),"response":response});
    format!(
        ": heartbeat\r\n\r\nevent: response.{}\r\ndata: {event}\r\n\r\ndata: [DONE]\r\n\r\n",
        response["status"].as_str().unwrap()
    )
    .into_bytes()
}

#[test]
fn prepares_sdk_transport_without_rewriting_inference_content() {
    for stream in [
        None,
        Some(json!(false)),
        Some(Value::Null),
        Some(json!(true)),
    ] {
        let mut body = json!({"model":"mock", "store":false, "input":[{"role":"user","content":"hello"}],
            "reasoning":{"effort":"low"}, "text":{"format":{"type":"json_object"}}, "max_output_tokens":32768});
        let mut expected = body.clone();
        expected
            .as_object_mut()
            .unwrap()
            .remove("max_output_tokens");
        expected["stream"] = json!(true);
        if let Some(value) = &stream {
            body["stream"] = value.clone();
        }
        let compatibility = Compatibility::prepare(&mut body).unwrap();
        assert_eq!(body, expected);
        assert_eq!(
            compatibility,
            Compatibility {
                delivery: if stream == Some(json!(true)) {
                    Delivery::Stream
                } else {
                    Delivery::Json
                },
                ignored_output_limit: true,
            }
        );
    }
}

#[test]
fn rejects_malformed_options_without_mutating_the_request() {
    for mut body in [
        json!({"stream":"true"}),
        json!({"max_output_tokens":0}),
        json!({"max_output_tokens":-1}),
        json!({"max_output_tokens":1.5}),
    ] {
        let before = body.clone();
        assert!(Compatibility::prepare(&mut body).is_err());
        assert_eq!(body, before);
    }
}

struct Fragmented<'a>(&'a [u8]);

impl Read for Fragmented<'_> {
    fn read(&mut self, target: &mut [u8]) -> io::Result<usize> {
        let size = target.len().min(3);
        self.0.read(&mut target[..size])
    }
}

#[test]
fn collects_full_terminal_objects_across_arbitrary_chunk_boundaries() {
    for status in ["completed", "failed", "incomplete"] {
        let expected = terminal(status);
        assert_eq!(
            collect_response(Fragmented(&sse(&expected))).unwrap(),
            expected
        );
    }
    let multiline = b"data: {\"type\":\"response.completed\",\n\
                      data: \"response\":{\"id\":\"resp\",\"status\":\"completed\"}}\n\n";
    assert_eq!(
        collect_response(multiline.as_slice()).unwrap(),
        json!({"id":"resp","status":"completed"})
    );
}

#[test]
fn rejects_truncated_invalid_and_oversized_streams() {
    for bytes in [
        b"data: [DONE]\n\n".as_slice(),
        b"data: {\"type\":\"response.created\"}\n\n".as_slice(),
        b"data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n"
            .as_slice(),
        b"data: invalid\n\n".as_slice(),
        b"data: {\"type\":\"error\",\"message\":\"failed\"}\n\n".as_slice(),
    ] {
        assert!(collect_response(bytes).is_err());
    }
    assert!(collect_response(io::repeat(b'x').take((MAX_EVENT_BYTES + 1) as u64)).is_err());
    let oversized_event = format!(
        "data: {}\ndata: {}\n\n",
        "x".repeat(MAX_EVENT_BYTES / 2),
        "x".repeat(MAX_EVENT_BYTES / 2)
    );
    assert!(collect_response(oversized_event.as_bytes()).is_err());
}

struct BrokenTail;

impl Read for BrokenTail {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::other("trailing RPC failed"))
    }
}

#[test]
fn only_a_complete_terminal_survives_transport_failure() {
    let expected = terminal("completed");
    assert_eq!(
        collect_response(sse(&expected).as_slice().chain(BrokenTail)).unwrap(),
        expected
    );
    assert!(collect_response(BrokenTail).is_err());
}

fn exchange(
    request: Value,
    status: u16,
    body: Vec<u8>,
) -> (u16, reqwest::header::HeaderMap, Vec<u8>) {
    let server = Server::http("127.0.0.1:0").unwrap();
    let address = server.server_addr().to_ip().unwrap();
    let mut request = request;
    let compatibility = Compatibility::prepare(&mut request).unwrap();
    let worker = std::thread::spawn(move || {
        compatibility
            .respond(
                server.recv().unwrap().into(),
                StatusCode(status),
                vec![
                    Header::from_bytes("content-type", "text/event-stream; charset=utf-8").unwrap(),
                    Header::from_bytes("x-codex-turn-state", "opaque-route").unwrap(),
                ],
                body.as_slice(),
            )
            .unwrap();
    });
    let client = reqwest::blocking::Client::builder()
        .no_proxy()
        .build()
        .unwrap();
    let response = client
        .post(format!("http://{address}/v1/responses"))
        .body("{}")
        .send()
        .unwrap();
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    let bytes = response.bytes().unwrap().to_vec();
    worker.join().unwrap();
    (status, headers, bytes)
}

#[test]
fn nonstream_http_returns_json_usage_and_compatibility_headers() {
    let expected = terminal("completed");
    let (status, headers, body) = exchange(
        json!({"max_output_tokens":32768}),
        /*status*/ 200,
        sse(&expected),
    );
    assert_eq!(
        (status, serde_json::from_slice::<Value>(&body).unwrap()),
        (200, expected)
    );
    assert_eq!(headers["content-type"], "application/json");
    assert_eq!(headers["x-codex-turn-state"], "opaque-route");
    assert_eq!(headers["x-codex-ignored-parameters"], "max_output_tokens");
    assert_eq!(headers["x-codex-output-token-limit"], "not-enforced");
}

#[test]
fn streaming_and_http_errors_keep_original_bytes() {
    for (request, code, payload) in [
        (json!({"stream":true}), 200, sse(&terminal("completed"))),
        (
            json!({"stream":false}),
            429,
            b"{\"error\":{\"message\":\"quota\"}}".to_vec(),
        ),
    ] {
        let (status, _, body) = exchange(request, code, payload.clone());
        assert_eq!((status, body), (code, payload));
    }
}

#[test]
fn incomplete_transport_is_an_http_error_not_an_empty_success() {
    let (status, _, body) = exchange(json!({}), /*status*/ 200, b"data: [DONE]\n\n".to_vec());
    assert_eq!(status, 502);
    let response: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(response["error"]["code"], "invalid_upstream_stream");
}

#[test]
fn reconstructs_codex_empty_terminal_output_from_complete_items() {
    let expected = terminal("completed");
    let mut final_response = expected.clone();
    let items = final_response["output"].take().as_array().unwrap().clone();
    final_response["output"] = json!([]);
    let mut bytes = Vec::new();
    // Arrival order does not determine output order, and complete tool and
    // reasoning items must survive alongside message content.
    for index in (0..items.len()).rev() {
        for kind in ["response.output_item.added", "response.output_item.done"] {
            let event = json!({"type":kind,"output_index":index,"item":items[index]});
            bytes.extend_from_slice(format!("data: {event}\n\n").as_bytes());
        }
    }
    bytes.extend(sse(&final_response));
    assert_eq!(collect_response(Fragmented(&bytes)).unwrap(), expected);
}

#[test]
fn empty_completed_output_cannot_hide_unfinished_or_unbounded_items() {
    let mut final_response = terminal("completed");
    final_response["output"] = json!([]);
    let incomplete =
        b"data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{}}\n\n";
    assert!(
        collect_response(incomplete.as_slice().chain(sse(&final_response).as_slice())).is_err()
    );
    let mut oversized = Vec::new();
    for index in 0..2 {
        let event = json!({"type":"response.output_item.done","output_index":index,
            "item":{"type":"message","content":"x".repeat(MAX_EVENT_BYTES / 2)}});
        oversized.extend_from_slice(format!("data: {event}\n\n").as_bytes());
    }
    oversized.extend(sse(&final_response));
    assert!(collect_response(oversized.as_slice()).is_err());
}

#[test]
fn upstream_error_event_preserves_the_following_failed_response() {
    let expected = terminal("failed");
    let error = b"data: {\"type\":\"error\",\"error\":{\"code\":\"server_is_overloaded\"}}\n\n";
    assert_eq!(
        collect_response(error.as_slice().chain(sse(&expected).as_slice())).unwrap(),
        expected
    );
}
