use super::Event;
use super::StreamBody;
use pretty_assertions::assert_eq;
use std::io::Cursor;
use std::io::Read;
use std::sync::mpsc;

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
