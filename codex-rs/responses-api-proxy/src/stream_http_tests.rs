use anyhow::Context;
use anyhow::Result;
use pretty_assertions::assert_eq;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::sync::mpsc;
use std::time::Duration;
use tiny_http::Server;
use tiny_http::StatusCode;

struct ControlledBody(mpsc::Receiver<Vec<u8>>);

impl Read for ControlledBody {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let Ok(bytes) = self.0.recv() else {
            return Ok(0);
        };
        buf[..bytes.len()].copy_from_slice(&bytes);
        Ok(bytes.len())
    }
}

#[test]
fn flushes_small_chunk_before_upstream_completes() -> Result<()> {
    let server = Server::http("127.0.0.1:0").map_err(|error| anyhow::anyhow!(error))?;
    let address = server.server_addr().to_ip().context("TCP address")?;
    let (sender, receiver) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        super::respond(
            server.recv()?.into(),
            StatusCode(200),
            &[],
            ControlledBody(receiver),
        )
    });
    let mut client = TcpStream::connect(address)?;
    client.set_read_timeout(Some(Duration::from_secs(5)))?;
    client.write_all(
        b"POST /v1/responses HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n",
    )?;
    let first_chunk = b"data: first\n\n";
    sender.send(first_chunk.to_vec())?;
    let mut response = BufReader::new(client);
    let mut line = String::new();
    loop {
        response.read_line(&mut line)?;
        if line == "\r\n" {
            break;
        }
        line.clear();
    }
    line.clear();
    response.read_line(&mut line)?;
    assert_eq!(line, format!("{:x}\r\n", first_chunk.len()));
    let mut bytes = vec![0; first_chunk.len() + 2];
    response.read_exact(&mut bytes)?;
    assert_eq!(bytes, b"data: first\n\n\r\n");

    // EOF is released only after observing the first chunk at the HTTP client.
    drop(sender);
    let mut terminal_chunk = [0; 5];
    response.read_exact(&mut terminal_chunk)?;
    assert_eq!(terminal_chunk, *b"0\r\n\r\n");
    worker.join().expect("HTTP worker panicked")?;
    Ok(())
}
