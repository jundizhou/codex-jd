use std::io;
use std::io::Read;
use std::io::Write;
use tiny_http::Header;
use tiny_http::Request;
use tiny_http::StatusCode;

/// Flush every transport chunk. tiny_http's default chunk encoder buffers small SSE deltas.
pub(crate) fn respond(
    req: Request,
    status: StatusCode,
    headers: &[Header],
    mut body: impl Read,
) -> io::Result<()> {
    let version = req.http_version().clone();
    let mut writer = req.into_writer();
    write!(
        writer,
        "HTTP/{} {} {}\r\n",
        version,
        status.0,
        status.default_reason_phrase()
    )?;
    for header in headers {
        write!(writer, "{header}\r\n")?;
    }
    writer.write_all(b"Transfer-Encoding: chunked\r\nX-Accel-Buffering: no\r\n\r\n")?;
    writer.flush()?;
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let n = body.read(&mut buffer)?;
        if n == 0 {
            writer.write_all(b"0\r\n\r\n")?;
            return writer.flush();
        }
        write!(writer, "{n:x}\r\n")?;
        writer.write_all(&buffer[..n])?;
        writer.write_all(b"\r\n")?;
        writer.flush()?;
    }
}

#[cfg(test)]
#[path = "stream_http_tests.rs"]
mod tests;
