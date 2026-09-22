//! Shared request-header boundary for the HTTP proxy and raw app-server RPC.

use http::HeaderMap;
use http::HeaderName;
use http::HeaderValue;

/// Preserves application headers while removing caller credentials, transport
/// framing, local queue metadata and identities owned by the upstream transport.
/// Identity metadata left in the result must be rewritten by the caller.
/// Names are case-insensitive; duplicates and invalid or unbounded input fail.
pub fn raw_responses_headers<'a>(
    supplied: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<HeaderMap, &'static str> {
    let mut headers = HeaderMap::new();
    let mut bytes = 0;
    for (name, value) in supplied {
        bytes += name.len() + value.len();
        if headers.len() >= 128 || bytes > 64 * 1024 || name.len() > 256 || value.len() > 8192 {
            return Err("raw Responses headers exceed size limits");
        }
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| "invalid raw Responses header name")?;
        let value =
            HeaderValue::from_str(value).map_err(|_| "invalid raw Responses header value")?;
        value
            .to_str()
            .map_err(|_| "invalid raw Responses header text")?;
        if headers.insert(name, value).is_some() {
            return Err("duplicate raw Responses header");
        }
    }
    // Connection can nominate additional hop-by-hop fields, regardless of order.
    let connection = headers
        .get("connection")
        .map(|value| value.to_str().unwrap_or_default().to_owned())
        .unwrap_or_default();
    for name in connection
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
    {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| "invalid Connection header token")?;
        headers.remove(name);
    }
    let excluded = headers
        .keys()
        .filter(|name| {
            let name = name.as_str();
            matches!(
                name,
                "authorization"
                    | "proxy-authorization"
                    | "api-key"
                    | "x-api-key"
                    | "cookie"
                    | "cookie2"
                    | "set-cookie"
                    | "chatgpt-account-id"
                    | "openai-organization"
                    | "openai-project"
                    | "x-oai-attestation"
                    | "user-agent"
                    | "originator"
                    | "session-id"
                    | "thread-id"
                    | "x-client-request-id"
                    | "host"
                    | "content-length"
                    | "content-encoding"
                    | "accept-encoding"
                    | "connection"
                    | "keep-alive"
                    | "proxy-connection"
                    | "proxy-authenticate"
                    | "te"
                    | "trailer"
                    | "transfer-encoding"
                    | "upgrade"
                    | "expect"
                    | "forwarded"
                    | "via"
                    | "x-real-ip"
            ) || name.starts_with("x-forwarded-")
                || name.starts_with("x-codex-queue-")
        })
        .cloned()
        .collect::<Vec<_>>();
    for name in excluded {
        headers.remove(name);
    }
    Ok(headers)
}

#[cfg(test)]
#[path = "raw_responses_headers_tests.rs"]
mod tests;
