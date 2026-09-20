//! Keeps accounting attached to requests through admission, errors and streaming responses.
use crate::request_metrics::Completion;
use crate::request_metrics::METRICS;
use std::io;
use std::io::Read;
use std::io::Write;
use std::ops::Deref;
use std::ops::DerefMut;
use tiny_http::Response;
use tiny_http::StatusCode;

pub(crate) struct Request {
    inner: tiny_http::Request,
    completion: Option<Completion>,
}

impl From<tiny_http::Request> for Request {
    fn from(inner: tiny_http::Request) -> Self {
        let path = inner.url().split('?').next().unwrap_or_default();
        let mut completion = (path == "/v1/responses").then(|| METRICS.start());
        if let Some(completion) = &mut completion {
            completion.client_headers = inner
                .headers()
                .iter()
                .take(128)
                .map(|header| {
                    let name = header.field.as_str().to_ascii_lowercase().to_string();
                    let secret = name.contains("authorization")
                        || name.contains("cookie")
                        || name.contains("token")
                        || name.contains("api-key");
                    let value = if secret {
                        "[REDACTED]".to_owned()
                    } else {
                        header.value.as_str().chars().take(2048).collect()
                    };
                    serde_json::json!({"name":name,"value":value})
                })
                .collect();
        }
        Self { inner, completion }
    }
}

impl Deref for Request {
    type Target = tiny_http::Request;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Request {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Request {
    pub(crate) fn capture_upstream(&mut self, path: &std::path::Path) {
        if let Some(completion) = &mut self.completion
            && let Ok(file) = std::fs::File::open(path)
        {
            let mut bytes = Vec::new();
            if file.take(2 * 1024 * 1024).read_to_end(&mut bytes).is_ok() {
                completion.upstream = serde_json::from_slice(&bytes).ok();
            }
        }
        let _ = std::fs::remove_file(path);
    }

    pub(crate) fn capture_body(&mut self, body: &[u8]) {
        if let Some(completion) = &mut self.completion {
            completion.capture(body);
        }
    }

    pub(crate) fn respond<R: Read>(mut self, response: Response<R>) -> io::Result<()> {
        if let Some(completion) = &mut self.completion {
            completion.status = response.status_code().0;
        }
        let result = self.inner.respond(response);
        if let Some(completion) = &mut self.completion {
            completion.delivered = result.is_ok();
        }
        result
    }

    pub(crate) fn into_writer(
        mut self,
        status: StatusCode,
    ) -> (Box<dyn Write + Send>, Option<Completion>) {
        if let Some(completion) = &mut self.completion {
            completion.status = status.0;
        }
        (self.inner.into_writer(), self.completion)
    }
}

#[cfg(test)]
#[path = "monitored_request_tests.rs"]
mod tests;
