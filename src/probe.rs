//! HTTP client construction and range-support probing.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, ACCEPT_RANGES, CONTENT_RANGE, RANGE};
use reqwest::{Client, StatusCode};

/// What the probe learned about the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Probe {
    /// Server honours byte ranges and reported a definite total size.
    Ranged { total: u64 },
    /// Server cannot (or will not) serve ranges; download as one stream.
    /// `len` is the `Content-Length` if the server advertised one.
    Single { len: Option<u64> },
}

/// Build the shared HTTP client with default headers, timeout and user agent.
pub fn build_client(
    user_agent: &str,
    timeout_secs: u64,
    extra_headers: &[String],
) -> Result<Client> {
    let mut headers = HeaderMap::new();
    for raw in extra_headers {
        let (name, value) = raw
            .split_once(':')
            .with_context(|| format!("invalid header (expected 'Name: value'): {raw}"))?;
        let name = HeaderName::from_bytes(name.trim().as_bytes())
            .with_context(|| format!("invalid header name: {name}"))?;
        let value = HeaderValue::from_str(value.trim())
            .with_context(|| format!("invalid header value: {value}"))?;
        headers.insert(name, value);
    }

    let mut builder = Client::builder()
        .user_agent(user_agent)
        .default_headers(headers)
        .redirect(reqwest::redirect::Policy::limited(10));
    if timeout_secs > 0 {
        // Use connect + read (idle) timeouts, NOT a total request timeout. The
        // single-stream fallback reads one long-lived response body; a total
        // timeout would abort a healthy slow transfer mid-stream and corrupt
        // whatever the pipe consumer already received. read_timeout resets per
        // read, so it only fires on a genuinely stalled connection.
        let t = Duration::from_secs(timeout_secs);
        builder = builder.connect_timeout(t).read_timeout(t);
    }
    builder.build().context("failed to build HTTP client")
}

/// Probe the URL with a one-byte range request to detect range support and
/// the total content length.
pub async fn probe(client: &Client, url: &str) -> Result<Probe> {
    let resp = client
        .get(url)
        .header(RANGE, "bytes=0-0")
        .send()
        .await
        .with_context(|| format!("probe request failed for {url}"))?;

    let status = resp.status();

    if status == StatusCode::PARTIAL_CONTENT {
        // Authoritative: parse total from Content-Range "bytes 0-0/TOTAL".
        if let Some(total) = resp
            .headers()
            .get(CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_content_range_total)
        {
            tracing::debug!(total, "server supports ranges (206)");
            return Ok(Probe::Ranged { total });
        }
        // 206 without a usable total — cannot plan chunks; stream as one.
        tracing::warn!("206 response lacked a parseable Content-Range total; using single stream");
        return Ok(Probe::Single { len: None });
    }

    if status == StatusCode::RANGE_NOT_SATISFIABLE {
        // A compliant server answers `bytes=0-0` with 416 only for a 0-byte
        // resource (e.g. S3 on an empty object). Treat it as empty rather than
        // a hard error.
        tracing::debug!("416 to bytes=0-0: treating resource as empty");
        return Ok(Probe::Ranged { total: 0 });
    }

    if status.is_success() {
        let accepts = resp
            .headers()
            .get(ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.eq_ignore_ascii_case("bytes"))
            .unwrap_or(false);
        let len = resp.content_length();
        tracing::debug!(
            accept_ranges = accepts,
            content_length = ?len,
            "server returned {status}; using single stream",
        );
        return Ok(Probe::Single { len });
    }

    bail!("unexpected status {status} probing {url}");
}

/// Extract the total size from a `Content-Range` value such as `bytes 0-0/12345`.
/// Returns `None` when the total is unknown (`*`) or malformed.
fn parse_content_range_total(value: &str) -> Option<u64> {
    let total = value.rsplit('/').next()?.trim();
    if total == "*" {
        return None;
    }
    total.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::parse_content_range_total;

    #[test]
    fn parses_total() {
        assert_eq!(parse_content_range_total("bytes 0-0/12345"), Some(12345));
        assert_eq!(parse_content_range_total("bytes 0-1023/2048"), Some(2048));
    }

    #[test]
    fn unknown_or_bad_total() {
        assert_eq!(parse_content_range_total("bytes 0-0/*"), None);
        assert_eq!(parse_content_range_total("garbage"), None);
    }
}
