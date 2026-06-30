//! A small configurable HTTP test server used by the integration tests.
//!
//! Supports three behaviours:
//! * `Mode::Range` — honours `Range` requests with 206 + `Content-Range`.
//! * `Mode::NoRange` — ignores `Range`, always replies 200 with the full body
//!   (exercises the single-stream fallback).
//! * `Mode::FlakyRange` — like `Range`, but the first time it sees each distinct
//!   byte range it replies 503, then succeeds on retry (exercises per-chunk
//!   retry). The probe (`bytes=0-0`) never fails.
//!
//! [`serial_guard`] serialises the heavy network tests: spinning up many
//! servers and multi-connection downloads at once on a loaded machine starves
//! the test servers, so each test runs one at a time. The tool itself is fully
//! concurrent; this only bounds the test harness.

#![allow(dead_code)]

use std::collections::HashSet;
use std::io::Read;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Acquire the process-wide test lock; released when the guard drops.
pub fn serial_guard() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

use tiny_http::{Header, Response, Server};

#[derive(Clone, Copy, Debug)]
pub enum Mode {
    Range,
    NoRange,
    FlakyRange,
    /// Reply 416 Range Not Satisfiable to any range request (models an empty
    /// resource on an RFC-compliant server such as S3).
    Empty416,
    /// Answer the `bytes=0-0` probe with 206 + a total, but reply 200 (full
    /// body, range ignored) to every real chunk request.
    RangeChunks200,
    /// Like NoRange, but drip the 200 body slowly in pieces (exercises the
    /// idle/read timeout vs a total request timeout on the single-stream path).
    SlowNoRange,
    /// Like Range, but the first time it sees each distinct range it trickles
    /// the 206 body far below any sane `--min-speed`, then serves it at full
    /// speed on retry (exercises the throughput floor re-dispatching a chunk).
    TrickleFirstChunk,
    /// Like Range, but the first time it sees each distinct range it answers
    /// 200 (range ignored, as a flaky edge during failover would), then the
    /// correct 206 on retry (exercises the bounded soft-status retry).
    FlakyStatus200,
}

pub struct TestServer {
    pub url: String,
    server: Arc<Server>,
    stop: Arc<AtomicBool>,
    handles: Vec<JoinHandle<()>>,
    accept_encoding: Arc<Mutex<Vec<String>>>,
    chunk_requests: Arc<AtomicU64>,
}

impl TestServer {
    /// Start a server bound to an ephemeral port serving `data`.
    pub fn start(data: Vec<u8>, mode: Mode) -> Self {
        let server = Arc::new(Server::http("127.0.0.1:0").expect("bind test server"));
        let addr = server.server_addr();
        let url = format!("http://{}/file", addr.to_ip().expect("ip addr"));

        let data = Arc::new(data);
        let stop = Arc::new(AtomicBool::new(false));
        let seen: Arc<Mutex<HashSet<(u64, u64)>>> = Arc::new(Mutex::new(HashSet::new()));
        let accept_encoding: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let chunk_requests = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let server = Arc::clone(&server);
            let data = Arc::clone(&data);
            let stop = Arc::clone(&stop);
            let seen = Arc::clone(&seen);
            let accept_encoding = Arc::clone(&accept_encoding);
            let chunk_requests = Arc::clone(&chunk_requests);
            // Poll with a timeout so every worker re-checks the stop flag and
            // exits promptly on teardown. (`Server::unblock` only wakes one
            // blocked `recv`, which would hang the join with several workers.)
            handles.push(thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    match server.recv_timeout(Duration::from_millis(100)) {
                        Ok(Some(request)) => handle_request(
                            request,
                            &data,
                            mode,
                            &seen,
                            &accept_encoding,
                            &chunk_requests,
                        ),
                        Ok(None) => continue,
                        Err(_) => break,
                    }
                }
            }));
        }

        TestServer {
            url,
            server,
            stop,
            handles,
            accept_encoding,
            chunk_requests,
        }
    }

    /// Number of real (non-probe) chunk range requests the server received.
    /// Used to assert that a chunk was re-dispatched (requested more than once).
    pub fn chunk_request_count(&self) -> u64 {
        self.chunk_requests.load(Ordering::Relaxed)
    }

    /// All `Accept-Encoding` header values the server observed, in arrival order.
    /// A byte-exact downloader must not advertise content-encoding, so this
    /// should be empty after a download.
    pub fn accept_encoding_seen(&self) -> Vec<String> {
        self.accept_encoding.lock().unwrap().clone()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.server.unblock();
        for h in self.handles.drain(..) {
            let _ = h.join();
        }
    }
}

fn handle_request(
    request: tiny_http::Request,
    data: &[u8],
    mode: Mode,
    seen: &Mutex<HashSet<(u64, u64)>>,
    accept_encoding: &Mutex<Vec<String>>,
    chunk_requests: &AtomicU64,
) {
    let total = data.len() as u64;
    if let Some(ae) = request
        .headers()
        .iter()
        .find(|h| h.field.equiv("Accept-Encoding"))
    {
        accept_encoding
            .lock()
            .unwrap()
            .push(ae.value.as_str().to_string());
    }
    let range = request
        .headers()
        .iter()
        .find(|h| h.field.equiv("Range"))
        .and_then(|h| parse_range(h.value.as_str(), total));
    let is_probe = matches!(range, Some((0, 0)));
    if range.is_some() && !is_probe {
        chunk_requests.fetch_add(1, Ordering::Relaxed);
    }

    match mode {
        Mode::Empty416 => {
            // Any range request on an empty/unsatisfiable resource -> 416.
            let _ =
                request.respond(Response::empty(416).with_header(header("Connection", "close")));
        }
        Mode::RangeChunks200 if !is_probe => {
            // Real chunk request: ignore the range, return the full 200 body.
            let _ = request.respond(
                Response::from_data(data.to_vec())
                    .with_status_code(200)
                    .with_header(header("Connection", "close")),
            );
        }
        Mode::Range | Mode::FlakyRange | Mode::RangeChunks200 => match range {
            Some((start, end)) => {
                if matches!(mode, Mode::FlakyRange) && !is_probe {
                    let first_time = seen.lock().unwrap().insert((start, end));
                    if first_time {
                        let _ = request.respond(Response::empty(503));
                        return;
                    }
                }
                respond_range(request, data, start, end, total);
            }
            None => respond_full_200(request, data, true),
        },
        Mode::TrickleFirstChunk => match range {
            Some((start, end)) => {
                // First sight of a real chunk: drip it below --min-speed so the
                // throughput floor drops it; the retry (range already seen) is
                // served at full speed.
                if !is_probe && seen.lock().unwrap().insert((start, end)) {
                    trickle_range(request, data, start, end, total);
                } else {
                    respond_range(request, data, start, end, total);
                }
            }
            None => respond_full_200(request, data, true),
        },
        Mode::FlakyStatus200 => match range {
            Some((start, end)) => {
                // First sight of a real chunk: answer 200 (range ignored) like a
                // flaky edge; serve the correct 206 on retry.
                if !is_probe && seen.lock().unwrap().insert((start, end)) {
                    let _ = request.respond(
                        Response::from_data(data.to_vec())
                            .with_status_code(200)
                            .with_header(header("Connection", "close")),
                    );
                } else {
                    respond_range(request, data, start, end, total);
                }
            }
            None => respond_full_200(request, data, true),
        },
        Mode::NoRange => respond_full_200(request, data, false),
        Mode::SlowNoRange => {
            // Drip the full 200 body in pieces with small gaps so no single
            // read exceeds the idle timeout while the total transfer does.
            let reader = SlowReader::new(data.to_vec(), 32 * 1024, Duration::from_millis(150));
            let resp = Response::new(
                tiny_http::StatusCode(200),
                vec![header("Connection", "close")],
                reader,
                Some(data.len()),
                None,
            );
            let _ = request.respond(resp);
        }
    }
}

fn respond_range(request: tiny_http::Request, data: &[u8], start: u64, end: u64, total: u64) {
    let slice = &data[start as usize..=end as usize];
    // `Connection: close` keeps tiny_http off its keep-alive path, which can
    // intermittently leave a pooled request unread until the request timeout
    // fires. One request per connection is plenty for tests.
    let resp = Response::from_data(slice.to_vec())
        .with_status_code(206)
        .with_header(header(
            "Content-Range",
            &format!("bytes {start}-{end}/{total}"),
        ))
        .with_header(header("Accept-Ranges", "bytes"))
        .with_header(header("Connection", "close"));
    let _ = request.respond(resp);
}

/// Respond 206 for the range but drip the body at roughly 10 KiB/s, far below
/// any sane `--min-speed`, so the throughput floor drops and re-dispatches it.
fn trickle_range(request: tiny_http::Request, data: &[u8], start: u64, end: u64, total: u64) {
    let slice = data[start as usize..=end as usize].to_vec();
    let len = slice.len();
    let reader = SlowReader::new(slice, 2 * 1024, Duration::from_millis(200));
    let resp = Response::new(
        tiny_http::StatusCode(206),
        vec![
            header("Content-Range", &format!("bytes {start}-{end}/{total}")),
            header("Accept-Ranges", "bytes"),
            header("Connection", "close"),
        ],
        reader,
        Some(len),
        None,
    );
    let _ = request.respond(resp);
}

fn respond_full_200(request: tiny_http::Request, data: &[u8], advertise_ranges: bool) {
    let mut resp = Response::from_data(data.to_vec())
        .with_status_code(200)
        .with_header(header("Connection", "close"));
    if advertise_ranges {
        resp = resp.with_header(header("Accept-Ranges", "bytes"));
    }
    let _ = request.respond(resp);
}

fn header(name: &str, value: &str) -> Header {
    Header::from_bytes(name.as_bytes(), value.as_bytes()).expect("valid header")
}

/// A `Read` that yields `piece` bytes per call with a `gap` sleep before each
/// piece after the first, so tiny_http streams the body slowly.
struct SlowReader {
    data: Vec<u8>,
    pos: usize,
    piece: usize,
    gap: Duration,
}

impl SlowReader {
    fn new(data: Vec<u8>, piece: usize, gap: Duration) -> Self {
        Self {
            data,
            pos: 0,
            piece,
            gap,
        }
    }
}

impl Read for SlowReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.data.len() {
            return Ok(0);
        }
        if self.pos > 0 {
            thread::sleep(self.gap);
        }
        let n = self.piece.min(self.data.len() - self.pos).min(buf.len());
        buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// Parse `bytes=START-END` into inclusive absolute offsets, clamped to `total`.
fn parse_range(value: &str, total: u64) -> Option<(u64, u64)> {
    let spec = value.trim().strip_prefix("bytes=")?;
    let (start_s, end_s) = spec.split_once('-')?;
    let start: u64 = if start_s.is_empty() {
        0
    } else {
        start_s.parse().ok()?
    };
    let end: u64 = if end_s.is_empty() {
        total.saturating_sub(1)
    } else {
        end_s.parse().ok()?
    };
    if total == 0 {
        // No satisfiable range on an empty resource; fall back to a 200 reply.
        return None;
    }
    let end = end.min(total - 1);
    if start > end {
        return None;
    }
    Some((start, end))
}
