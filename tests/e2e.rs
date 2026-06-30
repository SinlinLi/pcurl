//! End-to-end tests driving the compiled `pcurl` binary against a local
//! HTTP server, asserting byte-exact, strictly-ordered output.

mod common;

use std::process::{Command, Stdio};

use common::{Mode, TestServer};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};

const BIN: &str = env!("CARGO_BIN_EXE_pcurl");

/// Deterministic pseudo-random bytes so failures are reproducible.
fn random_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut buf = vec![0u8; len];
    rng.fill_bytes(&mut buf);
    buf
}

/// Compare without dumping megabytes on failure.
fn assert_bytes_eq(got: &[u8], want: &[u8], ctx: &str) {
    if got.len() != want.len() {
        panic!(
            "{ctx}: length mismatch: got {} bytes, want {} bytes",
            got.len(),
            want.len()
        );
    }
    if let Some(i) = got.iter().zip(want).position(|(a, b)| a != b) {
        panic!(
            "{ctx}: byte mismatch at offset {i}: got {:#04x}, want {:#04x}",
            got[i], want[i]
        );
    }
}

/// Run the binary, capture stdout, and assert it exited successfully.
///
/// A short per-request timeout keeps the suite fast: the bundled tiny_http test
/// server occasionally leaves a connection's request unserved, and a quick
/// timeout lets pcurl's retry recover on a fresh connection in seconds
/// instead of waiting out the default 60s.
fn run_capture(url: &str, extra: &[&str]) -> Vec<u8> {
    let out = Command::new(BIN)
        .arg(url)
        .args(extra)
        .args(["--quiet", "-t", "5"])
        .stderr(Stdio::inherit())
        .output()
        .expect("spawn pcurl");
    assert!(
        out.status.success(),
        "pcurl exited with {:?} for {url} {extra:?}",
        out.status.code()
    );
    out.stdout
}

#[test]
fn multi_connection_byte_exact() {
    let _g = common::serial_guard();
    let data = random_bytes(5 * 1024 * 1024 + 12345, 1);
    let server = TestServer::start(data.clone(), Mode::Range);
    let got = run_capture(&server.url, &["-c", "8", "-s", "256K"]);
    assert_bytes_eq(&got, &data, "multi_connection");
}

#[test]
fn small_chunks_many_workers() {
    let _g = common::serial_guard();
    // Force lots of small chunks reassembled by many workers with a tiny buffer.
    let data = random_bytes(777 * 1024, 2);
    let server = TestServer::start(data.clone(), Mode::Range);
    let got = run_capture(
        &server.url,
        &["-c", "16", "-s", "8K", "--max-buffered", "4"],
    );
    assert_bytes_eq(&got, &data, "small_chunks");
}

#[test]
fn fallback_when_no_range_support() {
    let _g = common::serial_guard();
    let data = random_bytes(2 * 1024 * 1024 + 99, 3);
    let server = TestServer::start(data.clone(), Mode::NoRange);
    let got = run_capture(&server.url, &["-c", "8", "-s", "128K"]);
    assert_bytes_eq(&got, &data, "no_range_fallback");
}

#[test]
fn forced_single_stream_byte_exact() {
    let _g = common::serial_guard();
    // Server supports ranges, but --single forces the straight-through path.
    let data = random_bytes(1024 * 1024 + 7, 4);
    let server = TestServer::start(data.clone(), Mode::Range);
    let got = run_capture(&server.url, &["--single"]);
    assert_bytes_eq(&got, &data, "forced_single");
}

#[test]
fn per_chunk_retry_recovers() {
    let _g = common::serial_guard();
    // Every distinct range fails once with 503, then succeeds.
    let data = random_bytes(1024 * 1024, 5);
    let server = TestServer::start(data.clone(), Mode::FlakyRange);
    let got = run_capture(&server.url, &["-c", "8", "-s", "64K", "-r", "5"]);
    assert_bytes_eq(&got, &data, "flaky_retry");
}

#[test]
fn min_speed_floor_redispatches_trickling_chunk() {
    let _g = common::serial_guard();
    // Each chunk is trickled (~10 KiB/s) on first sight, then served fast on
    // retry. A 1 MiB/s floor over a 1s window drops the slow attempt; the retry
    // recovers it, so the final output is still byte-exact.
    let data = random_bytes(256 * 1024, 7);
    let num_chunks = 256 / 64; // 64K chunks
    let server = TestServer::start(data.clone(), Mode::TrickleFirstChunk);
    let got = run_capture(
        &server.url,
        &[
            "-c",
            "4",
            "-s",
            "64K",
            "--min-speed",
            "1M",
            "--min-speed-window",
            "1",
            "-r",
            "5",
        ],
    );
    assert_bytes_eq(&got, &data, "min_speed_trickle");
    // The byte-exact check alone would pass even with the floor disabled (the
    // trickle delivers the full body, just slowly). Assert the floor actually
    // fired: every chunk's first (trickled) attempt was dropped and re-fetched,
    // so each range was requested at least twice.
    let reqs = server.chunk_request_count();
    assert!(
        reqs >= 2 * num_chunks,
        "expected each of {num_chunks} chunks re-dispatched (>= {} requests), got {reqs}; \
         the min-speed floor did not fire",
        2 * num_chunks
    );
}

#[test]
fn http2_flag_downloads_byte_exact() {
    let _g = common::serial_guard();
    // Exercises the --http2 client-build branch (which skips http1_only). The
    // bundled server is HTTP/1.1, so this also covers a clean ALPN downgrade:
    // the download must still be byte-exact.
    let data = random_bytes(512 * 1024, 8);
    let server = TestServer::start(data.clone(), Mode::Range);
    let got = run_capture(&server.url, &["-c", "4", "-s", "64K", "--http2"]);
    assert_bytes_eq(&got, &data, "http2_flag");
}

#[test]
fn small_file_byte_exact() {
    let _g = common::serial_guard();
    let data = b"hello, pcurl!".to_vec();
    let server = TestServer::start(data.clone(), Mode::Range);
    let got = run_capture(&server.url, &["-c", "4", "-s", "4M"]);
    assert_bytes_eq(&got, &data, "small_file");
}

#[test]
fn empty_file() {
    let _g = common::serial_guard();
    let server = TestServer::start(Vec::new(), Mode::Range);
    let got = run_capture(&server.url, &[]);
    assert!(
        got.is_empty(),
        "expected empty output, got {} bytes",
        got.len()
    );
}

#[test]
fn writes_to_output_file() {
    let _g = common::serial_guard();
    let data = random_bytes(512 * 1024 + 3, 6);
    let server = TestServer::start(data.clone(), Mode::Range);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.bin");
    let status = Command::new(BIN)
        .arg(&server.url)
        .args(["-c", "6", "-s", "32K", "-q", "-t", "5", "-o"])
        .arg(&path)
        .status()
        .expect("spawn pcurl");
    assert!(status.success());
    let got = std::fs::read(&path).unwrap();
    assert_bytes_eq(&got, &data, "output_file");
}

#[test]
fn client_does_not_request_content_encoding() {
    // A byte-exact range downloader must not advertise Accept-Encoding, or a
    // server could transparently compress/encode bodies and corrupt the
    // per-range bytes. Verify no request carried Accept-Encoding.
    let _g = common::serial_guard();
    let data = random_bytes(300 * 1024, 11);
    let server = TestServer::start(data.clone(), Mode::Range);
    let got = run_capture(&server.url, &["-c", "4", "-s", "64K"]);
    assert_bytes_eq(&got, &data, "no_content_encoding");
    let seen = server.accept_encoding_seen();
    assert!(
        seen.is_empty(),
        "pcurl advertised Accept-Encoding (would invite transparent decoding): {seen:?}"
    );
}

#[test]
fn empty_resource_via_416() {
    // An empty resource on an RFC-compliant range server answers bytes=0-0 with
    // 416; pcurl must treat that as empty (exit 0, no output), not an error.
    let _g = common::serial_guard();
    let server = TestServer::start(Vec::new(), Mode::Empty416);
    let got = run_capture(&server.url, &[]);
    assert!(
        got.is_empty(),
        "expected empty output, got {} bytes",
        got.len()
    );
}

#[test]
fn range_answered_with_200_fails_fast() {
    // Probe reports range support (206), but chunk requests come back as full
    // 200s. pcurl must fail fast with guidance rather than retry forever.
    let _g = common::serial_guard();
    let data = random_bytes(1024 * 1024, 12);
    let server = TestServer::start(data, Mode::RangeChunks200);
    let start = std::time::Instant::now();
    let out = Command::new(BIN)
        .arg(&server.url)
        .args(["-c", "4", "-s", "64K", "-q", "-t", "5"])
        .output()
        .expect("spawn pcurl");
    let elapsed = start.elapsed();
    assert!(
        !out.status.success(),
        "expected nonzero exit when ranges are not honored"
    );
    assert!(
        elapsed.as_secs() < 15,
        "should fail fast on a non-retryable 200, took {elapsed:?}"
    );
}

#[test]
fn single_stream_slow_transfer_not_aborted() {
    // A healthy but slow single-stream transfer must not be killed by the
    // timeout: each read is well within the idle window, but the whole body
    // takes longer than the timeout. A total request timeout would corrupt the
    // stream by aborting mid-transfer.
    let _g = common::serial_guard();
    let data = random_bytes(512 * 1024, 13);
    let server = TestServer::start(data.clone(), Mode::SlowNoRange);
    let start = std::time::Instant::now();
    let out = Command::new(BIN)
        .arg(&server.url)
        .args(["-q", "-t", "1"]) // 1s idle timeout; drip gaps are 150ms
        .stderr(Stdio::inherit())
        .output()
        .expect("spawn pcurl");
    let elapsed = start.elapsed();
    assert!(
        out.status.success(),
        "slow transfer was aborted: {:?}",
        out.status
    );
    assert_bytes_eq(&out.stdout, &data, "slow_single_stream");
    assert!(
        elapsed.as_secs() >= 1,
        "expected the drip to exceed the 1s total-timeout window, took {elapsed:?}"
    );
}

#[test]
fn broken_pipe_exits_cleanly() {
    let _g = common::serial_guard();
    // Downstream consumer reads only a prefix and closes the pipe. pcurl
    // must stop without error (exit 0) rather than hang or crash.
    let data = random_bytes(8 * 1024 * 1024, 7);
    let server = TestServer::start(data.clone(), Mode::Range);
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("prefix.bin");
    let script = format!(
        "set -o pipefail; '{}' '{}' -c 8 -s 64K -q -t 5 | head -c 4096 > '{}'",
        BIN,
        server.url,
        out.display()
    );
    let start = std::time::Instant::now();
    let status = Command::new("bash")
        .arg("-c")
        .arg(&script)
        .status()
        .expect("spawn bash pipeline");
    let elapsed = start.elapsed();
    assert!(
        status.success(),
        "pipeline (with pipefail) failed: {:?}",
        status.code()
    );
    assert!(
        elapsed.as_secs() < 20,
        "pcurl took {elapsed:?} to stop after broken pipe (should be near-instant)"
    );
    let prefix = std::fs::read(&out).unwrap();
    assert_eq!(prefix.len(), 4096, "head should capture exactly 4096 bytes");
    assert_bytes_eq(&prefix, &data[..4096], "broken_pipe_prefix");
}
