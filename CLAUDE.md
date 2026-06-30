# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`pcurl` is a parallel HTTP range downloader (single binary, ~1500 lines of Rust)
that fetches a remote file over several connections and writes it to stdout in
the original byte order, ready to pipe straight into a decompressor
(`pcurl URL | zstd -d | tar x`). Async runtime is Tokio; HTTP is reqwest/rustls.

## Commands

```sh
cargo build --release                       # -> ./target/release/pcurl
cargo test                                  # unit + integration + e2e
cargo clippy --all-targets -- -D warnings   # CI denies warnings
cargo fmt --check                           # CI enforces formatting
```

- Run one test by name substring: `cargo test min_speed` (matches across all
  targets). One e2e test: `cargo test --test e2e <name>`. A unit module:
  `cargo test plan::`.
- The integration tests (`tests/`) compile and drive the real binary
  (`env!("CARGO_BIN_EXE_pcurl")`) against a local `tiny_http` server. They are
  serialized by a process-wide lock (`serial_guard()` in `tests/common/mod.rs`)
  and take ~15-20s; the `tar.zst` pipeline test (`tests/complex_pipeline.rs`)
  needs `zstd` on PATH. CI runs `cargo test --all -- --test-threads=2`.

## Architecture

One streaming pipeline, assembled in `main.rs::run` -> `pipeline`:

```
probe (probe.rs) -> ChunkPlan (plan.rs) -> worker pool (download.rs)
  -> mpsc channel -> ordered writer on a blocking thread (writer.rs) -> stdout/file
```

`cli.rs` defines flags; `progress.rs` and `logging.rs` write to stderr only.

Two paths branch off the probe:
- `Probe::Ranged { total }` -> `run_ranged_workers`: N workers fetch `Range`
  chunks in parallel and a `BTreeMap` reorders them.
- `Probe::Single` -> `run_single_stream`: one straight-through body, used when
  the origin does not support ranges (or `--single`).

Both feed the same writer through `ChunkMsg`.

## Load-bearing invariants (do not break these)

- **One semaphore permit per live chunk.** A worker acquires a permit BEFORE
  claiming the next chunk index (an atomic counter), the permit rides inside
  `ChunkMsg`, and it is dropped only after the writer writes that chunk. This
  single rule gives, simultaneously: bounded memory (`~ max_buffered *
  chunk_size`), an effectively bounded channel (the `unbounded_channel` cannot
  exceed `max_buffered` messages because a `ChunkMsg` cannot exist without a
  permit), and deadlock-freedom (indices are handed out strictly increasing, so
  the chunk the in-order writer needs next is always already in flight). Do not
  add a separate bounded channel, release permits early, or hand out indices out
  of order.

- **Byte-exactness via no content-decoding.** `Cargo.toml` builds reqwest with
  `default-features = false` and deliberately omits gzip/brotli/deflate/zstd, so
  it never sends `Accept-Encoding` and never decodes bodies — otherwise
  transparent decoding would corrupt per-range bytes. The test
  `client_does_not_request_content_encoding` guards this. Never add those
  features.

- **stdout carries only the downloaded bytes.** All logs and the progress line
  go to stderr. Never write anything else to stdout.

- **Idle (read) timeout, never a total request timeout.** `probe.rs::build_client`
  sets `connect_timeout` + `read_timeout` only. A total request timeout would
  abort a healthy slow transfer mid-body and corrupt whatever the pipe consumer
  already received.

- **HTTP/1.1 is the default** (`build_client` calls `.http1_only()` unless
  `--http2`), so each worker is its own TCP flow and parallelism actually beats
  per-connection rate limits. `--http2` multiplexes all workers onto one
  connection.

## Reliability model (why the retry/throughput code exists)

The output stream is single-life: it is piped into a decompressor and cannot be
resumed, so a single chunk's terminal failure aborts the whole run. The
defaults are therefore sized to survive ordinary network reality on a multi-hour
transfer, in `download.rs::fetch_chunk` / `try_fetch`:
- Per-chunk retry with capped exponential backoff + jitter, bounded by a
  wall-clock budget (`--retry-max-secs`, default 300s) rather than only an
  attempt count.
- `Retry-After` is honoured (clamped) on 429/503.
- A normally non-retryable status (a momentary 200/403 from one flaky edge) gets
  a few bounded retries before it is fatal (`SOFT_STATUS_RETRIES`).
- A windowed minimum-throughput floor (`--min-speed`, cumulative average over
  `--min-speed-window`) drops and re-dispatches a wedged/trickling chunk so one
  slow connection cannot stall the in-order writer.

## Exit semantics

`main.rs::finalize` maps the writer outcome to the process result. A broken
output pipe is success only when the producer did not also error (so a real
download failure is not masked by the consumer closing the pipe);
`WriterOutcome::Incomplete` (channel closed before every chunk arrived) is exit
1. In a shell pipeline, callers must use `set -o pipefail` to see pcurl's status.
`panic = "abort"` (release): a task panic aborts the process; the
"task panicked" `JoinError` branches are effectively debug/test-only.

## Adding a fetch/retry/throughput behavior

Extend the test server, do not mock reqwest. Add a `Mode` variant in
`tests/common/mod.rs` (see `FlakyRange`, `TrickleFirstChunk`, `FlakyStatus200`)
that reproduces the server behavior, then an e2e test in `tests/e2e.rs` driving
the binary and asserting byte-exact output with `assert_bytes_eq`. Where the
mechanism is not observable from output alone, assert on server-side state
(e.g. `TestServer::chunk_request_count()` proves a chunk was re-dispatched).
