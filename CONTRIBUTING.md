# Contributing

Thanks for your interest in pcurl. Issues and pull requests are welcome.

## Scope

pcurl is a focused tool: fetch a remote file over many connections and write it
to stdout strictly in order, so it can be piped into a decompressor. Features
that fit that goal (throughput, resilience, correctness on the streaming path)
are in scope. Resume is intentionally absent because the output is a single-life
stream piped into a consumer; please discuss large feature additions in an issue
first.

## Development

```sh
cargo test                       # unit + integration + end-to-end (needs zstd for the pipeline test)
cargo test --test e2e <name>     # one end-to-end test
cargo clippy --all-targets -- -D warnings
cargo fmt
```

CI runs `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and the
full test suite. Please make sure all three pass before opening a PR. The
integration tests drive the compiled binary against a local `tiny_http` server
and run serially, so the suite takes ~15-20s.

## Invariants to preserve

A few properties are load-bearing. Changes that touch the download or output path
must keep them (see `CLAUDE.md` for the reasoning):

- **Byte-exactness.** reqwest is built with no gzip/brotli/deflate/zstd features
  so it never advertises `Accept-Encoding` and never decodes bodies. Enabling any
  of those corrupts per-range bytes. A test guards this.
- **stdout carries only the downloaded bytes.** All logs and progress go to
  stderr.
- **One semaphore permit per live chunk.** This single rule provides the memory
  bound, the effectively-bounded channel, and deadlock-freedom. Do not release
  permits early, add a separate bounded channel, or hand out chunk indices out of
  order.
- **Idle (read) timeout, never a total request timeout** — a total timeout would
  abort a healthy slow transfer mid-body.

## Adding a fetch / retry / throughput behavior

Extend the test server rather than mocking reqwest: add a `Mode` variant in
`tests/common/mod.rs` (see `FlakyRange`, `TrickleFirstChunk`, `FlakyStatus200`)
that reproduces the server behavior, then an end-to-end test in `tests/e2e.rs`
that drives the binary and asserts byte-exact output. Where the behavior is not
observable from output alone, assert on server-side state (for example
`TestServer::chunk_request_count()` proves a chunk was re-dispatched).

## Commits and pull requests

- Keep commits focused; the subject line should say what changes and the body why.
- No emoji in code, comments, or commit messages.
- Describe the motivation in the PR and note any user-visible behavior change.
