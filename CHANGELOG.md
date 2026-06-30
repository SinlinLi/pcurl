# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/). While the version is `0.x`, the CLI
and behavior may change between minor releases.

## [Unreleased]

First public release. When you tag, rename this section to
`## [0.1.0] - YYYY-MM-DD`.

### Added

- Multi-connection range download with strict in-order streaming to stdout,
  ready to pipe straight into a decompressor (`pcurl URL | zstd -d | tar x`).
- Bounded memory via one semaphore permit per live chunk (peak
  `~ max_buffered * chunk_size`), with automatic back-pressure when the consumer
  is slower than the network.
- HTTP/1.1 by default so each connection is an independent TCP flow that can beat
  per-connection rate limits; `--http2` to multiplex onto one connection.
- Per-chunk resilience for long unattended transfers: a wall-clock retry budget
  (`--retry-max-secs`), capped exponential backoff with jitter, `Retry-After`
  handling, a bounded retry for a transient non-206 status, and a minimum
  throughput floor (`--min-speed`) that re-dispatches a wedged chunk.
- Single-stream fallback when the origin does not support ranges, with a
  Content-Length check so a truncated body is reported as a failure.
- SIGINT/SIGTERM handling (exit `130` with an attributable message); a broken
  output pipe is a clean stop and does not mask a producer error.
- Verbatim bytes (no transparent content-decoding), leveled stderr logging, and
  optional rotating file logs (`--log-dir`).
- READMEs in English, Simplified Chinese, Japanese, Korean, and Spanish.
- Tag-triggered release workflow that builds static musl binaries
  (`x86_64`, `aarch64`).
