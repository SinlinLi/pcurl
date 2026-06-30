//! Parallel range fetching and the single-stream fallback.
//!
//! Both paths feed the same ordered [`crate::writer`] through an
//! [`UnboundedSender<ChunkMsg>`]; back-pressure and the memory bound come from
//! the shared semaphore whose permits ride along with each chunk.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use reqwest::header::{RANGE, RETRY_AFTER};
use reqwest::{Client, StatusCode};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::plan::ChunkPlan;
use crate::writer::ChunkMsg;

/// Upper bound on per-chunk buffer pre-allocation, so a very large `--chunk-size`
/// cannot reserve an unreasonable amount up front. The buffer still grows as the
/// body arrives; this only caps the initial reservation.
const MAX_PREALLOC: u64 = 16 * 1024 * 1024;

/// How many times a normally non-retryable status (a momentary 200/403/4xx from
/// one flaky edge during a failover) is retried before it is treated as fatal.
/// A genuinely unsupported/forbidden resource still fails after a few seconds.
const SOFT_STATUS_RETRIES: u32 = 3;

/// Cap on how long a server's `Retry-After` is honoured, so a hostile or absurd
/// value cannot stall a chunk indefinitely.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(120);

/// Retry / backoff policy for a single chunk.
#[derive(Debug, Clone, Copy)]
pub struct RetryCfg {
    pub retries: u32,
    pub backoff_ms: u64,
    pub backoff_max_ms: u64,
    /// Minimum sustained chunk throughput in bytes/s; `0` disables the floor.
    /// A chunk below this for a full window is dropped and retried so one
    /// trickling connection cannot stall the strictly in-order writer.
    pub min_speed: u64,
    /// Window in seconds over which `min_speed` is averaged.
    pub min_speed_window_secs: u64,
    /// Wall-clock retry budget per chunk in seconds; `0` falls back to the
    /// count-based `retries` bound. When set, a chunk keeps retrying transient
    /// failures until this elapses, so a fast-failing outage (connection
    /// refused / immediate 5xx) gets minutes of tolerance instead of being
    /// capped at the sum of the backoffs.
    pub retry_max_secs: u64,
}

impl RetryCfg {
    /// Capped exponential backoff with light jitter for attempt `n` (0-based).
    fn backoff(&self, attempt: u32) -> Duration {
        let shift = attempt.min(20);
        let base = self
            .backoff_ms
            .saturating_mul(1u64 << shift)
            .min(self.backoff_max_ms);
        let jitter_span = base / 4 + 1;
        let jitter = pseudo_jitter() % jitter_span;
        Duration::from_millis(base.saturating_sub(jitter_span / 2).saturating_add(jitter))
    }
}

/// Cheap non-cryptographic jitter source (avoids a dependency on an RNG).
fn pseudo_jitter() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0)
}

/// Drive `connections` workers fetching `plan`'s chunks in parallel.
///
/// Returns `Ok(())` when every chunk was dispatched and sent (or the run was
/// cancelled, e.g. by a broken output pipe), and `Err` on the first
/// unrecoverable chunk failure (after which all workers are cancelled).
#[allow(clippy::too_many_arguments)]
pub async fn run_ranged_workers(
    client: Client,
    url: String,
    plan: ChunkPlan,
    connections: u32,
    sem: Arc<Semaphore>,
    tx: UnboundedSender<ChunkMsg>,
    retry: RetryCfg,
    token: CancellationToken,
) -> Result<()> {
    let next = Arc::new(AtomicU64::new(0));
    let mut set: JoinSet<Result<()>> = JoinSet::new();

    for worker_id in 0..connections {
        let client = client.clone();
        let url = url.clone();
        let sem = Arc::clone(&sem);
        let tx = tx.clone();
        let token = token.clone();
        let next = Arc::clone(&next);
        set.spawn(async move {
            worker(worker_id, client, url, plan, sem, tx, retry, token, next).await
        });
    }
    // Drop the original sender so the channel closes once all workers finish.
    drop(tx);

    let mut fatal: Option<anyhow::Error> = None;
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                token.cancel();
                fatal.get_or_insert(e);
            }
            Err(join_err) => {
                token.cancel();
                fatal.get_or_insert_with(|| anyhow!("worker task panicked: {join_err}"));
            }
        }
    }

    match fatal {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn worker(
    worker_id: u32,
    client: Client,
    url: String,
    plan: ChunkPlan,
    sem: Arc<Semaphore>,
    tx: UnboundedSender<ChunkMsg>,
    retry: RetryCfg,
    token: CancellationToken,
    next: Arc<AtomicU64>,
) -> Result<()> {
    loop {
        // Acquire a buffer slot before claiming work, so memory stays bounded.
        let permit = tokio::select! {
            _ = token.cancelled() => return Ok(()),
            p = Arc::clone(&sem).acquire_owned() => match p {
                Ok(p) => p,
                Err(_) => return Ok(()), // semaphore closed
            },
        };

        let index = next.fetch_add(1, Ordering::Relaxed);
        if index >= plan.num_chunks {
            return Ok(()); // permit dropped here
        }

        let (start, end) = plan.range(index);
        let expected = plan.len(index);

        // Race the fetch against cancellation so a broken pipe (or any fatal
        // error elsewhere) drops the in-flight request immediately instead of
        // waiting for the body to finish or time out.
        let bytes = tokio::select! {
            _ = token.cancelled() => return Ok(()),
            r = fetch_chunk(&client, &url, start, end, expected, &retry, &token) => match r {
                Ok(b) => b,
                Err(e) => {
                    if token.is_cancelled() {
                        return Ok(());
                    }
                    token.cancel();
                    return Err(e.context(format!(
                        "worker {worker_id}: chunk {index} (bytes {start}-{end}) failed"
                    )));
                }
            },
        };

        tracing::trace!(worker_id, index, len = bytes.len(), "chunk fetched");
        if tx
            .send(ChunkMsg {
                index,
                bytes,
                permit,
            })
            .is_err()
        {
            return Ok(()); // writer is gone
        }
    }
}

/// Reason a single fetch attempt failed.
enum FetchError {
    /// The server replied with an unexpected status code (body dropped unread).
    /// Carries the parsed `Retry-After` delay if the server advertised one.
    Status(StatusCode, Option<Duration>),
    /// A transport, timeout, or body-read error.
    Transport(anyhow::Error),
}

/// Whether an unexpected status is worth retrying. Server errors and explicit
/// back-pressure are transient; everything else (a full 200, 403, 404, ...)
/// will not become a 206 on retry.
fn is_retryable_status(s: StatusCode) -> bool {
    s.is_server_error() || s == StatusCode::TOO_MANY_REQUESTS || s == StatusCode::REQUEST_TIMEOUT
}

/// Fetch one chunk, retrying transient failures with backoff.
async fn fetch_chunk(
    client: &Client,
    url: &str,
    start: u64,
    end: u64,
    expected: u64,
    retry: &RetryCfg,
    token: &CancellationToken,
) -> Result<Bytes> {
    let started = Instant::now();
    let mut attempt = 0u32;
    // A momentary 200/403/4xx from one flaky edge should not instantly abort a
    // long run; count those separately and only declare them fatal after a few.
    let mut soft_status = 0u32;
    loop {
        if token.is_cancelled() {
            bail!("cancelled");
        }
        let mut retry_after: Option<Duration> = None;
        let last_err: anyhow::Error = match try_fetch(
            client,
            url,
            start,
            end,
            expected,
            retry.min_speed,
            retry.min_speed_window_secs,
        )
        .await
        {
            Ok(bytes) if bytes.len() as u64 == expected => return Ok(bytes),
            Ok(bytes) => anyhow!("short read: got {} bytes, expected {expected}", bytes.len()),
            Err(FetchError::Transport(e)) => e,
            Err(FetchError::Status(s, ra)) if is_retryable_status(s) => {
                retry_after = ra;
                anyhow!("server returned {s}")
            }
            Err(FetchError::Status(s, _)) => {
                // Not classically retryable, but a brief 200/403/4xx from a
                // single edge during a failover should be absorbed. Give it a
                // few attempts; a genuinely unsupported/forbidden resource then
                // fails fast with actionable guidance.
                soft_status += 1;
                if soft_status > SOFT_STATUS_RETRIES {
                    if s == StatusCode::OK {
                        bail!(
                            "server ignored the byte range and returned 200 (full body); \
                             it does not support ranges here — re-run with --single"
                        );
                    }
                    bail!("server returned non-retryable status {s} for the range request");
                }
                anyhow!("server returned {s} (transient? retrying)")
            }
        };

        // Budget: wall-clock when retry_max_secs is set (default), else count.
        let exhausted = if retry.retry_max_secs > 0 {
            started.elapsed().as_secs() >= retry.retry_max_secs
        } else {
            attempt >= retry.retries
        };
        if exhausted {
            return Err(last_err.context(format!(
                "giving up after {} attempts over {:.0?}",
                attempt + 1,
                started.elapsed()
            )));
        }

        // Honour Retry-After (clamped) when present, but never wait less than
        // the computed backoff.
        let backoff = retry.backoff(attempt);
        let delay = match retry_after {
            Some(ra) => ra.min(MAX_RETRY_AFTER).max(backoff),
            None => backoff,
        };
        tracing::debug!(start, end, attempt, ?delay, error = %last_err, "retrying chunk");
        tokio::select! {
            _ = token.cancelled() => bail!("cancelled"),
            _ = tokio::time::sleep(delay) => {}
        }
        attempt += 1;
    }
}

/// Single attempt: request the range and read exactly that range's body.
///
/// The body is read as a stream so a minimum-throughput floor can be enforced.
/// The connect/idle timeout only bounds a *total* stall; a connection trickling
/// a few bytes per read keeps resetting it and never fails. When `min_speed > 0`,
/// a sliding window checks sustained throughput and fails the attempt (so the
/// retry loop re-dispatches the chunk, typically onto a fresh connection) if it
/// stays below the floor — preventing one slow edge from wedging the whole stream.
async fn try_fetch(
    client: &Client,
    url: &str,
    start: u64,
    end: u64,
    expected: u64,
    min_speed: u64,
    window_secs: u64,
) -> Result<Bytes, FetchError> {
    let window = Duration::from_secs(window_secs.max(1));
    let resp = client
        .get(url)
        .header(RANGE, format!("bytes={start}-{end}"))
        .send()
        .await
        .map_err(|e| FetchError::Transport(anyhow::Error::new(e).context("request error")))?;

    let status = resp.status();
    if status != StatusCode::PARTIAL_CONTENT {
        // Capture Retry-After (delta-seconds form) before dropping the body
        // unread; never buffer a potentially huge 200 body.
        let retry_after = resp
            .headers()
            .get(RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.trim().parse::<u64>().ok())
            .map(Duration::from_secs);
        return Err(FetchError::Status(status, retry_after));
    }

    let mut stream = resp.bytes_stream();
    let mut buf = BytesMut::with_capacity(expected.min(MAX_PREALLOC) as usize);
    // Start the throughput clock on the first body frame, not here: the gap
    // until first byte (connection warm-up, a cold-storage range assemble) is
    // not transfer-rate and must not be charged against `min_speed`.
    let mut started: Option<Instant> = None;
    let mut received: u64 = 0;

    while let Some(item) = stream.next().await {
        let bytes = item.map_err(|e| {
            FetchError::Transport(anyhow::Error::new(e).context("reading chunk body"))
        })?;
        received = received.saturating_add(bytes.len() as u64);
        buf.extend_from_slice(&bytes);

        // After the chunk has been transferring for at least `window` seconds,
        // require its cumulative average rate to stay at or above `min_speed`;
        // otherwise drop it so the retry loop re-dispatches it (usually onto a
        // fresh connection). Sampling only after the warm-up window avoids
        // judging a chunk on a tiny initial sample, and the cumulative average
        // forgives a brief dip that recovers.
        if min_speed > 0 {
            let elapsed = started.get_or_insert_with(Instant::now).elapsed();
            if elapsed >= window && (received as f64) < min_speed as f64 * elapsed.as_secs_f64() {
                return Err(FetchError::Transport(anyhow!(
                    "chunk too slow: {received} bytes in {:.0?} is below --min-speed ({min_speed} B/s)",
                    elapsed,
                )));
            }
        }
    }

    Ok(buf.freeze())
}

/// Stream the whole resource as one ordered sequence of frames.
///
/// Used when the server does not support ranges (or the size is unknown). Each
/// frame takes a semaphore permit before being sent, providing back-pressure.
pub async fn run_single_stream(
    client: Client,
    url: String,
    sem: Arc<Semaphore>,
    tx: UnboundedSender<ChunkMsg>,
    token: CancellationToken,
) -> Result<()> {
    let resp = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("request failed for {url}"))?;
    let status = resp.status();
    if !status.is_success() {
        token.cancel();
        bail!("unexpected status {status} for {url}");
    }

    let mut stream = resp.bytes_stream();
    let mut index: u64 = 0;
    loop {
        let item = tokio::select! {
            _ = token.cancelled() => return Ok(()),
            it = stream.next() => it,
        };
        let Some(item) = item else { break }; // stream ended cleanly

        let bytes = match item {
            Ok(b) => b,
            Err(e) => {
                if token.is_cancelled() {
                    return Ok(());
                }
                token.cancel();
                return Err(anyhow::Error::new(e).context("error reading response body"));
            }
        };
        if bytes.is_empty() {
            continue;
        }

        let permit = tokio::select! {
            _ = token.cancelled() => return Ok(()),
            p = Arc::clone(&sem).acquire_owned() => match p {
                Ok(p) => p,
                Err(_) => return Ok(()),
            },
        };
        if tx
            .send(ChunkMsg {
                index,
                bytes,
                permit,
            })
            .is_err()
        {
            return Ok(()); // writer gone
        }
        index += 1;
    }
    drop(tx);
    Ok(())
}
