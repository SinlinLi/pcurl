//! Ordered reassembly and output.
//!
//! Workers fetch chunks out of order and send them here. The writer keeps a
//! small `BTreeMap` of chunks that arrived early and emits them to the output
//! strictly in index order, so the byte stream on stdout is identical to the
//! original file.
//!
//! Each in-flight/buffered chunk carries a semaphore permit that is released
//! only once the chunk has been written. That single invariant bounds peak
//! memory to roughly `max_buffered * chunk_size` and, because chunk indices are
//! dispatched in increasing order, guarantees the next chunk to write is always
//! already in flight — so reassembly never deadlocks.

use std::collections::BTreeMap;
use std::io::{ErrorKind, Write};
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::sync::CancellationToken;

use crate::progress::Progress;

/// A downloaded chunk on its way to the output.
pub struct ChunkMsg {
    pub index: u64,
    pub bytes: Bytes,
    /// Held until the chunk is written; dropping it (after the write) frees a
    /// buffer slot so a new chunk can be fetched. Held for its `Drop` effect.
    #[allow(dead_code)]
    pub permit: OwnedSemaphorePermit,
}

/// How the writer loop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterOutcome {
    /// All expected bytes were written (ranged: every chunk; single: stream end).
    Complete,
    /// The output pipe was closed by the consumer; a clean early stop.
    BrokenPipe,
    /// A bounded run's channel closed before all chunks arrived (upstream error).
    Incomplete,
}

/// Run the ordered writer loop to completion. Intended to run on a blocking
/// task because it performs synchronous writes to `out`.
///
/// * `expected = Some(n)` (ranged mode): write chunks `0..n` in order; if the
///   channel closes early, report [`WriterOutcome::Incomplete`].
/// * `expected = None` (single-stream mode): write frames in arrival order
///   until the channel closes, then report [`WriterOutcome::Complete`]. The
///   producer's own result is authoritative for success in this mode.
pub fn run(
    mut rx: UnboundedReceiver<ChunkMsg>,
    out: Box<dyn Write + Send>,
    expected: Option<u64>,
    progress: Arc<Progress>,
    token: CancellationToken,
) -> std::io::Result<WriterOutcome> {
    let mut out = std::io::BufWriter::with_capacity(1 << 20, out);
    let mut pending: BTreeMap<u64, ChunkMsg> = BTreeMap::new();
    let mut next_write: u64 = 0;

    loop {
        if matches!(expected, Some(n) if next_write >= n) {
            break;
        }
        let Some(msg) = rx.blocking_recv() else {
            // Channel closed.
            if expected.is_some() {
                return Ok(WriterOutcome::Incomplete);
            }
            break; // single-stream: normal end of stream
        };
        pending.insert(msg.index, msg);

        while let Some(msg) = pending.remove(&next_write) {
            let len = msg.bytes.len() as u64;
            match out.write_all(&msg.bytes) {
                Ok(()) => {
                    progress.add(len);
                    next_write += 1;
                    // `msg` (and its permit) drops here, freeing a buffer slot.
                }
                Err(e) if e.kind() == ErrorKind::BrokenPipe => {
                    token.cancel();
                    return Ok(WriterOutcome::BrokenPipe);
                }
                Err(e) => {
                    token.cancel();
                    return Err(e);
                }
            }
            if matches!(expected, Some(n) if next_write >= n) {
                break;
            }
        }
    }

    match out.flush() {
        Ok(()) => Ok(WriterOutcome::Complete),
        Err(e) if e.kind() == ErrorKind::BrokenPipe => Ok(WriterOutcome::BrokenPipe),
        Err(e) => Err(e),
    }
}
