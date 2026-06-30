//! Splitting a known content length into ordered byte ranges.

/// A plan describing how a file of `total` bytes is divided into chunks of at
/// most `chunk_size` bytes each.
#[derive(Debug, Clone, Copy)]
pub struct ChunkPlan {
    pub total: u64,
    pub chunk_size: u64,
    pub num_chunks: u64,
}

impl ChunkPlan {
    /// Build a plan for `total` bytes split into `chunk_size` pieces.
    pub fn new(total: u64, chunk_size: u64) -> Self {
        assert!(chunk_size > 0, "chunk size must be positive");
        let num_chunks = if total == 0 {
            0
        } else {
            total.div_ceil(chunk_size)
        };
        Self {
            total,
            chunk_size,
            num_chunks,
        }
    }

    /// Inclusive byte range `[start, end]` for chunk `index`, as used in an
    /// HTTP `Range: bytes=start-end` header.
    pub fn range(&self, index: u64) -> (u64, u64) {
        debug_assert!(index < self.num_chunks);
        // `start` is always < total (since (num_chunks - 1) * chunk_size < total),
        // so it never overflows. Use saturating_add for the upper bound so a
        // server-reported total near u64::MAX cannot overflow on the last chunk.
        let start = index * self.chunk_size;
        let end = start.saturating_add(self.chunk_size).min(self.total) - 1;
        (start, end)
    }

    /// Number of bytes in chunk `index`.
    pub fn len(&self, index: u64) -> u64 {
        let (start, end) = self.range(index);
        end - start + 1
    }
}

#[cfg(test)]
mod tests {
    use super::ChunkPlan;

    #[test]
    fn even_division() {
        let p = ChunkPlan::new(1000, 250);
        assert_eq!(p.num_chunks, 4);
        assert_eq!(p.range(0), (0, 249));
        assert_eq!(p.range(3), (750, 999));
        assert_eq!(p.len(3), 250);
    }

    #[test]
    fn ragged_last_chunk() {
        let p = ChunkPlan::new(1001, 250);
        assert_eq!(p.num_chunks, 5);
        assert_eq!(p.range(4), (1000, 1000));
        assert_eq!(p.len(4), 1);
    }

    #[test]
    fn ranges_tile_the_file_without_gaps() {
        let p = ChunkPlan::new(123_456, 1000);
        let mut next = 0u64;
        let mut covered = 0u64;
        for i in 0..p.num_chunks {
            let (start, end) = p.range(i);
            assert_eq!(start, next, "chunk {i} must start where the previous ended");
            next = end + 1;
            covered += p.len(i);
        }
        assert_eq!(next, p.total);
        assert_eq!(covered, p.total);
    }

    #[test]
    fn empty_file() {
        let p = ChunkPlan::new(0, 1000);
        assert_eq!(p.num_chunks, 0);
    }

    #[test]
    fn huge_total_does_not_overflow() {
        // A server-reported total near u64::MAX must not overflow the range
        // math on the last chunk (debug builds have overflow checks on).
        let p = ChunkPlan::new(u64::MAX, 8 * 1024 * 1024);
        let last = p.num_chunks - 1;
        let (start, end) = p.range(last);
        assert_eq!(end, u64::MAX - 1);
        assert!(start <= end);
        assert!(p.len(last) >= 1);
    }
}
