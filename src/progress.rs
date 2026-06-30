//! Progress reporting on stderr.
//!
//! A single shared counter is updated by the writer as bytes reach stdout; a
//! background task renders a carriage-return status line a few times per
//! second. Progress never touches stdout, so it cannot corrupt the piped data.

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Shared download progress state.
pub struct Progress {
    written: AtomicU64,
    total: Option<u64>,
    enabled: bool,
    start: Instant,
}

impl Progress {
    pub fn new(total: Option<u64>, enabled: bool) -> Arc<Self> {
        Arc::new(Self {
            written: AtomicU64::new(0),
            total,
            enabled,
            start: Instant::now(),
        })
    }

    /// Record `n` more bytes delivered to the output.
    #[inline]
    pub fn add(&self, n: u64) {
        self.written.fetch_add(n, Ordering::Relaxed);
    }

    pub fn written(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }

    /// Spawn the background renderer. Returns a no-op handle when disabled.
    pub fn spawn_reporter(self: &Arc<Self>, token: CancellationToken) -> JoinHandle<()> {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            if !this.enabled {
                return;
            }
            let mut ticker = tokio::time::interval(Duration::from_millis(200));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut last_bytes = 0u64;
            let mut last_at = Instant::now();
            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = ticker.tick() => {
                        let now = Instant::now();
                        let bytes = this.written();
                        let inst = {
                            let dt = now.duration_since(last_at).as_secs_f64();
                            if dt > 0.0 { (bytes - last_bytes) as f64 / dt } else { 0.0 }
                        };
                        last_bytes = bytes;
                        last_at = now;
                        this.render(bytes, inst);
                    }
                }
            }
        })
    }

    /// Render one status line to stderr in place.
    fn render(&self, bytes: u64, inst_bps: f64) {
        let elapsed = self.start.elapsed().as_secs_f64();
        let avg_bps = if elapsed > 0.0 {
            bytes as f64 / elapsed
        } else {
            0.0
        };
        let mut err = std::io::stderr().lock();
        let line = match self.total {
            Some(total) if total > 0 => {
                let pct = (bytes as f64 / total as f64) * 100.0;
                format!(
                    "\r{:>7} / {:>7}  {:5.1}%  {:>9}/s (avg {:>9}/s)  {:>6}",
                    human(bytes),
                    human(total),
                    pct,
                    human(inst_bps as u64),
                    human(avg_bps as u64),
                    elapsed_str(elapsed),
                )
            }
            _ => format!(
                "\r{:>7}  {:>9}/s (avg {:>9}/s)  {:>6}",
                human(bytes),
                human(inst_bps as u64),
                human(avg_bps as u64),
                elapsed_str(elapsed),
            ),
        };
        let _ = write!(err, "{line}");
        let _ = err.flush();
    }

    /// Print the final summary line (with a trailing newline) and stop.
    pub fn finish(&self) {
        if !self.enabled {
            return;
        }
        let bytes = self.written();
        let elapsed = self.start.elapsed().as_secs_f64();
        let avg = if elapsed > 0.0 {
            bytes as f64 / elapsed
        } else {
            0.0
        };
        let mut err = std::io::stderr().lock();
        let _ = writeln!(
            err,
            "\r{} in {} ({}/s){:<10}",
            human(bytes),
            elapsed_str(elapsed),
            human(avg as u64),
            "",
        );
        let _ = err.flush();
    }
}

/// Format a byte count with a binary unit suffix.
fn human(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

/// Format an elapsed duration as `Hh`, `Mm`, or `Ss`.
fn elapsed_str(secs: f64) -> String {
    let s = secs as u64;
    if s >= 3600 {
        format!("{}h{:02}m", s / 3600, (s % 3600) / 60)
    } else if s >= 60 {
        format!("{}m{:02}s", s / 60, s % 60)
    } else {
        format!("{secs:.1}s")
    }
}

#[cfg(test)]
mod tests {
    use super::human;

    #[test]
    fn human_units() {
        assert_eq!(human(512), "512 B");
        assert_eq!(human(1024), "1.0 KiB");
        assert_eq!(human(1536), "1.5 KiB");
        assert_eq!(human(1024 * 1024), "1.0 MiB");
    }
}
