//! Command-line interface definition.

use std::path::PathBuf;

use clap::Parser;

/// Parallel HTTP downloader that streams strictly in order to stdout.
///
/// Splits a remote file into byte ranges, fetches them with several
/// connections in parallel, reassembles them in order in a bounded buffer,
/// and writes the original byte stream to stdout so it can be piped straight
/// into a decompressor:
///
///   pcurl https://example.com/huge.tar.zst | zstd -d | tar x
#[derive(Debug, Parser)]
#[command(name = "pcurl", version, about, long_about = None)]
pub struct Cli {
    /// URL to download.
    pub url: String,

    /// Number of parallel connections (workers).
    #[arg(short = 'c', long, default_value_t = 8, value_parser = clap::value_parser!(u32).range(1..=1024))]
    pub connections: u32,

    /// Size of each range chunk, e.g. 4M, 512K, 1048576.
    #[arg(short = 's', long, default_value = "8M", value_parser = parse_size)]
    pub chunk_size: u64,

    /// Maximum number of chunks held in memory at once (downloading + buffered).
    ///
    /// Peak memory is roughly `max_buffered * chunk_size`. Defaults to twice the
    /// number of connections, giving each connection one chunk of read-ahead so a
    /// single slow chunk cannot stall the in-order writer and collapse throughput.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=65536))]
    pub max_buffered: Option<u32>,

    /// Per-chunk retry attempts after the first failure. The default is sized
    /// for long unattended transfers: with no resume, a chunk's retries are the
    /// only thing that rides out a transient origin/CDN blip, so a single hiccup
    /// does not abort the whole download.
    #[arg(short = 'r', long, default_value_t = 20, value_parser = clap::value_parser!(u32).range(0..=1000))]
    pub retries: u32,

    /// Connect and idle (read) timeout in seconds; resets on each read, so it
    /// bounds stalls without aborting a healthy slow transfer (0 disables it).
    #[arg(short = 't', long, default_value_t = 60)]
    pub timeout: u64,

    /// Minimum sustained per-chunk download speed (e.g. `64K`, `1M`); `0`
    /// disables it. A chunk whose throughput stays below this for ~15s is
    /// dropped and retried, so one trickling connection cannot wedge the
    /// strictly in-order stream. The default catches stalled/trickling
    /// connections; raise it (e.g. `1M`) on a fast link to also re-dispatch
    /// merely-slow edges. Applies to ranged parallel downloads only.
    #[arg(long, value_parser = parse_min_speed, default_value = "8K")]
    pub min_speed: u64,

    /// Window in seconds over which `--min-speed` is averaged before a chunk is
    /// judged too slow. To re-dispatch a merely-slow (not fully stalled) edge,
    /// set this shorter than a healthy chunk's transfer time
    /// (~ chunk-size / per-connection speed) and raise --min-speed; the default
    /// is tuned to catch stalled/trickling connections, not merely-slow ones.
    #[arg(long, default_value_t = 15, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub min_speed_window: u64,

    /// Base backoff in milliseconds; doubles each retry up to `backoff_max_ms`.
    #[arg(long, default_value_t = 200)]
    pub backoff_ms: u64,

    /// Maximum backoff in milliseconds.
    #[arg(long, default_value_t = 10_000)]
    pub backoff_max_ms: u64,

    /// Per-chunk wall-clock retry budget in seconds; a chunk keeps retrying a
    /// transient failure until this elapses (0 = use the --retries count
    /// instead). This is the real resilience knob for long unattended
    /// transfers: it bounds how long an outage one chunk can ride out
    /// regardless of how fast each attempt fails, so a fast-refusing outage
    /// does not abort the run as quickly as a fixed attempt count would.
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(0..=86_400))]
    pub retry_max_secs: u64,

    /// Write to this file instead of stdout.
    #[arg(short = 'o', long)]
    pub output: Option<PathBuf>,

    /// Force a single straight-through stream (no range parallelism).
    #[arg(long)]
    pub single: bool,

    /// Use HTTP/2 when the server offers it (ALPN). By default pcurl forces
    /// HTTP/1.1 so each parallel connection is a separate TCP flow; over HTTP/2
    /// the workers multiplex onto one TCP connection, which cannot beat
    /// per-connection rate limits.
    #[arg(long)]
    pub http2: bool,

    /// HTTP header to send, repeatable. Format: "Name: value".
    #[arg(short = 'H', long = "header", value_name = "HEADER")]
    pub headers: Vec<String>,

    /// Suppress the progress line on stderr.
    #[arg(short = 'q', long)]
    pub quiet: bool,

    /// Force the progress line even when stderr is not a terminal.
    #[arg(long, conflicts_with = "quiet")]
    pub progress: bool,

    /// Increase log verbosity on stderr (-v debug, -vv trace). Overridden by RUST_LOG.
    #[arg(short = 'v', long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Also write structured logs to a rotating file in this directory.
    #[arg(long, value_name = "DIR", env = "PCURL_LOG_DIR")]
    pub log_dir: Option<PathBuf>,

    /// Number of rotated log files to keep when --log-dir is set.
    #[arg(long, default_value_t = 7)]
    pub log_keep: usize,

    /// User-Agent header value.
    #[arg(long, default_value = concat!("pcurl/", env!("CARGO_PKG_VERSION")))]
    pub user_agent: String,
}

/// Default read-ahead: hold this many chunks per connection so a single slow
/// chunk at the write head cannot starve the other connections of buffer slots.
const READAHEAD_FACTOR: u32 = 2;

impl Cli {
    /// Effective in-memory chunk budget.
    pub fn max_buffered(&self) -> u32 {
        self.max_buffered.unwrap_or_else(|| {
            self.connections
                .saturating_mul(READAHEAD_FACTOR)
                .clamp(1, 65536)
        })
    }
}

/// Parse a human-friendly byte size such as `4M`, `512K`, `2G`, or a plain integer.
fn parse_size(input: &str) -> Result<u64, String> {
    let s = input.trim();
    if s.is_empty() {
        return Err("empty size".to_string());
    }
    let (num_part, mult) = match s.chars().last().unwrap() {
        'k' | 'K' => (&s[..s.len() - 1], 1024u64),
        'm' | 'M' => (&s[..s.len() - 1], 1024u64 * 1024),
        'g' | 'G' => (&s[..s.len() - 1], 1024u64 * 1024 * 1024),
        'b' | 'B' => (&s[..s.len() - 1], 1u64),
        c if c.is_ascii_digit() => (s, 1u64),
        other => return Err(format!("invalid size suffix '{other}'")),
    };
    let value: u64 = num_part
        .trim()
        .parse()
        .map_err(|_| format!("invalid number in size '{input}'"))?;
    let bytes = value
        .checked_mul(mult)
        .ok_or_else(|| format!("size '{input}' overflows u64"))?;
    if bytes == 0 {
        return Err("size must be greater than zero".to_string());
    }
    Ok(bytes)
}

/// Parse a `--min-speed` value: a byte size like `parse_size`, plus a literal
/// `0` to disable the throughput floor.
fn parse_min_speed(input: &str) -> Result<u64, String> {
    if input.trim() == "0" {
        return Ok(0);
    }
    parse_size(input)
}

#[cfg(test)]
mod tests {
    use super::parse_size;

    #[test]
    fn parses_plain_and_suffixed_sizes() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
        assert_eq!(parse_size("1K").unwrap(), 1024);
        assert_eq!(parse_size("4M").unwrap(), 4 * 1024 * 1024);
        assert_eq!(parse_size("2g").unwrap(), 2 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("512b").unwrap(), 512);
    }

    #[test]
    fn rejects_bad_sizes() {
        assert!(parse_size("").is_err());
        assert!(parse_size("0").is_err());
        assert!(parse_size("abc").is_err());
        assert!(parse_size("12x").is_err());
    }

    #[test]
    fn min_speed_allows_zero_and_suffixes() {
        use super::parse_min_speed;
        assert_eq!(parse_min_speed("0").unwrap(), 0);
        assert_eq!(parse_min_speed("8K").unwrap(), 8 * 1024);
        assert_eq!(parse_min_speed("1M").unwrap(), 1024 * 1024);
        assert!(parse_min_speed("nope").is_err());
    }

    #[test]
    fn max_buffered_defaults_to_twice_connections() {
        use clap::Parser;
        let cli = super::Cli::parse_from(["pcurl", "-c", "8", "http://x"]);
        assert_eq!(cli.max_buffered(), 16);
        // An explicit value overrides the read-ahead default.
        let cli = super::Cli::parse_from(["pcurl", "-c", "8", "--max-buffered", "5", "http://x"]);
        assert_eq!(cli.max_buffered(), 5);
    }
}
