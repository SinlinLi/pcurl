//! Structured logging setup.
//!
//! Logs always go to stderr (never stdout, which carries the downloaded
//! bytes). When a log directory is configured, logs are additionally written
//! to a daily-rotating file keeping the most recent `keep` files.
//!
//! Levels: TRACE / DEBUG / INFO / WARN / ERROR, filterable by module via the
//! standard `RUST_LOG` syntax, which always overrides the `-v` flags.

use std::path::Path;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

/// Guard that must be kept alive for the lifetime of the program so the
/// non-blocking file writer flushes pending log lines on shutdown. The inner
/// `WorkerGuard` is held purely for its `Drop` side effect.
#[must_use = "dropping the guard stops file logging and may lose buffered lines"]
pub struct LogGuard(#[allow(dead_code)] Option<WorkerGuard>);

/// Initialise the global tracing subscriber.
///
/// * `verbose` raises the default stderr level (0 = warn, 1 = debug, 2+ = trace).
/// * `log_dir` enables rotating file logs when `Some`.
/// * `keep` bounds the number of retained rotated files.
pub fn init(verbose: u8, log_dir: Option<&Path>, keep: usize) -> LogGuard {
    let default_level = match verbose {
        0 => "warn",
        1 => "info,pcurl=debug",
        2 => "debug,pcurl=trace",
        _ => "trace",
    };
    let env_filter =
        || EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level));

    let stderr_layer = fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(false)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .with_filter(env_filter());

    let registry = tracing_subscriber::registry().with(stderr_layer);

    if let Some(dir) = log_dir {
        if let Err(e) = std::fs::create_dir_all(dir) {
            eprintln!("pcurl: cannot create log dir {}: {e}", dir.display());
            registry.init();
            return LogGuard(None);
        }
        let appender = tracing_appender::rolling::Builder::new()
            .rotation(tracing_appender::rolling::Rotation::DAILY)
            .filename_prefix("pcurl")
            .filename_suffix("log")
            .max_log_files(keep.max(1))
            .build(dir)
            .expect("failed to build rolling log appender");
        let (non_blocking, guard) = tracing_appender::non_blocking(appender);
        let file_layer = fmt::layer()
            .with_writer(non_blocking)
            .with_ansi(false)
            .with_target(true)
            .with_filter(env_filter());
        registry.with(file_layer).init();
        LogGuard(Some(guard))
    } else {
        registry.init();
        LogGuard(None)
    }
}
