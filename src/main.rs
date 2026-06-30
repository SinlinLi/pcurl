//! pcurl: parallel HTTP download, strictly ordered stream to stdout.

mod cli;
mod download;
mod logging;
mod plan;
mod probe;
mod progress;
mod writer;

use std::future::Future;
use std::io::{IsTerminal, Write};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::cli::Cli;
use crate::download::RetryCfg;
use crate::plan::ChunkPlan;
use crate::probe::Probe;
use crate::progress::Progress;
use crate::writer::{ChunkMsg, WriterOutcome};

fn main() {
    let cli = Cli::parse();
    let guard = logging::init(cli.verbose, cli.log_dir.as_deref(), cli.log_keep);

    // The work is network I/O bound, not CPU bound, so a large worker pool
    // only adds context-switching overhead. Size it to the connection count,
    // capped, rather than defaulting to one thread per core.
    let worker_threads = (cli.connections as usize).clamp(2, 8);
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("pcurl: failed to start runtime: {e}");
            std::process::exit(1);
        }
    };

    let result = runtime.block_on(run(cli));
    // Shut the runtime down before exiting so background tasks stop cleanly.
    drop(runtime);

    let code = match result {
        Ok(()) => 0,
        Err(e) => {
            tracing::error!(error = format!("{e:#}"), "download failed");
            eprintln!("pcurl: error: {e:#}");
            1
        }
    };

    drop(guard); // flush rotating-file logs
    std::process::exit(code);
}

async fn run(cli: Cli) -> Result<()> {
    let client = probe::build_client(&cli.user_agent, cli.timeout, &cli.headers)?;

    let out: Box<dyn Write + Send> = match &cli.output {
        Some(path) => Box::new(
            std::fs::File::create(path)
                .with_context(|| format!("creating output file {}", path.display()))?,
        ),
        None => Box::new(std::io::stdout()),
    };

    let show_progress = !cli.quiet && (cli.progress || std::io::stderr().is_terminal());

    let probed = if cli.single {
        Probe::Single { len: None }
    } else {
        probe::probe(&client, &cli.url).await?
    };

    let sem = Arc::new(Semaphore::new(cli.max_buffered() as usize));
    let retry = RetryCfg {
        retries: cli.retries,
        backoff_ms: cli.backoff_ms,
        backoff_max_ms: cli.backoff_max_ms,
    };

    match probed {
        Probe::Ranged { total } => {
            if total == 0 {
                tracing::info!("empty resource; nothing to download");
                return Ok(());
            }
            let plan = ChunkPlan::new(total, cli.chunk_size);
            tracing::info!(
                total,
                num_chunks = plan.num_chunks,
                chunk_size = cli.chunk_size,
                connections = cli.connections,
                max_buffered = cli.max_buffered(),
                "ranged parallel download"
            );
            let url = cli.url.clone();
            let connections = cli.connections;
            pipeline(
                out,
                Some(plan.num_chunks),
                Some(total),
                show_progress,
                sem,
                move |tx, token, sem| {
                    download::run_ranged_workers(
                        client,
                        url,
                        plan,
                        connections,
                        sem,
                        tx,
                        retry,
                        token,
                    )
                },
            )
            .await
        }
        Probe::Single { len } => {
            tracing::info!(
                ?len,
                "single-stream download (ranges unsupported or forced)"
            );
            let url = cli.url.clone();
            pipeline(out, None, len, show_progress, sem, move |tx, token, sem| {
                download::run_single_stream(client, url, sem, tx, token)
            })
            .await
        }
    }
}

/// Wire up the channel, ordered writer, progress reporter, and a producer
/// (the worker pool or the single-stream reader), then reconcile their results.
async fn pipeline<F, Fut>(
    out: Box<dyn Write + Send>,
    expected: Option<u64>,
    progress_total: Option<u64>,
    show_progress: bool,
    sem: Arc<Semaphore>,
    make_producer: F,
) -> Result<()>
where
    F: FnOnce(UnboundedSender<ChunkMsg>, CancellationToken, Arc<Semaphore>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let token = CancellationToken::new();
    let (tx, rx) = mpsc::unbounded_channel::<ChunkMsg>();
    let progress = Progress::new(progress_total, show_progress);
    let reporter = progress.spawn_reporter(token.clone());

    // The ordered writer runs on a blocking thread (synchronous stdout writes).
    let blocking = {
        let progress = Arc::clone(&progress);
        let token = token.clone();
        tokio::task::spawn_blocking(move || writer::run(rx, out, expected, progress, token))
    };

    // Supervisor: the instant the writer ends (success, broken pipe, panic, or
    // I/O error) release any worker blocked on the semaphore so the producer
    // can wind down instead of deadlocking.
    let writer_task = {
        let token = token.clone();
        let sem = Arc::clone(&sem);
        tokio::spawn(async move {
            let res = blocking.await;
            token.cancel();
            sem.close();
            res
        })
    };

    let producer_res = make_producer(tx, token.clone(), Arc::clone(&sem)).await;

    let writer_res = writer_task
        .await
        .map_err(|e| anyhow!("writer supervisor task failed: {e}"))?
        .map_err(|e| anyhow!("writer task panicked: {e}"))?;

    token.cancel();
    let _ = reporter.await;
    progress.finish();

    finalize(producer_res, writer_res)
}

/// Combine the producer and writer outcomes into a single program result.
fn finalize(producer_res: Result<()>, writer_res: std::io::Result<WriterOutcome>) -> Result<()> {
    match writer_res {
        Ok(WriterOutcome::BrokenPipe) => {
            // Consumer closed the pipe (e.g. `| head`). Expected, not an error.
            tracing::info!("output closed by consumer (broken pipe); stopped early");
            Ok(())
        }
        Ok(WriterOutcome::Complete) => producer_res,
        Ok(WriterOutcome::Incomplete) => {
            producer_res.and(Err(anyhow!("download ended before all bytes were written")))
        }
        Err(io_err) => Err(anyhow::Error::new(io_err).context("failed writing output")),
    }
}
