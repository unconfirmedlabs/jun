//! jun — Sui checkpoint indexer. ClickHouse backend (SQLite next).

use anyhow::Result;
use clap::{Parser, Subcommand};
use jun_archive::{SuiArchiveClient, DEFAULT_ARCHIVE, DEFAULT_PROXY};
use jun_clickhouse::ClickHouseClient;
use jun_pipeline::{run_replay, ReplayConfig, RunMode};
use jun_types::ExtractMask;

// Default ClickHouse HTTP URL.
const DEFAULT_CH_URL: &str = "http://localhost:8123";

#[derive(Parser)]
#[command(name = "jun", version, about = "Sui checkpoint indexer")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create target database + all tables.
    Init(InitArgs),
    /// Replay a checkpoint range into ClickHouse.
    Replay(ReplayArgs),
}

#[derive(Parser)]
struct InitArgs {
    /// ClickHouse HTTP URL. Default: http://localhost:8123.
    #[arg(long, env = "JUN_CLICKHOUSE_URL", default_value = DEFAULT_CH_URL)]
    clickhouse: String,
    #[arg(long, env = "JUN_CLICKHOUSE_DATABASE", default_value = "jun")]
    database: String,
    #[arg(long, env = "JUN_CLICKHOUSE_USERNAME")]
    username: Option<String>,
    #[arg(long, env = "JUN_CLICKHOUSE_PASSWORD")]
    password: Option<String>,
}

#[derive(Parser)]
struct ReplayArgs {
    #[arg(long)]
    from: u64,
    #[arg(long)]
    to: u64,

    #[arg(long, env = "JUN_ARCHIVE_PROXY", default_value = DEFAULT_PROXY)]
    proxy_url: String,
    #[arg(long, env = "JUN_ARCHIVE_URL", default_value = DEFAULT_ARCHIVE)]
    archive_url: String,

    /// ClickHouse HTTP URL. Default: http://localhost:8123.
    #[arg(long, env = "JUN_CLICKHOUSE_URL", default_value = DEFAULT_CH_URL)]
    clickhouse: String,
    #[arg(long, env = "JUN_CLICKHOUSE_DATABASE", default_value = "jun")]
    database: String,
    #[arg(long, env = "JUN_CLICKHOUSE_USERNAME")]
    username: Option<String>,
    #[arg(long, env = "JUN_CLICKHOUSE_PASSWORD")]
    password: Option<String>,

    #[arg(long, default_value_t = 8)]
    chunk_concurrency: usize,
    #[arg(long, default_value_t = 4)]
    decode_concurrency: usize,
    #[arg(long, default_value_t = 500)]
    batch_size: usize,
    #[arg(long, default_value_t = 4)]
    write_concurrency: usize,

    /// Call init before replaying (idempotent — useful for one-shot runs).
    #[arg(long)]
    init: bool,

    /// Run mode: full | no-write (fetch+decode, drop) | fetch-only (count frames).
    #[arg(long, default_value = "full")]
    mode: String,
}

async fn build_clickhouse_client(
    url: &str,
    database: &str,
    username: Option<&String>,
    password: Option<&String>,
) -> Result<ClickHouseClient> {
    ClickHouseClient::connect(
        url,
        database,
        username.cloned(),
        password.cloned(),
    ).await
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Init(args) => {
            let client = build_clickhouse_client(&args.clickhouse, &args.database, args.username.as_ref(), args.password.as_ref()).await?;
            client.init_schema().await?;
            println!("initialized database `{}` with {} tables", args.database, jun_clickhouse::ALL_DDL.len());
        }
        Cmd::Replay(args) => {
            let ch = build_clickhouse_client(&args.clickhouse, &args.database, args.username.as_ref(), args.password.as_ref()).await?;
            if args.init {
                ch.init_schema().await?;
            }
            let archive = SuiArchiveClient::new(&args.proxy_url, &args.archive_url);
            let mode = match args.mode.as_str() {
                "full" => RunMode::Full,
                "no-write" => RunMode::NoWrite,
                "fetch-only" => RunMode::FetchOnly,
                other => anyhow::bail!("unknown --mode {other} (expected full|no-write|fetch-only)"),
            };
            let cfg = ReplayConfig {
                from_seq: args.from,
                to_seq: args.to,
                chunk_concurrency: args.chunk_concurrency,
                decode_concurrency: args.decode_concurrency,
                batch_size: args.batch_size,
                write_concurrency: args.write_concurrency,
                mask: ExtractMask::ALL,
                mode,
            };
            let range = args.to - args.from + 1;
            eprintln!(
                "replay [{}, {}] ({} checkpoints) mode={:?} chunk_conc={} decode_conc={} batch={} write_conc={}",
                args.from, args.to, range, cfg.mode,
                cfg.chunk_concurrency, cfg.decode_concurrency, cfg.batch_size, cfg.write_concurrency,
            );
            let stats = run_replay(archive, ch, cfg).await?;
            if stats.bytes > 0 {
                println!(
                    "done: {} cps in {:.2}s = {:.1} cps/s | {:.1} MB/s bytes",
                    stats.checkpoints, stats.elapsed_sec, stats.cps_per_sec,
                    stats.bytes as f64 / 1e6 / stats.elapsed_sec,
                );
            } else {
                println!("done: {} cps in {:.2}s = {:.1} cps/s",
                    stats.checkpoints, stats.elapsed_sec, stats.cps_per_sec);
            }
        }
    }
    Ok(())
}
