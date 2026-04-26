//! jun — Sui checkpoint indexer. Three subcommands:
//!
//!   jun ingest    — pull per-checkpoint files from Mysten's archive,
//!                   package per-epoch zstd + idx, upload to R2.
//!   jun replay    — read epoch zstd from your R2 archive, decode,
//!                   broadcast on NATS or stdout pipe.
//!   jun stream    — subscribe to live gRPC, decode, broadcast on
//!                   NATS or stdout pipe.

use std::sync::Arc;

// mimalloc replaces the system allocator. The decode hot path allocates
// hundreds of KB per busy mid-mainnet checkpoint (prost full-tree decode +
// BCS Object decoding); mimalloc's segment-and-page model is meaningfully
// faster than glibc malloc for that pattern.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use anyhow::Result;
use clap::{Parser, Subcommand};
use jun_archive_reader::{SuiArchiveClient, DEFAULT_ARCHIVE, DEFAULT_PROXY};
use jun_checkpoint_stream::sink::NatsSink as GrpcNatsSink;
use jun_checkpoint_stream::{FileSink, NullSink, RecordSink, StdoutSink, StreamArgs as GrpcStreamArgs, TableFilter};
use jun_checkpoint_ingest::{ingest_epoch, IngestArgs as JunIngestArgs};
use jun_nats::{NatsSink, PipeSink};
use jun_pipeline::{run_replay, ReplayConfig, RunMode, Sink};
use jun_types::ExtractMask;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "jun", version, about = "Sui checkpoint indexer (ingest + replay + stream)")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Build per-epoch archive files (zstd + idx) from Mysten's per-checkpoint
    /// archive and upload to R2. One epoch per invocation; ephemeral fly
    /// machines spawn one of these per epoch boundary.
    Ingest(IngestArgs),
    /// Replay an epoch range from the R2 archive, decode, and broadcast on
    /// NATS or stdout pipe. Used for backfill, gap recovery, and ad-hoc
    /// historical queries.
    Replay(ReplayArgs),
    /// Subscribe to live gRPC from a Sui fullnode, decode, broadcast on NATS
    /// or stdout pipe. Always-on process, single instance per deployment.
    Stream(StreamArgs),
}

// ---------------------------------------------------------------------------
// jun ingest
// ---------------------------------------------------------------------------

#[derive(Parser)]
struct IngestArgs {
    /// Epoch number to package and upload.
    #[arg(long, env = "JUN_INGEST_EPOCH")]
    epoch: u64,

    /// Override start seq (smoke-test helper).
    #[arg(long)]
    from: Option<u64>,

    /// Override end seq, inclusive (smoke-test helper).
    #[arg(long)]
    to: Option<u64>,

    /// Mysten's per-checkpoint archive base URL.
    #[arg(long, env = "JUN_INGEST_SOURCE", default_value = "https://checkpoints.mainnet.sui.io")]
    source: String,

    /// Sui GraphQL endpoint used to resolve epoch → checkpoint range.
    #[arg(long, env = "JUN_GRAPHQL_URL", default_value = "https://graphql.mainnet.sui.io/graphql")]
    graphql_url: String,

    /// R2 bucket name. SigV4 creds + endpoint are read from
    /// `S3_ENDPOINT` / `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` /
    /// `AWS_REGION` (default `auto` for R2).
    #[arg(long, env = "S3_BUCKET")]
    bucket: String,

    /// Parallel per-checkpoint fetches against the Mysten source.
    #[arg(long, env = "JUN_INGEST_FETCH_WORKERS", default_value_t = 500)]
    fetch_workers: usize,

    /// Override the R2 object key for the .zst (default: `epoch-{N}.zst`).
    #[arg(long)]
    zst_key: Option<String>,

    /// Override the R2 object key for the .idx (default: `epoch-{N}.idx`).
    #[arg(long)]
    idx_key: Option<String>,

    /// Skip per-checkpoint BLS verification. ~5-10× faster on small machines;
    /// only safe when you trust the upstream archive bytes (Mysten's, in our
    /// case — already validator-attested). Default: verify.
    #[arg(long, env = "JUN_INGEST_NO_VERIFY")]
    no_verify: bool,

    /// Number of independent S3 clients used round-robin for UploadPart.
    /// Each client = own TCP+TLS connection, breaking past the
    /// ~30 MB/s single-connection ceiling against R2.
    #[arg(long, env = "JUN_INGEST_S3_POOL", default_value_t = 8)]
    s3_pool: usize,

    /// Number of independent reqwest clients used round-robin for fetching
    /// from Mysten's archive.
    #[arg(long, env = "JUN_INGEST_HTTP_POOL", default_value_t = 8)]
    http_pool: usize,

    /// Max simultaneous UploadPart RPCs in flight across the S3 pool.
    /// RAM upper bound = `upload_concurrency * 32 MiB`.
    #[arg(long, env = "JUN_INGEST_UPLOAD_CONCURRENCY", default_value_t = 64)]
    upload_concurrency: usize,

    /// Local source IPs for outbound Mysten fetches. `auto` discovers all
    /// non-loopback public IPv4 on the host (good for /29-assigned servers
    /// where Mysten's per-IP egress cap is the bottleneck). `off` lets the
    /// kernel pick. Comma-separated list pins exact addresses.
    #[arg(long, env = "JUN_INGEST_SOURCE_IPS", default_value = "auto")]
    source_ips: String,

    /// Bench-only: fetch + verify the full epoch from Mysten, drop the
    /// bytes, no R2 upload. Prints throughput. Useful for isolating
    /// fetch-side bottlenecks from upload-side ones.
    #[arg(long, env = "JUN_INGEST_FETCH_ONLY")]
    fetch_only: bool,
}

// ---------------------------------------------------------------------------
// jun replay
// ---------------------------------------------------------------------------

#[derive(Parser)]
struct ReplayArgs {
    /// Explicit checkpoint sequence range. Mutually exclusive with --epoch.
    #[arg(long, env = "JUN_FROM", conflicts_with = "epoch", requires = "to")]
    from: Option<u64>,
    #[arg(long, env = "JUN_TO", conflicts_with = "epoch", requires = "from")]
    to: Option<u64>,

    /// Process the entire range of one epoch (resolves first_seq/last_seq via
    /// the archive proxy). Convenient for layer-1 fan-out: spawn N workers,
    /// each `--epoch N` covers exactly one epoch's checkpoints. Mutually
    /// exclusive with --from/--to.
    #[arg(long, env = "JUN_EPOCH", conflicts_with_all = &["from", "to"])]
    epoch: Option<u64>,

    #[arg(long, env = "JUN_ARCHIVE_PROXY", default_value = DEFAULT_PROXY)]
    proxy_url: String,
    /// Archive hostname(s). Comma-separated for multi-host rotation —
    /// distinct CF custom domains pointing at the same R2 bucket pick up
    /// independent egress budgets. The first entry is used for one-off
    /// metadata calls.
    #[arg(long, env = "JUN_ARCHIVE_URL", default_value = DEFAULT_ARCHIVE, value_delimiter = ',')]
    archive_url: Vec<String>,

    #[arg(long, env = "JUN_CHUNK_CONCURRENCY", default_value_t = 8)]
    chunk_concurrency: usize,
    /// Size in MiB of each ranged GET against the .zst file. Default 16 MiB.
    /// Bigger = fewer requests + better TCP utilization, smaller = lower TTFB
    /// and tighter memory bound.
    #[arg(long, env = "JUN_CHUNK_MB", default_value_t = 16)]
    chunk_mb: u64,
    /// Number of HTTP clients (each = its own TCP connection) used in parallel
    /// for archive fetches. Bump on fat pipes — single-connection HTTP/2
    /// ceilings near 5 Gbit on this CDN edge.
    #[arg(long, env = "JUN_CLIENT_POOL", default_value_t = 4)]
    client_pool: usize,
    #[arg(long, env = "JUN_DECODE_CONCURRENCY", default_value_t = 4)]
    decode_concurrency: usize,
    #[arg(long, env = "JUN_BATCH_SIZE", default_value_t = 500)]
    batch_size: usize,
    #[arg(long, env = "JUN_WRITE_CONCURRENCY", default_value_t = 4)]
    write_concurrency: usize,

    /// Run mode: full | no-write (fetch+decode, drop) | fetch-only (count frames).
    /// `full` requires a sink — pair with `--nats <url>` (or future pipe modes).
    #[arg(long, env = "JUN_MODE", default_value = "no-write")]
    mode: String,

    /// NATS server URL to broadcast records to. Implies `--mode full`.
    /// Mutually exclusive with `--pipe`.
    #[arg(long, env = "JUN_NATS_URL", conflicts_with = "pipe")]
    nats: Option<String>,

    /// Subject prefix on NATS (default: "jun"). Subjects look like
    /// `<prefix>.transactions`, `<prefix>.balance_changes`, etc.
    #[arg(long, env = "JUN_NATS_PREFIX", default_value = "jun")]
    nats_prefix: String,

    /// Stream JSONL records to stdout. Implies `--mode full`. Each line is one
    /// record carrying a `_table` discriminator unless `--table` selects a
    /// single table (then untagged rows are emitted, ready for
    /// `clickhouse-client INSERT FORMAT JSONEachRow`).
    #[arg(long, env = "JUN_PIPE")]
    pipe: bool,

    /// Restrict pipe output to specific tables. Comma-separated. Allowed:
    /// checkpoints, transactions, move_calls, balance_changes,
    /// object_changes, dependencies.
    #[arg(long, env = "JUN_TABLE", value_delimiter = ',')]
    table: Vec<String>,
}

// ---------------------------------------------------------------------------
// jun stream
// ---------------------------------------------------------------------------

#[derive(Parser)]
struct StreamArgs {
    /// Sui fullnode gRPC endpoint (e.g. grpc://your-validator-fullnode:9000).
    #[arg(long, env = "JUN_GRPC_URL")]
    grpc: String,

    /// Optional starting checkpoint seq for resume after restart. Default:
    /// start from the next live checkpoint.
    #[arg(long, env = "JUN_STREAM_FROM_SEQ")]
    from_seq: Option<u64>,

    /// NATS server URL. Mutually exclusive with --pipe.
    #[arg(long, env = "JUN_NATS_URL", conflicts_with = "pipe")]
    nats: Option<String>,

    /// Subject prefix on NATS.
    #[arg(long, env = "JUN_NATS_PREFIX", default_value = "jun")]
    nats_prefix: String,

    /// Stream JSONL records to stdout.
    #[arg(long, env = "JUN_PIPE")]
    pipe: bool,

    /// Restrict pipe output to specific tables.
    #[arg(long, env = "JUN_TABLE", value_delimiter = ',')]
    table: Vec<String>,
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // Logs go to stderr so `--pipe` keeps stdout clean for JSONL.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Ingest(args) => run_ingest(args).await,
        Cmd::Replay(args) => do_replay(args).await,
        Cmd::Stream(args) => run_stream(args).await,
    }
}

// ---------------------------------------------------------------------------
// jun ingest impl
// ---------------------------------------------------------------------------

async fn run_ingest(args: IngestArgs) -> Result<()> {
    eprintln!(
        "ingest epoch={} source={} bucket={} fetch_workers={}",
        args.epoch, args.source, args.bucket, args.fetch_workers,
    );
    let source_ips: Vec<std::net::IpAddr> = match args.source_ips.as_str() {
        "off" | "" => Vec::new(),
        "auto" => jun_checkpoint_ingest::detect_local_ipv4(),
        csv => csv
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.parse::<std::net::IpAddr>())
            .collect::<std::result::Result<Vec<_>, _>>()?,
    };
    let result = ingest_epoch(JunIngestArgs {
        epoch: args.epoch,
        from: args.from,
        to: args.to,
        bucket: args.bucket,
        archive_url: args.source,
        graphql_url: args.graphql_url,
        fetch_workers: args.fetch_workers,
        zst_key: args.zst_key,
        idx_key: args.idx_key,
        no_verify: args.no_verify,
        s3_pool_size: args.s3_pool,
        http_pool_size: args.http_pool,
        upload_concurrency: args.upload_concurrency,
        source_ips,
        fetch_only: args.fetch_only,
    })
    .await?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

// ---------------------------------------------------------------------------
// jun stream impl
// ---------------------------------------------------------------------------

async fn run_stream(args: StreamArgs) -> Result<()> {
    let filter = TableFilter { tables: args.table.clone() };

    let sink: Arc<dyn RecordSink> = if let Some(url) = args.nats.as_deref() {
        Arc::new(GrpcNatsSink::connect(url, &args.nats_prefix, filter.clone()).await?)
    } else if args.pipe {
        // `--pipe` without a path means stdout JSONL. File-pipe mode is
        // available via `JUN_PIPE_PATH` env if a caller needs it; not wired
        // to a CLI flag here to keep the surface flat.
        if let Ok(path) = std::env::var("JUN_PIPE_PATH") {
            Arc::new(FileSink::open(&PathBuf::from(path), filter.clone()).await?)
        } else {
            Arc::new(StdoutSink::new(filter.clone()))
        }
    } else {
        Arc::new(NullSink)
    };

    let sink_desc = if let Some(u) = args.nats.as_deref() {
        format!("nats={u} prefix={}", args.nats_prefix)
    } else if args.pipe {
        format!("pipe=jsonl tables={}",
            if args.table.is_empty() { "all".into() } else { args.table.join(",") })
    } else {
        "null (--no-write)".to_string()
    };
    eprintln!(
        "stream grpc={} from_seq={:?} sink={}",
        args.grpc, args.from_seq, sink_desc
    );

    let grpc_args = GrpcStreamArgs {
        url: args.grpc,
        start_seq: args.from_seq,
        mask: ExtractMask::ALL,
        tls: None,
    };

    tokio::select! {
        res = jun_checkpoint_stream::stream_records(grpc_args, sink.clone()) => res,
        _ = tokio::signal::ctrl_c() => {
            eprintln!("interrupted; flushing sink");
            sink.flush().await?;
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// jun replay impl  (existing logic, unchanged)
// ---------------------------------------------------------------------------

async fn do_replay(args: ReplayArgs) -> Result<()> {
    // Specifying any sink (--nats / --pipe) implies Full mode; otherwise honor --mode.
    let mode = if args.nats.is_some() || args.pipe {
        RunMode::Full
    } else {
        match args.mode.as_str() {
            "full" => RunMode::Full,
            "no-write" => RunMode::NoWrite,
            "fetch-only" => RunMode::FetchOnly,
            other => anyhow::bail!(
                "unknown --mode {other} (expected full|no-write|fetch-only)"
            ),
        }
    };
    let sink: Option<Arc<dyn Sink>> = if let Some(url) = args.nats.as_deref() {
        let s = NatsSink::connect(url, args.nats_prefix.clone(), ExtractMask::ALL).await?;
        Some(Arc::new(s))
    } else if args.pipe {
        let tables = if args.table.is_empty() { None } else { Some(args.table.clone()) };
        Some(Arc::new(PipeSink::new(tables, ExtractMask::ALL)))
    } else {
        None
    };
    if mode == RunMode::Full && sink.is_none() {
        anyhow::bail!("--mode full requires a sink (e.g. --nats <url> or --pipe)");
    }
    let archive = SuiArchiveClient::with_hosts(
        &args.proxy_url,
        args.archive_url.clone(),
        args.client_pool,
    );
    // Resolve range: either explicit --from/--to or --epoch.
    let (from_seq, to_seq) = if let Some(ep) = args.epoch {
        let meta = archive.find_epoch_by_number(ep).await?;
        eprintln!("epoch {ep}: seq [{}, {}] ({} checkpoints, {:.2} GB compressed)",
            meta.first_seq, meta.last_seq, meta.count, meta.zst_bytes as f64 / 1e9);
        (meta.first_seq, meta.last_seq)
    } else {
        match (args.from, args.to) {
            (Some(f), Some(t)) => (f, t),
            _ => anyhow::bail!("specify --epoch <N> or both --from <seq> --to <seq>"),
        }
    };
    let cfg = ReplayConfig {
        from_seq,
        to_seq,
        chunk_concurrency: args.chunk_concurrency,
        chunk_bytes: args.chunk_mb * 1024 * 1024,
        decode_concurrency: args.decode_concurrency,
        batch_size: args.batch_size,
        write_concurrency: args.write_concurrency,
        mask: ExtractMask::ALL,
        mode,
        decoder: jun_checkpoint_decoder::Decoder::default(),
    };
    let range = to_seq - from_seq + 1;
    let sink_desc = if let Some(u) = args.nats.as_deref() {
        format!(" nats={u} prefix={}", args.nats_prefix)
    } else if args.pipe {
        if args.table.is_empty() {
            " pipe=jsonl tables=all".to_string()
        } else {
            format!(" pipe=jsonl tables={}", args.table.join(","))
        }
    } else {
        String::new()
    };
    eprintln!(
        "replay [{}, {}] ({} checkpoints) mode={:?} chunk_conc={} decode_conc={} batch={} write_conc={}{}",
        from_seq, to_seq, range, cfg.mode,
        cfg.chunk_concurrency, cfg.decode_concurrency, cfg.batch_size, cfg.write_concurrency,
        sink_desc,
    );
    let stats = run_replay(archive, sink, cfg).await?;
    if stats.bytes > 0 {
        eprintln!(
            "done: {} cps in {:.2}s = {:.1} cps/s | {:.1} MB/s bytes",
            stats.checkpoints, stats.elapsed_sec, stats.cps_per_sec,
            stats.bytes as f64 / 1e6 / stats.elapsed_sec,
        );
    } else {
        eprintln!("done: {} cps in {:.2}s = {:.1} cps/s",
            stats.checkpoints, stats.elapsed_sec, stats.cps_per_sec);
    }
    Ok(())
}
