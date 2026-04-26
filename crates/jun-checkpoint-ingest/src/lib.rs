//! Streaming Sui checkpoint ingester for jun.
//!
//! Pulls per-checkpoint `.binpb.zst` files from Mysten's archive, BLS-verifies
//! each against the epoch's committee, and writes a single concatenated
//! `epoch-N.zst` (RFC 8878 multi-frame zstd stream) plus side-car `epoch-N.idx`
//! to an S3-compatible bucket via streaming multipart upload — no disk staging.
//!
//! Public surface:
//!   - `EpochJob`, `EpochResult`, `run_epoch` — the streaming pipeline.
//!   - `make_s3_client` — builds an S3 client from `S3_ENDPOINT` env.
//!   - `archive::fetch_zst` — single-checkpoint fetch with backoff.
//!   - `verify::resolve_epoch_committee` — bootstrap a committee from the
//!     predecessor's end-of-epoch checkpoint.
//!   - `graphql::resolve_epoch_range` — epoch → (first_seq, last_seq).
//!   - `d1::{D1Creds, upsert_epoch}` — Cloudflare D1 routing-row update.
//!
//! All of this is plumbed through a single `ingest_epoch` convenience wrapper
//! used by the `jun ingest` CLI.

pub mod archive;
pub mod d1;
pub mod epoch;
pub mod graphql;
pub mod verify;

pub use d1::{D1Creds, upsert_epoch};
pub use epoch::{run_epoch, EpochJob, EpochResult};

/// R2 connection config, sourced from env vars (`S3_*` / `AWS_*`).
#[derive(Clone, Debug)]
pub struct R2Config {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl R2Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            endpoint: std::env::var("S3_ENDPOINT").context("S3_ENDPOINT required")?,
            region: std::env::var("AWS_REGION").unwrap_or_else(|_| "auto".to_string()),
            bucket: std::env::var("S3_BUCKET").context("S3_BUCKET required")?,
            access_key_id: std::env::var("AWS_ACCESS_KEY_ID")
                .context("AWS_ACCESS_KEY_ID required")?,
            secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")
                .context("AWS_SECRET_ACCESS_KEY required")?,
        })
    }
}

use anyhow::{Context, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// ── Connection pools ─────────────────────────────────────────────────────────
//
// Each client owns its own TCP+TLS context, which means encryption work
// runs on a different tokio worker per client. Single-client uploads to R2
// cap at ~30 MB/s on a busy CDN edge because all Hyper streams multiplex
// over one TCP connection whose TLS encryption is single-core. With N
// clients and round-robin distribution, throughput scales close to N×.
//
// The `pick()` method uses a relaxed atomic — the slight skew from races
// is harmless (any client is correct, we just want roughly even spread).

/// Round-robin pool of `s3::Bucket` instances (rust-s3). Each `Bucket`
/// owns its own internal `reqwest::Client` (= one TCP+TLS connection
/// pool), so a pool of N buckets gives N parallel encryption pipelines —
/// the trick we need to break past the per-connection R2 upload ceiling.
///
/// Note: rust-s3's `ClientOptions` doesn't expose source-IP binding, so
/// each Bucket inherits the kernel's default source IP. That's fine
/// because R2 throttles per-account on the upload side, not per-IP.
pub struct S3Pool {
    buckets: Vec<Box<s3::Bucket>>,
    next: AtomicUsize,
}

impl S3Pool {
    pub fn new(cfg: &R2Config, size: usize) -> Result<Self> {
        let size = size.max(1);
        let creds = s3::creds::Credentials::new(
            Some(&cfg.access_key_id),
            Some(&cfg.secret_access_key),
            None,
            None,
            None,
        )
        .map_err(|e| anyhow::anyhow!("build credentials: {e}"))?;
        let region = s3::Region::Custom {
            region: cfg.region.clone(),
            endpoint: cfg.endpoint.clone(),
        };
        let mut buckets = Vec::with_capacity(size);
        for _ in 0..size {
            let b = s3::Bucket::new(&cfg.bucket, region.clone(), creds.clone())
                .map_err(|e| anyhow::anyhow!("build bucket: {e}"))?
                .with_path_style();
            buckets.push(b);
        }
        Ok(Self {
            buckets,
            next: AtomicUsize::new(0),
        })
    }

    pub fn pick(&self) -> &s3::Bucket {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.buckets.len();
        &self.buckets[i]
    }

    pub fn len(&self) -> usize {
        self.buckets.len()
    }
}

/// Round-robin pool of `reqwest::Client` instances. Each gets its own
/// connection pool keyed to the same upstream host, but distinct TCP
/// connections.
///
/// When `source_ips` is non-empty, clients are round-robined across those
/// local IPs via `local_address`. On a host with a /29 (5 usable v4 IPs),
/// a pool of size 8 gets ~1.6 clients per IP — multiplying past Mysten's
/// per-IP egress cap.
pub struct HttpPool {
    clients: Vec<reqwest::Client>,
    next: AtomicUsize,
}

impl HttpPool {
    /// Build a pool of `size` clients. If `source_ips` is non-empty, bind
    /// each client to one of those addresses round-robin; otherwise let the
    /// kernel pick the source IP per connection.
    pub fn new(size: usize, source_ips: &[std::net::IpAddr]) -> Result<Self> {
        let size = size.max(1);
        let mut clients = Vec::with_capacity(size);
        for i in 0..size {
            let mut b = reqwest::Client::builder()
                .tcp_keepalive(std::time::Duration::from_secs(60))
                .pool_max_idle_per_host(64);
            if !source_ips.is_empty() {
                let ip = source_ips[i % source_ips.len()];
                b = b.local_address(Some(ip));
            }
            clients.push(b.build().context("build reqwest client")?);
        }
        Ok(Self {
            clients,
            next: AtomicUsize::new(0),
        })
    }

    pub fn pick(&self) -> &reqwest::Client {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        &self.clients[i]
    }

    pub fn len(&self) -> usize {
        self.clients.len()
    }
}

/// Auto-detect non-loopback IPv4 addresses on this host. Used as the
/// default for `source_ips` when the CLI flag is `auto`.
pub fn detect_local_ipv4() -> Vec<std::net::IpAddr> {
    if_addrs::get_if_addrs()
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|ifa| match ifa.addr {
            if_addrs::IfAddr::V4(v4) if !ifa.is_loopback() => {
                let ip = v4.ip;
                // Skip RFC1918 + link-local + Docker bridges. Keep public
                // routable space only — those are the IPs Mysten sees.
                if ip.is_private() || ip.is_link_local() || ip.is_unspecified() {
                    None
                } else {
                    Some(std::net::IpAddr::V4(ip))
                }
            }
            _ => None,
        })
        .collect()
}

/// High-level entrypoint used by the `jun ingest` CLI.
///
/// Resolves the epoch range (unless `from`/`to` are explicitly provided),
/// bootstraps the committee from the predecessor's end-of-epoch checkpoint
/// (skipped for epoch 0), runs the streaming multipart pipeline, and — when
/// `D1Creds::from_env()` returns Some — upserts the routing row.
pub struct IngestArgs {
    pub epoch: u64,
    pub from: Option<u64>,
    pub to: Option<u64>,
    pub bucket: String,
    pub archive_url: String,
    pub graphql_url: String,
    pub fetch_workers: usize,
    pub zst_key: Option<String>,
    pub idx_key: Option<String>,
    /// Skip per-checkpoint BLS signature verification. Trades safety for
    /// throughput — the upstream Mysten archive's bytes are trusted. Use
    /// when ingest is time-sensitive and the source is reputable.
    pub no_verify: bool,
    /// Number of independent `aws_sdk_s3::Client` instances used in
    /// round-robin for UploadPart. 1 = original behavior. 8 typically
    /// saturates a 1 Gbit/s pipe to R2 from a 32-core box.
    pub s3_pool_size: usize,
    /// Number of independent `reqwest::Client` instances used in
    /// round-robin for fetching from Mysten's archive.
    pub http_pool_size: usize,
    /// Maximum simultaneous UploadPart RPCs in flight across all S3 clients.
    pub upload_concurrency: usize,
    /// Local IP addresses to round-robin source-bind reqwest clients to.
    /// Empty = let the kernel choose. Use [`detect_local_ipv4`] for auto.
    pub source_ips: Vec<std::net::IpAddr>,
    /// Bench-only mode: fetch + (optionally) verify each checkpoint, drop
    /// the bytes, and report throughput. Skips the S3 multipart upload and
    /// idx PutObject entirely. Useful for isolating Mysten archive fetch
    /// throughput from R2 upload throughput when tuning concurrency.
    pub fetch_only: bool,
}

pub async fn ingest_epoch(args: IngestArgs) -> Result<EpochResult> {
    let http = Arc::new(HttpPool::new(args.http_pool_size, &args.source_ips)?);
    if args.source_ips.is_empty() {
        tracing::info!("http pool: {} client(s), kernel-chosen source IP", http.len());
    } else {
        tracing::info!(
            "http pool: {} client(s) round-robin across {} local IP(s): {:?}",
            http.len(),
            args.source_ips.len(),
            args.source_ips,
        );
    }

    let (from, to) = match (args.from, args.to) {
        (Some(f), Some(t)) => (f, t),
        _ => {
            tracing::info!("resolving range for epoch {}...", args.epoch);
            graphql::resolve_epoch_range(http.pick(), &args.graphql_url, args.epoch).await?
        }
    };
    tracing::info!(
        "epoch {}: {} → {} ({} checkpoints)",
        args.epoch, from, to, to - from + 1
    );

    let zst_key = args.zst_key.unwrap_or_else(|| format!("epoch-{}.zst", args.epoch));
    let idx_key = args.idx_key.unwrap_or_else(|| format!("epoch-{}.idx", args.epoch));

    // Bootstrap + pre-deserialize the committee once. Each Mysten cp's
    // signers are a subset of this fixed committee, so caching the
    // validator pubkeys here saves ~6ms × 374K ≈ 37 minutes of cumulative
    // CPU work per epoch on a verify-on ingest.
    let committee: Option<Arc<jun_checkpoint_verifier::Committee>> = if args.no_verify {
        tracing::info!("--no-verify: skipping BLS verification (trusting upstream)");
        None
    } else if from > 0 {
        tracing::info!(
            "bootstrapping committee from predecessor EoE (seq {})",
            from - 1
        );
        let packed =
            verify::resolve_epoch_committee(&http, &args.archive_url, from - 1).await?;
        let committee = jun_checkpoint_verifier::Committee::from_packed(&packed)
            .context("deserialize committee pubkeys")?;
        tracing::info!(
            "committee cached: {} validators (~{} GB total stake)",
            committee.len(),
            committee.total_stake / 1_000_000_000
        );
        Some(Arc::new(committee))
    } else {
        tracing::info!("epoch 0: no predecessor — ingesting without BLS verification");
        None
    };

    if args.fetch_only {
        return run_fetch_only(
            http,
            args.archive_url,
            args.epoch,
            from,
            to,
            args.fetch_workers,
            committee,
        )
        .await;
    }

    let r2_config = R2Config::from_env()?;
    let s3 = Arc::new(S3Pool::new(&r2_config, args.s3_pool_size)?);
    // rust-s3's `Bucket` doesn't expose source-IP binding (R2 throttles
    // per-account, not per-IP, so it wouldn't help anyway). Each Bucket
    // owns its own reqwest::Client → independent TCP/TLS context, which
    // is what scales upload throughput past the per-connection ceiling.
    tracing::info!("s3 pool: {} client(s) (kernel-chosen source IP)", s3.len());
    let result = epoch::run_epoch(EpochJob {
        epoch: args.epoch,
        from,
        to,
        zst_key,
        idx_key,
        bucket: args.bucket,
        archive_url: args.archive_url,
        fetch_workers: args.fetch_workers,
        committee,
        http,
        s3,
        upload_concurrency: args.upload_concurrency,
    })
    .await?;

    if let Some(creds) = D1Creds::from_env() {
        d1::upsert_epoch(&creds, &result).await?;
        tracing::info!("D1 row upserted for epoch {}", result.epoch);
    }

    Ok(result)
}

/// Bench-mode: fetch + optionally verify (batched) the entire epoch, drop
/// the bytes. No upload, no idx, no D1. Prints throughput on completion.
///
/// Mirrors `run_epoch`'s fetch → verify pipeline (with the same
/// `CheckpointBatch` batching path) but skips the upload side. Used to
/// isolate Mysten fetch + BLS verify cost from R2 upload cost.
async fn run_fetch_only(
    http: Arc<HttpPool>,
    archive_url: String,
    epoch: u64,
    from: u64,
    to: u64,
    fetch_workers: usize,
    committee: Option<Arc<jun_checkpoint_verifier::Committee>>,
) -> Result<EpochResult> {
    use futures::stream::FuturesUnordered;
    use futures::StreamExt;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;
    use tokio::sync::{mpsc, Semaphore};

    let count = to - from + 1;
    let started = Instant::now();
    let downloaded = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    let fetch_sem = Arc::new(Semaphore::new(fetch_workers));
    let verify_sem = Arc::new(Semaphore::new(num_cpus::get().max(1)));

    // raw_tx → verifier_handle → (just discard, no writer needed)
    let (raw_tx, mut raw_rx) = mpsc::channel::<(u64, Vec<u8>)>(2048);

    // Fetch task — same shape as run_epoch's, just sends to raw_tx.
    let fetch_handle = {
        let downloaded = downloaded.clone();
        let bytes_c = bytes.clone();
        let http = http.clone();
        let archive_url = archive_url.clone();
        tokio::spawn(async move {
            let mut tasks = FuturesUnordered::new();
            for seq in from..=to {
                let permit = fetch_sem.clone().acquire_owned().await.unwrap();
                let http = http.clone();
                let archive_url = archive_url.clone();
                let downloaded = downloaded.clone();
                let bytes_c = bytes_c.clone();
                let tx = raw_tx.clone();
                tasks.push(tokio::spawn(async move {
                    let _permit = permit;
                    let zst = archive::fetch_zst(&http, &archive_url, seq, 6).await?;
                    downloaded.fetch_add(1, Ordering::Relaxed);
                    bytes_c.fetch_add(zst.len() as u64, Ordering::Relaxed);
                    tx.send((seq, zst)).await.ok();
                    Result::<()>::Ok(())
                }));
                while tasks.len() > fetch_workers * 2 {
                    if let Some(res) = tasks.next().await {
                        res??;
                    }
                }
            }
            while let Some(res) = tasks.next().await {
                res??;
            }
            drop(raw_tx);
            Result::<()>::Ok(())
        })
    };

    // Verifier task — batched BLS via CheckpointBatch, or pass-through if
    // no committee. Drops verified bytes (this is bench mode).
    let verify_handle = tokio::spawn(async move {
        const BATCH: usize = 64;
        match committee {
            None => {
                while let Some(_item) = raw_rx.recv().await {
                    // drop
                }
            }
            Some(committee) => {
                let mut buf: Vec<(u64, Vec<u8>)> = Vec::with_capacity(BATCH);
                let mut pending: FuturesUnordered<
                    tokio::task::JoinHandle<Result<()>>,
                > = FuturesUnordered::new();
                let dispatch_one = |chunk: Vec<(u64, Vec<u8>)>,
                                    sem: Arc<Semaphore>,
                                    committee: Arc<jun_checkpoint_verifier::Committee>|
                 -> tokio::task::JoinHandle<Result<()>> {
                    tokio::spawn(async move {
                        let permit = sem.acquire_owned().await.unwrap();
                        tokio::task::spawn_blocking(move || {
                            let _p = permit;
                            let mut batch =
                                jun_checkpoint_verifier::CheckpointBatch::new();
                            for (_, zst) in &chunk {
                                batch.add_zst(zst, &*committee, epoch)?;
                            }
                            batch.finalize()?;
                            Result::<()>::Ok(())
                        })
                        .await
                        .map_err(|e| anyhow::anyhow!("verify join: {e}"))?
                    })
                };
                while let Some(item) = raw_rx.recv().await {
                    buf.push(item);
                    if buf.len() >= BATCH {
                        let chunk = std::mem::take(&mut buf);
                        pending.push(dispatch_one(
                            chunk,
                            verify_sem.clone(),
                            committee.clone(),
                        ));
                    }
                    // Reap completed verifies eagerly (non-blocking).
                    use futures::future::FutureExt;
                    while let Some(done) = pending.next().now_or_never().flatten() {
                        done.map_err(|e| anyhow::anyhow!("verify outer join: {e}"))??;
                    }
                }
                if !buf.is_empty() {
                    let chunk = std::mem::take(&mut buf);
                    pending.push(dispatch_one(
                        chunk,
                        verify_sem.clone(),
                        committee.clone(),
                    ));
                }
                while let Some(done) = pending.next().await {
                    done.map_err(|e| anyhow::anyhow!("verify outer join: {e}"))??;
                }
            }
        }
        Result::<()>::Ok(())
    });

    fetch_handle.await.map_err(|e| anyhow::anyhow!("fetch panic: {e}"))??;
    verify_handle.await.map_err(|e| anyhow::anyhow!("verify panic: {e}"))??;

    let elapsed = started.elapsed();
    let total_bytes = bytes.load(Ordering::Relaxed);
    let cps = downloaded.load(Ordering::Relaxed);
    tracing::info!(
        "fetch-only done: {} cps, {} bytes ({:.1} MB/s) in {:.1}s ({} cps/s)",
        cps,
        total_bytes,
        (total_bytes as f64) / elapsed.as_secs_f64() / 1_048_576.0,
        elapsed.as_secs_f64(),
        cps as f64 / elapsed.as_secs_f64(),
    );

    Ok(EpochResult {
        epoch,
        first_seq: from,
        last_seq: to,
        count,
        zst_key: String::new(),
        idx_key: String::new(),
        zst_parts: 0,
        zst_bytes: total_bytes,
        idx_bytes: 0,
        zst_sha256: String::new(),
        idx_sha256: String::new(),
        elapsed_secs: elapsed.as_secs(),
    })
}
