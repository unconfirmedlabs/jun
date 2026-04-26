//! jun-archive-reader — client for the unconfirmedlabs Sui checkpoint archive.
//!
//! Port of `@unconfirmedlabs/sui-checkpoint-archive` (TS SDK). Provides:
//!   - Epoch routing via the proxy's `/epochs/:N` endpoint
//!   - `.idx` parsing (20-byte entries: seq/offset/length)
//!   - `iterate_epoch` — ranged GETs against `epoch-N.zst` in R2, yields
//!     per-checkpoint zstd-decompressed proto bytes
//!
//! Single-epoch streaming only. Callers needing a cross-epoch range compose
//! this in a loop (e.g. in jun-pipeline).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use futures::stream::Stream;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use tracing::{debug, info, warn};

/// Logged exactly once per process so we can verify whether reqwest negotiated
/// HTTP/2 with the upstream proxy — answers the "are we actually multiplexing?"
/// question.
static PROTO_LOGGED: OnceLock<()> = OnceLock::new();

/// Fetch attempt budget for ranged chunk GETs. Transient errors (timeout,
/// connect/body, 5xx, 429) retry up to MAX_FETCH_ATTEMPTS - 1 times with
/// exponential backoff. 4xx (except 429) fail immediately.
const MAX_FETCH_ATTEMPTS: u32 = 4;
const RETRY_BASE: Duration = Duration::from_millis(250);
const RETRY_CAP: Duration = Duration::from_secs(4);

/// Returns true if the status code is worth retrying (server-side / rate-limit).
fn is_retryable_status(s: StatusCode) -> bool {
    s == StatusCode::TOO_MANY_REQUESTS || s.is_server_error()
}

/// Fetch one byte range [start, end] from `url`, retrying transient errors.
async fn fetch_range_retry(
    client: &Client,
    url: &str,
    start: u64,
    end: u64,
) -> Result<Bytes> {
    let mut attempt: u32 = 0;
    let mut delay = RETRY_BASE;
    loop {
        attempt += 1;
        let res: Result<Bytes> = async {
            let resp = client
                .get(url)
                .header("range", format!("bytes={start}-{end}"))
                .send()
                .await?;
            let status = resp.status();
            if !status.is_success() {
                if is_retryable_status(status) && attempt < MAX_FETCH_ATTEMPTS {
                    return Err(anyhow!("retryable status {status}"));
                }
                return Err(anyhow!("HTTP {status} on {url} bytes={start}-{end}"));
            }
            // Log negotiated protocol on the first successful response so we
            // can confirm HTTP/2 multiplexing instead of inferring it.
            if PROTO_LOGGED.set(()).is_ok() {
                info!(version = ?resp.version(), "negotiated HTTP protocol");
            }
            Ok(resp.bytes().await?)
        }
        .await;

        match res {
            Ok(b) => return Ok(b),
            Err(e) if attempt < MAX_FETCH_ATTEMPTS => {
                warn!(attempt, ?delay, error = %e, "retrying chunk GET");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(RETRY_CAP);
            }
            Err(e) => return Err(e.context(format!("after {attempt} attempts"))),
        }
    }
}

pub const DEFAULT_PROXY: &str = "https://checkpoints.mainnet.sui.unconfirmed.cloud";
pub const DEFAULT_ARCHIVE: &str = "https://archive.checkpoints.mainnet.sui.unconfirmed.cloud";

/// 16 MiB default — matches the TS SDK. Small enough to bound the window,
/// large enough to amortize TCP + CF edge round-trip overhead. Tunable per
/// `iterate_epoch` call for benchmarking different sizes.
pub const CHUNK_BYTES: u64 = 16 * 1024 * 1024;
pub const IDX_ENTRY_BYTES: usize = 20;

#[derive(Debug, Clone, Deserialize)]
pub struct EpochMetadata {
    pub epoch: u64,
    pub first_seq: u64,
    pub last_seq: u64,
    pub zst_bytes: u64,
    pub zst_sha256: String,
    pub idx_sha256: String,
    pub count: u64,
    pub zst_key: String,
    pub idx_key: String,
    pub idx_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct IdxEntry {
    pub seq: u64,
    pub offset: u64,
    pub length: u32,
}

pub fn parse_idx_file(bytes: &[u8]) -> Result<Vec<IdxEntry>> {
    if bytes.len() % IDX_ENTRY_BYTES != 0 {
        return Err(anyhow!(
            "idx file size {} is not a multiple of {IDX_ENTRY_BYTES}",
            bytes.len()
        ));
    }
    let count = bytes.len() / IDX_ENTRY_BYTES;
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let s = i * IDX_ENTRY_BYTES;
        out.push(IdxEntry {
            seq: u64::from_le_bytes(bytes[s..s + 8].try_into().unwrap()),
            offset: u64::from_le_bytes(bytes[s + 8..s + 16].try_into().unwrap()),
            length: u32::from_le_bytes(bytes[s + 16..s + 20].try_into().unwrap()),
        });
    }
    Ok(out)
}

#[derive(Debug, Deserialize)]
struct EpochList {
    epochs: Vec<EpochMetadata>,
}

/// Default number of `reqwest::Client`s in the connection pool. Each client
/// owns its own TCP connection (HTTP/2 stream-multiplexed). On a fat pipe
/// like the claude box, a single TCP connection saturates a CDN edge at
/// ~5 Gbit; running multiple connections in parallel breaks past that
/// per-connection window.
pub const DEFAULT_CLIENT_POOL: usize = 4;

#[derive(Clone)]
pub struct SuiArchiveClient {
    /// Pool of HTTP clients. Each one owns its own TCP connection. We
    /// round-robin chunk fetches across them to break past the
    /// per-connection bandwidth ceiling.
    http_pool: Vec<Client>,
    /// Atomic counter used to round-robin client + archive_url selection.
    next: std::sync::Arc<AtomicUsize>,
    proxy_url: String,
    /// One or more archive hostnames, all serving the same R2 bucket. We
    /// rotate per chunk fetch so distinct (source_IP, host_header) pairs
    /// can pick up independent egress budgets at the CF edge.
    archive_urls: Vec<String>,
}

impl SuiArchiveClient {
    /// Create a client with the default connection pool size and a single
    /// archive hostname.
    pub fn new(proxy_url: impl Into<String>, archive_url: impl Into<String>) -> Self {
        Self::with_pool_size(proxy_url, archive_url, DEFAULT_CLIENT_POOL)
    }

    /// Create a client with an explicit pool size and one archive hostname.
    pub fn with_pool_size(
        proxy_url: impl Into<String>,
        archive_url: impl Into<String>,
        pool_size: usize,
    ) -> Self {
        Self::with_hosts(proxy_url, vec![archive_url.into()], pool_size)
    }

    /// Full constructor: multiple archive hostnames + pool size. All hostnames
    /// must serve the same R2 bucket (they're rotated per chunk fetch). Pass
    /// a single-element vec for the classic single-host setup.
    pub fn with_hosts(
        proxy_url: impl Into<String>,
        archive_urls: Vec<String>,
        pool_size: usize,
    ) -> Self {
        assert!(!archive_urls.is_empty(), "at least one archive_url required");
        let pool_size = pool_size.max(1);
        let http_pool = (0..pool_size)
            .map(|_| {
                Client::builder()
                    .pool_idle_timeout(Duration::from_secs(90))
                    .tcp_keepalive(Duration::from_secs(30))
                    .timeout(Duration::from_secs(60))
                    .build()
                    .expect("reqwest client build")
            })
            .collect();
        let archive_urls = archive_urls
            .into_iter()
            .map(|u| u.trim_end_matches('/').to_string())
            .collect();
        Self {
            http_pool,
            next: std::sync::Arc::new(AtomicUsize::new(0)),
            proxy_url: proxy_url.into().trim_end_matches('/').to_string(),
            archive_urls,
        }
    }

    pub fn default_unconfirmed() -> Self {
        Self::new(DEFAULT_PROXY, DEFAULT_ARCHIVE)
    }

    /// Pick the next (client, archive_url) pair. We advance the counter once
    /// and project it onto both pools — so chunk fetches stripe across the
    /// cartesian product of (client, host) without a separate counter per pool.
    fn pick(&self) -> (Client, &str) {
        let i = self.next.fetch_add(1, Ordering::Relaxed);
        let client = self.http_pool[i % self.http_pool.len()].clone();
        let url = self.archive_urls[i % self.archive_urls.len()].as_str();
        (client, url)
    }

    /// Test-only accessors so unit tests can verify pool-size clamping,
    /// trailing-slash normalization, and round-robin behavior without
    /// touching the network.
    #[cfg(test)]
    pub(crate) fn pool_len(&self) -> usize {
        self.http_pool.len()
    }

    #[cfg(test)]
    pub(crate) fn archive_urls(&self) -> &[String] {
        &self.archive_urls
    }

    #[cfg(test)]
    pub(crate) fn proxy_url(&self) -> &str {
        &self.proxy_url
    }

    /// Test-only round-robin pick that returns just the host (skipping the
    /// `Client` clone, which doesn't matter for distribution checks).
    #[cfg(test)]
    pub(crate) fn pick_host_for_test(&self) -> String {
        let (_c, u) = self.pick();
        u.to_string()
    }

    /// Test-only round-robin pick that returns the indices into both pools so
    /// tests can verify (client_idx, host_idx) striping independently.
    #[cfg(test)]
    pub(crate) fn pick_indices_for_test(&self) -> (usize, usize) {
        let i = self.next.fetch_add(1, Ordering::Relaxed);
        (i % self.http_pool.len(), i % self.archive_urls.len())
    }

    /// Reference to the first client. Used for one-off metadata calls
    /// (find_epoch, get_epoch_idx) that don't need to be parallelized.
    fn metadata_client(&self) -> &Client {
        &self.http_pool[0]
    }

    /// First archive URL — used for `get_epoch_idx` (small one-shot).
    fn metadata_archive_url(&self) -> &str {
        self.archive_urls[0].as_str()
    }

    pub async fn find_epoch_by_number(&self, epoch: u64) -> Result<EpochMetadata> {
        let url = format!("{}/epochs/{}", self.proxy_url, epoch);
        let resp = self.metadata_client().get(&url).send().await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Err(anyhow!("epoch {epoch} not indexed"));
        }
        let resp = resp.error_for_status().context(format!("GET {url}"))?;
        Ok(resp.json().await?)
    }

    /// Resolve the epoch containing `seq`. Falls back to listing `/epochs`
    /// if the proxy doesn't expose a fast per-seq route.
    pub async fn find_epoch(&self, seq: u64) -> Result<EpochMetadata> {
        let fast = format!("{}/checkpoints/{}/epoch", self.proxy_url, seq);
        if let Ok(resp) = self.metadata_client().get(&fast).send().await {
            if resp.status().is_success() {
                if let Ok(meta) = resp.json::<EpochMetadata>().await {
                    return Ok(meta);
                }
            }
        }
        let url = format!("{}/epochs", self.proxy_url);
        let resp = self.metadata_client().get(&url).send().await?.error_for_status()?;
        let list: EpochList = resp.json().await?;
        list.epochs
            .into_iter()
            .find(|m| seq >= m.first_seq && seq <= m.last_seq)
            .ok_or_else(|| anyhow!("no epoch indexed for seq {seq}"))
    }

    pub async fn get_epoch_idx(&self, meta: &EpochMetadata) -> Result<Vec<u8>> {
        let url = format!("{}/{}", self.metadata_archive_url(), meta.idx_key);
        // The .idx is small (20 B/entry) but a single transient failure here
        // aborts the whole replay. Reuse the same retry budget as chunk GETs.
        let bytes = fetch_range_retry(self.metadata_client(), &url, 0, meta.idx_bytes.saturating_sub(1)).await?;
        Ok(bytes.to_vec())
    }

    /// Stream checkpoints in [start_seq, end_seq] within a single epoch as
    /// zstd-decompressed proto bytes.
    ///
    /// Concurrency model: a `JoinSet` owned by this stream future drives up to
    /// `concurrency` ranged GETs in flight. On stream drop, the JoinSet's Drop
    /// aborts every in-flight task — no detached background work, no
    /// half-delivered channels. Panics in fetch tasks surface as errors via
    /// `JoinError` instead of silently closing a channel.
    pub fn iterate_epoch(
        &self,
        epoch: u64,
        start_seq: u64,
        end_seq: u64,
        concurrency: usize,
    ) -> impl Stream<Item = Result<(u64, Bytes)>> + '_ {
        self.iterate_epoch_with_chunk(epoch, start_seq, end_seq, concurrency, CHUNK_BYTES)
    }

    /// Like `iterate_epoch` but with a tunable chunk size. Bigger chunks =
    /// fewer requests + better TCP window utilization, smaller chunks =
    /// lower latency to first-byte and tighter memory bound.
    pub fn iterate_epoch_with_chunk(
        &self,
        epoch: u64,
        start_seq: u64,
        end_seq: u64,
        concurrency: usize,
        chunk_bytes: u64,
    ) -> impl Stream<Item = Result<(u64, Bytes)>> + '_ {
        let chunk_bytes = chunk_bytes.max(1024 * 1024);
        async_stream::try_stream! {
            let meta = self.find_epoch_by_number(epoch).await?;
            debug!(
                epoch,
                first = meta.first_seq,
                last = meta.last_seq,
                bytes = meta.zst_bytes,
                "streaming epoch"
            );

            let idx_bytes = self.get_epoch_idx(&meta).await?;
            let all = parse_idx_file(&idx_bytes)?;
            let entries: Vec<IdxEntry> = all
                .into_iter()
                .filter(|e| e.seq >= start_seq && e.seq <= end_seq)
                .collect();
            if entries.is_empty() {
                return;
            }

            let first_offset = entries.first().unwrap().offset;
            let last = entries.last().unwrap();
            let last_byte = last.offset + last.length as u64;
            let start_chunk = first_offset / chunk_bytes;
            let end_chunk = (last_byte + chunk_bytes - 1) / chunk_bytes;

            // zst_url base path; host varies per-fetch via pick().
            let zst_key = meta.zst_key.clone();
            let epoch_size = meta.zst_bytes;
            let concurrency = concurrency.max(1);

            use std::collections::BTreeMap;
            use tokio::task::JoinSet;

            let mut joiner: JoinSet<Result<(u64, Bytes)>> = JoinSet::new();
            let mut next_to_spawn = start_chunk;
            let mut pending: BTreeMap<u64, Bytes> = BTreeMap::new();
            let mut window: Vec<u8> = Vec::new();
            let mut window_start = start_chunk * chunk_bytes;
            let mut next_expect = start_chunk;

            for entry in entries {
                let frame_start = entry.offset;
                let frame_end = frame_start + entry.length as u64;

                // Frame bytes are [frame_start, frame_end) — half-open. We have
                // enough once window_end >= frame_end. Strict `<` here was
                // historically `<=`, which extended the loop one byte past the
                // frame and only worked because non-last frames always had
                // more chunks behind them. The last frame in an epoch sits
                // flush against zst_bytes, so the off-by-one tries to fetch a
                // chunk that doesn't exist and exhausts the pool.
                while (window_start + window.len() as u64) < frame_end {
                    // Drop bytes behind the current frame, chunk_bytes-aligned.
                    let behind = frame_start.saturating_sub(window_start);
                    if behind >= chunk_bytes {
                        let drop = (behind / chunk_bytes) * chunk_bytes;
                        window.drain(..drop as usize);
                        window_start += drop;
                    }

                    // Top up the in-flight pool — never more than `concurrency`.
                    // Each chunk is dispatched to a different (client, host)
                    // pair. Multiple TCP connections across distinct
                    // hostnames so independent CF per-host budgets stack.
                    while joiner.len() < concurrency && next_to_spawn < end_chunk {
                        let chunk_idx = next_to_spawn;
                        next_to_spawn += 1;
                        let (client, base) = self.pick();
                        let url = format!("{}/{}", base, zst_key);
                        let start = chunk_idx * chunk_bytes;
                        let end = (start + chunk_bytes).min(epoch_size) - 1;
                        joiner.spawn(async move {
                            let bytes = fetch_range_retry(&client, &url, start, end).await?;
                            Ok::<(u64, Bytes), anyhow::Error>((chunk_idx, bytes))
                        });
                    }

                    // If our next-in-order chunk is already buffered, consume it
                    // before blocking on the joiner.
                    if let Some(chunk) = pending.remove(&next_expect) {
                        window.extend_from_slice(&chunk);
                        next_expect += 1;
                        continue;
                    }

                    match joiner.join_next().await {
                        Some(Ok(Ok((idx, bytes)))) => {
                            pending.insert(idx, bytes);
                        }
                        Some(Ok(Err(e))) => Err(e)?,
                        Some(Err(join_err)) => {
                            Err(anyhow!("chunk fetch task panicked: {join_err}"))?;
                        }
                        None => {
                            // Pool empty AND no more to spawn AND we still need bytes —
                            // genuine logic bug if reachable (chunk math wrong).
                            Err(anyhow!(
                                "chunk pool exhausted at seq {} (window_end={}, need={})",
                                entry.seq,
                                window_start + window.len() as u64,
                                frame_end,
                            ))?;
                        }
                    }
                }

                let local_start = (frame_start - window_start) as usize;
                let local_end = local_start + entry.length as usize;
                let frame = &window[local_start..local_end];
                let decompressed = zstd::decode_all(frame)
                    .with_context(|| format!("zstd decode frame at seq {}", entry.seq))?;
                yield (entry.seq, Bytes::from(decompressed));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- parse_idx_file ----

    #[test]
    fn parse_idx_roundtrip() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&100_000u64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&1234u32.to_le_bytes());
        buf.extend_from_slice(&100_001u64.to_le_bytes());
        buf.extend_from_slice(&1234u64.to_le_bytes());
        buf.extend_from_slice(&5678u32.to_le_bytes());
        let entries = parse_idx_file(&buf).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].seq, 100_000);
        assert_eq!(entries[0].length, 1234);
        assert_eq!(entries[1].offset, 1234);
    }

    #[test]
    fn parse_idx_empty_input_yields_empty_vec() {
        let entries = parse_idx_file(&[]).expect("empty input is valid");
        assert!(entries.is_empty());
    }

    #[test]
    fn parse_idx_single_entry() {
        let mut buf = Vec::with_capacity(IDX_ENTRY_BYTES);
        buf.extend_from_slice(&42u64.to_le_bytes());
        buf.extend_from_slice(&0xDEAD_BEEFu64.to_le_bytes());
        buf.extend_from_slice(&777u32.to_le_bytes());
        assert_eq!(buf.len(), IDX_ENTRY_BYTES);

        let entries = parse_idx_file(&buf).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].seq, 42);
        assert_eq!(entries[0].offset, 0xDEAD_BEEF);
        assert_eq!(entries[0].length, 777);
    }

    #[test]
    fn parse_idx_multiple_entries_decodes_each() {
        // Build 5 entries with distinct values across all three fields.
        let mut buf = Vec::with_capacity(5 * IDX_ENTRY_BYTES);
        let cases: [(u64, u64, u32); 5] = [
            (1, 0, 100),
            (2, 100, 200),
            (3, 300, 400),
            (4, 700, 50),
            (u64::MAX, u64::MAX - 1, u32::MAX),
        ];
        for (seq, off, len) in &cases {
            buf.extend_from_slice(&seq.to_le_bytes());
            buf.extend_from_slice(&off.to_le_bytes());
            buf.extend_from_slice(&len.to_le_bytes());
        }
        let entries = parse_idx_file(&buf).unwrap();
        assert_eq!(entries.len(), cases.len());
        for (i, (seq, off, len)) in cases.iter().enumerate() {
            assert_eq!(entries[i].seq, *seq, "seq mismatch at {i}");
            assert_eq!(entries[i].offset, *off, "offset mismatch at {i}");
            assert_eq!(entries[i].length, *len, "length mismatch at {i}");
        }
    }

    #[test]
    fn parse_idx_invalid_size_is_error() {
        // 21 bytes — one byte more than a single entry.
        let buf = vec![0u8; IDX_ENTRY_BYTES + 1];
        let err = parse_idx_file(&buf).expect_err("must reject non-multiple-of-20");
        let msg = format!("{err}");
        assert!(
            msg.contains("not a multiple"),
            "error message should mention size mismatch: {msg}"
        );
    }

    #[test]
    fn parse_idx_off_by_one_short_is_error() {
        // 19 bytes — one byte less than a single entry.
        let buf = vec![0u8; IDX_ENTRY_BYTES - 1];
        assert!(parse_idx_file(&buf).is_err());
    }

    // ---- is_retryable_status ----

    #[test]
    fn retryable_status_success_and_client_errors_are_not_retryable() {
        assert!(!is_retryable_status(StatusCode::OK));
        assert!(!is_retryable_status(StatusCode::NOT_FOUND));
        assert!(!is_retryable_status(StatusCode::UNAUTHORIZED));
        assert!(!is_retryable_status(StatusCode::FORBIDDEN));
        assert!(!is_retryable_status(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn retryable_status_429_is_retryable() {
        assert!(is_retryable_status(StatusCode::TOO_MANY_REQUESTS));
    }

    #[test]
    fn retryable_status_5xx_is_retryable() {
        assert!(is_retryable_status(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(is_retryable_status(StatusCode::BAD_GATEWAY));
        assert!(is_retryable_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(is_retryable_status(StatusCode::GATEWAY_TIMEOUT));
    }

    // ---- EpochMetadata serde ----

    #[test]
    fn epoch_metadata_deserializes_representative_blob() {
        // Realistic-shaped JSON. EpochMetadata is Deserialize-only; we
        // construct the JSON by hand and assert every field round-trips
        // correctly.
        let json = r#"{
            "epoch": 1080,
            "first_seq": 167000000,
            "last_seq": 167123456,
            "zst_bytes": 4294967296,
            "zst_sha256": "a".repeat_replace_marker,
            "idx_sha256": "b".repeat_replace_marker,
            "count": 123457,
            "zst_key": "epochs/1080/epoch-1080.zst",
            "idx_key": "epochs/1080/epoch-1080.idx",
            "idx_bytes": 2469140
        }"#;
        // The above contains placeholders so the test source stays
        // single-line readable; do the actual substitution here.
        let zst_sha = "a".repeat(64);
        let idx_sha = "b".repeat(64);
        let json = json
            .replace("\"a\".repeat_replace_marker", &format!("\"{zst_sha}\""))
            .replace("\"b\".repeat_replace_marker", &format!("\"{idx_sha}\""));

        let meta: EpochMetadata = serde_json::from_str(&json).expect("valid JSON");
        assert_eq!(meta.epoch, 1080);
        assert_eq!(meta.first_seq, 167_000_000);
        assert_eq!(meta.last_seq, 167_123_456);
        assert_eq!(meta.zst_bytes, 4_294_967_296);
        assert_eq!(meta.zst_sha256, zst_sha);
        assert_eq!(meta.idx_sha256, idx_sha);
        assert_eq!(meta.count, 123_457);
        assert_eq!(meta.zst_key, "epochs/1080/epoch-1080.zst");
        assert_eq!(meta.idx_key, "epochs/1080/epoch-1080.idx");
        assert_eq!(meta.idx_bytes, 2_469_140);
    }

    #[test]
    fn epoch_metadata_missing_field_is_error() {
        // Drop `count` to confirm strict deserialization.
        let json = r#"{
            "epoch": 1,
            "first_seq": 0,
            "last_seq": 10,
            "zst_bytes": 100,
            "zst_sha256": "x",
            "idx_sha256": "y",
            "zst_key": "k.zst",
            "idx_key": "k.idx",
            "idx_bytes": 20
        }"#;
        let res: Result<EpochMetadata, _> = serde_json::from_str(json);
        assert!(res.is_err(), "missing `count` must fail");
    }

    // ---- SuiArchiveClient constructors ----

    #[test]
    fn with_pool_size_zero_clamps_to_one() {
        let c = SuiArchiveClient::with_pool_size("https://proxy.example", "https://archive.example", 0);
        assert_eq!(c.pool_len(), 1, "pool_size=0 must clamp to 1");
    }

    #[test]
    fn with_pool_size_passes_through() {
        for n in [1usize, 2, 4, 8] {
            let c = SuiArchiveClient::with_pool_size(
                "https://proxy.example",
                "https://archive.example",
                n,
            );
            assert_eq!(c.pool_len(), n);
            assert_eq!(c.archive_urls().len(), 1);
        }
    }

    #[test]
    fn with_hosts_normalizes_trailing_slashes() {
        let c = SuiArchiveClient::with_hosts(
            "https://proxy.example/",
            vec![
                "https://a.example/".to_string(),
                "https://b.example//".to_string(),
                "https://c.example".to_string(),
            ],
            2,
        );
        assert_eq!(c.proxy_url(), "https://proxy.example");
        assert_eq!(
            c.archive_urls(),
            &[
                "https://a.example".to_string(),
                "https://b.example".to_string(),
                "https://c.example".to_string(),
            ]
        );
        assert_eq!(c.pool_len(), 2);
    }

    #[test]
    fn with_hosts_single_url() {
        let c = SuiArchiveClient::with_hosts(
            "https://proxy.example",
            vec!["https://only.example".to_string()],
            3,
        );
        assert_eq!(c.archive_urls().len(), 1);
        assert_eq!(c.archive_urls()[0], "https://only.example");
        assert_eq!(c.pool_len(), 3);
    }

    #[test]
    #[should_panic(expected = "at least one archive_url required")]
    fn with_hosts_empty_vec_panics() {
        let _ = SuiArchiveClient::with_hosts("https://proxy.example", vec![], 1);
    }

    // ---- pick / round-robin ----

    #[test]
    fn pick_round_robin_single_host_rotates_clients() {
        // 4 clients, 1 host: every call yields the same host but client
        // index advances 0,1,2,3,0,1,2,3.
        let c = SuiArchiveClient::with_pool_size(
            "https://proxy.example",
            "https://archive.example",
            4,
        );
        let mut client_indices = Vec::new();
        let mut host_indices = Vec::new();
        for _ in 0..8 {
            let (ci, hi) = c.pick_indices_for_test();
            client_indices.push(ci);
            host_indices.push(hi);
        }
        assert_eq!(client_indices, vec![0, 1, 2, 3, 0, 1, 2, 3]);
        assert_eq!(host_indices, vec![0; 8]);
    }

    #[test]
    fn pick_round_robin_4_clients_2_hosts_independent() {
        // With pool_size=4 and 2 hosts, the single counter projects onto
        // both pools: client_idx = i%4, host_idx = i%2. Over 8 calls we
        // expect clients 0..3 twice and hosts 0,1 four times each in
        // alternation.
        let c = SuiArchiveClient::with_hosts(
            "https://proxy.example",
            vec![
                "https://a.example".to_string(),
                "https://b.example".to_string(),
            ],
            4,
        );
        let mut clients = Vec::new();
        let mut hosts = Vec::new();
        for _ in 0..8 {
            let (ci, hi) = c.pick_indices_for_test();
            clients.push(ci);
            hosts.push(hi);
        }
        assert_eq!(clients, vec![0, 1, 2, 3, 0, 1, 2, 3]);
        assert_eq!(hosts, vec![0, 1, 0, 1, 0, 1, 0, 1]);
    }

    #[test]
    fn pick_distributes_across_hosts() {
        // 2 clients, 3 hosts → over 6 calls each host appears exactly twice
        // and each client exactly thrice.
        let c = SuiArchiveClient::with_hosts(
            "https://proxy.example",
            vec![
                "https://a.example".to_string(),
                "https://b.example".to_string(),
                "https://c.example".to_string(),
            ],
            2,
        );

        let mut host_counts = [0usize; 3];
        let mut client_counts = [0usize; 2];
        for _ in 0..6 {
            let (ci, hi) = c.pick_indices_for_test();
            client_counts[ci] += 1;
            host_counts[hi] += 1;
        }
        assert_eq!(host_counts, [2, 2, 2]);
        assert_eq!(client_counts, [3, 3]);
    }

    #[test]
    fn pick_host_strings_match_archive_urls() {
        // Sanity: pick() returns hosts from the (normalized) archive_urls.
        let c = SuiArchiveClient::with_hosts(
            "https://proxy.example",
            vec![
                "https://a.example/".to_string(),
                "https://b.example".to_string(),
            ],
            1,
        );
        let h0 = c.pick_host_for_test();
        let h1 = c.pick_host_for_test();
        let h2 = c.pick_host_for_test();
        assert_eq!(h0, "https://a.example");
        assert_eq!(h1, "https://b.example");
        assert_eq!(h2, "https://a.example"); // wraps
    }
}
