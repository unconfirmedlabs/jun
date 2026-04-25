//! jun-archive — client for the unconfirmedlabs Sui checkpoint archive.
//!
//! Port of `@unconfirmedlabs/sui-checkpoint-archive` (TS SDK). Provides:
//!   - Epoch routing via the proxy's `/epochs/:N` endpoint
//!   - `.idx` parsing (20-byte entries: seq/offset/length)
//!   - `iterate_epoch` — ranged GETs against `epoch-N.zst` in R2, yields
//!     per-checkpoint zstd-decompressed proto bytes
//!
//! Single-epoch streaming only. Callers needing a cross-epoch range compose
//! this in a loop (e.g. in jun-pipeline).

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

/// 16 MiB — matches the TS SDK. Small enough to bound the window, large
/// enough to amortize TCP + CF edge round-trip overhead.
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

#[derive(Clone)]
pub struct SuiArchiveClient {
    http: Client,
    proxy_url: String,
    archive_url: String,
}

impl SuiArchiveClient {
    pub fn new(proxy_url: impl Into<String>, archive_url: impl Into<String>) -> Self {
        let http = Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(30))
            .timeout(Duration::from_secs(60))
            .build()
            .expect("reqwest client build");
        Self {
            http,
            proxy_url: proxy_url.into().trim_end_matches('/').to_string(),
            archive_url: archive_url.into().trim_end_matches('/').to_string(),
        }
    }

    pub fn default_unconfirmed() -> Self {
        Self::new(DEFAULT_PROXY, DEFAULT_ARCHIVE)
    }

    pub async fn find_epoch_by_number(&self, epoch: u64) -> Result<EpochMetadata> {
        let url = format!("{}/epochs/{}", self.proxy_url, epoch);
        let resp = self.http.get(&url).send().await?;
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
        if let Ok(resp) = self.http.get(&fast).send().await {
            if resp.status().is_success() {
                if let Ok(meta) = resp.json::<EpochMetadata>().await {
                    return Ok(meta);
                }
            }
        }
        let url = format!("{}/epochs", self.proxy_url);
        let resp = self.http.get(&url).send().await?.error_for_status()?;
        let list: EpochList = resp.json().await?;
        list.epochs
            .into_iter()
            .find(|m| seq >= m.first_seq && seq <= m.last_seq)
            .ok_or_else(|| anyhow!("no epoch indexed for seq {seq}"))
    }

    pub async fn get_epoch_idx(&self, meta: &EpochMetadata) -> Result<Vec<u8>> {
        let url = format!("{}/{}", self.archive_url, meta.idx_key);
        // The .idx is small (20 B/entry) but a single transient failure here
        // aborts the whole replay. Reuse the same retry budget as chunk GETs.
        let bytes = fetch_range_retry(&self.http, &url, 0, meta.idx_bytes.saturating_sub(1)).await?;
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
            let start_chunk = first_offset / CHUNK_BYTES;
            let end_chunk = (last_byte + CHUNK_BYTES - 1) / CHUNK_BYTES;

            let zst_url = format!("{}/{}", self.archive_url, meta.zst_key);
            let epoch_size = meta.zst_bytes;
            let client = self.http.clone();
            let concurrency = concurrency.max(1);

            use std::collections::BTreeMap;
            use tokio::task::JoinSet;

            let mut joiner: JoinSet<Result<(u64, Bytes)>> = JoinSet::new();
            let mut next_to_spawn = start_chunk;
            let mut pending: BTreeMap<u64, Bytes> = BTreeMap::new();
            let mut window: Vec<u8> = Vec::new();
            let mut window_start = start_chunk * CHUNK_BYTES;
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
                    // Drop bytes behind the current frame, CHUNK_BYTES-aligned.
                    let behind = frame_start.saturating_sub(window_start);
                    if behind >= CHUNK_BYTES {
                        let drop = (behind / CHUNK_BYTES) * CHUNK_BYTES;
                        window.drain(..drop as usize);
                        window_start += drop;
                    }

                    // Top up the in-flight pool — never more than `concurrency`.
                    while joiner.len() < concurrency && next_to_spawn < end_chunk {
                        let chunk_idx = next_to_spawn;
                        next_to_spawn += 1;
                        let client = client.clone();
                        let url = zst_url.clone();
                        let start = chunk_idx * CHUNK_BYTES;
                        let end = (start + CHUNK_BYTES).min(epoch_size) - 1;
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
}
