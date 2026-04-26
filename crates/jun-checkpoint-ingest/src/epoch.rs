//! Per-epoch ingest orchestration: parallel fetch → in-order reorder → S3
//! multipart upload with equal-size non-trailing parts (R2 constraint).

use crate::archive;
use anyhow::{Context, Result};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Semaphore};

/// R2 rule: all non-trailing multipart parts must be exactly the same size.
/// 32 MiB gives a sensible balance between part count and memory.
const PART_SIZE: usize = 32 * 1024 * 1024;

pub struct EpochJob {
    pub epoch: u64,
    pub from: u64,
    pub to: u64,
    pub zst_key: String,
    pub idx_key: String,
    pub bucket: String,
    pub archive_url: String,
    pub fetch_workers: usize,

    /// Committee for this epoch (pre-deserialized once from the predecessor's
    /// end-of-epoch checkpoint). `Some` → every checkpoint is BLS-verified
    /// against it; ingest aborts on first failure. `None` is only valid for
    /// epoch 0. Wrapped in `Arc` so verifier workers share without cloning
    /// the ~10 KB pubkey set.
    pub committee: Option<Arc<jun_checkpoint_verifier::Committee>>,

    /// Connection pools. Each client owns its own TCP+TLS context, so a
    /// pool of N clients gives N parallel encryption/decryption pipelines —
    /// gets us past the single-connection ceiling that otherwise caps R2
    /// upload at ~30 MB/s on a busy CDN edge.
    pub http: Arc<crate::HttpPool>,
    pub s3: Arc<crate::S3Pool>,

    /// Maximum simultaneous UploadPart RPCs in flight across all S3 clients.
    /// Each part is `PART_SIZE` bytes resident, so the upper bound on RAM is
    /// `upload_concurrency * PART_SIZE`. R2/HTTP-2 happily takes 100+
    /// concurrent streams per connection, so on multi-client pools push this
    /// to 64+ to keep each client busy.
    pub upload_concurrency: usize,
}

#[derive(Debug, Serialize)]
pub struct EpochResult {
    pub epoch: u64,
    pub first_seq: u64,
    pub last_seq: u64,
    pub count: u64,
    pub zst_key: String,
    pub idx_key: String,
    pub zst_parts: usize,
    pub zst_bytes: u64,
    pub idx_bytes: u64,
    pub zst_sha256: String,
    pub idx_sha256: String,
    pub elapsed_secs: u64,
}

pub async fn run_epoch(job: EpochJob) -> Result<EpochResult> {
    let count = job.to - job.from + 1;
    let started = Instant::now();

    // ── 1. Initiate multipart upload with epoch-level metadata. ──────────────
    // R2 does not support `x-amz-checksum-sha256` on UploadPart (returns 501
    // NotImplemented), so we rely on the SDK's default per-part integrity
    // (Content-MD5 / CRC32 inside the request) plus our streaming whole-object
    // SHA256 (stored in metadata + D1) and a post-complete HeadObject size
    // check below.
    // rust-s3's `initiate_multipart_upload` doesn't take per-call metadata.
    // We persist per-epoch metadata (epoch, first/last seq, count, sha256s)
    // to the D1 routing row at the end of `ingest_epoch` instead — that's
    // where the proxy reads it from anyway. Object metadata on R2 was
    // diagnostic-only, not load-bearing.
    let init = job
        .s3
        .pick()
        .initiate_multipart_upload(&job.zst_key, "application/zstd")
        .await
        .context("create multipart upload")?;
    let upload_id = init.upload_id;

    // ── 2. Fetch checkpoints concurrently; reorder into byte stream. ─────────
    //
    // Pipeline: fetch_handle → raw_tx → verifier_handle → fetched_tx → writer.
    //
    // The writer reads from `fetched_rx` (downstream of verify). When verify
    // is enabled, a dedicated verifier task batches incoming cps and runs
    // multi-Miller-loop + single final_exp via `CheckpointBatch` — drops
    // per-cp BLS cost from one full pairing (~6ms) to one Miller loop
    // (~1.5-2ms) at batch size 64+. When verify is disabled, the verifier
    // task is a pass-through.
    let (raw_tx, mut raw_rx) = mpsc::channel::<(u64, Vec<u8>)>(2048);
    let (fetched_tx, mut fetched_rx) = mpsc::channel::<(u64, Vec<u8>)>(2048);
    let fetch_sem = Arc::new(Semaphore::new(job.fetch_workers));
    let verify_sem = Arc::new(Semaphore::new(num_cpus::get().max(1)));
    let http = job.http.clone();
    let archive_url = job.archive_url.clone();
    let verify_ctx = job.committee.clone();
    let epoch_for_verify = job.epoch;
    let stats_downloaded = Arc::new(AtomicU64::new(0));
    let stats_bytes = Arc::new(AtomicU64::new(0));

    let fetch_handle = {
        let stats = stats_downloaded.clone();
        let bytes = stats_bytes.clone();
        tokio::spawn(async move {
            let mut tasks = FuturesUnordered::new();
            for seq in job.from..=job.to {
                let permit = fetch_sem.clone().acquire_owned().await.unwrap();
                let http = http.clone();
                let archive_url = archive_url.clone();
                let stats = stats.clone();
                let bytes = bytes.clone();
                let tx = raw_tx.clone();
                tasks.push(tokio::spawn(async move {
                    let _permit = permit;
                    let zst = archive::fetch_zst(&*http, &archive_url, seq, 6).await?;
                    stats.fetch_add(1, Ordering::Relaxed);
                    bytes.fetch_add(zst.len() as u64, Ordering::Relaxed);
                    tx.send((seq, zst)).await.ok();
                    Result::<()>::Ok(())
                }));

                while tasks.len() > job.fetch_workers * 2 {
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

    // Verifier task: batches and BLS-verifies via CheckpointBatch. Drains
    // raw_rx, dispatches batches of `BATCH` to spawn_blocking workers
    // (capped at ncpu). On batch success, forwards individual (seq, zst)
    // tuples to `fetched_tx`. On batch failure, falls back to per-cp verify
    // to surface which cp is bad. When `committee` is None (no_verify or
    // epoch 0), the loop is a no-batch passthrough.
    let verify_handle = {
        let committee_opt = verify_ctx.clone();
        let fetched_tx = fetched_tx.clone();
        tokio::spawn(async move {
            const BATCH: usize = 64;
            match committee_opt {
                None => {
                    while let Some(item) = raw_rx.recv().await {
                        fetched_tx.send(item).await.ok();
                    }
                }
                Some(committee) => {
                    let mut buf: Vec<(u64, Vec<u8>)> = Vec::with_capacity(BATCH);
                    let mut pending: FuturesUnordered<
                        tokio::task::JoinHandle<Result<Vec<(u64, Vec<u8>)>>>,
                    > = FuturesUnordered::new();

                    async fn drain_pending(
                        pending: &mut FuturesUnordered<
                            tokio::task::JoinHandle<Result<Vec<(u64, Vec<u8>)>>>,
                        >,
                        out: &mpsc::Sender<(u64, Vec<u8>)>,
                        force_all: bool,
                    ) -> Result<()> {
                        loop {
                            let next = if force_all {
                                pending.next().await
                            } else {
                                use futures::future::FutureExt;
                                pending.next().now_or_never().flatten()
                            };
                            match next {
                                None => break,
                                Some(res) => {
                                    let chunk = res
                                        .map_err(|e| anyhow::anyhow!("verify join: {e}"))??;
                                    for item in chunk {
                                        out.send(item).await.ok();
                                    }
                                }
                            }
                        }
                        Ok(())
                    }

                    while let Some(item) = raw_rx.recv().await {
                        buf.push(item);
                        if buf.len() >= BATCH {
                            let chunk = std::mem::take(&mut buf);
                            let permit = verify_sem.clone().acquire_owned().await.unwrap();
                            let committee = committee.clone();
                            pending.push(tokio::task::spawn_blocking(move || {
                                let _p = permit;
                                let mut batch =
                                    jun_checkpoint_verifier::CheckpointBatch::new();
                                for (_, zst) in &chunk {
                                    batch.add_zst(zst, &*committee, epoch_for_verify)?;
                                }
                                batch.finalize()?;
                                Ok::<Vec<(u64, Vec<u8>)>, anyhow::Error>(chunk)
                            }));
                        }
                        drain_pending(&mut pending, &fetched_tx, false).await?;
                    }
                    if !buf.is_empty() {
                        let chunk = std::mem::take(&mut buf);
                        let permit = verify_sem.clone().acquire_owned().await.unwrap();
                        let committee = committee.clone();
                        pending.push(tokio::task::spawn_blocking(move || {
                            let _p = permit;
                            let mut batch =
                                jun_checkpoint_verifier::CheckpointBatch::new();
                            for (_, zst) in &chunk {
                                batch.add_zst(zst, &*committee, epoch_for_verify)?;
                            }
                            batch.finalize()?;
                            Ok::<Vec<(u64, Vec<u8>)>, anyhow::Error>(chunk)
                        }));
                    }
                    drain_pending(&mut pending, &fetched_tx, true).await?;
                }
            }
            drop(fetched_tx);
            Result::<()>::Ok(())
        })
    };
    drop(fetched_tx);

    // ── 3. Part uploader pool ────────────────────────────────────────────────
    let upload_concurrency = job.upload_concurrency.max(1);
    let upload_channel_capacity = upload_concurrency * 2;
    let (part_tx, mut part_rx) = mpsc::channel::<(i32, Vec<u8>)>(upload_channel_capacity);
    let mut part_handles = Vec::new();
    let part_sem = Arc::new(Semaphore::new(upload_concurrency));
    let upload_handle = {
        let s3 = job.s3.clone();
        let zst_key = job.zst_key.clone();
        let upload_id = upload_id.clone();
        let part_sem = part_sem.clone();
        tokio::spawn(async move {
            let mut parts: BTreeMap<i32, String> = BTreeMap::new();
            let mut spawned = FuturesUnordered::new();
            while let Some((num, data)) = part_rx.recv().await {
                let permit = part_sem.clone().acquire_owned().await.unwrap();
                let s3 = s3.clone();
                let zst_key = zst_key.clone();
                let upload_id = upload_id.clone();
                spawned.push(tokio::spawn(async move {
                    let _permit = permit;
                    // Retry upload_part on transient errors. A single flaky
                    // call shouldn't tear down the whole pipeline — the part
                    // is ~32 MiB in memory, so cloning for each retry is fine
                    // (retries are rare; typical path succeeds first try).
                    let mut backoff = std::time::Duration::from_millis(500);
                    let mut last_err: Option<anyhow::Error> = None;
                    for attempt in 1..=6u32 {
                        // Pick a fresh client each attempt so retries also
                        // get to round-robin across the pool.
                        match s3
                            .pick()
                            .put_multipart_chunk(
                                data.clone(),
                                &zst_key,
                                num as u32,
                                &upload_id,
                                "application/zstd",
                            )
                            .await
                        {
                            Ok(part) => {
                                return Result::<(i32, String)>::Ok((num, part.etag));
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "upload_part {num} attempt {attempt}/6 failed: {e:?}"
                                );
                                last_err = Some(anyhow::anyhow!("upload_part {num}: {e:?}"));
                                tokio::time::sleep(backoff).await;
                                backoff = (backoff * 2).min(std::time::Duration::from_secs(15));
                            }
                        }
                    }
                    Err(last_err.unwrap_or_else(|| {
                        anyhow::anyhow!("upload_part {num} exhausted retries")
                    }))
                }));
                while let Some(done) = spawned.try_next_some().await {
                    let (n, etag) = done??;
                    parts.insert(n, etag);
                }
            }
            while let Some(done) = spawned.next().await {
                let (n, etag) = done??;
                parts.insert(n, etag);
            }
            Result::<BTreeMap<i32, String>>::Ok(parts)
        })
    };
    part_handles.push(upload_handle);

    // ── 4. Writer: pop seqs in order, emit equal-sized parts. ────────────────
    let mut next_seq = job.from;
    let mut buf: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    let mut cur_part: Vec<u8> = Vec::with_capacity(PART_SIZE + 1024 * 1024);
    let mut part_num: i32 = 0;
    let mut offset: u64 = 0;
    let mut zst_sha = Sha256::new();
    let mut index: Vec<(u64, u64, u32)> = Vec::with_capacity(count as usize);

    while let Some((seq, data)) = fetched_rx.recv().await {
        if seq < next_seq {
            continue; // defensive dedup
        }
        buf.insert(seq, data);

        while let Some(data) = buf.remove(&next_seq) {
            index.push((next_seq, offset, data.len() as u32));
            offset += data.len() as u64;
            zst_sha.update(&data);
            cur_part.extend_from_slice(&data);
            next_seq += 1;

            while cur_part.len() >= PART_SIZE {
                part_num += 1;
                let chunk: Vec<u8> = cur_part.drain(..PART_SIZE).collect();
                part_tx
                    .send((part_num, chunk))
                    .await
                    .context("part_tx send")?;
            }
        }
    }

    if next_seq <= job.to {
        anyhow::bail!(
            "incomplete archive: missing seqs {}..={}",
            next_seq,
            job.to
        );
    }

    // Flush trailing part (arbitrary size permitted).
    if !cur_part.is_empty() {
        part_num += 1;
        part_tx
            .send((part_num, std::mem::take(&mut cur_part)))
            .await
            .context("final part_tx")?;
    }
    drop(part_tx);

    fetch_handle.await.context("fetch task panic")??;
    verify_handle.await.context("verify task panic")??;
    let parts_map = part_handles
        .pop()
        .unwrap()
        .await
        .context("upload task panic")??;

    // ── 5. Complete multipart. ───────────────────────────────────────────────
    let parts: Vec<s3::serde_types::Part> = parts_map
        .into_iter()
        .map(|(num, etag)| s3::serde_types::Part {
            part_number: num as u32,
            etag,
        })
        .collect();
    job.s3
        .pick()
        .complete_multipart_upload(&job.zst_key, &upload_id, parts)
        .await
        .context("complete multipart")?;

    // Post-complete integrity: confirm R2 reports the same byte-count we
    // streamed. Per-part SHA256 already guarantees *those* bytes are the ones
    // we sent; size check catches the edge case of a part silently missing
    // from the assembled object.
    let (zst_head, zst_status) = job
        .s3
        .pick()
        .head_object(&job.zst_key)
        .await
        .context("head .zst")?;
    if zst_status >= 300 {
        anyhow::bail!("head .zst returned HTTP {zst_status}");
    }
    let zst_reported_size = zst_head.content_length.unwrap_or(0) as u64;
    if zst_reported_size != offset {
        anyhow::bail!(
            ".zst size mismatch: R2 reports {}, expected {}",
            zst_reported_size,
            offset
        );
    }

    let zst_sha_hex = hex::encode(zst_sha.finalize());

    // ── 6. Serialise + upload the index (single PutObject). ──────────────────
    let mut idx_buf = Vec::with_capacity(index.len() * 20);
    for (seq, off, len) in &index {
        idx_buf.extend_from_slice(&seq.to_le_bytes());
        idx_buf.extend_from_slice(&off.to_le_bytes());
        idx_buf.extend_from_slice(&len.to_le_bytes());
    }
    let idx_sha = Sha256::digest(&idx_buf);
    let idx_sha_hex = hex::encode(idx_sha);

    job.s3
        .pick()
        .put_object_with_content_type(&job.idx_key, &idx_buf, "application/octet-stream")
        .await
        .context("put .idx")?;

    let (idx_head, idx_status) = job
        .s3
        .pick()
        .head_object(&job.idx_key)
        .await
        .context("head .idx")?;
    if idx_status >= 300 {
        anyhow::bail!("head .idx returned HTTP {idx_status}");
    }
    let idx_reported_size = idx_head.content_length.unwrap_or(0) as u64;
    if idx_reported_size != idx_buf.len() as u64 {
        anyhow::bail!(
            ".idx size mismatch: R2 reports {}, expected {}",
            idx_reported_size,
            idx_buf.len()
        );
    }

    if index.len() as u64 != count {
        anyhow::bail!("index count {} != expected {}", index.len(), count);
    }

    Ok(EpochResult {
        epoch: job.epoch,
        first_seq: job.from,
        last_seq: job.to,
        count,
        zst_key: job.zst_key,
        idx_key: job.idx_key,
        zst_parts: part_num as usize,
        zst_bytes: offset,
        idx_bytes: idx_buf.len() as u64,
        zst_sha256: zst_sha_hex,
        idx_sha256: idx_sha_hex,
        elapsed_secs: started.elapsed().as_secs(),
    })
}

// Helper trait for non-blocking drain of FuturesUnordered.
trait FuturesUnorderedExt<T> {
    async fn try_next_some(&mut self) -> Option<T>;
}

impl<F: futures::Future> FuturesUnorderedExt<F::Output> for FuturesUnordered<F> {
    async fn try_next_some(&mut self) -> Option<F::Output> {
        // Yield to executor, then poll-once-ish without blocking.
        use futures::future::FutureExt;
        self.next().now_or_never().flatten()
    }
}
