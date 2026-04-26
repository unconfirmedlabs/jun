//! jun-pipeline — archive → decode → sink, with backpressure.
//!
//! Three tasks connected by bounded channels:
//!   1. FETCH: stream checkpoint bytes from each epoch in the requested range
//!   2. DECODE: parse proto → ExtractedCheckpoint (one checkpoint per call)
//!   3. WRITE: hand each batch to a `Sink` (NATS broadcast, stdout pipe, …)
//!
//! Sinks live in the caller (the `jun` binary or library users). The pipeline
//! itself is database-agnostic — it produces `ExtractedCheckpoint` and pushes
//! through whatever sink is plugged in.

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use jun_archive_reader::SuiArchiveClient;
use jun_checkpoint_decoder::{decode_with, Decoder};
use jun_types::{ExtractMask, ExtractedCheckpoint};
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Generic sink for decoded checkpoints. Implementors include NATS publishers,
/// stdout pipes (jsonl/proto), or in-process consumers (juna lib path).
#[async_trait]
pub trait Sink: Send + Sync {
    /// Write a batch of decoded checkpoints. Implementations should treat the
    /// batch as atomic for retry / cursor purposes.
    async fn write_batch(&self, batch: Vec<ExtractedCheckpoint>) -> Result<u64>;
}

/// Which pipeline stages to actually run. Lower modes are strict subsets,
/// so `FetchOnly` is the cheapest profiling option.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunMode {
    /// fetch + decode + write — the normal ingest.
    Full,
    /// fetch + decode, drop the result. Isolates write cost.
    NoWrite,
    /// fetch only, count frames and total bytes. Isolates archive stream throughput.
    FetchOnly,
}

pub struct ReplayConfig {
    pub from_seq: u64,
    pub to_seq: u64,
    pub chunk_concurrency: usize,  // per-epoch chunk fetch concurrency
    pub chunk_bytes: u64,          // size of each ranged GET against the .zst
    pub decode_concurrency: usize, // parallel decoders
    pub batch_size: usize,         // checkpoints per sink write batch
    pub write_concurrency: usize,  // max sink batches in flight at once
    pub mask: ExtractMask,
    pub mode: RunMode,
    pub decoder: Decoder,          // prost (default) or skip
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            from_seq: 0,
            to_seq: 0,
            chunk_concurrency: 8,
            chunk_bytes: jun_archive_reader::CHUNK_BYTES,
            decode_concurrency: 4,
            batch_size: 500,
            write_concurrency: 4,
            mask: ExtractMask::ALL,
            mode: RunMode::Full,
            decoder: Decoder::Prost,
        }
    }
}

pub struct ReplayStats {
    pub checkpoints: u64,
    pub bytes: u64,
    pub elapsed_sec: f64,
    pub cps_per_sec: f64,
}

async fn run_fetch_only(
    archive: &Arc<SuiArchiveClient>,
    from_seq: u64,
    to_seq: u64,
    chunk_concurrency: usize,
    chunk_bytes: u64,
    started: Instant,
) -> Result<ReplayStats> {
    let mut cursor = from_seq;
    let mut total: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut last_report = Instant::now();
    while cursor <= to_seq {
        let meta = archive.find_epoch(cursor).await?;
        let start = cursor.max(meta.first_seq);
        let end = to_seq.min(meta.last_seq);
        info!(epoch = meta.epoch, start, end, "streaming epoch (fetch-only)");
        let stream = archive.iterate_epoch_with_chunk(meta.epoch, start, end, chunk_concurrency, chunk_bytes);
        futures::pin_mut!(stream);
        while let Some(item) = stream.next().await {
            let (_seq, bytes) = item?;
            total += 1;
            total_bytes += bytes.len() as u64;
            if last_report.elapsed().as_secs() >= 1 {
                let elapsed = started.elapsed().as_secs_f64();
                info!(
                    total,
                    cps_per_sec = (total as f64 / elapsed) as u64,
                    mb_per_sec = (total_bytes as f64 / 1e6 / elapsed) as u64,
                    "fetch-only progress"
                );
                last_report = Instant::now();
            }
        }
        cursor = end + 1;
    }
    let elapsed = started.elapsed().as_secs_f64();
    Ok(ReplayStats {
        checkpoints: total,
        bytes: total_bytes,
        elapsed_sec: elapsed,
        cps_per_sec: total as f64 / elapsed.max(0.001),
    })
}

pub async fn run_replay(
    archive: SuiArchiveClient,
    sink: Option<Arc<dyn Sink>>,
    cfg: ReplayConfig,
) -> Result<ReplayStats> {
    let archive = Arc::new(archive);
    let started = Instant::now();

    // --- Fetch-only fast path: count frames + bytes, skip decode + write ---
    if cfg.mode == RunMode::FetchOnly {
        return run_fetch_only(
            &archive, cfg.from_seq, cfg.to_seq, cfg.chunk_concurrency, cfg.chunk_bytes, started,
        ).await;
    }

    // Full mode requires a sink; NoWrite skips the actual sink call but the
    // rest of the pipeline still wires through.
    let sink = if cfg.mode == RunMode::Full {
        Some(sink.ok_or_else(|| anyhow::anyhow!("Sink required for mode {:?}", cfg.mode))?)
    } else {
        None
    };

    // Stage 1: raw bytes (seq, decompressed_proto). flume bounded — multi-
    // consumer (each decoder receives directly, no shared mutex), much higher
    // throughput than `Arc<Mutex<mpsc::Receiver>>`. Buffer of 2048 lets the
    // fetcher run well ahead and absorb the network jitter we see in paired
    // benchmarks.
    let (raw_tx, raw_rx) = flume::bounded::<(u64, bytes::Bytes)>(2048);

    // Stage 2: decoded checkpoints — bounded so writer backpressure applies.
    // Bigger buffer here too — keeps the writer fed during decode bursts.
    let (dec_tx, mut dec_rx) = mpsc::channel::<ExtractedCheckpoint>(2048);

    // Figure out which epochs cover [from_seq, to_seq]. We walk epoch-by-epoch,
    // calling find_epoch at the edges.
    let from_seq = cfg.from_seq;
    let to_seq = cfg.to_seq;
    let chunk_concurrency = cfg.chunk_concurrency;
    let chunk_bytes = cfg.chunk_bytes;

    // --- fetch task --------------------------------------------------------
    let archive_f = archive.clone();
    let fetch = tokio::spawn(async move {
        let mut cursor = from_seq;
        while cursor <= to_seq {
            let meta = match archive_f.find_epoch(cursor).await {
                Ok(m) => m,
                Err(e) => { warn!("find_epoch({cursor}) failed: {e}"); return Err::<(), anyhow::Error>(e); }
            };
            let start = cursor.max(meta.first_seq);
            let end = to_seq.min(meta.last_seq);
            info!(epoch = meta.epoch, start, end, "streaming epoch");
            let stream = archive_f.iterate_epoch_with_chunk(meta.epoch, start, end, chunk_concurrency, chunk_bytes);
            futures::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                let (seq, bytes) = item?;
                if raw_tx.send_async((seq, bytes)).await.is_err() { return Ok(()); }
            }
            cursor = end + 1;
        }
        Ok(())
    });

    // --- decode task (pool) -----------------------------------------------
    // flume::Receiver is multi-consumer-safe and lock-free. Each worker holds
    // its own Receiver clone and pulls directly — no shared mutex, no contention
    // when scaling decode_concurrency past ~16.
    let decode_concurrency = cfg.decode_concurrency.max(1);
    let mask = cfg.mask;
    let decoder = cfg.decoder;
    let decode = tokio::spawn(async move {
        let mut workers: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        for _ in 0..decode_concurrency {
            let dec_tx = dec_tx.clone();
            let raw_rx = raw_rx.clone();
            workers.push(tokio::spawn(async move {
                loop {
                    let Ok((_seq, bytes)) = raw_rx.recv_async().await else { break };
                    // decode is CPU-bound; run on blocking pool so we don't
                    // starve the tokio scheduler.
                    let res = tokio::task::spawn_blocking(move || decode_with(&bytes, mask, decoder))
                        .await
                        .ok()
                        .and_then(|r| r.ok());
                    if let Some(ck) = res {
                        if dec_tx.send(ck).await.is_err() { break; }
                    }
                }
            }));
        }
        drop(dec_tx);
        for w in workers { let _ = w.await; }
    });

    // --- write task --------------------------------------------------------
    // We let up to `write_concurrency` batches be in flight at once so that
    // the decode stage isn't blocked on a single writer. A bounded JoinSet
    // enforces backpressure.
    let batch_size = cfg.batch_size.max(1);
    let write_concurrency = cfg.write_concurrency.max(1);
    let mut total: u64 = 0;
    let mut batch: Vec<ExtractedCheckpoint> = Vec::with_capacity(batch_size);
    let mut last_report = Instant::now();
    let write_enabled = cfg.mode == RunMode::Full;
    let mut in_flight: tokio::task::JoinSet<Result<u64>> = tokio::task::JoinSet::new();

    let flush = |taken: Vec<ExtractedCheckpoint>,
                 in_flight: &mut tokio::task::JoinSet<Result<u64>>,
                 sink: &Option<Arc<dyn Sink>>| {
        let n = taken.len() as u64;
        if let (true, Some(s)) = (write_enabled, sink.as_ref()) {
            let s = s.clone();
            in_flight.spawn(async move {
                s.write_batch(taken).await
            });
        } else {
            in_flight.spawn(async move { Ok(n) });
        }
    };

    while let Some(ck) = dec_rx.recv().await {
        batch.push(ck);
        if batch.len() >= batch_size {
            // Cap concurrency by draining one completed batch before queueing more.
            while in_flight.len() >= write_concurrency {
                if let Some(res) = in_flight.join_next().await {
                    total += res??;
                }
            }
            let taken = std::mem::take(&mut batch);
            batch = Vec::with_capacity(batch_size);
            flush(taken, &mut in_flight, &sink);

            if last_report.elapsed().as_secs() >= 1 {
                let elapsed = started.elapsed().as_secs_f64();
                info!(total, cps_per_sec = (total as f64 / elapsed) as u64, "progress");
                last_report = Instant::now();
            }
        }
    }
    if !batch.is_empty() {
        let taken = std::mem::take(&mut batch);
        flush(taken, &mut in_flight, &sink);
    }
    while let Some(res) = in_flight.join_next().await {
        total += res??;
    }

    fetch.await??;
    decode.await?;

    let elapsed = started.elapsed().as_secs_f64();
    Ok(ReplayStats {
        checkpoints: total,
        bytes: 0,
        elapsed_sec: elapsed,
        cps_per_sec: total as f64 / elapsed.max(0.001),
    })
}
