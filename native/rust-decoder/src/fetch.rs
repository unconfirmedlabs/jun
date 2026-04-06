//! Archive checkpoint range downloader — Rust owns fetch + decode.
//!
//! Spawns a detached thread with a tokio runtime that concurrently fetches
//! checkpoints from the archive CDN, decompresses + decodes each one, and
//! streams results back to JS via a threadsafe callback.
//!
//! Each result gets its own allocated buffer (not shared) so the callback
//! pointer remains valid until the next callback overwrites it. The JS side
//! copies the bytes before returning from the callback.

use reqwest::Client;
use tokio::runtime::Builder;
use tokio::task::JoinSet;

/// Callback signature: (checkpoint_seq, data_ptr, data_len).
/// Called once per decoded checkpoint. `RANGE_DONE_SEQ` signals completion.
pub type OutputCallback = extern "C" fn(seq: u64, data_ptr: *const u8, data_len: u32);

/// Sentinel sequence number signaling that the range download is complete.
pub const RANGE_DONE_SEQ: u64 = u64::MAX;

/// Spawn a detached thread that downloads and decodes a checkpoint range.
/// Returns immediately — results stream back via `callback`.
pub fn spawn_download_and_decode_range(
    archive_url: String,
    from: u64,
    to: u64,
    concurrency: u32,
    callback: OutputCallback,
) -> Result<(), Box<dyn std::error::Error>> {
    std::thread::Builder::new()
        .name("jun-rust-fetch-decode".to_string())
        .spawn(move || {
            if let Err(err) = run_download_and_decode(&archive_url, from, to, concurrency, callback) {
                eprintln!("[rust-fetch] range {from}..{to} failed: {err}");
            }
            callback(RANGE_DONE_SEQ, std::ptr::null(), 0);
        })?;
    Ok(())
}

fn run_download_and_decode(
    archive_url: &str,
    from: u64,
    to: u64,
    concurrency: u32,
    callback: OutputCallback,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let concurrency = concurrency.max(1) as usize;
    let base_url = archive_url.trim_end_matches('/').to_string();

    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;

    runtime.block_on(async {
        let client = Client::builder()
            .pool_max_idle_per_host(concurrency)
            .build()?;

        let mut join_set = JoinSet::new();
        let mut next_seq = from;

        // Seed the join set with initial concurrent tasks
        let initial = concurrency.min((to - from + 1) as usize);
        for _ in 0..initial {
            if next_seq > to {
                break;
            }
            spawn_fetch(&mut join_set, &client, &base_url, next_seq);
            next_seq += 1;
        }

        // Each result gets its own heap-allocated buffer. The buffer is leaked
        // (ownership transferred to JS) because Bun's threadsafe callback is
        // asynchronous — it queues to the event loop and returns immediately.
        // Rust cannot know when JS has finished copying, so the buffer must
        // outlive the callback. JS is responsible for the copy; the leaked
        // memory is bounded by concurrency × max_checkpoint_size.
        //
        // TODO: Use a pool of pre-allocated buffers with a channel to reclaim
        // them after JS signals completion, to avoid unbounded leaking.
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok((seq, compressed))) => {
                    let mut output_buf = vec![0u8; compressed.len() * 20]; // generous estimate
                    match decode_compressed_to_binary(&compressed, &mut output_buf) {
                        Ok(bytes_written) => {
                            let ptr = output_buf.as_ptr();
                            // Leak the buffer — JS copies via toArrayBuffer before GC
                            std::mem::forget(output_buf);
                            callback(seq, ptr, bytes_written as u32);
                        }
                        Err(err) => {
                            eprintln!("[rust-fetch] decode failed for checkpoint {seq}: {err}");
                        }
                    }
                }
                Ok(Err(err)) => {
                    eprintln!("[rust-fetch] fetch failed: {err}");
                }
                Err(err) => {
                    eprintln!("[rust-fetch] task panicked: {err}");
                }
            }

            // Refill: launch next fetch to maintain concurrency
            if next_seq <= to {
                spawn_fetch(&mut join_set, &client, &base_url, next_seq);
                next_seq += 1;
            }
        }

        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })?;

    Ok(())
}

/// Decompress zstd + decode checkpoint to binary output format.
fn decode_compressed_to_binary(
    compressed: &[u8],
    output: &mut [u8],
) -> Result<usize, Box<dyn std::error::Error>> {
    crate::decode_checkpoint_binary_inner(compressed, output)
}

/// Spawn an async fetch task for a single checkpoint.
fn spawn_fetch(
    join_set: &mut JoinSet<Result<(u64, Vec<u8>), String>>,
    client: &Client,
    base_url: &str,
    seq: u64,
) {
    let client = client.clone();
    let url = format!("{base_url}/{seq}.binpb.zst");

    join_set.spawn(async move {
        let response = client
            .get(&url)
            .send()
            .await
            .map_err(|err| format!("GET {url}: {err}"))?;

        if !response.status().is_success() {
            return Err(format!("GET {url}: HTTP {}", response.status()));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|err| format!("read {url}: {err}"))?;

        Ok((seq, bytes.to_vec()))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    static RESULT_COUNT: AtomicU32 = AtomicU32::new(0);

    #[test]
    fn test_download_and_decode_range() {
        RESULT_COUNT.store(0, Ordering::SeqCst);

        extern "C" fn test_callback(seq: u64, data_ptr: *const u8, data_len: u32) {
            if seq == RANGE_DONE_SEQ {
                return;
            }
            assert!(!data_ptr.is_null(), "data_ptr should not be null for seq {seq}");
            assert!(data_len > 40, "decoded binary should be >40 bytes for seq {seq}, got {data_len}");
            RESULT_COUNT.fetch_add(1, Ordering::SeqCst);
        }

        let result = run_download_and_decode(
            "https://checkpoints.mainnet.sui.io",
            260845974,
            260845978,
            5,
            test_callback,
        );

        assert!(result.is_ok(), "download_and_decode should succeed: {:?}", result.err());
        assert_eq!(RESULT_COUNT.load(Ordering::SeqCst), 5, "should decode exactly 5 checkpoints");
    }

    #[test]
    fn bench_download_and_decode_1000() {
        use std::time::Instant;

        RESULT_COUNT.store(0, Ordering::SeqCst);

        extern "C" fn bench_callback(seq: u64, _data_ptr: *const u8, _data_len: u32) {
            if seq == RANGE_DONE_SEQ { return; }
            RESULT_COUNT.fetch_add(1, Ordering::SeqCst);
        }

        let start = Instant::now();
        let result = run_download_and_decode(
            "https://checkpoints.mainnet.sui.io",
            260845974,
            260846973,  // 1000 checkpoints
            200,        // concurrency
            bench_callback,
        );
        let elapsed = start.elapsed().as_secs_f64();
        let count = RESULT_COUNT.load(Ordering::SeqCst);

        assert!(result.is_ok());
        eprintln!(
            "\n=== Rust fetch+decode: {} checkpoints in {:.2}s = {} cp/s ===\n",
            count, elapsed, (count as f64 / elapsed) as u32
        );
    }
}
