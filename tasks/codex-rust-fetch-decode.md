# Codex Task: Add `download_and_decode_archive_checkpoint_range` to Rust decoder

## Repo
`github.com/unconfirmedlabs/jun`, branch `baseline/mysten-bcs-full-decode`

## Crate
`native/rust-decoder/`

## Problem

The JS worker event loop blocks for ~3ms on each synchronous Rust FFI decode call. With 125 concurrent fetches per worker, this blocks the event loop for 375ms/s, throttling fetch throughput from 27K req/s (pure fetch ceiling) to 4K req/s (pipeline actual). The Rust FFI call prevents the JS event loop from processing incoming HTTP responses while it's decoding.

## Solution

New FFI entry point where Rust owns the full fetch + decode pipeline internally using tokio + reqwest. JS calls it once with a checkpoint range, Rust fetches and decodes all checkpoints concurrently, and calls back to JS with each result. The JS event loop is never blocked.

## New FFI entry point

```rust
/// Download, decompress, and decode a range of archive checkpoints.
///
/// Rust internally:
/// 1. Spawns a tokio runtime
/// 2. Fetches checkpoints concurrently via reqwest (HTTP GET to archive CDN)
/// 3. Decompresses zstd
/// 4. BCS-decodes via sui-types
/// 5. Writes binary output for each checkpoint
/// 6. Calls output_callback with each result
///
/// The callback is called from a Rust thread — Bun FFI callbacks run on
/// the JS event loop when control returns, so this effectively streams
/// results without blocking.
#[no_mangle]
pub extern "C" fn download_and_decode_archive_checkpoint_range(
    archive_url_ptr: *const u8,
    archive_url_len: u32,
    from_checkpoint: u64,
    to_checkpoint: u64,
    concurrency: u32,
    // Output buffer (caller-allocated, shared across all callbacks)
    output_ptr: *mut u8,
    output_capacity: u32,
    // Callback: called once per decoded checkpoint
    // Args: seq (checkpoint number), data_ptr (output bytes), data_len (bytes written)
    // The data_ptr points into the output buffer — JS must copy before returning
    output_callback: extern "C" fn(seq: u64, data_ptr: *const u8, data_len: u32),
) -> i32;  // Returns 0 on success, negative on error
```

## New Cargo dependencies

```toml
[dependencies]
# ... existing deps ...
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
reqwest = { version = "0.12", features = ["rustls-tls"], default-features = false }
```

Don't use native-tls — use rustls to avoid OpenSSL dependency. reqwest + rustls compiles cleanly on all platforms.

## Implementation outline

Create `src/fetch.rs`:

```rust
use std::sync::Arc;
use tokio::runtime::Runtime;
use reqwest::Client;

pub fn download_and_decode_range(
    archive_url: &str,
    from: u64,
    to: u64,
    concurrency: u32,
    output_buf: &mut [u8],
    callback: extern "C" fn(u64, *const u8, u32),
) -> Result<(), Box<dyn std::error::Error>> {
    let rt = Runtime::new()?;
    rt.block_on(async {
        let client = Client::builder()
            .pool_max_idle_per_host(concurrency as usize)
            .build()?;
        
        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency as usize));
        let mut handles = Vec::new();
        
        for seq in from..=to {
            let permit = semaphore.clone().acquire_owned().await?;
            let client = client.clone();
            let url = format!("{}/{}.binpb.zst", archive_url, seq);
            
            handles.push(tokio::spawn(async move {
                let resp = client.get(&url).send().await?;
                let bytes = resp.bytes().await?;
                drop(permit);
                Ok::<(u64, Vec<u8>), Box<dyn std::error::Error + Send + Sync>>((seq, bytes.to_vec()))
            }));
        }
        
        // Process results as they complete
        // For each result: decompress + decode + write to output_buf + callback
        for handle in handles {
            match handle.await? {
                Ok((seq, compressed)) => {
                    // Decompress
                    let mut decoder = zstd::Decoder::new(&compressed[..])?;
                    let mut decompressed = Vec::new();
                    std::io::Read::read_to_end(&mut decoder, &mut decompressed)?;
                    
                    // Decode to binary
                    let bytes_written = crate::extract::extract_checkpoint_binary(&decompressed, output_buf)?;
                    
                    // Callback to JS
                    callback(seq, output_buf.as_ptr(), bytes_written as u32);
                }
                Err(e) => {
                    eprintln!("[rust-fetch] checkpoint failed: {}", e);
                }
            }
        }
        
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}
```

Note: the above is a sketch. The actual implementation should:
- Use `tokio::sync::mpsc` or `futures::stream::FuturesUnordered` to process results as they complete (not wait for all handles)
- Handle the output buffer carefully (callback must be called sequentially since they share the buffer)
- Properly report errors without panicking

## Important design decisions

1. **Single tokio runtime per call** — created at FFI entry, shut down on return. Not persistent across calls.

2. **Callback from Rust thread** — the `output_callback` is called from the tokio runtime's thread. In Bun FFI, callbacks are `extern "C"` function pointers. Test whether Bun's `JSCallback` / `FFICallback` supports being called from Rust threads. If not, use the polling alternative below.

3. **Output buffer reuse** — the caller allocates one large buffer (e.g., 64MB). Rust writes each checkpoint's binary output into it, calls the callback with pointer + length, and JS copies out before the next callback. This avoids per-checkpoint allocation on the Rust side.

4. **Concurrency** — reqwest connection pool with `pool_max_idle_per_host` set to the concurrency level. `tokio::sync::Semaphore` limits concurrent in-flight requests.

5. **Error handling** — failed checkpoints are skipped (logged to stderr). The callback is not called for failed checkpoints. The return value indicates overall success/partial failure.

## Alternative if Bun FFI callbacks don't work from Rust threads

Instead of a callback, use a shared memory polling model:

```rust
#[no_mangle]
pub extern "C" fn download_and_decode_archive_checkpoint_range(
    archive_url_ptr: *const u8,
    archive_url_len: u32,
    from_checkpoint: u64,
    to_checkpoint: u64,
    concurrency: u32,
    // Ring buffer for results
    ring_ptr: *mut u8,
    ring_capacity: u32,
    // Atomic counters for producer/consumer coordination
    write_pos: *mut u64,  // Rust increments after writing a result
    read_pos: *mut u64,   // JS increments after consuming a result
) -> i32;
```

Each result in the ring buffer is: `u64 seq + u32 data_len + data_bytes`. JS polls `write_pos > read_pos` on each event loop tick to consume results.

**Test the callback pattern first** — if Bun supports it, it's much simpler.

## JS side integration

In `src/checkpoint-native-decoder.ts`, add:

```typescript
export function downloadAndDecodeRange(
    archiveUrl: string,
    from: bigint,
    to: bigint,
    concurrency: number,
    onCheckpoint: (seq: bigint, binary: Uint8Array) => void,
): void {
    const urlBuf = new TextEncoder().encode(archiveUrl);
    const outBuf = new Uint8Array(64 * 1024 * 1024);
    
    const callback = new JSCallback(
        (seq: number, dataPtr: number, dataLen: number) => {
            // Copy binary out of shared buffer before returning
            const binary = new Uint8Array(dataLen);
            // ... copy from dataPtr using toArrayBuffer or similar ...
            onCheckpoint(BigInt(seq), binary);
        },
        { args: ["u64", "ptr", "u32"], returns: "void" }
    );
    
    nativeDecoder.download_and_decode_archive_checkpoint_range(
        ptr(urlBuf), urlBuf.length,
        Number(from), Number(to),
        concurrency,
        ptr(outBuf), outBuf.length,
        callback.ptr,
    );
    
    callback.close();
}
```

## Archive source integration

In `src/pipeline/sources/archive.ts`, the archive source would use this instead of the worker pool when the native decoder is available:

```typescript
if (isNativeCheckpointDecoderAvailable() && hasDownloadAndDecodeRange()) {
    // Rust owns fetch + decode — no workers needed
    downloadAndDecodeRange(archiveUrl, from, to, concurrency, (seq, binary) => {
        // Buffer results and yield via async generator
    });
} else {
    // Fallback: JS worker pool (existing code)
}
```

## Verification

1. `cargo test` — existing tests must still pass
2. `cargo build --release`
3. FFI smoke test on a small range:
   ```bash
   cd /path/to/jun
   bun -e "
   import { downloadAndDecodeRange } from './src/checkpoint-native-decoder.ts';
   let count = 0;
   downloadAndDecodeRange('https://checkpoints.mainnet.sui.io', 260845974n, 260846073n, 100, (seq, binary) => {
       count++;
   });
   console.log('decoded:', count);  // expect: 100
   "
   ```
4. Full epoch benchmark (target >8000 cp/s sustained):
   ```bash
   bun run src/cli.ts pipeline snapshot --epoch 1086 --everything --stdout --workers 8 --concurrency 1000 --quiet --yes
   ```
   Current best: 4059 cp/s with 8 JS workers.

## What NOT to change

- `decode_checkpoint_binary` — existing per-checkpoint FFI stays for live gRPC path
- `decode_checkpoint` (JSON path) — stays for debugging
- Live gRPC source (`src/pipeline/sources/grpc.ts`) — unaffected
- Binary parser (`src/binary-parser.ts`) — same output format
- Existing Rust decode logic in `extract.rs` / `binary.rs` — reused internally

## Do NOT add tokio as a persistent runtime

Create and destroy the runtime per `download_and_decode_archive_checkpoint_range` call. This keeps the FFI boundary clean — one call, one range processed, no background threads leaking after the call returns.
