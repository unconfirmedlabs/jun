#!/usr/bin/env bun
/**
 * Multi-worker checkpoint prefetcher.
 *
 * Splits a checkpoint range across W Bun worker threads, each running C
 * concurrent fetches — W separate event loops instead of one overloaded loop.
 *
 * Usage:
 *   bun scripts/prefetch.mjs <from> <to> [workers] [concurrency-per-worker]
 *
 * Examples:
 *   bun scripts/prefetch.mjs 261512560 261843572          # epoch 1088
 *   bun scripts/prefetch.mjs 261512560 261843572 8 500    # explicit config
 *
 * Env:
 *   JUN_ARCHIVE_URL   archive base URL (default: https://checkpoints.mainnet.sui.io)
 *   JUN_NETWORK       cache sub-directory (default: mainnet)
 */
import { mkdirSync } from "fs";
import { join } from "path";
import { homedir } from "os";
import { fileURLToPath } from "url";

const [, , fromArg, toArg, workersArg, concArg] = process.argv;

if (!fromArg || !toArg) {
  console.error("usage: bun scripts/prefetch.mjs <from> <to> [workers] [concurrency-per-worker]");
  process.exit(1);
}

const FROM = BigInt(fromArg);
const TO = BigInt(toArg);
const WORKER_COUNT = parseInt(workersArg ?? "8");
const CONCURRENCY_PER_WORKER = parseInt(concArg ?? "500");
const ARCHIVE = process.env.JUN_ARCHIVE_URL ?? "https://checkpoints.mainnet.sui.io";
const NETWORK = process.env.JUN_NETWORK ?? "mainnet";

const cacheDir = join(
  process.env.XDG_CACHE_HOME ?? join(homedir(), ".jun", "cache"),
  "checkpoints",
  NETWORK,
);
mkdirSync(cacheDir, { recursive: true });

const total = Number(TO - FROM + 1n);
const chunkSize = Math.ceil(total / WORKER_COUNT);
const workerUrl = join(fileURLToPath(import.meta.url), "..", "prefetch-worker.mjs");

console.error(
  `[prefetch] ${total.toLocaleString()} checkpoints  workers=${WORKER_COUNT}  concurrency=${CONCURRENCY_PER_WORKER}  total_concurrent=${WORKER_COUNT * CONCURRENCY_PER_WORKER}`,
);

let totalFetched = 0;
let totalSkipped = 0;
const start = Date.now();

const interval = setInterval(() => {
  const elapsed = (Date.now() - start) / 1000;
  const done = totalFetched + totalSkipped;
  const rate = Math.round(totalFetched / Math.max(elapsed, 0.001));
  const eta = rate > 0 ? Math.round((total - done) / rate) : "?";
  const mbit = Math.round(rate * 26 * 8 / 1000); // ~26 KB/checkpoint
  process.stderr.write(
    `[prefetch] ${done.toLocaleString()}/${total.toLocaleString()}  rate=${rate.toLocaleString()} req/s  ~${mbit} Mbit/s  ETA=${eta}s\n`,
  );
}, 5000);

const workerPromises = Array.from({ length: WORKER_COUNT }, (_, i) => {
  const workerFrom = FROM + BigInt(i * chunkSize);
  const workerTo = workerFrom + BigInt(chunkSize) - 1n < TO
    ? workerFrom + BigInt(chunkSize) - 1n
    : TO;

  if (workerFrom > TO) return Promise.resolve();

  return new Promise((resolve) => {
    const worker = new Worker(workerUrl, {
      workerData: {
        from: workerFrom.toString(),
        to: workerTo.toString(),
        cacheDir,
        archive: ARCHIVE,
        concurrency: CONCURRENCY_PER_WORKER,
      },
    });

    worker.postMessage({
      from: workerFrom.toString(),
      to: workerTo.toString(),
      cacheDir,
      archive: ARCHIVE,
      concurrency: CONCURRENCY_PER_WORKER,
    });

    worker.onmessage = (e) => {
      // Intermediate progress update
      if (e.data.progress) {
        totalFetched += e.data.fetched - (worker._lastFetched ?? 0);
        totalSkipped += e.data.skipped - (worker._lastSkipped ?? 0);
        worker._lastFetched = e.data.fetched;
        worker._lastSkipped = e.data.skipped;
        return;
      }
      // Final result
      totalFetched += e.data.fetched - (worker._lastFetched ?? 0);
      totalSkipped += e.data.skipped - (worker._lastSkipped ?? 0);
      worker.terminate();
      resolve(undefined);
    };

    worker.onerror = (err) => {
      console.error(`[prefetch] worker ${i} error:`, err.message);
      worker.terminate();
      resolve(undefined);
    };
  });
});

await Promise.all(workerPromises);
clearInterval(interval);

const elapsed = ((Date.now() - start) / 1000).toFixed(1);
const rate = Math.round(totalFetched / parseFloat(elapsed));
console.error(
  `[prefetch] done  fetched=${totalFetched.toLocaleString()}  cached=${totalSkipped.toLocaleString()}  time=${elapsed}s  rate=${rate.toLocaleString()} req/s`,
);
