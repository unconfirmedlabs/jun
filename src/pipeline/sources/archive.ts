/**
 * Archive checkpoint source — historical backfill via worker pool.
 *
 * Workers own fetch + decode (Rust binary FFI when available, JS fallback).
 * Main thread receives decoded results, optionally parses binary, yields checkpoints.
 */
import { createCheckpointDecoderPool, type CheckpointDecoderPool, type WorkerWriteConfig } from "../../checkpoint-decoder-pool.ts";
import { createLogger } from "../../logger.ts";
import type { Logger } from "../../logger.ts";
import { type Source, type Checkpoint } from "../types.ts";
import type { BalanceChange, ProcessedCheckpoint } from "../types.ts";

export interface ArchiveSourceConfig {
  /** Archive base URL */
  archiveUrl: string;
  /** Network name for cache directory scoping (default: "mainnet") */
  network?: string;
  /** Starting checkpoint sequence */
  from: bigint;
  /** Optional: ending checkpoint (default: fetch latest from gRPC) */
  to?: bigint;
  /** Concurrent HTTP fetches (default: 50) */
  concurrency?: number;
  /** Number of decoder worker threads (default: auto) */
  workers?: number;
  /** Coin types for archive balance computation (null = disabled) */
  balanceCoinTypes?: string[] | "*";
  /** Enabled processor set used to filter worker-preprocessed rows. */
  enabledProcessors?: {
    balances: boolean;
    transactions: boolean;
    objectChanges: boolean;
    dependencies: boolean;
    inputs: boolean;
    commands: boolean;
    systemTransactions: boolean;
    unchangedConsensusObjects: boolean;
    events: boolean;
    checkpoints: boolean;
  };
  /** gRPC URL for fetching latest checkpoint */
  grpcUrl?: string;
  /** Skip ordered drain — yield checkpoints as they arrive from workers.
   *  Safe for snapshot mode where insert order doesn't matter. */
  unorderedDrain?: boolean;
  /** Extraction mask bitfield — controls which record types Rust decodes. 0x7FF = all. */
  extractMask?: number;
  /** Pass for worker-level ClickHouse writes (replay only). Still add createReplayClickHouseStorage
   *  to pipeline for table init and shutdown lifecycle. */
  clickhouseWriteConfig?: {
    url?: string;
    database?: string;
    username?: string;
    password?: string;
  };
  /** Pass for worker-level Postgres writes (replay only). */
  postgresWriteConfig?: { url: string };
}

function filterBalanceChanges(
  changes: BalanceChange[],
  balanceCoinTypes: string[] | "*" | undefined,
): BalanceChange[] {
  if (balanceCoinTypes === undefined || balanceCoinTypes === "*") return changes;
  const allowed = new Set(balanceCoinTypes);
  return changes.filter(change => allowed.has(change.coinType));
}

function filterPreProcessedCheckpoint(
  processed: ProcessedCheckpoint,
  enabled: ArchiveSourceConfig["enabledProcessors"],
  balanceCoinTypes: string[] | "*" | undefined,
): ProcessedCheckpoint {
  return {
    ...processed,
    events: enabled?.events ? processed.events : [],
    balanceChanges: enabled?.balances
      ? filterBalanceChanges(processed.balanceChanges, balanceCoinTypes)
      : [],
    transactions: enabled?.transactions ? processed.transactions : [],
    moveCalls: enabled?.transactions ? processed.moveCalls : [],
    objectChanges: enabled?.objectChanges ? processed.objectChanges : [],
    dependencies: enabled?.dependencies ? processed.dependencies : [],
    inputs: enabled?.inputs ? processed.inputs : [],
    commands: enabled?.commands ? processed.commands : [],
    systemTransactions: enabled?.systemTransactions ? processed.systemTransactions : [],
    unchangedConsensusObjects: enabled?.unchangedConsensusObjects
      ? processed.unchangedConsensusObjects
      : [],
  };
}

function checkpointFromParsed(
  parsed: { checkpoint: Checkpoint; processed: ProcessedCheckpoint },
  enabledProcessors: ArchiveSourceConfig["enabledProcessors"],
  balanceCoinTypes: string[] | "*" | undefined,
): Checkpoint {
  const processed = filterPreProcessedCheckpoint(
    parsed.processed,
    enabledProcessors,
    balanceCoinTypes,
  );
  const checkpoint = parsed.checkpoint;
  (checkpoint as Checkpoint & { _preProcessed?: ProcessedCheckpoint })._preProcessed = processed;
  return checkpoint;
}

export function createArchiveSource(config: ArchiveSourceConfig): Source {
  const log: Logger = createLogger().child({ component: "source:archive" });
  let stopped = false;
  let decoderPool: CheckpointDecoderPool | null = null;

  return {
    name: "backfill",

    async *stream(): AsyncIterable<Checkpoint> {
      const fetchConcurrency = config.concurrency ?? 50;
      const workerCount = config.workers ?? Math.max(1, Math.min(4, (navigator.hardwareConcurrency ?? 4) - 1));
      const balanceCoinTypes = config.balanceCoinTypes;

      decoderPool = createCheckpointDecoderPool(workerCount);
      log.info(
        { workers: workerCount, fetchConcurrency, from: config.from.toString() },
        "archive source starting",
      );

      // Determine upper bound
      let to = config.to;
      if (!to && config.grpcUrl) {
        const { createGrpcClient } = await import("../../grpc.ts");
        const client = createGrpcClient({ url: config.grpcUrl });
        const stream = client.subscribeCheckpoints();
        for await (const response of stream) {
          to = BigInt(response.cursor);
          break;
        }
        client.close();
      }

      if (!to) {
        throw new Error("Archive source requires either 'to' checkpoint or 'grpcUrl' to determine latest");
      }

      log.info({ from: config.from.toString(), to: to.toString(), total: (to - config.from + 1n).toString() }, "backfill range");
      const pending = new Map<bigint, Checkpoint>();
      // Hard cap: prevents OOM if workers stall or are severely unbalanced.
      // Use --unordered for large backfills where insert order doesn't matter.
      const MAX_PENDING = 100_000;
      let watermark = config.from - 1n;
      const endSeq = to;

      let totalProcessed = 0;

      try {
        const pool = decoderPool!;
        const totalCheckpoints = Number(endSeq - config.from + 1n);

        // ─── Phase 1: Fetch all compressed checkpoints to disk cache ────
        // Single connection pool on main thread = maximum CDN throughput.
        // Cache dir: ~/.jun/cache/checkpoints/{network}/ (or XDG_CACHE_HOME)
        const { mkdirSync, existsSync } = await import("fs");
        const { join } = await import("path");
        const { homedir } = await import("os");
        const cacheBase = process.env.XDG_CACHE_HOME ?? join(homedir(), ".jun", "cache");
        const network = config.network ?? "mainnet";
        const cacheDir = join(cacheBase, "checkpoints", network);
        mkdirSync(cacheDir, { recursive: true });

        log.info({ concurrency: fetchConcurrency, total: totalCheckpoints, cacheDir }, "phase 1: fetching checkpoints");
        const fetchStart = performance.now();
        let fetchNext = config.from;
        let fetchDone = 0;
        let fetchSkipped = 0;
        let fetchFailed = 0;

        async function fetchWorker() {
          while (fetchNext <= endSeq && !stopped) {
            const seq = fetchNext++;
            const cachePath = join(cacheDir, `${seq}.binpb.zst`);
            // Skip if already cached (stat syscall, O(1) even with millions of files)
            if (existsSync(cachePath)) {
              fetchSkipped++;
              fetchDone++;
              continue;
            }
            try {
              const resp = await fetch(`${config.archiveUrl}/${seq}.binpb.zst`);
              if (resp.ok) {
                await Bun.write(cachePath, await resp.arrayBuffer());
                fetchDone++;
                if (fetchDone % 10000 === 0) {
                  const elapsed = (performance.now() - fetchStart) / 1000;
                  console.error(`[fetch] ${fetchDone}/${totalCheckpoints} (${Math.round(fetchDone / elapsed)} req/s, ${fetchSkipped} cached)`);
                }
              } else {
                fetchFailed++;
                log.warn({ seq: seq.toString(), status: resp.status }, "archive fetch failed — checkpoint will be missing");
              }
            } catch (err) {
              fetchFailed++;
              log.warn({ seq: seq.toString(), err: String(err) }, "archive fetch error — checkpoint will be missing");
            }
          }
        }

        await Promise.all(Array.from({ length: fetchConcurrency }, () => fetchWorker()));
        const fetchElapsed = (performance.now() - fetchStart) / 1000;
        log.info(
          { fetched: fetchDone, skipped: fetchSkipped, failed: fetchFailed, elapsed: `${fetchElapsed.toFixed(1)}s`, rate: Math.round(fetchDone / fetchElapsed) },
          "phase 1 complete",
        );
        if (fetchFailed > 0) {
          log.warn({ fetchFailed }, "phase 1 completed with fetch failures — run will have checkpoint gaps");
        }

        // ─── Phase 2: Decode + write (worker write path) ──────────────
        // For ClickHouse and Postgres, workers decode AND write directly.
        // Main thread only tracks progress, yields nothing to pipeline.
        const workerWriteConfig: WorkerWriteConfig | null = config.clickhouseWriteConfig
          ? {
              backend: "clickhouse",
              clickhouseUrl: config.clickhouseWriteConfig.url,
              clickhouseDatabase: config.clickhouseWriteConfig.database,
              clickhouseUsername: config.clickhouseWriteConfig.username,
              clickhousePassword: config.clickhouseWriteConfig.password,
              batchSize: 2000,
              balanceCoinTypes: Array.isArray(balanceCoinTypes) ? balanceCoinTypes : null,
            }
          : config.postgresWriteConfig
          ? {
              backend: "postgres",
              postgresUrl: config.postgresWriteConfig.url,
              batchSize: 1000,
              balanceCoinTypes: Array.isArray(balanceCoinTypes) ? balanceCoinTypes : null,
            }
          : null;

        if (workerWriteConfig) {
          log.info({ workers: workerCount }, "phase 2: decode + write (workers)");
          const writeStart = performance.now();
          let totalWritten = 0;
          await pool.decodeAndWriteCachedRange(
            config.from,
            endSeq,
            cacheDir,
            config.extractMask,
            workerWriteConfig,
            (decoded) => {
              totalWritten = decoded;
              const elapsed = (performance.now() - writeStart) / 1000;
              const rate = Math.round(decoded / (elapsed || 1));
              process.stderr.write(`\r[backfill] ${decoded.toLocaleString()} written | ${rate}/s  `);
            },
          );
          const elapsed = (performance.now() - writeStart) / 1000;
          process.stderr.write(`\r[backfill] ${totalWritten.toLocaleString()} written | ${Math.round(totalWritten / (elapsed || 1))}/s | ${elapsed.toFixed(1)}s\n`);
          log.info({ written: totalWritten, elapsed: `${elapsed.toFixed(1)}s` }, "phase 2 complete (worker writes)");
          return; // workers wrote directly — yield nothing to pipeline
        }

        // ─── Phase 2: Decode from disk cache via worker pool ───────────
        // No network latency. Workers do Rust FFI decode.
        log.info({ workers: workerCount }, "phase 2: decoding checkpoints");
        const decodeStart = performance.now();

        // Workers own the decode loop — each gets a range of files.
        // No per-checkpoint postMessage from main thread.
        const resultStream = pool.decodeCachedRange(config.from, endSeq, cacheDir, config.extractMask);

        for await (const result of resultStream) {
          if (stopped) break;
          const seq = result.seq;

          let checkpoint: Checkpoint;
          if (result.parsed) {
            checkpoint = checkpointFromParsed(
              result.parsed,
              config.enabledProcessors,
              balanceCoinTypes,
            );
          } else {
            continue;
          }

          totalProcessed++;

          if (config.unorderedDrain) {
            yield checkpoint;
          } else {
            pending.set(seq, checkpoint);
            if (pending.size > MAX_PENDING) {
              throw new Error(
                `Pending checkpoint buffer exceeded ${MAX_PENDING} entries (watermark: ${watermark}, size: ${pending.size}). ` +
                `Workers may be stalled or severely unbalanced. Use unorderedDrain for large backfills.`,
              );
            }
            while (pending.has(watermark + 1n)) {
              watermark += 1n;
              const ready = pending.get(watermark)!;
              pending.delete(watermark);
              yield ready;
            }
          }
        }

        const decodeElapsed = (performance.now() - decodeStart) / 1000;
        log.info(
          { decoded: totalProcessed, elapsed: `${decodeElapsed.toFixed(1)}s`, rate: Math.round(totalProcessed / decodeElapsed) },
          "phase 2 complete",
        );
      } finally {
        if (decoderPool) {
          decoderPool.shutdown();
          decoderPool = null;
        }
      }

      if (!stopped && watermark < endSeq) {
        log.warn(
          {
            nextExpected: (watermark + 1n).toString(),
            end: endSeq.toString(),
            pending: pending.size,
          },
          "archive stream ended before yielding the full requested range",
        );
      }
    },

    async stop(): Promise<void> {
      stopped = true;
      if (decoderPool) {
        decoderPool.shutdown();
        decoderPool = null;
      }
    },
  };
}
