/**
 * Archive checkpoint source — historical backfill via worker pool.
 *
 * Workers own fetch + decode (Rust binary FFI when available, JS fallback).
 * Main thread receives decoded results, optionally parses binary, yields checkpoints.
 */
import { createCheckpointDecoderPool, type CheckpointDecoderPool } from "../../checkpoint-decoder-pool.ts";
import { isNativeCheckpointDecoderAvailable } from "../../checkpoint-native-decoder.ts";
import { createLogger } from "../../logger.ts";
import type { Logger } from "../../logger.ts";
import { emptyProcessed, type Source, type Checkpoint } from "../types.ts";
import type { BalanceChange, ProcessedCheckpoint } from "../types.ts";

export interface ArchiveSourceConfig {
  /** Archive base URL */
  archiveUrl: string;
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
  /** Snapshot-only optimization: workers own SQLite shard writes, main DB merges later. */
  sqliteShardingPath?: string;
  /** gRPC URL for fetching latest checkpoint */
  grpcUrl?: string;
  /** Skip ordered drain — yield checkpoints as they arrive from workers.
   *  Safe for snapshot mode where insert order doesn't matter. */
  unorderedDrain?: boolean;
  /** Extraction mask bitfield — controls which record types Rust decodes. 0x7FF = all. */
  extractMask?: number;
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

      decoderPool = createCheckpointDecoderPool(workerCount, {
        balanceCoinTypes,
      });
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
      let watermark = config.from - 1n;
      const endSeq = to;

      let totalProcessed = 0;

      try {
        const pool = decoderPool!;
        const totalCheckpoints = Number(endSeq - config.from + 1n);

        // ─── Phase 1: Fetch all compressed checkpoints to disk cache ────
        // Single connection pool on main thread = maximum CDN throughput.
        // Cache dir: ~/.jun/cache/checkpoints/ (or XDG_CACHE_HOME)
        const { mkdirSync, existsSync } = await import("fs");
        const { join } = await import("path");
        const { homedir } = await import("os");
        const cacheBase = process.env.XDG_CACHE_HOME ?? join(homedir(), ".jun", "cache");
        const cacheDir = join(cacheBase, "checkpoints");
        mkdirSync(cacheDir, { recursive: true });

        log.info({ concurrency: fetchConcurrency, total: totalCheckpoints, cacheDir }, "phase 1: fetching checkpoints");
        const fetchStart = performance.now();
        let fetchNext = config.from;
        let fetchDone = 0;
        let fetchSkipped = 0;

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
              }
            } catch {}
          }
        }

        await Promise.all(Array.from({ length: fetchConcurrency }, () => fetchWorker()));
        const fetchElapsed = (performance.now() - fetchStart) / 1000;
        log.info(
          { fetched: fetchDone, skipped: fetchSkipped, elapsed: `${fetchElapsed.toFixed(1)}s`, rate: Math.round(fetchDone / fetchElapsed) },
          "phase 1 complete",
        );

        const shardingRequested = !!config.sqliteShardingPath;
        const canUseSqliteSharding = shardingRequested && isNativeCheckpointDecoderAvailable();
        if (shardingRequested && !canUseSqliteSharding) {
          log.warn("sqlite sharding requested but native checkpoint decoder is unavailable; falling back to main-thread write path");
        }

        if (canUseSqliteSharding) {
          const { createHash } = await import("crypto");
          const { mkdirSync, rmSync } = await import("fs");
          const { homedir } = await import("os");
          const { join } = await import("path");

          const shardHash = createHash("sha256")
            .update(`${config.from}-${endSeq}-${config.sqliteShardingPath}`)
            .digest("hex")
            .slice(0, 12);
          const shardDir = join(process.env.XDG_CACHE_HOME ?? join(homedir(), ".jun"), "shards", shardHash);

          rmSync(shardDir, { recursive: true, force: true });
          mkdirSync(shardDir, { recursive: true });

          log.info({ workers: workerCount, shardDir }, "phase 2: decode + write SQLite shards");
          const shardStart = performance.now();
          const shardPaths = await pool.decodeAndWriteRange(
            config.from,
            endSeq,
            cacheDir,
            shardDir,
            {
              balances: !!config.enabledProcessors?.balances,
              transactions: !!config.enabledProcessors?.transactions,
              objectChanges: !!config.enabledProcessors?.objectChanges,
              dependencies: !!config.enabledProcessors?.dependencies,
              inputs: !!config.enabledProcessors?.inputs,
              commands: !!config.enabledProcessors?.commands,
              systemTransactions: !!config.enabledProcessors?.systemTransactions,
              unchangedConsensusObjects: !!config.enabledProcessors?.unchangedConsensusObjects,
              events: !!config.enabledProcessors?.events,
              checkpoints: !!config.enabledProcessors?.checkpoints,
            },
          );

          const shardElapsed = (performance.now() - shardStart) / 1000;
          log.info(
            { workers: workerCount, elapsed: `${shardElapsed.toFixed(1)}s`, shardCount: shardPaths.length },
            "phase 2 complete",
          );

          if (stopped) {
            return;
          }

          const markerCheckpoint: Checkpoint = {
            sequenceNumber: endSeq,
            timestamp: new Date(0),
            transactions: [],
            source: "backfill",
            epoch: 0n,
            digest: "",
            previousDigest: null,
            contentDigest: null,
            totalNetworkTransactions: 0n,
            epochRollingGasCostSummary: {
              computationCost: "0",
              storageCost: "0",
              storageRebate: "0",
              nonRefundableStorageFee: "0",
            },
          };
          const markerProcessed = emptyProcessed(markerCheckpoint);
          (markerProcessed as any)._shardPaths = shardPaths;
          (markerProcessed as any)._shardSessionDir = shardDir;
          (markerCheckpoint as Checkpoint & { _preProcessed?: ProcessedCheckpoint })._preProcessed = markerProcessed;
          yield markerCheckpoint;
          return;
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
