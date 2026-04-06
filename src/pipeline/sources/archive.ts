/**
 * Archive checkpoint source — historical backfill via worker pool.
 *
 * Workers own fetch + decode (Rust binary FFI when available, JS fallback).
 * Main thread receives decoded results, optionally parses binary, yields checkpoints.
 */
import { checkpointFromGrpcResponse } from "../../checkpoint-response.ts";
import { parseBinaryCheckpoint } from "../../binary-parser.ts";
import { createCheckpointDecoderPool, type CheckpointDecoderPool } from "../../checkpoint-decoder-pool.ts";
import { createLogger } from "../../logger.ts";
import type { Logger } from "../../logger.ts";
import type { Source, Checkpoint } from "../types.ts";
import type { BalanceChange, ProcessedCheckpoint } from "../types.ts";

const textDecoder = new TextDecoder();

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
  };
  /** gRPC URL for fetching latest checkpoint */
  grpcUrl?: string;
  /** Skip ordered drain — yield checkpoints as they arrive from workers.
   *  Safe for snapshot mode where insert order doesn't matter. */
  unorderedDrain?: boolean;
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

function checkpointFromBinaryResult(
  binary: Uint8Array,
  enabledProcessors: ArchiveSourceConfig["enabledProcessors"],
  balanceCoinTypes: string[] | "*" | undefined,
): Checkpoint {
  const deferParsing = process.env.DEFER_BINARY_PARSING !== "0";
  if (deferParsing) {
    const view = new DataView(binary.buffer, binary.byteOffset, binary.byteLength);
    let pos = 40;

    const seqLen = view.getUint16(pos, true);
    pos += 2;
    const seqStr = textDecoder.decode(binary.subarray(pos, pos + seqLen));
    pos += seqLen;

    const epochLen = view.getUint16(pos, true);
    pos += 2;
    const epochStr = textDecoder.decode(binary.subarray(pos, pos + epochLen));
    pos += epochLen;

    const tsLen = view.getUint16(pos, true);
    pos += 2;
    const tsStr = textDecoder.decode(binary.subarray(pos, pos + tsLen));

    const checkpoint: Checkpoint = {
      sequenceNumber: BigInt(seqStr),
      timestamp: new Date(tsStr),
      transactions: [],
      source: "backfill",
      epoch: BigInt(epochStr),
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
    (checkpoint as any)._rawBinary = binary;
    (checkpoint as any)._enabledProcessors = enabledProcessors;
    (checkpoint as any)._balanceCoinTypes = balanceCoinTypes;
    return checkpoint;
  }

  const parsed = parseBinaryCheckpoint(binary);
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

        // ─── Phase 2: Decode from disk cache via worker pool ───────────
        // No network latency. Workers do Rust FFI decode.
        log.info({ workers: workerCount }, "phase 2: decoding checkpoints");
        const decodeStart = performance.now();

        // Workers own the decode loop — each gets a range of files.
        // No per-checkpoint postMessage from main thread.
        const resultStream = pool.decodeCachedRange(config.from, endSeq, cacheDir);

        for await (const result of resultStream) {
          if (stopped) break;
          const seq = result.seq;

          let checkpoint: Checkpoint;
          if (result.binary) {
            checkpoint = checkpointFromBinaryResult(
              result.binary,
              config.enabledProcessors,
              balanceCoinTypes,
            );
          } else if (result.payload) {
            checkpoint = checkpointFromGrpcResponse(
              result.payload.decoded,
              "backfill",
              result.payload.precomputedBalanceChanges,
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
