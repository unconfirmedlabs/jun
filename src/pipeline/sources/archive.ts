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

    const checkpoint: Checkpoint = {
      sequenceNumber: BigInt(seqStr),
      timestamp: new Date(),
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

      let parseTime = 0;
      let totalProcessed = 0;
      const profilingStart = performance.now();

      const emitCheckpoint = async function* (
        seq: bigint,
        checkpoint: Checkpoint,
      ): AsyncGenerator<Checkpoint> {
        if (config.unorderedDrain) {
          yield checkpoint;
          return;
        }

        pending.set(seq, checkpoint);
        while (pending.has(watermark + 1n)) {
          watermark += 1n;
          const ready = pending.get(watermark)!;
          pending.delete(watermark);
          yield ready;
        }
      };

      try {
        const pool = decoderPool!;
        const totalCheckpoints = Number(endSeq - config.from + 1n);

        // ─── Phase 1: Fetch all compressed checkpoints to disk ─────────
        // Single connection pool on main thread, maximum CDN throughput.
        // Writes to a temp directory to avoid memory pressure on small machines.
        const { mkdirSync, readFileSync, unlinkSync, rmdirSync } = await import("fs");
        const { join } = await import("path");
        const tmpDir = join(import.meta.dir, "..", "..", ".jun-fetch-cache");
        mkdirSync(tmpDir, { recursive: true });

        log.info({ concurrency: fetchConcurrency, total: totalCheckpoints }, "phase 1: fetching checkpoints to disk");
        const fetchStart = performance.now();
        let fetchNext = config.from;
        let fetchDone = 0;

        async function fetchWorker() {
          while (fetchNext <= endSeq && !stopped) {
            const seq = fetchNext++;
            try {
              const url = `${config.archiveUrl}/${seq}.binpb.zst`;
              const resp = await fetch(url);
              if (resp.ok) {
                await Bun.write(join(tmpDir, `${seq}.zst`), await resp.arrayBuffer());
                fetchDone++;
                if (fetchDone % 10000 === 0) {
                  const elapsed = (performance.now() - fetchStart) / 1000;
                  console.error(`[fetch] ${fetchDone}/${totalCheckpoints} (${Math.round(fetchDone / elapsed)} req/s)`);
                }
              }
            } catch {}
          }
        }

        await Promise.all(Array.from({ length: fetchConcurrency }, () => fetchWorker()));
        const fetchElapsed = (performance.now() - fetchStart) / 1000;
        log.info(
          { fetched: fetchDone, elapsed: `${fetchElapsed.toFixed(1)}s`, rate: Math.round(fetchDone / fetchElapsed) },
          "phase 1 complete",
        );

        // ─── Phase 2: Decode from disk via worker pool ─────────────────
        // No network. Workers do Rust FFI decode from cached compressed bytes.
        log.info({ workers: workerCount }, "phase 2: decoding checkpoints");
        const decodeStart = performance.now();

        // Drain signaling
        let drainResolve: (() => void) | null = null;
        function signalDrain() {
          if (drainResolve) {
            const resolve = drainResolve;
            drainResolve = null;
            resolve();
          }
        }
        function waitForDrain(): Promise<void> {
          return new Promise(resolve => { drainResolve = resolve; });
        }

        let allDecodesDone = false;
        const decodePromises: Promise<void>[] = [];

        for (let seq = config.from; seq <= endSeq && !stopped; seq++) {
          const filePath = join(tmpDir, `${seq}.zst`);
          let compressed: Uint8Array;
          try {
            compressed = new Uint8Array(readFileSync(filePath));
          } catch {
            continue; // Skip failed fetches
          }

          const p = pool.decode(seq, compressed).then(result => {
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
              return;
            }
            pending.set(seq, checkpoint);
            totalProcessed++;
            signalDrain();

            // Clean up cached file
            try { unlinkSync(filePath); } catch {}
          });
          decodePromises.push(p);
        }

        Promise.all(decodePromises).then(() => {
          allDecodesDone = true;
          signalDrain();
        });

        // Drain results as they arrive
        while (!stopped) {
          if (config.unorderedDrain) {
            let yielded = false;
            for (const [seq, cp] of pending) {
              pending.delete(seq);
              yield cp;
              yielded = true;
            }
            if (!yielded && allDecodesDone) break;
            if (!yielded) await waitForDrain();
          } else {
            while (pending.has(watermark + 1n)) {
              watermark += 1n;
              yield pending.get(watermark)!;
              pending.delete(watermark);
            }
            if (allDecodesDone && !pending.has(watermark + 1n)) break;
            await waitForDrain();
          }

          if (totalProcessed > 0 && totalProcessed % 5000 === 0) {
            const elapsed = (performance.now() - decodeStart) / 1000;
            const rate = Math.round(totalProcessed / Math.max(elapsed, 0.001));
            console.error(`[decode] ${totalProcessed}/${totalCheckpoints} (${rate} cp/s)`);
          }
        }

        await Promise.all(decodePromises);
        const decodeElapsed = (performance.now() - decodeStart) / 1000;
        log.info(
          { decoded: totalProcessed, elapsed: `${decodeElapsed.toFixed(1)}s`, rate: Math.round(totalProcessed / decodeElapsed) },
          "phase 2 complete",
        );

        // Cleanup temp dir
        try { rmdirSync(tmpDir, { recursive: true }); } catch {}
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
