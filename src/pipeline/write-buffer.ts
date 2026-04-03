/**
 * Pipeline write buffer — batches ProcessedCheckpoints for storage writes.
 *
 * Same patterns as src/buffer.ts (timer + threshold flush, backpressure,
 * p-retry, cursor coalescing) but works directly with pipeline Storage[]
 * and ProcessedCheckpoint[] types.
 */
import pRetry from "p-retry";
import type { ProcessedCheckpoint, Storage } from "./types.ts";
import type { StateManager } from "../state.ts";
import type { Logger } from "../logger.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface PipelineFlushStats {
  eventsWritten: number;
  balanceChangesWritten: number;
  checkpointsFlushed: number;
  flushDurationMs: number;
  bufferSizeAfter: number;
}

export interface PipelineWriteBufferConfig {
  /** Label for logging (e.g. "live", "backfill") */
  label: string;
  /** Flush interval in milliseconds */
  intervalMs: number;
  /** Maximum events to buffer before triggering a flush */
  maxBatchSize: number;
  /** p-retry attempts on flush failure (default: 3) */
  retries?: number;
  /** Callback invoked after each successful flush */
  onFlush?: (stats: PipelineFlushStats) => void;
  /** Callback invoked with the flushed batch (for broadcasts) */
  onBatchFlushed?: (batch: ProcessedCheckpoint[]) => void;
}

export interface PipelineWriteBuffer {
  /** Enqueue a processed checkpoint. Applies backpressure when buffer is full. */
  push(processed: ProcessedCheckpoint): Promise<void>;
  /** Force a flush of the current buffer. */
  flush(): Promise<void>;
  /** Start the interval timer. Must be called once. */
  start(): void;
  /** Stop the interval timer and flush remaining data. */
  stop(): Promise<void>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

export function createPipelineWriteBuffer(
  storages: Storage[],
  state: StateManager | null,
  cursorKey: string,
  config: PipelineWriteBufferConfig,
  log: Logger,
  acquireWriteLock?: () => Promise<() => void>,
): PipelineWriteBuffer {
  const { label, intervalMs, maxBatchSize, retries = 3, onFlush, onBatchFlushed } = config;

  let buffer: ProcessedCheckpoint[] = [];
  let cursors = new Map<string, bigint>();
  let processedSeqs = new Set<bigint>();
  let flushing: Promise<void> | null = null;
  let timer: Timer | null = null;
  let stopped = false;

  function countEvents(): number {
    let total = 0;
    for (const cp of buffer) {
      total += cp.events.length + cp.balanceChanges.length;
    }
    return total;
  }

  async function doFlush(): Promise<void> {
    if (buffer.length === 0) return;

    // Snapshot and clear
    const batch = buffer;
    const cursorSnapshot = new Map(cursors);
    const seqSnapshot = new Set(processedSeqs);
    buffer = [];
    cursors = new Map();
    processedSeqs = new Set();

    log.debug({ checkpoints: batch.length, label }, "flushing");

    try {
      await pRetry(
        async () => {
          const start = performance.now();

          // Acquire write lock (prevents concurrent flushes from different sources)
          const release = acquireWriteLock ? await acquireWriteLock() : () => {};
          try {
            // Write to all storages
            if (storages.length > 0) {
              await Promise.all(storages.map(storage => storage.write(batch)));
            }
          } finally {
            release();
          }

          // Update cursors (only after all storage writes succeed)
          if (state) {
            for (const [key, seq] of cursorSnapshot) {
              await state.setCheckpointCursor(key, seq);
            }
            if (seqSnapshot.size > 0) {
              await state.recordProcessedCheckpoints([...seqSnapshot]);
            }
          }

          const durationMs = performance.now() - start;

          let eventsWritten = 0;
          let balanceChangesWritten = 0;
          for (const cp of batch) {
            eventsWritten += cp.events.length;
            balanceChangesWritten += cp.balanceChanges.length;
          }

          const stats: PipelineFlushStats = {
            eventsWritten,
            balanceChangesWritten,
            checkpointsFlushed: batch.length,
            flushDurationMs: durationMs,
            bufferSizeAfter: buffer.length,
          };

          log.debug(
            { events: eventsWritten, checkpoints: batch.length, durationMs: Math.round(durationMs), remaining: buffer.length, label },
            "flush complete",
          );

          onFlush?.(stats);

          // Notify broadcasts
          if (onBatchFlushed) {
            try { onBatchFlushed(batch); } catch {}
          }
        },
        { retries, minTimeout: 1000 },
      );
    } catch (err) {
      // Restore batch to front of buffer so data is not lost
      buffer = batch.concat(buffer);
      for (const [key, seq] of cursorSnapshot) {
        const prev = cursors.get(key);
        if (prev === undefined || seq > prev) cursors.set(key, seq);
      }
      for (const seq of seqSnapshot) {
        processedSeqs.add(seq);
      }
      throw err;
    }
  }

  return {
    async push(processed: ProcessedCheckpoint): Promise<void> {
      buffer.push(processed);

      const seq = processed.checkpoint.sequenceNumber;

      // Track processed seq for gap detection
      processedSeqs.add(seq);

      // Coalesce cursors: keep highest seq per key
      const prev = cursors.get(cursorKey);
      if (prev === undefined || seq > prev) {
        cursors.set(cursorKey, seq);
      }

      // Threshold-based flush with backpressure
      if (countEvents() >= maxBatchSize) {
        if (flushing) {
          log.debug({ bufferSize: countEvents(), label }, "backpressure — awaiting in-flight flush");
          await flushing;
        }
        await doFlush();
      }
    },

    async flush(): Promise<void> {
      await doFlush();
    },

    start(): void {
      if (timer) return;
      timer = setInterval(async () => {
        try {
          const flushPromise = doFlush();
          flushing = flushPromise;
          await flushPromise;
        } catch (err) {
          log.error({ err, label }, "periodic flush error");
        } finally {
          flushing = null;
        }
      }, intervalMs);
    },

    async stop(): Promise<void> {
      stopped = true;
      if (timer) {
        clearInterval(timer);
        timer = null;
      }
      if (flushing) await flushing;
      await doFlush(); // final flush
    },
  };
}
