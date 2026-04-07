/**
 * Pipeline write buffer — batches ProcessedCheckpoints and writes to storages.
 *
 * Directly calls storage.write() on the main thread. No worker offload, no
 * serialization across thread boundaries, no silent error swallowing. The
 * worker/WriterChannel layer was removed when consolidating to the Mysten
 * BCS baseline — it will be reintroduced in a later branch as a measured
 * optimization.
 *
 * Patterns preserved from the old implementation: timer + threshold flush,
 * backpressure via awaited flush, p-retry on transient failures, cursor
 * coalescing, onFlush stats callback for metrics.
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
): PipelineWriteBuffer {
  const { label, intervalMs, maxBatchSize, retries = 3, onFlush, onBatchFlushed } = config;

  let buffer: ProcessedCheckpoint[] = [];
  let cursors = new Map<string, bigint>();
  let processedSeqs = new Set<bigint>();
  let flushing: Promise<void> | null = null;
  let timer: Timer | null = null;
  let stopped = false;

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

          // Write to all storages on the main thread. Errors propagate.
          if (storages.length > 0) {
            await Promise.all(storages.map(s => s.write(batch)));
          }

          // Record processed seqs (for gap detection)
          if (state && seqSnapshot.size > 0) {
            await state.recordProcessedCheckpoints([...seqSnapshot]);
          }

          // Advance cursor to highest seq from this batch
          if (state) {
            for (const [, seq] of cursorSnapshot) {
              await state.setCheckpointCursor(cursorKey, seq);
            }
          }

          const durationMs = performance.now() - start;

          let eventsWritten = 0;
          let balanceChangesWritten = 0;
          for (const cp of batch) {
            eventsWritten += cp.events.length;
            balanceChangesWritten += cp.balanceChanges.length;
          }

          log.debug(
            { events: eventsWritten, checkpoints: batch.length, durationMs: Math.round(durationMs), remaining: buffer.length, label },
            "flush complete",
          );

          onFlush?.({
            eventsWritten,
            balanceChangesWritten,
            checkpointsFlushed: batch.length,
            flushDurationMs: durationMs,
            bufferSizeAfter: buffer.length,
          });

          // Notify broadcasts (fire-and-forget)
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
      log.error({ err, label, checkpoints: batch.length }, "flush failed after retries");
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

      if (buffer.length >= maxBatchSize) {
        if (flushing) {
          log.debug({ bufferSize: buffer.length, label }, "backpressure — awaiting in-flight flush");
          await flushing;
        }
        const flushPromise = doFlush();
        flushing = flushPromise;
        try {
          await flushPromise;
        } finally {
          flushing = null;
        }
      }
    },

    async flush(): Promise<void> {
      await doFlush();
    },

    start(): void {
      if (timer) return;
      timer = setInterval(async () => {
        if (stopped) return;
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
      await doFlush(); // final flush — any errors here propagate to caller
    },
  };
}
