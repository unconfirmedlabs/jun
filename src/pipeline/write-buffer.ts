/**
 * Pipeline write buffer — batches ProcessedCheckpoints and sends to WriterChannels.
 *
 * Same patterns as before (timer + threshold flush, backpressure, p-retry,
 * cursor coalescing) but flushes to WriterChannel[] instead of Storage[].
 *
 * Cursor updates and broadcast notifications happen in the onAck callback
 * so they reflect actual writes, not just enqueue.
 */
import pRetry from "p-retry";
import type { ProcessedCheckpoint } from "./types.ts";
import type { WriterChannel, WriteAck } from "./writer.ts";
import { serializeBatch } from "./writer.ts";
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
  /** Callback invoked after each successful flush (fired on ack from writer) */
  onFlush?: (stats: PipelineFlushStats) => void;
  /** Callback invoked with the flushed batch (for broadcasts, fired on ack) */
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
  channels: WriterChannel[],
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

  // Register ack callbacks on all channels
  for (const channel of channels) {
    channel.onAck(async (ack: WriteAck) => {
      // Update cursors from ack
      if (state) {
        const cursor = BigInt(ack.cursor);
        await state.setCheckpointCursor(cursorKey, cursor);
      }

      // Fire stats callback
      onFlush?.({
        eventsWritten: ack.eventsWritten,
        balanceChangesWritten: ack.balanceChangesWritten,
        checkpointsFlushed: 1, // per-batch ack
        flushDurationMs: ack.flushDurationMs,
        bufferSizeAfter: buffer.length,
      });
    });
  }

  function countEvents(): number {
    let total = 0;
    for (const cp of buffer) {
      total += cp.events.length + cp.balanceChanges.length + cp.transactions.length + cp.moveCalls.length;
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

          // Serialize once, send to all channels
          const serialized = serializeBatch(batch);

          // Send each serialized checkpoint to all channels
          for (const sb of serialized) {
            if (channels.length > 0) {
              await Promise.all(channels.map(channel => channel.send(sb)));
            }
          }

          // Record processed seqs (for gap detection) — done eagerly since
          // the channel will handle actual write ordering
          if (state && seqSnapshot.size > 0) {
            await state.recordProcessedCheckpoints([...seqSnapshot]);
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

          // Notify broadcasts (fire-and-forget, not tied to ack)
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
