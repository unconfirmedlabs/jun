/**
 * WriteBuffer: batched event writer for Postgres.
 *
 * Accumulates decoded events and flushes to Postgres on a timer interval
 * or when a size threshold is reached. Supports backpressure — push() awaits
 * if a flush is in-flight and the buffer exceeds the threshold.
 *
 * Cursors are coalesced: multiple checkpoint sequences for the same key
 * collapse into a single cursor update per flush (highest seq wins).
 */
import pRetry from "p-retry";
import type { Logger } from "./logger.ts";
import type { DecodedEvent } from "./processor.ts";
import type { BalanceChange } from "./balance-processor.ts";
import type { StorageBackend } from "./output/storage.ts";
import type { BalanceWriter } from "./output/balance-writer.ts";
import type { StateManager } from "./state.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Stats emitted after each successful flush */
export interface FlushStats {
  eventsWritten: number;
  tablesWritten: number;
  flushDurationMs: number;
  cursorKeys: Map<string, bigint>;
  bufferSizeAfter: number;
}

/** Configuration for a WriteBuffer instance */
export interface WriteBufferConfig {
  /** Label for logging (e.g. "live", "backfill") */
  label: string;
  /** Flush interval in milliseconds */
  intervalMs: number;
  /** Maximum events to buffer before triggering a flush */
  maxEvents: number;
  /** p-retry attempts on flush failure (default: 3) */
  retries?: number;
  /** Callback invoked after each successful flush */
  onFlush?: (stats: FlushStats) => void;
  /** Callback invoked with decoded events after successful Postgres write. Used for SSE streaming. */
  onEvents?: (events: DecodedEvent[]) => void;
  /** Callback invoked with balance changes after successful Postgres write. */
  onBalanceChanges?: (changes: BalanceChange[]) => void;
}

export interface WriteBuffer {
  /** Enqueue events with associated cursor info. Applies backpressure when buffer is full. */
  push(events: DecodedEvent[], cursorKey: string, seq: bigint): Promise<void>;
  /** Enqueue balance changes. Flushed alongside events in the same flush cycle. */
  pushBalanceChanges(changes: BalanceChange[]): void;
  /** Force a flush of the current buffer. */
  flush(): Promise<void>;
  /** Start the interval timer. Must be called once. */
  start(): void;
  /** Stop the interval timer and flush remaining events. */
  stop(): Promise<void>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

export function createWriteBuffer(
  output: StorageBackend,
  state: StateManager,
  config: WriteBufferConfig,
  log: Logger,
  balanceWriter?: BalanceWriter | null,
): WriteBuffer {
  const { label, intervalMs, maxEvents, retries = 3, onFlush, onEvents, onBalanceChanges } = config;

  let buffer: DecodedEvent[] = [];
  let balanceBuffer: BalanceChange[] = [];
  let cursors = new Map<string, bigint>();
  let processedSeqs = new Set<bigint>();
  let flushing: Promise<void> | null = null;
  let timer: Timer | null = null;
  let stopped = false;

  /** Execute a flush: snapshot buffer, write to Postgres, update cursors. */
  async function doFlush(): Promise<void> {
    if (buffer.length === 0 && balanceBuffer.length === 0 && cursors.size === 0 && processedSeqs.size === 0) return;

    // Snapshot and clear
    const batch = buffer;
    const balanceBatch = balanceBuffer;
    const cursorSnapshot = new Map(cursors);
    const seqSnapshot = new Set(processedSeqs);
    buffer = [];
    balanceBuffer = [];
    cursors = new Map();
    processedSeqs = new Set();

    log.debug({ events: batch.length, balances: balanceBatch.length, checkpoints: seqSnapshot.size }, "flushing");

    const flushPromise = (async () => {
      try {
        await pRetry(
          async () => {
            const start = performance.now();

            // Group events by handler
            const grouped = new Map<string, DecodedEvent[]>();
            for (const event of batch) {
              const list = grouped.get(event.handlerName);
              if (list) {
                list.push(event);
              } else {
                grouped.set(event.handlerName, [event]);
              }
            }

            // Parallel inserts across event tables
            if (grouped.size > 0) {
              await Promise.all(
                Array.from(grouped.entries()).map(([handlerName, events]) =>
                  output.writeHandler(handlerName, events),
                ),
              );
            }

            // Write balance changes (ledger + running totals)
            if (balanceWriter && balanceBatch.length > 0) {
              await balanceWriter.write(balanceBatch);
            }

            // Update cursors (only after all inserts succeed)
            for (const [key, seq] of cursorSnapshot) {
              await state.setCheckpointCursor(key, seq);
            }

            // Record processed checkpoints for gap detection
            if (seqSnapshot.size > 0) {
              await state.recordProcessedCheckpoints([...seqSnapshot]);
            }

            const durationMs = performance.now() - start;

            const stats: FlushStats = {
              eventsWritten: batch.length,
              tablesWritten: grouped.size,
              flushDurationMs: durationMs,
              cursorKeys: cursorSnapshot,
              bufferSizeAfter: buffer.length,
            };

            log.debug(
              { events: stats.eventsWritten, tables: stats.tablesWritten, durationMs: Math.round(durationMs), remaining: stats.bufferSizeAfter },
              "flush complete",
            );

            onFlush?.(stats);

            // Broadcast events after successful Postgres write (for SSE streaming)
            if (onEvents && batch.length > 0) {
              try {
                onEvents(batch);
              } catch (err) {
                log.error({ err }, "onEvents callback failed");
              }
            }

            // Notify balance change listeners
            if (onBalanceChanges && balanceBatch.length > 0) {
              try {
                onBalanceChanges(balanceBatch);
              } catch (err) {
                log.error({ err }, "onBalanceChanges callback failed");
              }
            }
          },
          { retries, minTimeout: 1000 },
        );
      } catch (err) {
        // Restore all batches to front of buffers so data is not lost
        buffer = batch.concat(buffer);
        balanceBuffer = balanceBatch.concat(balanceBuffer);
        for (const [key, seq] of cursorSnapshot) {
          const prev = cursors.get(key);
          if (prev === undefined || seq > prev) cursors.set(key, seq);
        }
        for (const seq of seqSnapshot) {
          processedSeqs.add(seq);
        }
        throw err;
      }
    })();

    flushing = flushPromise;
    try {
      await flushPromise;
    } finally {
      flushing = null;
    }
  }

  return {
    async push(events: DecodedEvent[], cursorKey: string, seq: bigint): Promise<void> {
      if (events.length > 0) {
        buffer.push(...events);
      }

      // Track every checkpoint seq pushed (including empty ones)
      processedSeqs.add(seq);

      // Coalesce cursors: keep highest seq per key
      const prev = cursors.get(cursorKey);
      if (prev === undefined || seq > prev) {
        cursors.set(cursorKey, seq);
      }

      log.trace({ events: events.length, cursor: seq.toString(), bufferSize: buffer.length }, "push");

      // Threshold-based flush with backpressure (count events + balance changes together)
      if (buffer.length + balanceBuffer.length >= maxEvents) {
        if (flushing) {
          log.debug({ bufferSize: buffer.length }, "backpressure — awaiting in-flight flush");
          await flushing;
        }
        await doFlush();
      }
    },

    pushBalanceChanges(changes: BalanceChange[]): void {
      if (changes.length > 0) {
        balanceBuffer.push(...changes);
      }
    },

    async flush(): Promise<void> {
      if (flushing) await flushing;
      await doFlush();
    },

    start(): void {
      timer = setInterval(() => {
        if (!stopped && !flushing) {
          doFlush().catch((err) => {
            log.error({ err }, "flush error");
          });
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
      // Best-effort final flush — error already propagated if flush() rejected
      try {
        await doFlush();
      } catch (err) {
        log.error({ err }, "final flush on stop failed");
      }
    },
  };
}
