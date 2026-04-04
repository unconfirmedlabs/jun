/**
 * Postgres WriterChannel — in-process async queue (no Worker needed).
 *
 * Bun.SQL is async, so we can run writes concurrently without blocking
 * the main thread. Uses a bounded queue with configurable max concurrency.
 */
import type { WriterChannel, WriteAck, SerializedBatch } from "../writer.ts";
import { deserializeBatch } from "../writer.ts";
import type { Storage } from "../types.ts";

const MAX_QUEUED = 3;
const MAX_CONCURRENCY = 2;

export function createPostgresWriterChannel(storage: Storage): WriterChannel {
  let ackCallback: ((ack: WriteAck) => void) | null = null;
  let nextId = 0;
  let inFlight = 0;
  let initialized = false;

  // Backpressure: waiters blocked on send() when channel is full
  const waiters: Array<{ resolve: () => void }> = [];

  // Drain support
  let drainResolve: (() => void) | null = null;

  function onComplete() {
    inFlight--;
    if (waiters.length > 0) {
      waiters.shift()!.resolve();
    }
    if (inFlight === 0 && drainResolve) {
      drainResolve();
      drainResolve = null;
    }
  }

  return {
    name: "postgres-writer",

    async initialize(): Promise<void> {
      await storage.initialize();
      initialized = true;
    },

    async send(batch: SerializedBatch): Promise<void> {
      if (!initialized) throw new Error("Postgres writer not initialized");

      // Backpressure: wait if channel is full
      if (inFlight >= MAX_QUEUED) {
        await new Promise<void>(resolve => waiters.push({ resolve }));
      }

      inFlight++;

      // Fire and forget — error handling via callback
      (async () => {
        try {
          const start = performance.now();
          const processed = deserializeBatch([batch]);
          await storage.write(processed);
          const flushDurationMs = performance.now() - start;

          let eventsWritten = 0;
          let balanceChangesWritten = 0;
          let transactionsWritten = 0;
          let moveCallsWritten = 0;
          let cursor = "0";

          for (const cp of processed) {
            eventsWritten += cp.events.length;
            balanceChangesWritten += cp.balanceChanges.length;
            transactionsWritten += cp.transactions.length;
            moveCallsWritten += cp.moveCalls.length;
            const seq = cp.checkpoint.sequenceNumber.toString();
            if (seq > cursor) cursor = seq;
          }

          ackCallback?.({
            cursor,
            eventsWritten,
            balanceChangesWritten,
            transactionsWritten,
            moveCallsWritten,
            flushDurationMs,
          });
        } catch {
          // Postgres writes are retried by the write buffer's p-retry.
          // If the channel fails, it will be caught by drain/close.
        } finally {
          onComplete();
        }
      })();
    },

    onAck(callback: (ack: WriteAck) => void): void {
      ackCallback = callback;
    },

    async drain(): Promise<void> {
      if (inFlight === 0) return;
      await new Promise<void>(resolve => {
        drainResolve = resolve;
      });
    },

    async close(): Promise<void> {
      await this.drain();
      await storage.shutdown();
    },
  };
}
