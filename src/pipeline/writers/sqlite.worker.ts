/**
 * SQLite writer worker — runs createSqlStorage in a worker thread.
 *
 * Receives serialized batches from the main thread, deserializes them,
 * calls storage.write(), and posts ack/error messages back.
 */
/// <reference lib="webworker" />
import { createSqlStorage } from "../destinations/sql.ts";
import { deserializeBatch } from "../writer.ts";
import type { SerializedBatch } from "../writer.ts";
import type { WriteAck } from "../writer.ts";
import type { Storage } from "../types.ts";

declare var self: Worker;

let storage: Storage | null = null;

self.onmessage = async (event: MessageEvent) => {
  const msg = event.data;

  if (msg.type === "init") {
    try {
      storage = createSqlStorage(msg.config);
      await storage.initialize();
      postMessage({ type: "initialized" });
    } catch (err) {
      postMessage({
        type: "error",
        id: -1,
        message: err instanceof Error ? err.message : String(err),
      });
    }
    return;
  }

  if (msg.type === "write") {
    const { id, encoded } = msg as { id: number; encoded: string };

    try {
      const start = performance.now();

      // JSON decode + deserialize: string -> bigint, ISO string -> Date
      const batch: SerializedBatch[] = JSON.parse(encoded);
      const processed = deserializeBatch(batch);

      await storage!.write(processed);

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

      const ack: WriteAck = {
        cursor,
        eventsWritten,
        balanceChangesWritten,
        transactionsWritten,
        moveCallsWritten,
        flushDurationMs,
      };

      postMessage({ type: "ack", id, ack });
    } catch (err) {
      postMessage({
        type: "error",
        id,
        message: err instanceof Error ? err.message : String(err),
      });
    }
    return;
  }

  if (msg.type === "shutdown") {
    try {
      if (storage) {
        await storage.shutdown();
        storage = null;
      }
    } catch {
      // Best effort shutdown
    }
    postMessage({ type: "closed" });
    return;
  }
};
