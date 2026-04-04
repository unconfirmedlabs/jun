/**
 * SQLite WriterChannel — spawns a Bun.Worker for off-thread SQLite writes.
 *
 * Communication protocol:
 *   Main -> Worker: { type: "init", config }
 *   Main -> Worker: { type: "write", id, batches }
 *   Main -> Worker: { type: "shutdown" }
 *   Worker -> Main: { type: "initialized" }
 *   Worker -> Main: { type: "ack", id, ack }
 *   Worker -> Main: { type: "error", id, message }
 *   Worker -> Main: { type: "closed" }
 */
import path from "path";
import type { WriterChannel, WriteAck, SerializedBatch } from "../writer.ts";
import type { SqlStorageConfig } from "../destinations/sql.ts";

const MAX_QUEUED = 3;

export function createSqliteWriterChannel(config: SqlStorageConfig): WriterChannel {
  let worker: Worker | null = null;
  let ackCallback: ((ack: WriteAck) => void) | null = null;
  let nextId = 0;
  let inFlight = 0;

  // Backpressure: waiters blocked on send() when channel is full
  const waiters: Array<{ resolve: () => void }> = [];

  // Track in-flight writes for error handling and drain
  const pending = new Map<number, { resolve: () => void; reject: (err: Error) => void }>();

  // Drain support: resolve when inFlight reaches 0
  let drainResolve: (() => void) | null = null;

  function handleAck(id: number, ack: WriteAck) {
    inFlight--;
    const p = pending.get(id);
    if (p) {
      pending.delete(id);
      p.resolve();
    }
    // Unblock a waiting send() if any
    if (waiters.length > 0) {
      waiters.shift()!.resolve();
    }
    // Fire user callback
    ackCallback?.(ack);
    // Resolve drain if nothing left
    if (inFlight === 0 && drainResolve) {
      drainResolve();
      drainResolve = null;
    }
  }

  function handleError(id: number, message: string) {
    inFlight--;
    const p = pending.get(id);
    if (p) {
      pending.delete(id);
      p.reject(new Error(`SQLite worker error: ${message}`));
    }
    // Unblock a waiting send() if any
    if (waiters.length > 0) {
      waiters.shift()!.resolve();
    }
    if (inFlight === 0 && drainResolve) {
      drainResolve();
      drainResolve = null;
    }
  }

  return {
    name: "sqlite-writer",

    async initialize(): Promise<void> {
      const workerUrl = path.join(import.meta.dir, "sqlite.worker.ts");
      worker = new Worker(workerUrl);

      const initPromise = new Promise<void>((resolve, reject) => {
        worker!.onmessage = (event: MessageEvent) => {
          const msg = event.data;
          if (msg.type === "initialized") {
            // Switch to normal message handler
            worker!.onmessage = (event: MessageEvent) => {
              const msg = event.data;
              if (msg.type === "ack") {
                handleAck(msg.id, msg.ack);
              } else if (msg.type === "error") {
                handleError(msg.id, msg.message);
              } else if (msg.type === "closed") {
                // Handled in close()
              }
            };
            resolve();
          } else if (msg.type === "error") {
            reject(new Error(`SQLite worker init failed: ${msg.message}`));
          }
        };
      });

      worker.postMessage({ type: "init", config });
      await initPromise;
    },

    async send(batch: SerializedBatch[]): Promise<void> {
      if (!worker) throw new Error("SQLite writer not initialized");

      // Backpressure: wait if channel is full
      if (inFlight >= MAX_QUEUED) {
        await new Promise<void>(resolve => waiters.push({ resolve }));
      }

      const id = nextId++;
      inFlight++;

      const sendPromise = new Promise<void>((resolve, reject) => {
        pending.set(id, { resolve, reject });
      });

      worker.postMessage({ type: "write", id, batch });

      // Don't await here — send() returns once the message is posted.
      // The ack will resolve the pending promise, but we let it run in background.
      // Error handling is done via the pending map.
      sendPromise.catch(() => {}); // prevent unhandled rejection
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
      if (!worker) return;

      // Wait for all in-flight writes to complete
      await this.drain();

      // Send shutdown and wait for closed confirmation
      const closePromise = new Promise<void>((resolve) => {
        const prevHandler = worker!.onmessage;
        worker!.onmessage = (event: MessageEvent) => {
          const msg = event.data;
          if (msg.type === "closed") {
            resolve();
          } else {
            // Forward other messages to existing handler
            prevHandler?.call(worker, event);
          }
        };
      });

      worker.postMessage({ type: "shutdown" });
      await closePromise;

      worker.terminate();
      worker = null;
    },
  };
}
