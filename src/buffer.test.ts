import { test, expect, describe, mock, beforeEach } from "bun:test";
import pino from "pino";
import { createWriteBuffer, type FlushStats } from "./buffer.ts";
import type { DecodedEvent } from "./processor.ts";
import type { StorageBackend } from "./output/storage.ts";
import type { StateManager } from "./state.ts";

const testLog = pino({ level: "silent" });

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

function makeEvent(handlerName: string, seq: bigint, eventSeq = 0): DecodedEvent {
  return {
    handlerName,
    checkpointSeq: seq,
    txDigest: `tx_${seq}_${eventSeq}`,
    eventSeq,
    sender: "0xsender",
    timestamp: new Date(),
    data: { field1: "value" },
  };
}

function createMockOutput(): StorageBackend & { calls: { handler: string; count: number }[] } {
  const calls: { handler: string; count: number }[] = [];
  return {
    name: "test",
    calls,
    async migrate() {},
    async write() {},
    async writeHandler(handlerName: string, events: DecodedEvent[]) {
      calls.push({ handler: handlerName, count: events.length });
    },
    async shutdown() {},
  };
}

function createMockState(): StateManager & {
  cursors: Map<string, bigint>;
  processedSeqs: bigint[];
} {
  const cursors = new Map<string, bigint>();
  const processedSeqs: bigint[] = [];
  return {
    cursors,
    processedSeqs,
    async getCheckpointCursor(key: string) {
      return cursors.get(key) ?? null;
    },
    async setCheckpointCursor(key: string, seq: bigint) {
      cursors.set(key, seq);
    },
    async recordProcessedCheckpoints(seqs: bigint[]) {
      processedSeqs.push(...seqs);
    },
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("WriteBuffer", () => {
  test("threshold-based flush triggers when maxEvents reached", async () => {
    const output = createMockOutput();
    const state = createMockState();

    const buffer = createWriteBuffer(output, state, {
      label: "test",
      intervalMs: 60_000, // Very long interval so only threshold triggers
      maxEvents: 3,
    }, testLog);

    // Push 3 events (equals maxEvents) — should trigger flush
    await buffer.push(
      [makeEvent("HandlerA", 1n), makeEvent("HandlerA", 1n, 1), makeEvent("HandlerA", 2n)],
      "test:net",
      2n,
    );

    // Give flush a tick to complete
    await new Promise((r) => setTimeout(r, 10));

    expect(output.calls.length).toBe(1);
    expect(output.calls[0]!.handler).toBe("HandlerA");
    expect(output.calls[0]!.count).toBe(3);
    expect(state.cursors.get("test:net")).toBe(2n);

    await buffer.stop();
  });

  test("cursor coalescing keeps highest seq per key", async () => {
    const output = createMockOutput();
    const state = createMockState();

    const buffer = createWriteBuffer(output, state, {
      label: "test",
      intervalMs: 60_000,
      maxEvents: 100, // Won't trigger threshold
    }, testLog);

    // Push with different sequences for same key
    await buffer.push([makeEvent("A", 5n)], "key1", 5n);
    await buffer.push([makeEvent("A", 3n)], "key1", 3n); // Lower — should be ignored
    await buffer.push([makeEvent("A", 7n)], "key1", 7n); // Higher — should win

    // Push for a different key
    await buffer.push([makeEvent("A", 10n)], "key2", 10n);

    // Force flush
    await buffer.flush();

    expect(state.cursors.get("key1")).toBe(7n);
    expect(state.cursors.get("key2")).toBe(10n);

    await buffer.stop();
  });

  test("flush stats callback receives correct data", async () => {
    const output = createMockOutput();
    const state = createMockState();
    const stats: FlushStats[] = [];

    const buffer = createWriteBuffer(output, state, {
      label: "test",
      intervalMs: 60_000,
      maxEvents: 100,
      onFlush: (s) => stats.push(s),
    }, testLog);

    await buffer.push(
      [makeEvent("HandlerA", 1n), makeEvent("HandlerB", 2n)],
      "test:net",
      2n,
    );

    await buffer.flush();

    expect(stats.length).toBe(1);
    expect(stats[0]!.eventsWritten).toBe(2);
    expect(stats[0]!.tablesWritten).toBe(2);
    expect(stats[0]!.flushDurationMs).toBeGreaterThan(0);
    expect(stats[0]!.cursorKeys.get("test:net")).toBe(2n);

    await buffer.stop();
  });

  test("stop() flushes remaining events", async () => {
    const output = createMockOutput();
    const state = createMockState();

    const buffer = createWriteBuffer(output, state, {
      label: "test",
      intervalMs: 60_000,
      maxEvents: 100,
    }, testLog);

    buffer.start();

    await buffer.push([makeEvent("A", 1n)], "key", 1n);

    // Stop should flush the remaining event
    await buffer.stop();

    expect(output.calls.length).toBe(1);
    expect(state.cursors.get("key")).toBe(1n);
  });

  test("empty push does not trigger flush", async () => {
    const output = createMockOutput();
    const state = createMockState();

    const buffer = createWriteBuffer(output, state, {
      label: "test",
      intervalMs: 60_000,
      maxEvents: 1,
    }, testLog);

    // Push empty events but with a cursor
    await buffer.push([], "key", 5n);

    // Should not have flushed (0 events < maxEvents of 1)
    expect(output.calls.length).toBe(0);

    // But flush should still update the cursor
    await buffer.flush();
    expect(state.cursors.get("key")).toBe(5n);

    await buffer.stop();
  });

  test("parallel inserts across tables via writeHandler", async () => {
    const output = createMockOutput();
    const state = createMockState();

    const buffer = createWriteBuffer(output, state, {
      label: "test",
      intervalMs: 60_000,
      maxEvents: 100,
    }, testLog);

    // Push events for 3 different handlers
    await buffer.push(
      [
        makeEvent("HandlerA", 1n),
        makeEvent("HandlerB", 1n),
        makeEvent("HandlerC", 1n),
      ],
      "key",
      1n,
    );

    await buffer.flush();

    // Should have called writeHandler for each handler
    expect(output.calls.length).toBe(3);
    const handlers = output.calls.map((c) => c.handler).sort();
    expect(handlers).toEqual(["HandlerA", "HandlerB", "HandlerC"]);

    await buffer.stop();
  });

  test("records processed checkpoints for gap detection", async () => {
    const output = createMockOutput();
    const state = createMockState();

    const buffer = createWriteBuffer(output, state, {
      label: "test",
      intervalMs: 60_000,
      maxEvents: 100,
    }, testLog);

    await buffer.push([makeEvent("A", 5n)], "key", 5n);
    await buffer.push([makeEvent("A", 7n)], "key", 7n);

    await buffer.flush();

    // Should have recorded checkpoint seqs 5 and 7 (from events) + 7 (from cursor)
    expect(state.processedSeqs).toContain(5n);
    expect(state.processedSeqs).toContain(7n);

    await buffer.stop();
  });

  test("timer-based flush triggers after intervalMs", async () => {
    const output = createMockOutput();
    const state = createMockState();

    const buffer = createWriteBuffer(output, state, {
      label: "test",
      intervalMs: 50,
      maxEvents: 100, // High threshold so only the timer triggers
    }, testLog);

    buffer.start();

    await buffer.push([makeEvent("A", 1n), makeEvent("A", 2n)], "key", 2n);

    // No flush yet (below threshold, timer hasn't fired)
    expect(output.calls.length).toBe(0);

    // Wait for the timer to fire
    await new Promise((r) => setTimeout(r, 100));

    expect(output.calls.length).toBe(1);
    expect(output.calls[0]!.count).toBe(2);
    expect(state.cursors.get("key")).toBe(2n);

    await buffer.stop();
  });

  test("retry then succeed: writeHandler fails twice then succeeds", async () => {
    let attempts = 0;
    const output: StorageBackend = {
      name: "test",
      async migrate() {},
      async write() {},
      async shutdown() {},
      async writeHandler() {
        attempts++;
        if (attempts <= 2) throw new Error("connection lost");
      },
    };
    const state = createMockState();
    const stats: FlushStats[] = [];

    const buffer = createWriteBuffer(output, state, {
      label: "test",
      intervalMs: 60_000,
      maxEvents: 100,
      retries: 3,
      onFlush: (s) => stats.push(s),
    }, testLog);

    await buffer.push([makeEvent("A", 1n)], "key", 1n);
    await buffer.flush();

    expect(attempts).toBe(3); // 2 failures + 1 success
    expect(stats.length).toBe(1);
    expect(stats[0]!.eventsWritten).toBe(1);

    await buffer.stop();
  });

  test("retry exhaustion crashes when all retries fail", async () => {
    const output: StorageBackend = {
      name: "test",
      async migrate() {},
      async write() {},
      async shutdown() {},
      async writeHandler() {
        throw new Error("permanent failure");
      },
    };
    const state = createMockState();

    const buffer = createWriteBuffer(output, state, {
      label: "test",
      intervalMs: 60_000,
      maxEvents: 100,
      retries: 1,
    }, testLog);

    await buffer.push([makeEvent("A", 1n)], "key", 1n);

    await expect(buffer.flush()).rejects.toThrow("permanent failure");

    await buffer.stop();
  });

  test("large batch exceeding maxEvents flushes all events", async () => {
    const output = createMockOutput();
    const state = createMockState();

    const buffer = createWriteBuffer(output, state, {
      label: "test",
      intervalMs: 60_000,
      maxEvents: 10,
    }, testLog);

    // Push 100 events at once
    const events = Array.from({ length: 100 }, (_, i) => makeEvent("A", BigInt(i + 1), i));
    await buffer.push(events, "key", 100n);

    // Give any async flushes time to settle
    await new Promise((r) => setTimeout(r, 50));

    // Force flush any remainder
    await buffer.flush();

    const totalFlushed = output.calls.reduce((sum, c) => sum + c.count, 0);
    expect(totalFlushed).toBe(100);

    await buffer.stop();
  });

  test("flush when truly empty: no events, no cursors", async () => {
    const output = createMockOutput();
    const state = createMockState();

    const buffer = createWriteBuffer(output, state, {
      label: "test",
      intervalMs: 60_000,
      maxEvents: 100,
    }, testLog);

    // Flush on a fresh buffer with nothing pushed
    await buffer.flush();

    expect(output.calls.length).toBe(0);
    expect(state.cursors.size).toBe(0);
    expect(state.processedSeqs.length).toBe(0);

    await buffer.stop();
  });

  test("multiple sequential flushes each write their own batch", async () => {
    const output = createMockOutput();
    const state = createMockState();

    const buffer = createWriteBuffer(output, state, {
      label: "test",
      intervalMs: 60_000,
      maxEvents: 100,
    }, testLog);

    // First batch
    await buffer.push([makeEvent("A", 1n), makeEvent("A", 2n)], "key", 2n);
    await buffer.flush();

    expect(output.calls.length).toBe(1);
    expect(output.calls[0]!.count).toBe(2);
    expect(state.cursors.get("key")).toBe(2n);

    // Second batch
    await buffer.push([makeEvent("B", 3n)], "key", 3n);
    await buffer.flush();

    expect(output.calls.length).toBe(2);
    expect(output.calls[1]!.handler).toBe("B");
    expect(output.calls[1]!.count).toBe(1);
    expect(state.cursors.get("key")).toBe(3n);

    await buffer.stop();
  });

  test("stop() without start() flushes remaining events", async () => {
    const output = createMockOutput();
    const state = createMockState();

    const buffer = createWriteBuffer(output, state, {
      label: "test",
      intervalMs: 60_000,
      maxEvents: 100,
    }, testLog);

    // Never call buffer.start()
    await buffer.push([makeEvent("A", 1n), makeEvent("A", 2n)], "key", 2n);

    await buffer.stop();

    expect(output.calls.length).toBe(1);
    expect(output.calls[0]!.count).toBe(2);
    expect(state.cursors.get("key")).toBe(2n);
  });

  test("onFlush not provided: flush completes without error", async () => {
    const output = createMockOutput();
    const state = createMockState();

    // No onFlush callback
    const buffer = createWriteBuffer(output, state, {
      label: "test",
      intervalMs: 60_000,
      maxEvents: 2,
    }, testLog);

    // Push enough to trigger threshold flush
    await buffer.push([makeEvent("A", 1n), makeEvent("A", 2n)], "key", 2n);

    // Give threshold flush time to complete
    await new Promise((r) => setTimeout(r, 10));

    expect(output.calls.length).toBe(1);
    expect(output.calls[0]!.count).toBe(2);

    await buffer.stop();
  });

  test("backpressure: push awaits in-flight flush", async () => {
    let flushResolve: () => void;
    const flushGate = new Promise<void>((r) => { flushResolve = r; });

    const output: StorageBackend = {
      name: "test",
      async migrate() {},
      async write() {},
      async shutdown() {},
      async writeHandler() {
        // Block until gate opens
        await flushGate;
      },
    };
    const state = createMockState();

    const buffer = createWriteBuffer(output, state, {
      label: "test",
      intervalMs: 60_000,
      maxEvents: 2,
    }, testLog);

    // Trigger first flush (will block on flushGate)
    const push1 = buffer.push(
      [makeEvent("A", 1n), makeEvent("A", 2n)],
      "key",
      2n,
    );

    // Small delay to let the flush start
    await new Promise((r) => setTimeout(r, 10));

    // Second push should block due to backpressure
    let push2Resolved = false;
    const push2 = buffer.push(
      [makeEvent("A", 3n), makeEvent("A", 4n)],
      "key",
      4n,
    ).then(() => { push2Resolved = true; });

    await new Promise((r) => setTimeout(r, 50));
    expect(push2Resolved).toBe(false);

    // Release the gate
    flushResolve!();

    await push1;
    await push2;
    expect(push2Resolved).toBe(true);

    await buffer.stop();
  });
});
