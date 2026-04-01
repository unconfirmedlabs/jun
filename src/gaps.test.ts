import { test, expect, describe } from "bun:test";
import pino from "pino";
import { createGapDetector } from "./gaps.ts";
import type { StateManager } from "./state.ts";
import type { DecodedEvent, EventProcessor } from "./processor.ts";
import type { WriteBuffer, FlushStats } from "./buffer.ts";
import type { ArchiveClient } from "./archive.ts";
import type { GrpcCheckpointResponse } from "./grpc.ts";

// ---------------------------------------------------------------------------
// Mock helpers
// ---------------------------------------------------------------------------

function createMockSql(processedCheckpoints: Set<bigint>) {
  return {
    unsafe(query: string, params?: unknown[]) {
      // Handle findGaps query
      if (query.includes("generate_series")) {
        const from = BigInt(params![0] as string);
        const to = BigInt(params![1] as string);
        const missing: { seq: string }[] = [];
        for (let i = from; i <= to; i++) {
          if (!processedCheckpoints.has(i)) {
            missing.push({ seq: i.toString() });
          }
        }
        return Promise.resolve(missing);
      }

      // Handle MIN query
      if (query.includes("MIN")) {
        if (processedCheckpoints.size === 0) {
          return Promise.resolve([{ min_seq: null }]);
        }
        const min = [...processedCheckpoints].sort((a, b) => (a < b ? -1 : 1))[0];
        return Promise.resolve([{ min_seq: min!.toString() }]);
      }

      return Promise.resolve([]);
    },
  };
}

function createMockState(cursors: Record<string, bigint>): StateManager {
  return {
    async getCheckpointCursor(key: string) {
      return cursors[key] ?? null;
    },
    async setCheckpointCursor(key: string, seq: bigint) {
      cursors[key] = seq;
    },
    async recordProcessedCheckpoints() {},
  };
}

function createMockProcessor(eventsPerCheckpoint: number): EventProcessor {
  return {
    process(response: GrpcCheckpointResponse): DecodedEvent[] {
      const seq = BigInt(response.cursor);
      const events: DecodedEvent[] = [];
      for (let i = 0; i < eventsPerCheckpoint; i++) {
        events.push({
          handlerName: "TestHandler",
          checkpointSeq: seq,
          txDigest: `tx_${seq}_${i}`,
          eventSeq: i,
          sender: "0xsender",
          timestamp: new Date(),
          data: {},
        });
      }
      return events;
    },
  };
}

function createMockBuffer(): WriteBuffer & { pushed: { events: number; key: string; seq: bigint }[] } {
  const pushed: { events: number; key: string; seq: bigint }[] = [];
  return {
    pushed,
    async push(events: DecodedEvent[], cursorKey: string, seq: bigint) {
      pushed.push({ events: events.length, key: cursorKey, seq });
    },
    async flush() {},
    start() {},
    async stop() {},
  };
}

function createMockArchive(available: Set<bigint>): ArchiveClient {
  return {
    async fetchCheckpoint(seq: bigint): Promise<GrpcCheckpointResponse> {
      if (!available.has(seq)) {
        throw new Error(`Checkpoint ${seq} not available`);
      }
      return {
        cursor: seq.toString(),
        checkpoint: {
          sequenceNumber: seq.toString(),
          summary: { timestamp: { seconds: "1000", nanos: 0 } },
          transactions: [],
        },
      };
    },
  };
}

const testLog = pino({ level: "silent" });

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("GapDetector", () => {
  test("findGaps returns empty for contiguous range", async () => {
    const processed = new Set([1n, 2n, 3n, 4n, 5n]);
    const sql = createMockSql(processed);
    const state = createMockState({});
    const detector = createGapDetector(sql, state, "test", testLog);

    const gaps = await detector.findGaps(1n, 5n);
    expect(gaps).toEqual([]);
  });

  test("findGaps detects holes in range", async () => {
    const processed = new Set([1n, 2n, 3n, 5n, 6n, 8n, 9n, 10n]);
    const sql = createMockSql(processed);
    const state = createMockState({});
    const detector = createGapDetector(sql, state, "test", testLog);

    const gaps = await detector.findGaps(1n, 10n);
    expect(gaps).toEqual([4n, 7n]);
  });

  test("findGaps handles empty range", async () => {
    const processed = new Set<bigint>();
    const sql = createMockSql(processed);
    const state = createMockState({});
    const detector = createGapDetector(sql, state, "test", testLog);

    const gaps = await detector.findGaps(10n, 5n); // from > to
    expect(gaps).toEqual([]);
  });

  test("findGaps handles all missing", async () => {
    const processed = new Set<bigint>();
    const sql = createMockSql(processed);
    const state = createMockState({});
    const detector = createGapDetector(sql, state, "test", testLog);

    const gaps = await detector.findGaps(1n, 5n);
    expect(gaps).toEqual([1n, 2n, 3n, 4n, 5n]);
  });

  test("repairGaps fetches and processes missing checkpoints", async () => {
    const sql = createMockSql(new Set());
    const state = createMockState({});
    const detector = createGapDetector(sql, state, "test", testLog);

    const processor = createMockProcessor(2);
    const buffer = createMockBuffer();
    const archive = createMockArchive(new Set([4n, 7n]));

    const repaired = await detector.repairGaps(
      [4n, 7n],
      processor,
      buffer,
      archive,
      "gaps:test",
    );

    expect(repaired).toBe(2);
    expect(buffer.pushed.length).toBe(2);
    expect(buffer.pushed[0]!.seq).toBe(4n);
    expect(buffer.pushed[0]!.events).toBe(2);
    expect(buffer.pushed[1]!.seq).toBe(7n);
    expect(buffer.pushed[1]!.key).toBe("gaps:test");
  });

  test("repairGaps skips unavailable checkpoints without crashing", async () => {
    const sql = createMockSql(new Set());
    const state = createMockState({});
    const detector = createGapDetector(sql, state, "test", testLog);

    const processor = createMockProcessor(1);
    const buffer = createMockBuffer();
    // Only checkpoint 4 available, not 7
    const archive = createMockArchive(new Set([4n]));

    const repaired = await detector.repairGaps(
      [4n, 7n],
      processor,
      buffer,
      archive,
      "gaps:test",
    );

    expect(repaired).toBe(1);
    expect(buffer.pushed.length).toBe(1);
  });

  test("repairGaps with empty gaps returns 0", async () => {
    const sql = createMockSql(new Set());
    const state = createMockState({});
    const detector = createGapDetector(sql, state, "test", testLog);

    const processor = createMockProcessor(0);
    const buffer = createMockBuffer();
    const archive = createMockArchive(new Set());

    const repaired = await detector.repairGaps(
      [],
      processor,
      buffer,
      archive,
      "gaps:test",
    );

    expect(repaired).toBe(0);
  });

  test("findGaps with single checkpoint (from == to) when checkpoint exists", async () => {
    const processed = new Set([5n]);
    const sql = createMockSql(processed);
    const state = createMockState({});
    const detector = createGapDetector(sql, state, "test", testLog);

    const gaps = await detector.findGaps(5n, 5n);
    expect(gaps).toEqual([]);
  });

  test("findGaps with single checkpoint (from == to) when checkpoint missing", async () => {
    const processed = new Set<bigint>();
    const sql = createMockSql(processed);
    const state = createMockState({});
    const detector = createGapDetector(sql, state, "test", testLog);

    const gaps = await detector.findGaps(5n, 5n);
    expect(gaps).toEqual([5n]);
  });

  test("repairGaps with zero events per checkpoint", async () => {
    const sql = createMockSql(new Set());
    const state = createMockState({});
    const detector = createGapDetector(sql, state, "test", testLog);

    // Processor returns 0 events per checkpoint
    const processor = createMockProcessor(0);
    const buffer = createMockBuffer();
    const archive = createMockArchive(new Set([3n, 7n]));

    const repaired = await detector.repairGaps(
      [3n, 7n],
      processor,
      buffer,
      archive,
      "gaps:test",
    );

    // Both checkpoints should still count as repaired
    expect(repaired).toBe(2);
    expect(buffer.pushed.length).toBe(2);
    expect(buffer.pushed[0]!.events).toBe(0);
    expect(buffer.pushed[1]!.events).toBe(0);
  });

  test("startPeriodicRepair with no backfill cursor does not attempt repair", async () => {
    const processed = new Set([1n, 2n, 3n]);
    const sql = createMockSql(processed);
    // No backfill cursor — state returns null for all keys
    const state = createMockState({});
    const detector = createGapDetector(sql, state, "test", testLog, 60_000);

    const processor = createMockProcessor(1);
    const buffer = createMockBuffer();
    const archive = createMockArchive(new Set());

    const stop = detector.startPeriodicRepair(processor, buffer, archive);

    // Give the immediate run() a tick to execute
    await new Promise((resolve) => setTimeout(resolve, 50));

    // No repair should have been attempted — buffer should be untouched
    expect(buffer.pushed.length).toBe(0);

    stop();
  });

  test("startPeriodicRepair stop function clears timer", async () => {
    const processed = new Set([1n, 2n, 3n, 5n]);
    const sql = createMockSql(processed);
    const state = createMockState({ "backfill:test": 10n });
    // Use a very short interval so we can verify it stops
    const detector = createGapDetector(sql, state, "test", testLog, 100);

    const processor = createMockProcessor(1);
    const buffer = createMockBuffer();
    const archive = createMockArchive(new Set([4n, 6n, 7n, 8n, 9n, 10n]));

    const stop = detector.startPeriodicRepair(processor, buffer, archive);

    // Let the immediate run complete
    await new Promise((resolve) => setTimeout(resolve, 50));

    const repairedAfterFirst = buffer.pushed.length;
    expect(repairedAfterFirst).toBeGreaterThan(0);

    // Stop the periodic repair
    stop();

    // Wait longer than the interval to verify no more runs happen
    await new Promise((resolve) => setTimeout(resolve, 200));

    // No additional repairs should have happened after stop
    expect(buffer.pushed.length).toBe(repairedAfterFirst);
  });
});
