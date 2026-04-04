import { test, expect, describe, beforeAll, afterAll } from "bun:test";
import pino from "pino";
import { createServer, createMetrics, type IndexerServer, type IndexerMetrics } from "./serve.ts";
import { createBroadcastManager, type BroadcastManager } from "./broadcast.ts";
import type { StateManager } from "./state.ts";
import type { FlushStats } from "./serve.ts";

const testLog = pino({ level: "silent" });

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

function createMockState(): StateManager {
  const cursors = new Map<string, bigint>();
  return {
    async getCheckpointCursor(key) { return cursors.get(key) ?? null; },
    async setCheckpointCursor(key, seq) { cursors.set(key, seq); },
    async recordProcessedCheckpoints() {},
  };
}

function createMockSql(result: any[] = [{ id: 1, name: "test" }]) {
  return {
    begin: async (fn: any) => {
      const tx = {
        unsafe: async (query: string) => {
          if (query.startsWith("SET")) return [];
          if (query.includes("error_trigger")) throw new Error("relation does not exist");
          return result;
        },
      };
      return fn(tx);
    },
  };
}

function makeFlushStats(overrides?: Partial<FlushStats>): FlushStats {
  return {
    eventsWritten: 10,
    tablesWritten: 2,
    flushDurationMs: 42,
    cursorKeys: new Map([["live:test", 100n]]),
    bufferSizeAfter: 0,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Server tests
// ---------------------------------------------------------------------------

describe("createServer", () => {
  let server: IndexerServer;
  let metrics: IndexerMetrics;
  let broadcast: BroadcastManager;
  let baseUrl: string;

  beforeAll(() => {
    metrics = createMetrics();
    broadcast = createBroadcastManager([], testLog);
    const sql = createMockSql();
    const state = createMockState();
    server = createServer({ port: 0 }, { sql, state, metrics, broadcast, log: testLog });
    baseUrl = `http://127.0.0.1:${server.port}`;
  });

  afterAll(async () => {
    await server.stop();
  });

  test("GET /health returns 200 with status ok", async () => {
    const res = await fetch(`${baseUrl}/health`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body).toEqual({ status: "ok" });
  });

  test("GET /status returns 200 with metrics snapshot", async () => {
    metrics.setLiveCursor(500n);
    metrics.recordLiveFlush(makeFlushStats());
    metrics.recordThrottleState(10, false);

    const res = await fetch(`${baseUrl}/status`);
    expect(res.status).toBe(200);
    const body = await res.json();

    expect(body.uptime).toBeGreaterThanOrEqual(0);
    expect(body.live.cursor).toBe("500");
    expect(body.live.lastFlush.eventsWritten).toBe(10);
    expect(body.throttle.concurrency).toBe(10);
    expect(body.throttle.paused).toBe(false);
  });

  test("GET /query?sql=SELECT 1 returns rows", async () => {
    const res = await fetch(`${baseUrl}/query?sql=${encodeURIComponent("SELECT 1")}`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.rows).toBeDefined();
    expect(body.count).toBeGreaterThanOrEqual(0);
    expect(typeof body.truncated).toBe("boolean");
  });

  test("GET /query without sql param returns 400", async () => {
    const res = await fetch(`${baseUrl}/query`);
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toContain("missing");
  });

  test("GET /query?sql=DROP TABLE x returns 403", async () => {
    const res = await fetch(`${baseUrl}/query?sql=${encodeURIComponent("DROP TABLE x")}`);
    expect(res.status).toBe(403);
    const body = await res.json();
    expect(body.error).toContain("SELECT");
  });

  test("GET /query with block comment bypass returns 403", async () => {
    const sql = "/* harmless */ DROP TABLE x";
    const res = await fetch(`${baseUrl}/query?sql=${encodeURIComponent(sql)}`);
    expect(res.status).toBe(403);
  });

  test("GET /query with line comment bypass returns 403", async () => {
    const sql = "-- just a comment\nDROP TABLE x";
    const res = await fetch(`${baseUrl}/query?sql=${encodeURIComponent(sql)}`);
    expect(res.status).toBe(403);
  });

  test("GET /query?sql=WITH ... SELECT ... returns 200", async () => {
    const sql = "WITH cte AS (SELECT 1 AS n) SELECT * FROM cte";
    const res = await fetch(`${baseUrl}/query?sql=${encodeURIComponent(sql)}`);
    expect(res.status).toBe(200);
  });

  test("GET /query?sql=EXPLAIN SELECT 1 returns 200", async () => {
    const res = await fetch(`${baseUrl}/query?sql=${encodeURIComponent("EXPLAIN SELECT 1")}`);
    expect(res.status).toBe(200);
  });

  test("GET /query?sql=INSERT returns 403", async () => {
    const res = await fetch(`${baseUrl}/query?sql=${encodeURIComponent("INSERT INTO t VALUES (1)")}`);
    expect(res.status).toBe(403);
  });

  test("GET /query?sql=UPDATE returns 403", async () => {
    const res = await fetch(`${baseUrl}/query?sql=${encodeURIComponent("UPDATE t SET x = 1")}`);
    expect(res.status).toBe(403);
  });

  test("GET /query?sql=DELETE returns 403", async () => {
    const res = await fetch(`${baseUrl}/query?sql=${encodeURIComponent("DELETE FROM t")}`);
    expect(res.status).toBe(403);
  });

  test("GET /metrics returns prometheus text format", async () => {
    const res = await fetch(`${baseUrl}/metrics`);
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("text/plain");
    const text = await res.text();
    expect(text).toContain("jun_uptime_seconds");
    expect(text).toContain("# HELP");
    expect(text).toContain("# TYPE");
  });

  test("GET /nonexistent returns 404", async () => {
    const res = await fetch(`${baseUrl}/nonexistent`);
    expect(res.status).toBe(404);
    const body = await res.json();
    expect(body.error).toBe("not found");
  });

  test("server.port returns the actual port", () => {
    expect(server.port).toBeGreaterThan(0);
  });
});

describe("createServer with SQL errors", () => {
  let server: IndexerServer;
  let baseUrl: string;

  beforeAll(() => {
    const metrics = createMetrics();
    const broadcast = createBroadcastManager([], testLog);
    const sql = createMockSql();
    const state = createMockState();
    server = createServer({ port: 0 }, { sql, state, metrics, broadcast, log: testLog });
    baseUrl = `http://127.0.0.1:${server.port}`;
  });

  afterAll(async () => {
    await server.stop();
  });

  test("GET /query with SQL error returns 400", async () => {
    const res = await fetch(`${baseUrl}/query?sql=${encodeURIComponent("SELECT error_trigger")}`);
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toContain("relation does not exist");
  });
});

// ---------------------------------------------------------------------------
// SSE tests
// ---------------------------------------------------------------------------

describe("SSE streams", () => {
  let server: IndexerServer;
  let broadcast: BroadcastManager;
  let baseUrl: string;

  beforeAll(() => {
    const metrics = createMetrics();
    broadcast = createBroadcastManager([], testLog);
    const sql = createMockSql();
    const state = createMockState();
    server = createServer({ port: 0 }, { sql, state, metrics, broadcast, log: testLog });
    baseUrl = `http://127.0.0.1:${server.port}`;
  });

  afterAll(async () => {
    await server.stop();
  });

  test("SSE connection returns event-stream content type", async () => {
    const controller = new AbortController();
    const res = await fetch(`${baseUrl}/stream/events`, { signal: controller.signal });

    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toBe("text/event-stream");
    expect(res.headers.get("cache-control")).toBe("no-cache");

    controller.abort();
  });

  test("SSE sends connected message on connect", async () => {
    const controller = new AbortController();
    const res = await fetch(`${baseUrl}/stream/events`, { signal: controller.signal });
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();

    const { value } = await reader.read();
    const text = decoder.decode(value);

    expect(text).toContain('"type":"connected"');

    controller.abort();
  });

  test("SSE receives broadcast events", async () => {
    const controller = new AbortController();
    const res = await fetch(`${baseUrl}/stream/events`, { signal: controller.signal });
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();

    // Read the connected message first
    await reader.read();

    // Broadcast an event
    broadcast.broadcastDecodedEvents([{
      handlerName: "TestHandler",
      checkpointSeq: 100n,
      txDigest: "tx_abc",
      eventSeq: 0,
      sender: "0xsender",
      timestamp: new Date("2026-04-01T00:00:00Z"),
      data: { amount: "42" },
    }], "live");

    const { value } = await reader.read();
    const text = decoder.decode(value);

    expect(text).toContain('"handlerName":"TestHandler"');
    expect(text).toContain('"txDigest":"tx_abc"');
    expect(text).toContain('"source":"live"');
    expect(text).toContain('"amount":"42"');

    controller.abort();
  });

  test("SSE filters by handler name", async () => {
    const controller = new AbortController();
    const res = await fetch(`${baseUrl}/stream/events?handler=TargetEvent`, { signal: controller.signal });
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();

    await reader.read(); // connected message

    // Broadcast two events — one matching, one not
    broadcast.broadcastDecodedEvents([
      { handlerName: "OtherEvent", checkpointSeq: 1n, txDigest: "tx1", eventSeq: 0, sender: "0x1", timestamp: new Date(), data: {} },
      { handlerName: "TargetEvent", checkpointSeq: 2n, txDigest: "tx2", eventSeq: 0, sender: "0x2", timestamp: new Date(), data: {} },
    ], "live");

    const { value } = await reader.read();
    const text = decoder.decode(value);

    // Should only contain the target event
    expect(text).toContain("TargetEvent");
    expect(text).not.toContain("OtherEvent");

    controller.abort();
  });

  test("SSE filters by source", async () => {
    const controller = new AbortController();
    const res = await fetch(`${baseUrl}/stream/events?source=live`, { signal: controller.signal });
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();

    await reader.read(); // connected message

    // Broadcast from backfill — should be filtered out
    broadcast.broadcastDecodedEvents([
      { handlerName: "E", checkpointSeq: 1n, txDigest: "bf_tx", eventSeq: 0, sender: "0x1", timestamp: new Date(), data: {} },
    ], "backfill");

    // Broadcast from live — should arrive
    broadcast.broadcastDecodedEvents([
      { handlerName: "E", checkpointSeq: 2n, txDigest: "live_tx", eventSeq: 0, sender: "0x2", timestamp: new Date(), data: {} },
    ], "live");

    const { value } = await reader.read();
    const text = decoder.decode(value);

    expect(text).toContain("live_tx");
    expect(text).not.toContain("bf_tx");

    controller.abort();
  });

  test("SSE invalid source returns 400", async () => {
    const res = await fetch(`${baseUrl}/stream/events?source=invalid`);
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toContain("source must be");
  });

  test("broadcast is no-op with zero clients and targets", () => {
    const freshBroadcast = createBroadcastManager([], testLog);
    // Should not throw
    freshBroadcast.broadcastDecodedEvents([
      { handlerName: "E", checkpointSeq: 1n, txDigest: "tx", eventSeq: 0, sender: "0x1", timestamp: new Date(), data: {} },
    ], "live");
    expect(freshBroadcast.sseClientCount()).toBe(0);
  });

  test("/stream/checkpoints receives checkpoint summaries", async () => {
    const controller = new AbortController();
    const res = await fetch(`${baseUrl}/stream/checkpoints`, { signal: controller.signal });
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();

    expect(res.status).toBe(200);
    await reader.read(); // connected message

    broadcast.broadcast({
      cursor: "500",
      checkpoint: {
        sequenceNumber: "500",
        summary: { timestamp: { seconds: "1711929600", nanos: 0 } },
        transactions: [{ digest: "tx1", events: null }, { digest: "tx2", events: null }],
      },
    }, "live");

    const { value } = await reader.read();
    const text = decoder.decode(value);

    expect(text).toContain('"seq":"500"');
    expect(text).toContain('"txCount":2');
    expect(text).toContain('"source":"live"');

    controller.abort();
  });

  test("/stream/transactions receives transaction summaries", async () => {
    const controller = new AbortController();
    const res = await fetch(`${baseUrl}/stream/transactions`, { signal: controller.signal });
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();

    await reader.read(); // connected message

    broadcast.broadcast({
      cursor: "600",
      checkpoint: {
        sequenceNumber: "600",
        summary: { timestamp: { seconds: "1711929600", nanos: 0 } },
        transactions: [
          { digest: "tx_abc", events: { events: [{ packageId: "", module: "", sender: "0x1", eventType: "E", contents: { name: "E", value: new Uint8Array() } }] } },
          { digest: "tx_def", events: null },
        ],
      },
    }, "live");

    const { value } = await reader.read();
    const text = decoder.decode(value);

    expect(text).toContain('"digest":"tx_abc"');
    expect(text).toContain('"eventCount":1');
    expect(text).toContain('"digest":"tx_def"');

    controller.abort();
  });

  test("/stream/checkpoints filters by source", async () => {
    const controller = new AbortController();
    const res = await fetch(`${baseUrl}/stream/checkpoints?source=live`, { signal: controller.signal });
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();

    await reader.read(); // connected message

    // Backfill checkpoint — should be filtered
    broadcast.broadcast({
      cursor: "100",
      checkpoint: { sequenceNumber: "100", summary: null, transactions: [] },
    }, "backfill");

    // Live checkpoint — should arrive
    broadcast.broadcast({
      cursor: "200",
      checkpoint: { sequenceNumber: "200", summary: null, transactions: [] },
    }, "live");

    const { value } = await reader.read();
    const text = decoder.decode(value);

    expect(text).toContain('"seq":"200"');
    expect(text).not.toContain('"seq":"100"');

    controller.abort();
  });
});

// ---------------------------------------------------------------------------
// Metrics unit tests
// ---------------------------------------------------------------------------

describe("createMetrics", () => {
  test("snapshot returns initial state", () => {
    const metrics = createMetrics();
    const snap = metrics.snapshot();

    expect(snap.uptime).toBeGreaterThanOrEqual(0);
    expect(snap.live.cursor).toBeNull();
    expect(snap.live.lastFlush).toBeNull();
    expect(snap.backfill.cursor).toBeNull();
    expect(snap.backfill.lastFlush).toBeNull();
    expect(snap.throttle.concurrency).toBe(0);
    expect(snap.throttle.paused).toBe(false);
    expect(snap.counters.liveCheckpoints).toBe(0);
    expect(snap.counters.backfillCheckpoints).toBe(0);
    expect(snap.counters.liveEventsTotal).toBe(0);
    expect(snap.counters.backfillEventsTotal).toBe(0);
    expect(snap.counters.liveFlushes).toBe(0);
    expect(snap.counters.backfillFlushes).toBe(0);
  });

  test("snapshot reflects cursor updates", () => {
    const metrics = createMetrics();
    metrics.setLiveCursor(100n);
    metrics.setBackfillCursor(50n);

    const snap = metrics.snapshot();
    expect(snap.live.cursor).toBe("100");
    expect(snap.backfill.cursor).toBe("50");
  });

  test("snapshot reflects flush stats", () => {
    const metrics = createMetrics();
    metrics.recordLiveFlush(makeFlushStats({ eventsWritten: 42 }));
    metrics.recordBackfillFlush(makeFlushStats({ eventsWritten: 200 }));

    const snap = metrics.snapshot();
    expect(snap.live.lastFlush!.eventsWritten).toBe(42);
    expect(snap.backfill.lastFlush!.eventsWritten).toBe(200);
    expect(snap.live.lastFlush!.timestamp).toBeDefined();
  });

  test("snapshot reflects throttle state", () => {
    const metrics = createMetrics();
    metrics.recordThrottleState(5, true);

    const snap = metrics.snapshot();
    expect(snap.throttle.concurrency).toBe(5);
    expect(snap.throttle.paused).toBe(true);
  });

  test("uptime increases over time", async () => {
    const metrics = createMetrics();
    const snap1 = metrics.snapshot();

    await new Promise((r) => setTimeout(r, 50));

    const snap2 = metrics.snapshot();
    expect(snap2.uptime).toBeGreaterThanOrEqual(snap1.uptime);
  });

  test("latest flush overwrites previous", () => {
    const metrics = createMetrics();
    metrics.recordLiveFlush(makeFlushStats({ eventsWritten: 10 }));
    metrics.recordLiveFlush(makeFlushStats({ eventsWritten: 20 }));

    const snap = metrics.snapshot();
    expect(snap.live.lastFlush!.eventsWritten).toBe(20);
  });

  test("counters accumulate across flushes", () => {
    const metrics = createMetrics();
    metrics.recordLiveFlush(makeFlushStats({ eventsWritten: 10 }));
    metrics.recordLiveFlush(makeFlushStats({ eventsWritten: 20 }));
    metrics.recordBackfillFlush(makeFlushStats({ eventsWritten: 100 }));

    const snap = metrics.snapshot();
    expect(snap.counters.liveEventsTotal).toBe(30);
    expect(snap.counters.backfillEventsTotal).toBe(100);
    expect(snap.counters.liveFlushes).toBe(2);
    expect(snap.counters.backfillFlushes).toBe(1);
  });

  test("checkpoint counters increment", () => {
    const metrics = createMetrics();
    metrics.recordLiveCheckpoint();
    metrics.recordLiveCheckpoint();
    metrics.recordLiveCheckpoint();
    metrics.recordBackfillCheckpoint();

    const snap = metrics.snapshot();
    expect(snap.counters.liveCheckpoints).toBe(3);
    expect(snap.counters.backfillCheckpoints).toBe(1);
  });

  test("prometheus() returns valid prometheus text format", () => {
    const metrics = createMetrics();
    metrics.setLiveCursor(500n);
    metrics.setBackfillCursor(200n);
    metrics.recordLiveCheckpoint();
    metrics.recordLiveCheckpoint();
    metrics.recordBackfillCheckpoint();
    metrics.recordLiveFlush(makeFlushStats({ eventsWritten: 42 }));
    metrics.recordBackfillFlush(makeFlushStats({ eventsWritten: 100 }));
    metrics.recordThrottleState(8, false);

    const text = metrics.prometheus();

    // Verify it's a string with prometheus format lines
    expect(typeof text).toBe("string");
    expect(text).toContain("# HELP jun_uptime_seconds");
    expect(text).toContain("# TYPE jun_uptime_seconds gauge");

    // Gauges
    expect(text).toContain("jun_live_cursor 500");
    expect(text).toContain("jun_backfill_cursor 200");
    expect(text).toContain("jun_backfill_concurrency 8");
    expect(text).toContain("jun_backfill_paused 0");

    // Counters
    expect(text).toContain('jun_checkpoints_processed_total{mode="live"} 2');
    expect(text).toContain('jun_checkpoints_processed_total{mode="backfill"} 1');
    expect(text).toContain('jun_events_flushed_total{buffer="live"} 42');
    expect(text).toContain('jun_events_flushed_total{buffer="backfill"} 100');
    expect(text).toContain('jun_flushes_total{buffer="live"} 1');
    expect(text).toContain('jun_flushes_total{buffer="backfill"} 1');

    // Flush duration (in seconds)
    expect(text).toContain('jun_flush_duration_seconds{buffer="live"}');
    expect(text).toContain('jun_flush_duration_seconds{buffer="backfill"}');

    // Buffer sizes
    expect(text).toContain('jun_buffer_size{buffer="live"} 0');
    expect(text).toContain('jun_buffer_size{buffer="backfill"} 0');
  });

  test("prometheus() with no data returns valid format", () => {
    const metrics = createMetrics();
    const text = metrics.prometheus();

    expect(text).toContain("jun_uptime_seconds");
    expect(text).toContain("jun_live_cursor 0");
    expect(text).toContain("jun_backfill_cursor 0");
    expect(text).toContain('jun_checkpoints_processed_total{mode="live"} 0');
    expect(text).toContain("jun_backfill_paused 0");
  });
});
