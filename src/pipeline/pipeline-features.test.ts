/**
 * Feature parity baseline tests.
 *
 * These exercise the standalone modules that the pipeline integrates.
 * They must pass before AND after the defineIndexer → pipeline migration.
 * They do NOT depend on defineIndexer — they test the shared utilities directly.
 */
import { test, expect, describe, afterEach } from "bun:test";
import { createMetrics, type IndexerMetrics } from "../serve.ts";
import { createAdaptiveThrottle, type AdaptiveThrottle } from "../throttle.ts";
import { createLogger } from "../logger.ts";
import { createPipeline } from "./pipeline.ts";
import { parsePipelineConfig } from "./config-parser.ts";
import { fetchRemoteConfig } from "../remote-config.ts";
import { tmpdir } from "os";
import { join } from "path";
import { unlinkSync } from "fs";
import type { Source, Processor, Checkpoint, ProcessedCheckpoint } from "./types.ts";

const log = createLogger();

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

describe("metrics (standalone)", () => {
  test("creates metrics with zeroed counters", () => {
    const m = createMetrics();
    const snap = m.snapshot();
    expect(snap.counters.liveCheckpoints).toBe(0);
    expect(snap.counters.backfillCheckpoints).toBe(0);
    expect(snap.counters.liveEventsTotal).toBe(0);
    expect(snap.counters.backfillEventsTotal).toBe(0);
    expect(snap.live.cursor).toBeNull();
    expect(snap.backfill.cursor).toBeNull();
  });

  test("records live checkpoint cursor and count", () => {
    const m = createMetrics();
    m.setLiveCursor(100n);
    m.recordLiveCheckpoint();
    m.recordLiveCheckpoint();
    const snap = m.snapshot();
    expect(snap.live.cursor).toBe("100");
    expect(snap.counters.liveCheckpoints).toBe(2);
  });

  test("records backfill checkpoint cursor and count", () => {
    const m = createMetrics();
    m.setBackfillCursor(500n);
    m.recordBackfillCheckpoint();
    const snap = m.snapshot();
    expect(snap.backfill.cursor).toBe("500");
    expect(snap.counters.backfillCheckpoints).toBe(1);
  });

  test("records flush stats", () => {
    const m = createMetrics();
    m.recordLiveFlush({
      eventsWritten: 10,
      tablesWritten: 2,
      flushDurationMs: 15.5,
      cursorKeys: new Map(),
      bufferSizeAfter: 0,
    });
    const snap = m.snapshot();
    expect(snap.counters.liveFlushes).toBe(1);
    expect(snap.counters.liveEventsTotal).toBe(10);
    expect(snap.live.lastFlush).not.toBeNull();
    expect(snap.live.lastFlush!.eventsWritten).toBe(10);
  });

  test("records throttle state", () => {
    const m = createMetrics();
    m.recordThrottleState(8, false);
    const snap = m.snapshot();
    expect(snap.throttle.concurrency).toBe(8);
    expect(snap.throttle.paused).toBe(false);
  });

  test("prometheus output contains expected metric names", () => {
    const m = createMetrics();
    m.setLiveCursor(42n);
    m.recordLiveCheckpoint();
    const prom = m.prometheus();
    expect(prom).toContain("jun_live_cursor");
    expect(prom).toContain("jun_checkpoints_processed_total");
    expect(prom).toContain("jun_uptime_seconds");
  });

  test("uptime increases over time", async () => {
    const m = createMetrics();
    await Bun.sleep(1100); // uptime is in seconds (integer), need >1s
    const snap = m.snapshot();
    expect(snap.uptime).toBeGreaterThanOrEqual(1);
  });
});

// ---------------------------------------------------------------------------
// Adaptive Throttle
// ---------------------------------------------------------------------------

describe("adaptive throttle (standalone)", () => {
  test("starts at initial concurrency", () => {
    const t = createAdaptiveThrottle({ initialConcurrency: 10 }, log);
    expect(t.getConcurrency()).toBe(10);
    expect(t.isPaused()).toBe(false);
  });

  test("does not scale during baseline window", () => {
    const t = createAdaptiveThrottle({
      initialConcurrency: 10,
      baselineWindow: 5,
    }, log);
    // First 5 flushes establish baseline — concurrency shouldn't change
    for (let i = 0; i < 5; i++) {
      t.recordLatency(100);
    }
    expect(t.getConcurrency()).toBe(10);
  });

  test("scales down when latency exceeds threshold", () => {
    const t = createAdaptiveThrottle({
      initialConcurrency: 10,
      baselineWindow: 3,
      scaleDownThreshold: 2.0,
    }, log);
    // Establish baseline at ~100ms
    for (let i = 0; i < 3; i++) t.recordLatency(100);
    // High latency → should scale down
    for (let i = 0; i < 5; i++) t.recordLatency(500);
    expect(t.getConcurrency()).toBeLessThan(10);
  });

  test("pauses after consecutive failures", () => {
    const t = createAdaptiveThrottle({
      initialConcurrency: 10,
      failurePauseCount: 3,
      pauseDurationMs: 100, // short for testing
    }, log);
    t.recordFailure();
    t.recordFailure();
    expect(t.isPaused()).toBe(false);
    t.recordFailure();
    expect(t.isPaused()).toBe(true);
  });

  test("waitIfPaused resolves immediately when not paused", async () => {
    const t = createAdaptiveThrottle({ initialConcurrency: 10 }, log);
    const start = Date.now();
    await t.waitIfPaused();
    expect(Date.now() - start).toBeLessThan(50);
  });
});

// ---------------------------------------------------------------------------
// Pipeline stop() propagation
// ---------------------------------------------------------------------------

describe("pipeline stop propagation", () => {
  test("stop() signals sources to stop", async () => {
    let sourceStopped = false;
    let yielded = 0;

    const mockSource: Source = {
      name: "test-source",
      async *stream() {
        while (!sourceStopped) {
          yielded++;
          yield {
            sequenceNumber: BigInt(yielded),
            timestamp: new Date(),
            transactions: [],
            source: "live" as const,
          };
          await Bun.sleep(10);
        }
      },
      async stop() { sourceStopped = true; },
    };

    const pipeline = createPipeline()
      .source(mockSource)
      .configure({ quiet: true });

    // Run in background, stop after a short delay
    const runPromise = pipeline.run();
    await Bun.sleep(50);
    await pipeline.stop();
    await runPromise;

    expect(sourceStopped).toBe(true);
    expect(yielded).toBeGreaterThan(0);
  });
});

// ---------------------------------------------------------------------------
// Config parse → reload cycle
// ---------------------------------------------------------------------------

describe("config parse + reload", () => {
  const tmpFile = join(tmpdir(), `jun-parity-test-${Date.now()}.yml`);

  afterEach(() => {
    try { unlinkSync(tmpFile); } catch {}
  });

  test("parsePipelineConfig produces processors with reload()", () => {
    const config = parsePipelineConfig(`
sources:
  live:
    grpc: fullnode.testnet.sui.io:443
processors:
  balances:
    coinTypes:
      - "0x2::sui::SUI"
storage:
  postgres:
    url: postgres://localhost/test
`);
    const balanceProc = config.processors.find(p => p.name === "balanceChanges");
    expect(balanceProc).toBeDefined();
    expect(typeof balanceProc!.reload).toBe("function");
  });

  test("event decoder processor has reload()", () => {
    const config = parsePipelineConfig(`
sources:
  live:
    grpc: fullnode.testnet.sui.io:443
processors:
  events:
    TestEvent:
      type: "0x1::m::E"
      fields:
        amount: u64
storage:
  postgres:
    url: postgres://localhost/test
`);
    const eventProc = config.processors.find(p => p.name === "events");
    expect(eventProc).toBeDefined();
    expect(typeof eventProc!.reload).toBe("function");
  });

  test("file:// config reload detects changes via etag", async () => {
    await Bun.write(tmpFile, `
sources:
  live:
    grpc: fullnode.testnet.sui.io:443
processors:
  balances:
    coinTypes:
      - "0x2::sui::SUI"
storage:
  postgres:
    url: postgres://localhost/test
`);

    const first = await fetchRemoteConfig(`file://${tmpFile}`);
    expect(first).not.toBeNull();

    // Same content → etag matches → null
    const same = await fetchRemoteConfig(`file://${tmpFile}`, { etag: first!.etag });
    expect(same).toBeNull();

    // Change content → new etag → returns content
    await Bun.sleep(10);
    await Bun.write(tmpFile, `
sources:
  live:
    grpc: fullnode.testnet.sui.io:443
processors:
  balances:
    coinTypes:
      - "0x2::sui::SUI"
      - "0x9f992cc2430a1f442ca7a5ca7638169f5d5c00e0ebc3977a65e9ac6e497fe5ef::wal::WAL"
storage:
  postgres:
    url: postgres://localhost/test
`);

    const changed = await fetchRemoteConfig(`file://${tmpFile}`, { etag: first!.etag });
    expect(changed).not.toBeNull();
    expect(changed!.content).toContain("WAL");
  });
});

// ---------------------------------------------------------------------------
// BCS schema / field resolution (unchanged by migration)
// ---------------------------------------------------------------------------

describe("schema layer (unchanged)", () => {
  test("buildBcsSchema + generateDDL still work", async () => {
    const { buildBcsSchema, generateDDL } = await import("../schema.ts");
    const fields = { item_id: "address" as const, amount: "u64" as const };
    const schema = buildBcsSchema(fields);
    expect(schema).toBeDefined();

    const ddl = generateDDL("test_event", fields);
    expect(ddl).toContain("CREATE TABLE");
    expect(ddl).toContain("item_id TEXT NOT NULL");
    expect(ddl).toContain("amount NUMERIC NOT NULL");
  });
});
