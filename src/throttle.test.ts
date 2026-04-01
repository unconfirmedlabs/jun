import { test, expect, describe } from "bun:test";
import pino from "pino";
import { createAdaptiveThrottle } from "./throttle.ts";

const testLog = pino({ level: "silent" });

describe("AdaptiveThrottle", () => {
  test("returns initial concurrency before baseline", () => {
    const throttle = createAdaptiveThrottle({
      initialConcurrency: 10,
      baselineWindow: 5,
    }, testLog);

    expect(throttle.getConcurrency()).toBe(10);

    // Record 3 latencies (less than baseline window of 5)
    throttle.recordLatency(100);
    throttle.recordLatency(110);
    throttle.recordLatency(90);

    // Still at initial concurrency — baseline not yet established
    expect(throttle.getConcurrency()).toBe(10);
  });

  test("establishes baseline after N flushes", () => {
    const throttle = createAdaptiveThrottle({
      initialConcurrency: 10,
      baselineWindow: 3,
    }, testLog);

    throttle.recordLatency(100);
    throttle.recordLatency(110);
    throttle.recordLatency(90);

    // Baseline established. Next latency triggers actual evaluation.
    // Record a normal latency — no change expected
    throttle.recordLatency(105);
    expect(throttle.getConcurrency()).toBe(10);
  });

  test("scales down when p95 exceeds 2x baseline", () => {
    const throttle = createAdaptiveThrottle({
      initialConcurrency: 10,
      baselineWindow: 3,
      scaleDownThreshold: 2.0,
    }, testLog);

    // Establish baseline (p95 ~110ms)
    throttle.recordLatency(100);
    throttle.recordLatency(110);
    throttle.recordLatency(90);

    // Record one high latency (>2x baseline) — triggers single halving
    throttle.recordLatency(500);

    // Should have halved concurrency once: 10 → 5
    expect(throttle.getConcurrency()).toBe(5);
  });

  test("does not scale below minConcurrency", () => {
    const throttle = createAdaptiveThrottle({
      initialConcurrency: 2,
      minConcurrency: 1,
      baselineWindow: 3,
      scaleDownThreshold: 2.0,
    }, testLog);

    throttle.recordLatency(100);
    throttle.recordLatency(110);
    throttle.recordLatency(90);

    // Multiple scale-downs
    for (let i = 0; i < 20; i++) {
      throttle.recordLatency(500);
    }

    expect(throttle.getConcurrency()).toBe(1);
  });

  test("scales up after sustained good latency", () => {
    const throttle = createAdaptiveThrottle({
      initialConcurrency: 5,
      baselineWindow: 3,
      scaleUpThreshold: 1.2,
      scaleUpSustainMs: 50, // Very short for testing
    }, testLog);

    // Establish baseline (p95 ~110ms)
    throttle.recordLatency(100);
    throttle.recordLatency(110);
    throttle.recordLatency(90);

    // Record good latencies below 1.2x baseline
    throttle.recordLatency(50);

    // Wait for sustain period
    const start = Date.now();
    while (Date.now() - start < 60) {
      // busy wait
    }

    // This should trigger scale up
    throttle.recordLatency(50);

    expect(throttle.getConcurrency()).toBe(6);
  });

  test("does not scale above maxConcurrency", () => {
    const throttle = createAdaptiveThrottle({
      initialConcurrency: 63,
      maxConcurrency: 64,
      baselineWindow: 3,
      scaleUpThreshold: 1.2,
      scaleUpSustainMs: 10,
    }, testLog);

    throttle.recordLatency(100);
    throttle.recordLatency(110);
    throttle.recordLatency(90);

    // Sustained good latency
    throttle.recordLatency(50);
    const start = Date.now();
    while (Date.now() - start < 20) {}
    throttle.recordLatency(50); // → 64

    const start2 = Date.now();
    while (Date.now() - start2 < 20) {}
    throttle.recordLatency(50); // Should stay at 64

    expect(throttle.getConcurrency()).toBe(64);
  });

  test("consecutive failures trigger pause", () => {
    const throttle = createAdaptiveThrottle({
      initialConcurrency: 10,
      failurePauseCount: 3,
      pauseDurationMs: 100,
    }, testLog);

    expect(throttle.isPaused()).toBe(false);

    throttle.recordFailure();
    throttle.recordFailure();
    expect(throttle.isPaused()).toBe(false);

    throttle.recordFailure(); // 3rd failure → pause
    expect(throttle.isPaused()).toBe(true);
    expect(throttle.getConcurrency()).toBe(1); // Reset to min
  });

  test("successful flush resets failure counter", () => {
    const throttle = createAdaptiveThrottle({
      initialConcurrency: 10,
      baselineWindow: 3,
      failurePauseCount: 3,
    }, testLog);

    // Establish baseline
    throttle.recordLatency(100);
    throttle.recordLatency(100);
    throttle.recordLatency(100);

    // 2 failures, then success, then 2 failures — should NOT pause
    throttle.recordFailure();
    throttle.recordFailure();
    throttle.recordLatency(100); // Resets counter
    throttle.recordFailure();
    throttle.recordFailure();

    expect(throttle.isPaused()).toBe(false);
  });

  test("waitIfPaused resolves after pause expires", async () => {
    const throttle = createAdaptiveThrottle({
      initialConcurrency: 10,
      failurePauseCount: 1,
      pauseDurationMs: 50,
    }, testLog);

    throttle.recordFailure(); // Immediate pause
    expect(throttle.isPaused()).toBe(true);

    const start = Date.now();
    await throttle.waitIfPaused();
    const elapsed = Date.now() - start;

    expect(elapsed).toBeGreaterThanOrEqual(40); // Allow some tolerance
    expect(throttle.isPaused()).toBe(false);
  });

  test("waitIfPaused resolves immediately when not paused", async () => {
    const throttle = createAdaptiveThrottle({
      initialConcurrency: 10,
    }, testLog);

    const start = Date.now();
    await throttle.waitIfPaused();
    const elapsed = Date.now() - start;

    expect(elapsed).toBeLessThan(10);
  });

  test("scale down then scale up recovery", () => {
    const throttle = createAdaptiveThrottle({
      initialConcurrency: 10,
      baselineWindow: 3,
      scaleDownThreshold: 2.0,
      scaleUpThreshold: 1.2,
      scaleUpSustainMs: 50,
    }, testLog);

    // Establish baseline (p95 ~110ms)
    throttle.recordLatency(100);
    throttle.recordLatency(110);
    throttle.recordLatency(90);

    // Spike latency to trigger scale-down: 10 → 5
    throttle.recordLatency(500);
    expect(throttle.getConcurrency()).toBe(5);

    // The rolling window still has [100, 110, 90, 500], so p95 is still high.
    // Flood with good latencies to push out the spike and bring p95 below 1.2x baseline.
    // Need enough entries that p95 of the window is below 1.2 * 110 = 132.
    // Each additional 50ms pushes the 500ms further from p95 position.
    for (let i = 0; i < 50; i++) {
      throttle.recordLatency(50);
    }

    // At this point the window is full of 50ms values, p95 < 132, goodSince is set.
    // Wait for scaleUpSustainMs to elapse
    const start = Date.now();
    while (Date.now() - start < 60) {}

    // Concurrency was halved further while high p95 persisted, now at min.
    // Record one more good latency to trigger scale-up evaluation.
    const before = throttle.getConcurrency();
    throttle.recordLatency(50);
    expect(throttle.getConcurrency()).toBe(before + 1);
  });

  test("latency between thresholds resets goodSince", () => {
    // Baseline = 100ms. scaleUpThreshold 1.2 → p95 < 120 is "good".
    // scaleDownThreshold 2.0 → p95 > 200 is "bad". Between 120-200 resets goodSince.
    const throttle = createAdaptiveThrottle({
      initialConcurrency: 10,
      baselineWindow: 3,
      scaleDownThreshold: 2.0,
      scaleUpThreshold: 1.2,
      scaleUpSustainMs: 80,
    }, testLog);

    // Establish baseline (p95 = 100ms)
    throttle.recordLatency(100);
    throttle.recordLatency(100);
    throttle.recordLatency(100);
    // Window: [100, 100, 100]. Baseline p95 = 100.

    // Fill window entirely with good latencies so p95 = 50 < 120
    for (let i = 0; i < 50; i++) {
      throttle.recordLatency(50);
    }
    // Window: 50 x 50ms. p95 = 50. goodSince is set.

    // Wait partway through sustain
    const start1 = Date.now();
    while (Date.now() - start1 < 40) {}

    // Inject "between threshold" entries. We need p95 index floor(50*0.95) = 47 to be >= 120.
    // Need >= 3 entries of 150 (since they'd occupy sorted positions 47, 48, 49).
    // But each push evicts old 50s, so the 150s accumulate in sorted top. Push 4 to be safe.
    throttle.recordLatency(150);
    throttle.recordLatency(150);
    throttle.recordLatency(150);
    // Window: 47 x 50ms, 3 x 150ms. sorted[47] = 150. p95 = 150.
    // 120 < 150 < 200 → between thresholds → goodSince reset to null.

    // Verify no scale change
    expect(throttle.getConcurrency()).toBe(10);

    // Now we need to get p95 back below 120 so goodSince gets freshly set.
    // The 3 entries of 150 sit in the window. We need to flush them out (push 47 more 50s
    // to evict all entries before the 150s, then 3 more to evict the 150s themselves).
    // Total: 50 entries of 50ms to completely fill window with 50s again.
    for (let i = 0; i < 50; i++) {
      throttle.recordLatency(50);
    }
    // Window: 50 x 50ms. p95 = 50 < 120. goodSince freshly set.

    // Wait less than sustain period — should NOT have scaled up
    const start2 = Date.now();
    while (Date.now() - start2 < 40) {}

    throttle.recordLatency(50);
    expect(throttle.getConcurrency()).toBe(10);

    // Wait the full sustain period from when goodSince was set
    const start3 = Date.now();
    while (Date.now() - start3 < 90) {}

    throttle.recordLatency(50);
    expect(throttle.getConcurrency()).toBe(11);
  });

  test("rolling window overflow (>50 latencies)", () => {
    const throttle = createAdaptiveThrottle({
      initialConcurrency: 10,
      baselineWindow: 3,
      scaleDownThreshold: 2.0,
    }, testLog);

    // Establish baseline (p95 ~100ms)
    throttle.recordLatency(100);
    throttle.recordLatency(100);
    throttle.recordLatency(100);

    // Record 50 high latencies (>2x baseline) — fills and overflows the 50-entry window
    // After first high latency, concurrency halves: 10→5→3→2→1 (floors at min=1)
    // But we want to verify the window behavior, so check p95 effect.
    // Re-create with higher min so we can observe window effects.
    const throttle2 = createAdaptiveThrottle({
      initialConcurrency: 10,
      minConcurrency: 1,
      baselineWindow: 3,
      scaleDownThreshold: 2.0,
      scaleUpThreshold: 1.2,
      scaleUpSustainMs: 10,
    }, testLog);

    // Establish baseline
    throttle2.recordLatency(100);
    throttle2.recordLatency(100);
    throttle2.recordLatency(100);

    // Record 50 high latencies — will cause scale-downs
    for (let i = 0; i < 50; i++) {
      throttle2.recordLatency(300);
    }

    // Concurrency should have been halved multiple times, bottoming at 1
    expect(throttle2.getConcurrency()).toBe(1);

    // Now record 10 low latencies — these replace part of the rolling window
    // but p95 should still be high since only 10 of 50 entries are low
    for (let i = 0; i < 10; i++) {
      throttle2.recordLatency(50);
    }

    // The rolling window is capped at 50 entries.
    // After 50 high + 10 low, window has 40 high + 10 low.
    // p95 of that is still 300 (> 2x baseline), so concurrency stays at 1.
    expect(throttle2.getConcurrency()).toBe(1);
  });

  test("multiple pause/unpause cycles", async () => {
    const throttle = createAdaptiveThrottle({
      initialConcurrency: 10,
      failurePauseCount: 2,
      pauseDurationMs: 50,
    }, testLog);

    // First pause cycle
    throttle.recordFailure();
    throttle.recordFailure();
    expect(throttle.isPaused()).toBe(true);
    expect(throttle.getConcurrency()).toBe(1);

    // Wait for pause to expire
    await throttle.waitIfPaused();
    expect(throttle.isPaused()).toBe(false);

    // Second pause cycle — trigger another pause
    throttle.recordFailure();
    throttle.recordFailure();
    expect(throttle.isPaused()).toBe(true);
    expect(throttle.getConcurrency()).toBe(1);

    // Wait again
    await throttle.waitIfPaused();
    expect(throttle.isPaused()).toBe(false);
  });

  test("baselineWindow of 1", () => {
    const throttle = createAdaptiveThrottle({
      initialConcurrency: 10,
      baselineWindow: 1,
      scaleDownThreshold: 2.0,
    }, testLog);

    // Single latency establishes baseline immediately
    throttle.recordLatency(100);

    // Next latency triggers evaluation — high latency should scale down
    throttle.recordLatency(500);
    expect(throttle.getConcurrency()).toBe(5);
  });

  test("high latency during baseline does NOT trigger scale-down", () => {
    const throttle = createAdaptiveThrottle({
      initialConcurrency: 10,
      baselineWindow: 5,
      scaleDownThreshold: 2.0,
    }, testLog);

    // Record very high latencies during baseline establishment
    throttle.recordLatency(1000);
    throttle.recordLatency(2000);
    throttle.recordLatency(3000);
    throttle.recordLatency(5000);

    // Concurrency should remain unchanged — still in baseline phase
    expect(throttle.getConcurrency()).toBe(10);

    // 5th latency establishes baseline, but itself is not evaluated
    throttle.recordLatency(4000);
    expect(throttle.getConcurrency()).toBe(10);
  });
});
