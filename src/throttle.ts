/**
 * Adaptive backfill throttle.
 *
 * Monitors Postgres INSERT latency and adjusts backfill concurrency:
 * - First N flushes establish a baseline p95 latency
 * - If p95 exceeds 2x baseline → halve concurrency
 * - If p95 stays below 1.2x baseline for 30s → increment concurrency
 * - 3 consecutive flush failures → pause for 60s, reset to min concurrency
 */
import type { Logger } from "./logger.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ThrottleConfig {
  /** Initial concurrency for backfill */
  initialConcurrency: number;
  /** Minimum concurrency (default: 1) */
  minConcurrency?: number;
  /** Maximum concurrency (default: 64) */
  maxConcurrency?: number;
  /** Number of initial flushes to establish baseline (default: 10) */
  baselineWindow?: number;
  /** Scale down when p95 exceeds this multiple of baseline (default: 2.0) */
  scaleDownThreshold?: number;
  /** Scale up when p95 is below this multiple of baseline (default: 1.2) */
  scaleUpThreshold?: number;
  /** Sustain good latency for this duration before scaling up (default: 30000ms) */
  scaleUpSustainMs?: number;
  /** Consecutive failures to trigger pause (default: 3) */
  failurePauseCount?: number;
  /** Pause duration in milliseconds (default: 60000) */
  pauseDurationMs?: number;
}

export interface AdaptiveThrottle {
  /** Get current concurrency value. Called before each backfill batch. */
  getConcurrency(): number;
  /** Report a successful flush latency in milliseconds. */
  recordLatency(durationMs: number): void;
  /** Report a flush failure (retries exhausted). */
  recordFailure(): void;
  /** Whether backfill is currently paused. */
  isPaused(): boolean;
  /** Wait until pause is over. Resolves immediately if not paused. */
  waitIfPaused(): Promise<void>;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function computeP95(arr: number[]): number {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length * 0.95)]!;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

export function createAdaptiveThrottle(config: ThrottleConfig, log: Logger): AdaptiveThrottle {
  const {
    initialConcurrency,
    minConcurrency = 1,
    maxConcurrency = 64,
    baselineWindow = 10,
    scaleDownThreshold = 2.0,
    scaleUpThreshold = 1.2,
    scaleUpSustainMs = 30_000,
    failurePauseCount = 3,
    pauseDurationMs = 60_000,
  } = config;

  let concurrency = initialConcurrency;

  // Latency tracking
  const latencies: number[] = [];
  const maxLatencies = 50;
  const baselineLatencies: number[] = [];
  let baselineP95: number | null = null;

  // Scale-up tracking
  let goodSince: number | null = null;

  // Failure tracking
  let consecutiveFailures = 0;
  let pausedUntil: number | null = null;

  return {
    getConcurrency(): number {
      return concurrency;
    },

    recordLatency(durationMs: number): void {
      // Push to rolling window
      latencies.push(durationMs);
      if (latencies.length > maxLatencies) latencies.shift();

      // Reset failure counter on success
      consecutiveFailures = 0;

      // Baseline establishment phase
      if (baselineP95 === null) {
        baselineLatencies.push(durationMs);
        if (baselineLatencies.length >= baselineWindow) {
          baselineP95 = computeP95(baselineLatencies);
          log.info({ p95: Math.round(baselineP95) }, "baseline established");
        }
        return;
      }

      // Compute current p95
      const p95 = computeP95(latencies);

      log.debug({ p95: Math.round(p95), baseline: Math.round(baselineP95), concurrency }, "recordLatency");

      // Scale down: latency is high
      if (p95 > baselineP95 * scaleDownThreshold) {
        const prev = concurrency;
        concurrency = Math.max(minConcurrency, Math.floor(concurrency / 2));
        goodSince = null;
        if (concurrency !== prev) {
          log.warn({ p95: Math.round(p95), baseline: Math.round(baselineP95), from: prev, to: concurrency }, "scaling down");
        }
        return;
      }

      // Scale up: latency is good and sustained
      if (p95 < baselineP95 * scaleUpThreshold) {
        if (goodSince === null) {
          goodSince = Date.now();
        } else if (Date.now() - goodSince >= scaleUpSustainMs) {
          const prev = concurrency;
          concurrency = Math.min(maxConcurrency, concurrency + 1);
          goodSince = null;
          if (concurrency !== prev) {
            log.info({ from: prev, to: concurrency }, "scaling up");
          }
        }
      } else {
        // Latency is between thresholds — reset sustain timer
        goodSince = null;
      }
    },

    recordFailure(): void {
      consecutiveFailures++;
      if (consecutiveFailures >= failurePauseCount) {
        pausedUntil = Date.now() + pauseDurationMs;
        concurrency = minConcurrency;
        consecutiveFailures = 0;
        log.warn({ failures: failurePauseCount, pauseSeconds: pauseDurationMs / 1000, concurrency }, "pausing backfill");
      }
    },

    isPaused(): boolean {
      return pausedUntil !== null && Date.now() < pausedUntil;
    },

    async waitIfPaused(): Promise<void> {
      if (pausedUntil === null) return;
      const remaining = pausedUntil - Date.now();
      if (remaining <= 0) {
        pausedUntil = null;
        return;
      }
      await sleep(remaining);
      pausedUntil = null;
    },
  };
}
