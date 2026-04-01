/**
 * jun/pipeline — Internal indexing pipeline components.
 *
 * These are the building blocks that `defineIndexer().run()` uses internally.
 * Import them directly when you need custom control over write buffering,
 * backfill throttling, or gap repair.
 *
 * @example Custom pipeline with manual buffer control
 * ```ts
 * import { createWriteBuffer, createAdaptiveThrottle, createGapDetector } from "jun/pipeline";
 * import { createPostgresOutput } from "jun/output/postgres";
 * import { createStateManager } from "jun/cursor";
 * import { createLogger } from "jun/logger";
 *
 * const log = createLogger();
 * const buffer = createWriteBuffer(output, state, {
 *   label: "custom",
 *   intervalMs: 500,
 *   maxEvents: 200,
 * }, log);
 *
 * buffer.start();
 * // ... push events from your own source
 * await buffer.stop();
 * ```
 */

// Write buffer — batched Postgres writes with backpressure
export { createWriteBuffer } from "./buffer.ts";
export type { WriteBuffer, WriteBufferConfig, FlushStats } from "./buffer.ts";

// Adaptive throttle — concurrency control based on Postgres latency
export { createAdaptiveThrottle } from "./throttle.ts";
export type { AdaptiveThrottle, ThrottleConfig } from "./throttle.ts";

// Gap detector — find and repair missing checkpoints
export { createGapDetector } from "./gaps.ts";
export type { GapDetector } from "./gaps.ts";
