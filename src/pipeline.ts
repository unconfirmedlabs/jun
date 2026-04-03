/**
 * jun/pipeline — Internal pipeline components.
 *
 * Low-level building blocks for custom pipeline control.
 * Most users should use `createPipeline()` from "jun" instead.
 *
 * @example
 * ```ts
 * import { createAdaptiveThrottle, createGapDetector } from "jun/pipeline";
 * ```
 */

// Write buffer — batched writes with backpressure
export { createWriteBuffer } from "./buffer.ts";
export type { WriteBuffer, WriteBufferConfig, FlushStats } from "./buffer.ts";

// Adaptive throttle — concurrency control based on Postgres latency
export { createAdaptiveThrottle } from "./throttle.ts";
export type { AdaptiveThrottle, ThrottleConfig } from "./throttle.ts";

// Gap detector — find and repair missing checkpoints
export { createGapDetector } from "./gaps.ts";
export type { GapDetector } from "./gaps.ts";
