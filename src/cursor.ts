/**
 * jun/cursor — Checkpoint cursor tracking and resumability.
 *
 * Manages the `indexer_checkpoints` and `processed_checkpoints` tables
 * in Postgres. Tracks where live and backfill modes left off so the
 * indexer can resume after restarts.
 *
 * @example
 * ```ts
 * import { createStateManager } from "jun/cursor";
 *
 * const state = await createStateManager(sql);
 * const cursor = await state.getCheckpointCursor("live:mainnet");
 * ```
 */

export { createStateManager } from "./state.ts";
export type { StateManager } from "./state.ts";
