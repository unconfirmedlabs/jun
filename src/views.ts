/**
 * jun/views — Materialized view lifecycle management.
 *
 * Creates Postgres materialized views on startup and refreshes them
 * on a configurable timer. YAML-config-only feature — not part of
 * the programmatic pipeline API.
 *
 * @example YAML config
 * ```yaml
 * views:
 *   daily_presses:
 *     sql: |
 *       SELECT date_trunc('day', sui_timestamp) AS day, count(*) AS presses
 *       FROM record_pressed
 *       GROUP BY 1
 *     refresh: 60s
 * ```
 */
import type { Logger } from "./logger.ts";
import { validateIdentifier } from "./output/storage.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ViewDef {
  /** SQL query for the materialized view */
  sql: string;
  /** Refresh interval as duration string: "30s", "5m", "1h" */
  refresh: string;
}

export interface ViewManager {
  /** Start periodic refresh timers for all views */
  start(): void;
  /** Stop all refresh timers */
  stop(): void;
}

// ---------------------------------------------------------------------------
// Duration parsing
// ---------------------------------------------------------------------------

/** Parse a duration string ("30s", "5m", "1h") to milliseconds */
export function parseDuration(duration: string): number {
  const match = duration.match(/^(\d+)(s|m|h)$/);
  if (!match) {
    throw new Error(`Invalid duration "${duration}". Expected format: 30s, 5m, 1h`);
  }
  const value = parseInt(match[1]!, 10);
  const unit = match[2]!;
  switch (unit) {
    case "s": return value * 1000;
    case "m": return value * 60 * 1000;
    case "h": return value * 60 * 60 * 1000;
    default: throw new Error(`Unknown duration unit: ${unit}`);
  }
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

/**
 * Create and initialize a ViewManager.
 * Creates materialized views in Postgres on construction.
 */
export async function createViewManager(
  sql: any,
  views: Record<string, ViewDef>,
  log: Logger,
): Promise<ViewManager> {
  const viewLog = log.child({ component: "views" });
  const timers: Timer[] = [];

  // Create materialized views on startup
  for (const [name, def] of Object.entries(views)) {
    validateIdentifier(name);
    const intervalMs = parseDuration(def.refresh);
    viewLog.info({ view: name, refresh: def.refresh, intervalMs }, "creating");

    try {
      await sql.unsafe(`CREATE MATERIALIZED VIEW IF NOT EXISTS ${name} AS ${def.sql}`);
      viewLog.info({ view: name }, "created");
    } catch (err) {
      viewLog.error({ view: name, err }, "failed to create view");
      throw err;
    }
  }

  async function refreshView(name: string): Promise<void> {
    const start = performance.now();
    try {
      await sql.unsafe(`REFRESH MATERIALIZED VIEW ${name}`);
      const durationMs = Math.round(performance.now() - start);
      viewLog.info({ view: name, durationMs }, "refreshed");
    } catch (err) {
      viewLog.error({ view: name, err }, "refresh failed");
      // Don't throw — refresh errors shouldn't crash the indexer
    }
  }

  return {
    start() {
      for (const [name, def] of Object.entries(views)) {
        const intervalMs = parseDuration(def.refresh);

        // Initial refresh
        refreshView(name);

        // Periodic refresh
        const timer = setInterval(() => refreshView(name), intervalMs);
        timers.push(timer);
        viewLog.debug({ view: name, intervalMs }, "refresh timer started");
      }
    },

    stop() {
      for (const timer of timers) {
        clearInterval(timer);
      }
      timers.length = 0;
      viewLog.info("all refresh timers stopped");
    },
  };
}
