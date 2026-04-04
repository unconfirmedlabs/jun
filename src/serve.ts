/**
 * jun/serve — HTTP query API for the indexer.
 *
 * Opt-in HTTP server that runs alongside the indexer in the same process.
 * Provides health checks, indexer status, and read-only SQL queries
 * against the indexed event tables in Postgres.
 *
 * @example
 * ```ts
 * await indexer.run({
 *   serve: { port: 8080 },
 * });
 * ```
 */
import type { Logger } from "./logger.ts";
import type { StateManager } from "./state.ts";
import type { FlushStats } from "./buffer.ts";
import type { BroadcastManager, SSEStreamType, SSEClient } from "./broadcast.ts";
import type { HotReloadContext } from "./hot-reload.ts";
import { stripComments, isReadOnly } from "./sql-helpers.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ServeConfig {
  /** Port to listen on */
  port: number;
  /** Hostname to bind (default: "127.0.0.1") */
  hostname?: string;
}

export interface ServeContext {
  sql: any;
  state: StateManager;
  metrics: IndexerMetrics;
  broadcast: BroadcastManager;
  hotReload?: HotReloadContext;
  log: Logger;
}

export interface IndexerServer {
  stop(): Promise<void>;
  readonly port: number;
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

interface FlushSnapshot {
  eventsWritten: number;
  tablesWritten: number;
  flushDurationMs: number;
  bufferSizeAfter: number;
  timestamp: string;
}

export interface MetricsSnapshot {
  uptime: number;
  live: { cursor: string | null; lastFlush: FlushSnapshot | null };
  backfill: { cursor: string | null; lastFlush: FlushSnapshot | null };
  throttle: { concurrency: number; paused: boolean };
  counters: {
    liveCheckpoints: number;
    backfillCheckpoints: number;
    liveEventsTotal: number;
    backfillEventsTotal: number;
    liveFlushes: number;
    backfillFlushes: number;
  };
}

export interface IndexerMetrics {
  setLiveCursor(seq: bigint): void;
  setBackfillCursor(seq: bigint): void;
  recordLiveCheckpoint(): void;
  recordBackfillCheckpoint(): void;
  recordLiveFlush(stats: FlushStats): void;
  recordBackfillFlush(stats: FlushStats): void;
  recordThrottleState(concurrency: number, paused: boolean): void;
  snapshot(): MetricsSnapshot;
  prometheus(): string;
}

export function createMetrics(): IndexerMetrics {
  let liveCursor: bigint | null = null;
  let backfillCursor: bigint | null = null;
  let liveFlush: FlushSnapshot | null = null;
  let backfillFlush: FlushSnapshot | null = null;
  let throttleConcurrency = 0;
  let throttlePaused = false;
  const startedAt = Date.now();

  // Cumulative counters
  let liveCheckpoints = 0;
  let backfillCheckpoints = 0;
  let liveEventsTotal = 0;
  let backfillEventsTotal = 0;
  let liveFlushes = 0;
  let backfillFlushes = 0;
  let liveLastFlushDurationMs = 0;
  let backfillLastFlushDurationMs = 0;
  let liveBufferSize = 0;
  let backfillBufferSize = 0;

  function toFlushSnapshot(stats: FlushStats): FlushSnapshot {
    return {
      eventsWritten: stats.eventsWritten,
      tablesWritten: stats.tablesWritten,
      flushDurationMs: Math.round(stats.flushDurationMs),
      bufferSizeAfter: stats.bufferSizeAfter,
      timestamp: new Date().toISOString(),
    };
  }

  return {
    setLiveCursor(seq) { liveCursor = seq; },
    setBackfillCursor(seq) { backfillCursor = seq; },
    recordLiveCheckpoint() { liveCheckpoints++; },
    recordBackfillCheckpoint() { backfillCheckpoints++; },
    recordLiveFlush(stats) {
      liveFlush = toFlushSnapshot(stats);
      liveFlushes++;
      liveEventsTotal += stats.eventsWritten;
      liveLastFlushDurationMs = stats.flushDurationMs;
      liveBufferSize = stats.bufferSizeAfter;
    },
    recordBackfillFlush(stats) {
      backfillFlush = toFlushSnapshot(stats);
      backfillFlushes++;
      backfillEventsTotal += stats.eventsWritten;
      backfillLastFlushDurationMs = stats.flushDurationMs;
      backfillBufferSize = stats.bufferSizeAfter;
    },
    recordThrottleState(concurrency, paused) {
      throttleConcurrency = concurrency;
      throttlePaused = paused;
    },
    snapshot(): MetricsSnapshot {
      return {
        uptime: Math.round((Date.now() - startedAt) / 1000),
        live: { cursor: liveCursor?.toString() ?? null, lastFlush: liveFlush },
        backfill: { cursor: backfillCursor?.toString() ?? null, lastFlush: backfillFlush },
        throttle: { concurrency: throttleConcurrency, paused: throttlePaused },
        counters: {
          liveCheckpoints,
          backfillCheckpoints,
          liveEventsTotal,
          backfillEventsTotal,
          liveFlushes,
          backfillFlushes,
        },
      };
    },
    prometheus(): string {
      const uptimeSec = (Date.now() - startedAt) / 1000;
      const lines: string[] = [];

      const gauge = (name: string, help: string, value: number | string) => {
        lines.push(`# HELP ${name} ${help}`);
        lines.push(`# TYPE ${name} gauge`);
        lines.push(`${name} ${value}`);
      };

      const counter = (name: string, help: string, value: number) => {
        lines.push(`# HELP ${name} ${help}`);
        lines.push(`# TYPE ${name} counter`);
        lines.push(`${name} ${value}`);
      };

      // Uptime
      gauge("jun_uptime_seconds", "Seconds since indexer started", uptimeSec.toFixed(1));

      // Cursors
      gauge("jun_live_cursor", "Latest live checkpoint sequence", liveCursor?.toString() ?? "0");
      gauge("jun_backfill_cursor", "Latest backfill checkpoint sequence", backfillCursor?.toString() ?? "0");

      // Checkpoint counters
      counter("jun_checkpoints_processed_total{mode=\"live\"}", "Total checkpoints processed by live loop", liveCheckpoints);
      counter("jun_checkpoints_processed_total{mode=\"backfill\"}", "Total checkpoints processed by backfill loop", backfillCheckpoints);

      // Event counters
      counter("jun_events_flushed_total{buffer=\"live\"}", "Total events flushed to Postgres by live buffer", liveEventsTotal);
      counter("jun_events_flushed_total{buffer=\"backfill\"}", "Total events flushed to Postgres by backfill buffer", backfillEventsTotal);

      // Flush counters
      counter("jun_flushes_total{buffer=\"live\"}", "Total buffer flushes by live buffer", liveFlushes);
      counter("jun_flushes_total{buffer=\"backfill\"}", "Total buffer flushes by backfill buffer", backfillFlushes);

      // Flush latency (last value)
      gauge("jun_flush_duration_seconds{buffer=\"live\"}", "Last flush duration in seconds for live buffer", (liveLastFlushDurationMs / 1000).toFixed(4));
      gauge("jun_flush_duration_seconds{buffer=\"backfill\"}", "Last flush duration in seconds for backfill buffer", (backfillLastFlushDurationMs / 1000).toFixed(4));

      // Buffer depth
      gauge("jun_buffer_size{buffer=\"live\"}", "Current events in live buffer", liveBufferSize);
      gauge("jun_buffer_size{buffer=\"backfill\"}", "Current events in backfill buffer", backfillBufferSize);

      // Throttle
      gauge("jun_backfill_concurrency", "Current adaptive backfill concurrency", throttleConcurrency);
      gauge("jun_backfill_paused", "Whether backfill is paused (1=paused, 0=running)", throttlePaused ? 1 : 0);

      lines.push("");
      return lines.join("\n");
    },
  };
}

// ---------------------------------------------------------------------------
// SSE handler
// ---------------------------------------------------------------------------

function handleSSEStream(
  streamType: SSEStreamType,
  url: URL,
  broadcast: BroadcastManager,
  log: Logger,
): Response {
  const source = url.searchParams.get("source") ?? undefined;
  const handler = url.searchParams.get("handler") ?? undefined;
  const sender = url.searchParams.get("sender") ?? undefined;
  const eventType = url.searchParams.get("type") ?? undefined;

  if (source && source !== "live" && source !== "backfill") {
    return Response.json({ error: 'source must be "live" or "backfill"' }, { status: 400 });
  }

  let cleanup: (() => void) | null = null;

  const readable = new ReadableStream<string>({
    start(controller) {
      const client: SSEClient = { controller, stream: streamType, handler, source, sender, eventType };
      cleanup = broadcast.addSSEClient(client);

      const filters: Record<string, string | null> = { source: source ?? null };
      if (streamType === "events") filters.handler = handler ?? null;
      if (streamType === "transactions") filters.sender = sender ?? null;
      if (streamType === "broadcast/events") {
        filters.type = eventType ?? null;
        filters.sender = sender ?? null;
      }

      controller.enqueue(`data: ${JSON.stringify({ type: "connected", stream: streamType, ...filters })}\n\n`);
      log.debug({ stream: streamType, ...filters, clients: broadcast.sseClientCount() }, "SSE client connected");
    },
    cancel() {
      cleanup?.();
      log.debug({ stream: streamType, clients: broadcast.sseClientCount() }, "SSE client disconnected");
    },
  });

  return new Response(readable, {
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      "connection": "keep-alive",
    },
  });
}

// ---------------------------------------------------------------------------
// Query handler
// ---------------------------------------------------------------------------

async function handleQuery(
  url: URL,
  sql: any,
  log: Logger,
): Promise<Response> {
  const rawSql = url.searchParams.get("sql");
  if (!rawSql) {
    return Response.json({ error: "missing ?sql= parameter" }, { status: 400 });
  }

  if (!isReadOnly(rawSql)) {
    return Response.json(
      { error: "only SELECT, WITH, and EXPLAIN queries are allowed" },
      { status: 403 },
    );
  }

  const limitParam = parseInt(url.searchParams.get("limit") ?? "1000", 10);
  const rowLimit = Math.min(Math.max(1, limitParam), 10000);
  const timeoutParam = parseInt(url.searchParams.get("timeout") ?? "5000", 10);
  const timeoutMs = Math.min(Math.max(100, timeoutParam), 30000);

  try {
    const rows = await sql.begin(async (transaction: any) => {
      await transaction.unsafe(`SET TRANSACTION READ ONLY`);
      await transaction.unsafe(`SET LOCAL statement_timeout = '${timeoutMs}ms'`);
      const stmt = rawSql.trim().replace(/;+\s*$/, "");
      return transaction.unsafe(`SELECT * FROM (${stmt}) AS _q LIMIT ${rowLimit + 1}`);
    });

    const result = Array.from(rows);
    const truncated = result.length > rowLimit;
    const sliced = truncated ? result.slice(0, rowLimit) : result;

    return Response.json({ rows: sliced, count: sliced.length, truncated });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    log.warn({ err: message, sql: rawSql.slice(0, 200) }, "query error");
    return Response.json({ error: message }, { status: 400 });
  }
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Admin reload handler
// ---------------------------------------------------------------------------

async function handleReload(
  req: Request,
  hotReload: HotReloadContext | undefined,
  log: Logger,
): Promise<Response> {
  const token = process.env.JUN_ADMIN_TOKEN;
  if (token) {
    const auth = req.headers.get("authorization");
    if (auth !== `Bearer ${token}`) {
      return Response.json({ error: "unauthorized" }, { status: 401 });
    }
  }

  if (!hotReload) {
    return Response.json({ error: "hot reload not available (run mode only)" }, { status: 501 });
  }

  try {
    // Limit request body to 1MB to prevent DoS
    const contentLength = parseInt(req.headers.get("content-length") ?? "0", 10);
    if (contentLength > 1_000_000) {
      return Response.json({ error: "request body too large (max 1MB)" }, { status: 413 });
    }

    const body = await req.text();

    let yamlContent: string;
    if (body.trim()) {
      // Body provided — use it directly
      yamlContent = body;
    } else if (hotReload.configUrl) {
      // No body — re-fetch from configured remote URL
      const { fetchRemoteConfig } = await import("./remote-config.ts");
      const result = await fetchRemoteConfig(hotReload.configUrl);
      yamlContent = result!.content;
      log.info({ url: hotReload.configUrl }, "fetched remote config");
    } else {
      return Response.json({ error: "request body must contain YAML config (or set configUrl for remote fetch)" }, { status: 400 });
    }

    const { parsePipelineConfig } = await import("./pipeline/config-parser.ts");
    const { applyReload } = await import("./hot-reload.ts");

    const parsed = parsePipelineConfig(yamlContent);
    // Extract event handlers from parsed processors
    const eventProc = parsed.processors.find(p => p.name === "events");
    const events = eventProc ? (eventProc as any)._reloadConfig ?? {} : {};
    const result = await applyReload(hotReload, events);

    log.info({ added: result.added, removed: result.removed, altered: result.altered }, "config reloaded");

    return Response.json(result);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    log.error({ err: message }, "reload failed");
    return Response.json({ error: message }, { status: 400 });
  }
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

export function createServer(config: ServeConfig, ctx: ServeContext): IndexerServer {
  const { sql, metrics, broadcast, hotReload, log: parentLog } = ctx;
  const log = parentLog.child({ component: "serve" });

  const server = Bun.serve({
    port: config.port,
    hostname: config.hostname ?? "127.0.0.1",
    routes: {
      "/health": () => Response.json({ status: "ok" }),
      "/status": () => Response.json(metrics.snapshot()),
      "/metrics": () => new Response(metrics.prometheus(), {
        headers: { "content-type": "text/plain; version=0.0.4; charset=utf-8" },
      }),
    },
    async fetch(req) {
      const url = new URL(req.url);
      if (url.pathname === "/query") {
        return handleQuery(url, sql, log);
      }
      if (url.pathname === "/stream/checkpoints") {
        return handleSSEStream("checkpoints", url, broadcast, log);
      }
      if (url.pathname === "/stream/transactions") {
        return handleSSEStream("transactions", url, broadcast, log);
      }
      if (url.pathname === "/stream/events") {
        return handleSSEStream("events", url, broadcast, log);
      }
      if (url.pathname === "/broadcast/events") {
        return handleSSEStream("broadcast/events", url, broadcast, log);
      }
      if (url.pathname === "/admin/reload" && req.method === "POST") {
        return handleReload(req, hotReload, log);
      }
      return Response.json({ error: "not found" }, { status: 404 });
    },
  });

  log.info({ port: server.port, hostname: config.hostname ?? "127.0.0.1" }, "started");

  return {
    get port() { return server.port; },
    async stop() {
      await server.stop();
      log.info("stopped");
    },
  };
}
