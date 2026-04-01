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
import type { DecodedEvent } from "./processor.ts";
import type { GrpcCheckpointResponse, GrpcTransaction } from "./grpc.ts";

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
  /** Broadcast a checkpoint to SSE clients on /stream/checkpoints. */
  broadcastCheckpoint(response: GrpcCheckpointResponse, source: "live" | "backfill"): void;
  /** Broadcast decoded events to SSE clients on /stream/events. Called after successful Postgres write. */
  broadcastEvents(events: DecodedEvent[], source: "live" | "backfill"): void;
  /** Register an SSE client. Returns a cleanup function. */
  addSSEClient(client: SSEClient): () => void;
  /** Number of connected SSE clients. */
  sseClientCount(): number;
  snapshot(): MetricsSnapshot;
  prometheus(): string;
}

export type SSEStreamType = "checkpoints" | "transactions" | "events";

export interface SSEClient {
  controller: ReadableStreamDefaultController<string>;
  stream: SSEStreamType;
  /** Filter: handler name (events only) */
  handler?: string;
  /** Filter: "live" | "backfill" */
  source?: string;
  /** Filter: sender address (transactions only) */
  sender?: string;
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

  // SSE client tracking
  const sseClients = new Set<SSEClient>();

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
    broadcastCheckpoint(response: GrpcCheckpointResponse, source: "live" | "backfill"): void {
      if (sseClients.size === 0) return;

      const cp = response.checkpoint;
      const ts = cp.summary?.timestamp;
      const timestamp = ts
        ? new Date(Number(BigInt(ts.seconds) * 1000n + BigInt(Math.floor(ts.nanos / 1_000_000)))).toISOString()
        : null;

      // Checkpoint summary
      const checkpointMsg = `data: ${JSON.stringify({
        seq: response.cursor,
        timestamp,
        txCount: cp.transactions.length,
        source,
      })}\n\n`;

      // Transaction summaries
      const txMessages: { sender: string | null; formatted: string }[] = [];
      for (const tx of cp.transactions) {
        const eventCount = tx.events?.events?.length ?? 0;
        const txSender = (tx as any).transaction?.sender ?? null;
        txMessages.push({
          sender: txSender,
          formatted: `data: ${JSON.stringify({
            checkpoint: response.cursor,
            digest: tx.digest,
            sender: txSender,
            eventCount,
            timestamp,
            source,
          })}\n\n`,
        });
      }

      for (const client of sseClients) {
        try {
          if (client.source && client.source !== source) continue;

          if (client.stream === "checkpoints") {
            client.controller.enqueue(checkpointMsg);
          } else if (client.stream === "transactions") {
            for (const msg of txMessages) {
              if (client.sender && client.sender !== msg.sender) continue;
              client.controller.enqueue(msg.formatted);
            }
          }
        } catch {
          sseClients.delete(client);
        }
      }
    },
    broadcastEvents(events: DecodedEvent[], source: "live" | "backfill"): void {
      if (sseClients.size === 0) return;

      // Serialize each event once, reuse across clients
      const messages: { handlerName: string; formatted: string }[] = [];
      for (const event of events) {
        messages.push({
          handlerName: event.handlerName,
          formatted: `data: ${JSON.stringify({
            handlerName: event.handlerName,
            checkpointSeq: event.checkpointSeq.toString(),
            txDigest: event.txDigest,
            eventSeq: event.eventSeq,
            sender: event.sender,
            timestamp: event.timestamp.toISOString(),
            source,
            data: event.data,
          })}\n\n`,
        });
      }

      for (const client of sseClients) {
        try {
          if (client.stream !== "events") continue;
          if (client.source && client.source !== source) continue;
          for (const msg of messages) {
            if (client.handler && client.handler !== msg.handlerName) continue;
            client.controller.enqueue(msg.formatted);
          }
        } catch {
          sseClients.delete(client);
        }
      }
    },
    addSSEClient(client: SSEClient): () => void {
      sseClients.add(client);
      return () => sseClients.delete(client);
    },
    sseClientCount(): number {
      return sseClients.size;
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

      const g = (name: string, help: string, value: number | string) => {
        lines.push(`# HELP ${name} ${help}`);
        lines.push(`# TYPE ${name} gauge`);
        lines.push(`${name} ${value}`);
      };

      const c = (name: string, help: string, value: number) => {
        lines.push(`# HELP ${name} ${help}`);
        lines.push(`# TYPE ${name} counter`);
        lines.push(`${name} ${value}`);
      };

      // Uptime
      g("jun_uptime_seconds", "Seconds since indexer started", uptimeSec.toFixed(1));

      // Cursors
      g("jun_live_cursor", "Latest live checkpoint sequence", liveCursor?.toString() ?? "0");
      g("jun_backfill_cursor", "Latest backfill checkpoint sequence", backfillCursor?.toString() ?? "0");

      // Checkpoint counters
      c("jun_checkpoints_processed_total{mode=\"live\"}", "Total checkpoints processed by live loop", liveCheckpoints);
      c("jun_checkpoints_processed_total{mode=\"backfill\"}", "Total checkpoints processed by backfill loop", backfillCheckpoints);

      // Event counters
      c("jun_events_flushed_total{buffer=\"live\"}", "Total events flushed to Postgres by live buffer", liveEventsTotal);
      c("jun_events_flushed_total{buffer=\"backfill\"}", "Total events flushed to Postgres by backfill buffer", backfillEventsTotal);

      // Flush counters
      c("jun_flushes_total{buffer=\"live\"}", "Total buffer flushes by live buffer", liveFlushes);
      c("jun_flushes_total{buffer=\"backfill\"}", "Total buffer flushes by backfill buffer", backfillFlushes);

      // Flush latency (last value)
      g("jun_flush_duration_seconds{buffer=\"live\"}", "Last flush duration in seconds for live buffer", (liveLastFlushDurationMs / 1000).toFixed(4));
      g("jun_flush_duration_seconds{buffer=\"backfill\"}", "Last flush duration in seconds for backfill buffer", (backfillLastFlushDurationMs / 1000).toFixed(4));

      // Buffer depth
      g("jun_buffer_size{buffer=\"live\"}", "Current events in live buffer", liveBufferSize);
      g("jun_buffer_size{buffer=\"backfill\"}", "Current events in backfill buffer", backfillBufferSize);

      // Throttle
      g("jun_backfill_concurrency", "Current adaptive backfill concurrency", throttleConcurrency);
      g("jun_backfill_paused", "Whether backfill is paused (1=paused, 0=running)", throttlePaused ? 1 : 0);

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
  metrics: IndexerMetrics,
  log: Logger,
): Response {
  const source = url.searchParams.get("source") ?? undefined;
  const handler = url.searchParams.get("handler") ?? undefined;
  const sender = url.searchParams.get("sender") ?? undefined;

  if (source && source !== "live" && source !== "backfill") {
    return Response.json({ error: 'source must be "live" or "backfill"' }, { status: 400 });
  }

  let cleanup: (() => void) | null = null;

  const readable = new ReadableStream<string>({
    start(controller) {
      const client: SSEClient = { controller, stream: streamType, handler, source, sender };
      cleanup = metrics.addSSEClient(client);

      const filters: Record<string, string | null> = { source: source ?? null };
      if (streamType === "events") filters.handler = handler ?? null;
      if (streamType === "transactions") filters.sender = sender ?? null;

      controller.enqueue(`data: ${JSON.stringify({ type: "connected", stream: streamType, ...filters })}\n\n`);
      log.debug({ stream: streamType, ...filters, clients: metrics.sseClientCount() }, "SSE client connected");
    },
    cancel() {
      cleanup?.();
      log.debug({ stream: streamType, clients: metrics.sseClientCount() }, "SSE client disconnected");
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

/** Strip SQL comments to prevent prefix check bypass */
function stripComments(sql: string): string {
  return sql
    .replace(/\/\*[\s\S]*?\*\//g, "")  // block comments
    .replace(/^--[^\n]*$/gm, "")        // line comments
    .trim();
}

/** Check if a query is read-only (SELECT, WITH, EXPLAIN) */
function isReadOnly(sql: string): boolean {
  const upper = stripComments(sql).toUpperCase();
  return upper.startsWith("SELECT") || upper.startsWith("WITH") || upper.startsWith("EXPLAIN");
}

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
    const rows = await sql.begin(async (tx: any) => {
      await tx.unsafe(`SET TRANSACTION READ ONLY`);
      await tx.unsafe(`SET LOCAL statement_timeout = '${timeoutMs}ms'`);
      return await tx.unsafe(rawSql.trim());
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

export function createServer(config: ServeConfig, ctx: ServeContext): IndexerServer {
  const { sql, metrics, log: parentLog } = ctx;
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
        return handleSSEStream("checkpoints", url, metrics, log);
      }
      if (url.pathname === "/stream/transactions") {
        return handleSSEStream("transactions", url, metrics, log);
      }
      if (url.pathname === "/stream/events") {
        return handleSSEStream("events", url, metrics, log);
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
