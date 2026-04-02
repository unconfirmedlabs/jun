/**
 * HTTP server worker thread.
 *
 * Runs Bun.serve() on a dedicated thread so HTTP requests are never blocked
 * by the indexer's backfill/processing work on the main thread.
 *
 * Communication with main thread:
 * - Main → Worker: config (port, hostname, database URL), metrics updates
 * - Worker → Main: SSE client registrations (forwarded to broadcast manager)
 *
 * The worker creates its own Postgres connection for /query and /admin/reload.
 */
/// <reference lib="webworker" />
import { SQL } from "bun";
import type { Logger } from "./logger.ts";
import { createLogger } from "./logger.ts";
import { createMetrics, type IndexerMetrics, type MetricsSnapshot } from "./serve.ts";

declare var self: Worker;

interface WorkerConfig {
  type: "config";
  port: number;
  hostname: string;
  database: string;
  adminToken?: string;
  configUrl?: string;
}

interface MetricsUpdate {
  type: "metrics";
  snapshot: MetricsSnapshot;
  prometheus: string;
}

let server: ReturnType<typeof Bun.serve> | null = null;
let latestSnapshot: MetricsSnapshot | null = null;
let latestPrometheus: string = "";
const log = createLogger();
const serveLog = log.child({ component: "serve" });

self.onmessage = async (event: MessageEvent) => {
  const message = event.data;

  if (message.type === "config") {
    const config = message as WorkerConfig;
    await startServer(config);
  } else if (message.type === "metrics") {
    const update = message as MetricsUpdate;
    latestSnapshot = update.snapshot;
    latestPrometheus = update.prometheus;
  }
};

async function startServer(config: WorkerConfig): Promise<void> {
  const sql = new SQL(config.database);

  // SQL safety helpers (same as serve.ts)
  function stripComments(sqlStr: string): string {
    return sqlStr.replace(/\/\*[\s\S]*?\*\//g, "").replace(/^--[^\n]*$/gm, "").trim();
  }

  function isReadOnly(sqlStr: string): boolean {
    const upper = stripComments(sqlStr).toUpperCase();
    return upper.startsWith("SELECT") || upper.startsWith("WITH") || upper.startsWith("EXPLAIN");
  }

  server = Bun.serve({
    port: config.port,
    hostname: config.hostname,
    routes: {
      "/health": () => Response.json({ status: "ok" }),
      "/status": () => Response.json(latestSnapshot ?? { error: "not ready" }),
      "/metrics": () => new Response(latestPrometheus || "# no data yet\n", {
        headers: { "content-type": "text/plain; version=0.0.4; charset=utf-8" },
      }),
    },
    async fetch(req) {
      const url = new URL(req.url);

      if (url.pathname === "/query") {
        return handleQuery(url, sql, config);
      }
      if (url.pathname === "/admin/reload" && req.method === "POST") {
        return handleReload(req, config);
      }
      // SSE streams still go through main thread for now (need broadcast manager)
      if (url.pathname.startsWith("/stream/") || url.pathname.startsWith("/broadcast/")) {
        return Response.json({ error: "SSE streams not available in worker mode yet" }, { status: 501 });
      }
      return Response.json({ error: "not found" }, { status: 404 });
    },
  });

  serveLog.info({ port: server.port, hostname: config.hostname }, "HTTP worker started");
  postMessage({ type: "ready", port: server.port });
}

async function handleQuery(
  url: URL,
  sql: any,
  config: WorkerConfig,
): Promise<Response> {
  const rawSql = url.searchParams.get("sql");
  if (!rawSql) {
    return Response.json({ error: "missing ?sql= parameter" }, { status: 400 });
  }

  function stripComments(s: string): string {
    return s.replace(/\/\*[\s\S]*?\*\//g, "").replace(/^--[^\n]*$/gm, "").trim();
  }

  const normalized = stripComments(rawSql).toUpperCase();
  if (!normalized.startsWith("SELECT") && !normalized.startsWith("WITH") && !normalized.startsWith("EXPLAIN")) {
    return Response.json({ error: "only SELECT, WITH, and EXPLAIN queries are allowed" }, { status: 403 });
  }

  const limitParam = parseInt(url.searchParams.get("limit") ?? "1000", 10);
  const rowLimit = Math.min(Math.max(1, limitParam), 10000);
  const timeoutParam = parseInt(url.searchParams.get("timeout") ?? "5000", 10);
  const timeoutMs = Math.min(Math.max(100, timeoutParam), 30000);

  try {
    const rows = await sql.begin(async (transaction: any) => {
      await transaction.unsafe("SET TRANSACTION READ ONLY");
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
    serveLog.warn({ err: message, sql: rawSql.slice(0, 200) }, "query error");
    return Response.json({ error: message }, { status: 400 });
  }
}

async function handleReload(
  req: Request,
  config: WorkerConfig,
): Promise<Response> {
  const token = config.adminToken;
  if (token) {
    const auth = req.headers.get("authorization");
    if (auth !== `Bearer ${token}`) {
      return Response.json({ error: "unauthorized" }, { status: 401 });
    }
  }

  // Forward reload to main thread
  const contentLength = parseInt(req.headers.get("content-length") ?? "0", 10);
  if (contentLength > 1_000_000) {
    return Response.json({ error: "request body too large (max 1MB)" }, { status: 413 });
  }

  const body = await req.text();
  postMessage({ type: "reload", body, configUrl: config.configUrl });

  // We can't wait for the result since it happens on the main thread
  return Response.json({ status: "reload initiated" });
}
