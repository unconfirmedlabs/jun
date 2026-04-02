/**
 * Jun — Sui Event Indexer for Bun
 *
 * Main entry point. Exports `defineIndexer()` which creates an indexer
 * with `live()`, `backfill()`, and `run()` modes.
 *
 * `run()` combines live streaming and historical backfill concurrently,
 * mediated by WriteBuffers that batch events and flush to Postgres.
 */
import { SQL } from "bun";
import path from "path";
import pMap from "p-map";
import pRetry from "p-retry";
import { createGrpcClient, type GrpcClient } from "./grpc.ts";
import { createArchiveClient, cachedGetCheckpoint, fetchCompressed, type ArchiveClient } from "./archive.ts";
import { createProcessor, type EventHandler, type EventProcessor } from "./processor.ts";
import { createPostgresOutput, type PostgresOutput } from "./output/postgres.ts";
import { createStateManager, type StateManager } from "./state.ts";
import { createWriteBuffer, type WriteBuffer } from "./buffer.ts";
import { createAdaptiveThrottle, type AdaptiveThrottle } from "./throttle.ts";
import { createGapDetector } from "./gaps.ts";
import { createLogger } from "./logger.ts";
import { createServer, createMetrics, type IndexerMetrics } from "./serve.ts";
import { createCheckpointDecoderPool, defaultWorkerCount, type CheckpointDecoderPool } from "./checkpoint-decoder-pool.ts";
import { createBroadcastManager, createNATSTarget, type BroadcastManager, type BroadcastTarget } from "./broadcast.ts";
import type { HotReloadContext } from "./hot-reload.ts";
import { createBalanceProcessor, type BalanceProcessor } from "./balance-processor.ts";
import { createBalanceWriter, type BalanceWriter } from "./output/balance-writer.ts";
import { resolveEventHandlerFields } from "./resolve-fields.ts";

// ---------------------------------------------------------------------------
// Public API types
// ---------------------------------------------------------------------------

export type { FieldDefs, FieldType } from "./schema.ts";
export type { EventHandler } from "./processor.ts";
export type { FlushStats, WriteBufferConfig, WriteBuffer } from "./buffer.ts";
export type { ThrottleConfig, AdaptiveThrottle } from "./throttle.ts";

export interface IndexerConfig {
  /** Sui network name (for logging/cursor keys) */
  network: string;
  /** gRPC endpoint URL (host:port, no protocol prefix) */
  grpcUrl: string;
  /** Postgres connection string */
  database: string;
  /** Event handler definitions */
  events: Record<string, EventHandler>;
  /** Backfill concurrency (default: 10) */
  backfillConcurrency?: number;
  /**
   * Starting checkpoint for backfill. Accepts:
   * - bigint: raw checkpoint sequence number
   * - "epoch:N": resolves first checkpoint of epoch N
   * - "timestamp:ISO": resolves checkpoint at/after the given timestamp
   * - "package:0x...": resolves publish checkpoint for the package
   * - numeric string: parsed as bigint
   */
  startCheckpoint?: bigint | string;
  /** Archive URL override (default: resolved from config.ts based on network) */
  archiveUrl?: string;
  /** Number of worker threads for backfill decoding (default: auto-detect based on CPU cores) */
  backfillWorkers?: number;
  /** Balance change indexing config. Tracks coin holders with running totals. */
  balances?: { coinTypes: string[] | "*" };
}

export type RunMode = "all" | "live-only" | "backfill-only";

export interface RunOptions {
  /** Which loops to run (default: "all") */
  mode?: RunMode;
  /** Enable periodic gap detection and repair (default: false) */
  repairGaps?: boolean;
  /** Start an HTTP server for health, status, and query endpoints */
  serve?: { port: number; hostname?: string };
  /** Additional broadcast targets (e.g. NATS). SSE is always available when serve is enabled. */
  broadcastTargets?: BroadcastTarget[];
  /** Remote config URL for /admin/reload without body */
  configUrl?: string;
}

export interface Indexer {
  /** Subscribe to live checkpoints, process events, write to Postgres. Long-running. */
  live(): Promise<void>;
  /** Backfill from a starting checkpoint to the latest. Exits when done. */
  backfill(options: { from: bigint }): Promise<void>;
  /** Run live + backfill concurrently with buffered writes. */
  run(options?: RunOptions): Promise<void>;
  /** Graceful shutdown */
  shutdown(): Promise<void>;
}

// ---------------------------------------------------------------------------
// Table name helpers
// ---------------------------------------------------------------------------

/** Convert handler name to snake_case table name */
function toTableName(handlerName: string): string {
  return handlerName
    .replace(/([A-Z])/g, "_$1")
    .toLowerCase()
    .replace(/^_/, "");
}

// ---------------------------------------------------------------------------
// Start checkpoint resolution
// ---------------------------------------------------------------------------

/**
 * Resolve a startCheckpoint spec to a concrete checkpoint sequence number.
 * Supports: bigint, "epoch:N", "timestamp:ISO", "package:0x...", numeric string.
 */
async function resolveStartCheckpoint(
  spec: bigint | string,
  grpcUrl: string,
): Promise<bigint> {
  // Raw bigint
  if (typeof spec === "bigint") return spec;

  const str = String(spec);

  // Epoch resolution
  if (str.startsWith("epoch:")) {
    const epoch = str.slice(6);
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(grpcUrl);
    const { response } = await sui.ledgerService.getEpoch({
      epoch,
      readMask: { paths: ["first_checkpoint"] },
    });
    const first = response.epoch?.firstCheckpoint;
    if (!first) throw new Error(`Could not resolve epoch ${epoch}`);
    return BigInt(first);
  }

  // Timestamp resolution (binary search)
  if (str.startsWith("timestamp:")) {
    const targetMs = new Date(str.slice(10)).getTime();
    if (isNaN(targetMs)) throw new Error(`Invalid timestamp: ${str.slice(10)}`);

    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(grpcUrl);

    // Get latest checkpoint as upper bound
    const { response: latestResp } = await sui.ledgerService.getLatestCheckpoint({
      readMask: { paths: ["sequence_number", "summary"] },
    });
    let high = BigInt(latestResp.checkpoint?.sequenceNumber ?? "0");
    let low = 0n;

    // Binary search for the first checkpoint at or after the target timestamp
    while (low < high) {
      const mid = low + (high - low) / 2n;
      const { response: cpResp } = await sui.ledgerService.getCheckpoint({
        checkpointId: { oneofKind: "sequenceNumber", sequenceNumber: mid.toString() },
        readMask: { paths: ["summary.timestamp"] },
      });
      const ts = cpResp.checkpoint?.summary?.timestamp;
      const cpMs = ts ? Number(BigInt(ts.seconds) * 1000n + BigInt(Math.floor(ts.nanos / 1_000_000))) : 0;

      if (cpMs < targetMs) {
        low = mid + 1n;
      } else {
        high = mid;
      }
    }

    return low;
  }

  // Package resolution
  if (str.startsWith("package:")) {
    const packageId = str.slice(8);
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(grpcUrl);

    // Get the package object to find its publish transaction
    const obj = await sui.core.getObject({ objectId: packageId });
    const prevTx = (obj as any).data?.previousTransaction;
    if (!prevTx) throw new Error(`Could not find publish transaction for package ${packageId}`);

    // Get the transaction to find its checkpoint
    const tx = await sui.core.getTransactionBlock({
      digest: prevTx,
      options: { showEffects: true },
    });
    const cpSeq = (tx as any).checkpoint;
    if (!cpSeq) throw new Error(`Could not resolve checkpoint for publish tx ${prevTx}`);
    return BigInt(cpSeq);
  }

  // Numeric string fallback
  try {
    return BigInt(str);
  } catch {
    throw new Error(`Invalid startCheckpoint: "${str}". Expected bigint, epoch:N, timestamp:ISO, or package:0x...`);
  }
}

// ---------------------------------------------------------------------------
// defineIndexer
// ---------------------------------------------------------------------------

export function defineIndexer(config: IndexerConfig): Indexer {
  const {
    network,
    grpcUrl,
    database,
    events,
    backfillConcurrency = 10,
    startCheckpoint: startCheckpointSpec,
    archiveUrl,
    backfillWorkers,
    balances: balancesConfig,
  } = config;

  const log = createLogger();

  let shutdownRequested = false;
  let abortController = new AbortController();

  // Initialized services (set by init(), cleaned up by shutdown())
  let _sql: any = null;
  let _grpcClient: GrpcClient | null = null;

  // Mutable handler table mapping (rebuilt after field resolution)
  let handlerTables: Record<string, { tableName: string; fields: FieldDefs }> = {};

  /** Initialize all services */
  async function init() {
    _sql = new SQL(database);
    _grpcClient = createGrpcClient({ url: grpcUrl });

    // Auto-resolve fields from chain for handlers that don't specify them
    if (Object.keys(events).length > 0) {
      await resolveEventHandlerFields(events, _grpcClient, log);
    }

    // Build handler table mapping (after fields are resolved)
    // Handler key = table name (snake_cased). Validate no duplicates.
    handlerTables = {};
    const tableNames = new Set<string>();
    for (const [name, handler] of Object.entries(events)) {
      const tableName = toTableName(name);
      if (tableNames.has(tableName)) {
        throw new Error(`Duplicate table name "${tableName}" — two event handlers resolve to the same table. Use distinct handler keys.`);
      }
      tableNames.add(tableName);
      handlerTables[name] = {
        tableName,
        fields: handler.fields!,
      };
    }

    const state = await createStateManager(_sql);
    const output = createPostgresOutput(_sql, handlerTables);
    await output.migrate();
    const processor = createProcessor(events);
    log.info({ network, events: Object.keys(events) }, "initialized");
    return { sql: _sql, state, output, processor, grpcClient: _grpcClient };
  }

  // SIGINT/SIGTERM handlers
  function setupSignalHandlers() {
    const handler = () => {
      if (shutdownRequested) {
        log.warn("force shutdown");
        process.exit(1);
      }
      log.info("shutting down gracefully...");
      shutdownRequested = true;
      abortController.abort();
    };
    process.on("SIGINT", handler);
    process.on("SIGTERM", handler);
  }

  // -------------------------------------------------------------------------
  // Internal loops for run() mode
  // -------------------------------------------------------------------------

  async function liveLoop(
    grpcClient: GrpcClient,
    getProcessor: () => EventProcessor,
    buffer: WriteBuffer,
    metrics: IndexerMetrics,
    broadcast: BroadcastManager,
    balanceProcessor: BalanceProcessor | null,
  ): Promise<void> {
    const liveLog = log.child({ component: "live" });
    const cursorKey = `live:${network}`;
    let reconnectDelay = 1000;
    let client = grpcClient;

    while (!shutdownRequested) {
      try {
        liveLog.info({ url: grpcUrl }, "connecting");
        client.close();
        client = createGrpcClient({ url: grpcUrl });
        _grpcClient = client;

        const stream = client.subscribeCheckpoints();
        reconnectDelay = 1000;

        for await (const response of stream) {
          if (shutdownRequested) break;

          const seq = BigInt(response.cursor);

          // Broadcast full checkpoint data to SSE + NATS clients
          broadcast.broadcast(response, "live");

          const decoded = getProcessor().process(response);

          // Extract balance changes if enabled
          if (balanceProcessor) {
            const changes = balanceProcessor.extract(response);
            buffer.pushBalances(changes);
          }

          await buffer.push(decoded, cursorKey, seq);
          metrics.setLiveCursor(seq);
          metrics.recordLiveCheckpoint();

          if (decoded.length > 0) {
            liveLog.info({ checkpoint: seq.toString(), events: decoded.length }, "events decoded");
          } else {
            liveLog.debug({ checkpoint: seq.toString() }, "no matching events");
          }
        }
      } catch (err) {
        if (shutdownRequested) break;

        liveLog.error({ err, reconnectIn: reconnectDelay / 1000 }, "stream error");

        await sleep(reconnectDelay);
        reconnectDelay = Math.min(reconnectDelay * 2, 30000);
      }
    }
  }

  async function backfillLoop(
    getProcessor: () => EventProcessor,
    buffer: WriteBuffer,
    state: StateManager,
    throttle: AdaptiveThrottle,
    startSeq: bigint,
    metrics: IndexerMetrics,
    broadcast: BroadcastManager,
    decoderPool: CheckpointDecoderPool,
    resolvedArchiveUrl: string,
    balanceProcessor: BalanceProcessor | null,
  ): Promise<void> {
    const backfillLog = log.child({ component: "backfill" });
    const cursorKey = `backfill:${network}`;

    // Determine the upper bound
    backfillLog.info("fetching latest checkpoint...");
    const latest = await getLatestCheckpoint(createGrpcClient({ url: grpcUrl }));
    const to = BigInt(latest);

    // Check saved cursor for resume
    const savedCursor = await state.getCheckpointCursor(cursorKey);
    const startFrom = savedCursor && savedCursor > startSeq ? savedCursor + 1n : startSeq;

    if (startFrom > to) {
      backfillLog.info({ cursor: savedCursor?.toString() }, "already complete");
      return;
    }

    if (savedCursor) {
      backfillLog.info({ from: startFrom.toString(), savedCursor: savedCursor.toString() }, "resuming");
    }

    const totalCheckpoints = to - startFrom + 1n;
    backfillLog.info({ from: startFrom.toString(), to: to.toString(), total: totalCheckpoints.toString() }, "starting");

    let processed = 0;
    let totalEvents = 0;
    const startTime = performance.now();

    // Pending map: holds decoded results until they can be drained contiguously.
    // Workers complete out of order, but we only push to the buffer in order.
    const pending = new Map<bigint, DecodedEvent[]>();
    let watermark = startFrom - 1n;

    /** Drain contiguous ready checkpoints from pending into the buffer, in order. */
    async function drainContiguous(): Promise<void> {
      while (pending.has(watermark + 1n)) {
        const next = watermark + 1n;
        const events = pending.get(next)!;
        pending.delete(next);
        await buffer.push(events, cursorKey, next);
        metrics.setBackfillCursor(next);
        metrics.recordBackfillCheckpoint();
        watermark = next;
      }
    }

    // Process in sliding windows (100 per window to yield frequently for HTTP)
    const windowSize = 100n;
    let windowStart = startFrom;

    while (windowStart <= to && !shutdownRequested) {
      await throttle.waitIfPaused();

      // Yield to event loop — lets HTTP handlers and live stream process
      await new Promise((r) => setTimeout(r, 0));

      const windowEnd = windowStart + windowSize - 1n < to ? windowStart + windowSize - 1n : to;
      const batch: bigint[] = [];
      for (let i = windowStart; i <= windowEnd; i++) {
        batch.push(i);
      }

      backfillLog.debug({ window: `${windowStart}..${windowEnd}`, concurrency: throttle.getConcurrency() }, "processing window");

      await pMap(
        batch,
        async (seq) => {
          if (shutdownRequested) return;

          try {
            // Fetch compressed bytes on main thread (I/O), decode in worker (CPU)
            const result = await pRetry(async () => {
              const ac = new AbortController();
              const timeout = setTimeout(() => ac.abort(), 30_000);
              try {
                const compressed = await fetchCompressed(seq, resolvedArchiveUrl, ac.signal);
                return await decoderPool.decode(seq, compressed);
              } finally {
                clearTimeout(timeout);
              }
            }, { retries: 3, minTimeout: 1000 });

            const response = result.decoded;

            // Broadcast full checkpoint data to SSE + NATS clients
            broadcast.broadcast(response, "backfill");

            const decoded = getProcessor().process(response);

            if (decoded.length > 0) {
              totalEvents += decoded.length;
            }

            // Balance changes: use worker-computed archive balances if available,
            // otherwise extract from gRPC balance_changes field (live mode)
            if (result.balanceChanges && result.balanceChanges.length > 0) {
              buffer.pushBalances(result.balanceChanges);
            } else if (balanceProcessor) {
              const changes = balanceProcessor.extract(response);
              buffer.pushBalances(changes);
            }

            pending.set(seq, decoded);
            await drainContiguous();

            processed++;

            if (processed % 100 === 0) {
              const elapsed = ((performance.now() - startTime) / 1000).toFixed(1);
              const rate = (processed / (parseFloat(elapsed) || 1)).toFixed(0);
              const total = Number(to - startFrom + 1n);
              const pct = ((processed / total) * 100).toFixed(1);
              backfillLog.info({ processed, total, pct, rate: `${rate}/s`, events: totalEvents, elapsed }, "progress");
            }
          } catch (err) {
            backfillLog.error({ checkpoint: seq.toString(), err }, "error processing checkpoint");
          }
        },
        { concurrency: throttle.getConcurrency(), signal: abortController.signal },
      ).catch((err) => {
        if (err.name === "AbortError") return;
        throw err;
      });

      windowStart = windowEnd + 1n;
    }

    const elapsed = ((performance.now() - startTime) / 1000).toFixed(1);
    backfillLog.info({ processed, events: totalEvents, elapsed }, "complete");
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  return {
    async live(): Promise<void> {
      const liveLog = log.child({ component: "live" });
      let { state, output, processor, grpcClient } = await init();
      setupSignalHandlers();

      const cursorKey = `live:${network}`;
      let reconnectDelay = 1000;

      while (!shutdownRequested) {
        try {
          liveLog.info({ url: grpcUrl }, "connecting");
          grpcClient.close();
          grpcClient = createGrpcClient({ url: grpcUrl });
          _grpcClient = grpcClient;

          const stream = grpcClient.subscribeCheckpoints();
          reconnectDelay = 1000;

          for await (const response of stream) {
            if (shutdownRequested) break;

            const seq = BigInt(response.cursor);
            const decoded = processor.process(response);

            if (decoded.length > 0) {
              await output.write(decoded);
              liveLog.info({ checkpoint: seq.toString(), events: decoded.length }, "events decoded");
            }

            await state.setCheckpointCursor(cursorKey, seq);
          }
        } catch (err) {
          if (shutdownRequested) break;

          liveLog.error({ err, reconnectIn: reconnectDelay / 1000 }, "stream error");

          await sleep(reconnectDelay);
          reconnectDelay = Math.min(reconnectDelay * 2, 30000);
        }
      }

      await this.shutdown();
    },

    async backfill(options: { from: bigint }): Promise<void> {
      const backfillLog = log.child({ component: "backfill" });
      const { state, output, processor, grpcClient } = await init();
      setupSignalHandlers();

      const cursorKey = `backfill:${network}`;
      const from = options.from;

      backfillLog.info("fetching latest checkpoint...");
      const latest = await getLatestCheckpoint(grpcClient);
      const to = BigInt(latest);
      backfillLog.info({ from: from.toString(), to: to.toString(), total: (to - from + 1n).toString() }, "starting");

      const savedCursor = await state.getCheckpointCursor(cursorKey);
      const startFrom = savedCursor && savedCursor > from ? savedCursor + 1n : from;

      if (startFrom > to) {
        backfillLog.info({ cursor: savedCursor?.toString() }, "already complete");
        await this.shutdown();
        return;
      }

      if (savedCursor) {
        backfillLog.info({ from: startFrom.toString(), savedCursor: savedCursor.toString() }, "resuming");
      }

      const checkpoints: bigint[] = [];
      for (let i = startFrom; i <= to; i++) {
        checkpoints.push(i);
      }

      let processed = 0;
      let totalEvents = 0;
      const startTime = performance.now();

      const completed = new Set<bigint>();
      let watermark = startFrom - 1n;

      async function advanceWatermark() {
        let next = watermark + 1n;
        while (completed.has(next)) {
          completed.delete(next);
          next++;
        }
        const newWatermark = next - 1n;
        if (newWatermark > watermark) {
          watermark = newWatermark;
          await state.setCheckpointCursor(cursorKey, watermark);
        }
      }

      await pMap(
        checkpoints,
        async (seq) => {
          if (shutdownRequested) return;

          try {
            const response = await cachedGetCheckpoint(seq, () => grpcClient.getCheckpoint(seq));
            const decoded = processor.process(response);

            if (decoded.length > 0) {
              await output.write(decoded);
              totalEvents += decoded.length;
            }

            completed.add(seq);
            await advanceWatermark();
            processed++;

            if (processed % 100 === 0) {
              const elapsed = ((performance.now() - startTime) / 1000).toFixed(1);
              const rate = (processed / (parseFloat(elapsed) || 1)).toFixed(0);
              backfillLog.info({ processed, total: checkpoints.length, rate: `${rate}/s`, events: totalEvents, elapsed }, "progress");
            }
          } catch (err) {
            backfillLog.error({ checkpoint: seq.toString(), err }, "error processing checkpoint");
          }
        },
        { concurrency: backfillConcurrency, signal: abortController.signal },
      ).catch((err) => {
        if (err.name === "AbortError") return;
        throw err;
      });

      const elapsed = ((performance.now() - startTime) / 1000).toFixed(1);
      backfillLog.info({ processed, events: totalEvents, elapsed }, "complete");

      await this.shutdown();
    },

    async run(options?: RunOptions): Promise<void> {
      const mode = options?.mode ?? "all";
      const { sql, state, output, processor: initialProcessor, grpcClient } = await init();
      setupSignalHandlers();

      log.info({ mode }, "run mode");

      const metrics = createMetrics();

      // Mutable processor reference for hot reload
      let currentProcessor = initialProcessor;
      const getProcessor = () => currentProcessor;

      // Hot reload context (passed to HTTP server for /admin/reload)
      const hotReloadCtx: HotReloadContext = {
        events: { ...events },
        getProcessor,
        setProcessor: (p) => { currentProcessor = p; },
        output,
        handlerTables: { ...handlerTables },
        sql,
        configUrl: options?.configUrl,
        log,
      };

      // Resolve start checkpoint
      let startSeq: bigint | null = null;
      if (startCheckpointSpec !== undefined) {
        log.info("resolving start checkpoint...");
        startSeq = await resolveStartCheckpoint(startCheckpointSpec, grpcUrl);
        log.info({ startCheckpoint: startSeq.toString() }, "resolved");
      } else if (mode !== "live-only") {
        const saved = await state.getCheckpointCursor(`backfill:${network}`);
        if (saved) {
          startSeq = saved + 1n;
          log.info({ cursor: startSeq.toString() }, "resuming backfill from saved cursor");
        }
      }

      // Create archive client for backfill
      const resolvedArchiveUrl = archiveUrl ?? getDefaultArchiveUrl(network);
      const archive = createArchiveClient({ archiveUrl: resolvedArchiveUrl });

      // Broadcast manager (SSE built-in + optional NATS via broadcastTargets)
      const broadcastManager = createBroadcastManager(options?.broadcastTargets ?? [], log);

      // Balance indexing (optional)
      let balanceProcessor: BalanceProcessor | null = null;
      let balanceWriter: BalanceWriter | null = null;
      if (balancesConfig) {
        balanceProcessor = createBalanceProcessor(balancesConfig.coinTypes);
        balanceWriter = createBalanceWriter(sql);
        await balanceWriter.migrate();
        log.info({ coinTypes: balancesConfig.coinTypes }, "balance indexing enabled");
      }

      // Create write buffers with different tuning
      const liveBufferLog = log.child({ component: "live:buffer" });
      const liveBuffer = createWriteBuffer(output, state, {
        label: "live",
        intervalMs: 200,
        maxEvents: 50,
        onFlush: (stats) => {
          metrics.recordLiveFlush(stats);
          if (stats.eventsWritten > 0) {
            liveBufferLog.info(
              { events: stats.eventsWritten, durationMs: Math.round(stats.flushDurationMs) },
              "flushed",
            );
          }
        },
        onEvents: (events) => broadcastManager.broadcastDecodedEvents(events, "live"),
      }, liveBufferLog, balanceWriter);

      const throttleLog = log.child({ component: "throttle" });
      const throttle = createAdaptiveThrottle({
        initialConcurrency: backfillConcurrency,
      }, throttleLog);

      const backfillBufferLog = log.child({ component: "backfill:buffer" });
      const backfillBuffer = createWriteBuffer(output, state, {
        label: "backfill",
        intervalMs: 1000,
        maxEvents: 500,
        onFlush: (stats) => {
          throttle.recordLatency(stats.flushDurationMs);
          metrics.recordBackfillFlush(stats);
          metrics.recordThrottleState(throttle.getConcurrency(), throttle.isPaused());
          if (stats.eventsWritten > 0) {
            backfillBufferLog.info(
              { events: stats.eventsWritten, durationMs: Math.round(stats.flushDurationMs), concurrency: throttle.getConcurrency() },
              "flushed",
            );
          }
        },
        onEvents: (events) => broadcastManager.broadcastDecodedEvents(events, "backfill"),
      }, backfillBufferLog, balanceWriter);

      // Start buffer timers
      liveBuffer.start();
      backfillBuffer.start();

      // HTTP server worker (opt-in — runs on a dedicated thread)
      let httpWorker: Worker | null = null;
      let metricsInterval: Timer | null = null;
      if (options?.serve) {
        const workerUrl = path.join(import.meta.dir, "serve-worker.ts");
        httpWorker = new Worker(workerUrl);

        // Send config to worker
        httpWorker.postMessage({
          type: "config",
          port: options.serve.port,
          hostname: options.serve.hostname ?? "127.0.0.1",
          database,
          adminToken: process.env.JUN_ADMIN_TOKEN,
          configUrl: options.configUrl,
        });

        // Forward reload messages from worker to hot reload
        httpWorker.onmessage = (event: MessageEvent) => {
          if (event.data.type === "reload" && hotReloadCtx) {
            const { applyReload } = require("./hot-reload.ts");
            const { parseIndexerConfig } = require("./indexer-config.ts");
            const { fetchRemoteConfig } = require("./remote-config.ts");

            (async () => {
              try {
                let yamlContent = event.data.body;
                if (!yamlContent?.trim() && event.data.configUrl) {
                  yamlContent = await fetchRemoteConfig(event.data.configUrl);
                }
                if (!yamlContent?.trim()) return;

                const parsed = parseIndexerConfig(yamlContent);
                await applyReload(hotReloadCtx, parsed.indexer.events);
                log.info("config reloaded via HTTP worker");
              } catch (err) {
                log.error({ err }, "reload from HTTP worker failed");
              }
            })();
          }
        };

        // Periodically send metrics to worker (every 500ms)
        metricsInterval = setInterval(() => {
          if (httpWorker) {
            httpWorker.postMessage({
              type: "metrics",
              snapshot: metrics.snapshot(),
              prometheus: metrics.prometheus(),
            });
          }
        }, 500);

        log.info({ port: options.serve.port }, "HTTP server worker starting");
      }

      // Checkpoint decoder pool (workers for CPU-bound decompress + decode)
      const workerCount = backfillWorkers ?? defaultWorkerCount();
      const decoderPool = createCheckpointDecoderPool(workerCount, balancesConfig?.coinTypes);
      log.info({ workers: workerCount }, "checkpoint decoder pool started");

      // Build concurrent tasks
      const tasks: Promise<void>[] = [];

      if (mode === "all" || mode === "live-only") {
        tasks.push(liveLoop(grpcClient, getProcessor, liveBuffer, metrics, broadcastManager, balanceProcessor));
      }

      if ((mode === "all" || mode === "backfill-only") && startSeq !== null) {
        tasks.push(
          backfillLoop(getProcessor, backfillBuffer, state, throttle, startSeq, metrics, broadcastManager, decoderPool, resolvedArchiveUrl, balanceProcessor),
        );
      } else if (mode === "backfill-only" && startSeq === null) {
        log.error("backfill requires startCheckpoint in config or a saved cursor");
      }

      // Gap repair
      let stopGapRepair: (() => void) | null = null;
      if (options?.repairGaps && startSeq !== null) {
        const gapLog = log.child({ component: "gaps" });
        const gapDetector = createGapDetector(sql, state, network, gapLog);
        stopGapRepair = gapDetector.startPeriodicRepair(getProcessor, backfillBuffer, archive);
      }

      // Run concurrently
      await Promise.all(tasks);

      // Cleanup
      if (stopGapRepair) stopGapRepair();
      decoderPool.shutdown();
      await broadcastManager.shutdown();
      if (metricsInterval) clearInterval(metricsInterval);
      if (httpWorker) httpWorker.terminate();
      await liveBuffer.stop();
      await backfillBuffer.stop();
      await this.shutdown();
    },

    async shutdown(): Promise<void> {
      shutdownRequested = true;
      if (_grpcClient) {
        _grpcClient.close();
        _grpcClient = null;
      }
      if (_sql) {
        await _sql.close();
        _sql = null;
      }
      log.info("shutdown complete");
    },
  };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Get the latest checkpoint sequence number via gRPC */
async function getLatestCheckpoint(client: GrpcClient): Promise<string> {
  const stream = client.subscribeCheckpoints();
  for await (const response of stream) {
    return response.cursor;
  }
  throw new Error("Failed to get latest checkpoint");
}

/** Default archive URLs by network name */
function getDefaultArchiveUrl(network: string): string {
  switch (network) {
    case "mainnet":
      return "https://checkpoints.mainnet.sui.io";
    case "testnet":
      return "https://checkpoints.testnet.sui.io";
    default:
      return "https://checkpoints.mainnet.sui.io";
  }
}
