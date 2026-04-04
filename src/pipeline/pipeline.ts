/**
 * Pipeline orchestrator — connects Sources → Processors → Destinations.
 *
 * Production-grade execution engine with:
 * - Buffered writes with backpressure and retry
 * - Checkpoint cursor persistence (resume after restart)
 * - Metrics (Prometheus format) + HTTP server (/health, /status, /metrics)
 * - Auto-reload (periodic config fetch → processor.reload())
 * - Adaptive backfill throttle
 * - Gap repair
 * - Graceful shutdown (SIGINT/SIGTERM)
 *
 * @example
 * ```ts
 * import { createPipeline } from "jun/pipeline";
 * import { createGrpcLiveSource } from "jun/pipeline/sources/grpc";
 * import { createEventDecoder } from "jun/pipeline/processors/events";
 * import { createSqlStorage } from "jun/pipeline/destinations/sql";
 *
 * const pipeline = createPipeline()
 *   .source(createGrpcLiveSource({ url: "fullnode.testnet.sui.io:443" }))
 *   .processor(createEventDecoder({ handlers: { ... } }))
 *   .storage(createSqlStorage({ url: "postgres://..." }))
 *   .run();
 * ```
 */
import path from "path";
import type {
  Source,
  Processor,
  Storage,
  Broadcast,
  Pipeline,
  PipelineConfig,
  ProcessedCheckpoint,
} from "./types.ts";
import { createPipelineWriteBuffer, type PipelineWriteBuffer, type PipelineFlushStats } from "./write-buffer.ts";
import { createLogger } from "../logger.ts";
import type { Logger } from "../logger.ts";
import { createMetrics, type IndexerMetrics } from "../serve.ts";
import { createStateManager, type StateManager } from "../state.ts";
import { createAdaptiveThrottle, type AdaptiveThrottle } from "../throttle.ts";
import { fetchRemoteConfig } from "../remote-config.ts";
import { createPostgresConnection } from "../db.ts";

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

export function createPipeline(): Pipeline {
  const sources: Source[] = [];
  const processors: Processor[] = [];
  const storages: Storage[] = [];
  const broadcasts: Broadcast[] = [];
  let config: PipelineConfig = {};
  let stopped = false;
  const log = createLogger();

  const pipeline: Pipeline = {
    source(source: Source): Pipeline {
      sources.push(source);
      return pipeline;
    },

    processor(processor: Processor): Pipeline {
      processors.push(processor);
      return pipeline;
    },

    storage(storage: Storage): Pipeline {
      storages.push(storage);
      return pipeline;
    },

    broadcast(broadcast: Broadcast): Pipeline {
      broadcasts.push(broadcast);
      return pipeline;
    },

    configure(pipelineConfig: PipelineConfig): Pipeline {
      config = { ...config, ...pipelineConfig };
      return pipeline;
    },

    async run(): Promise<void> {
      if (sources.length === 0) throw new Error("Pipeline has no sources");

      const humanOutput = !config.quiet;

      // Machine logs: off by default, enabled with --log or --log=LEVEL
      if (config.log) {
        const level = typeof config.log === "string" ? config.log : "info";
        process.env.LOG_LEVEL = level;
        log.level = level;
      } else {
        process.env.LOG_LEVEL = "silent";
        log.level = "silent";
      }

      // Initialize processors
      for (const processor of processors) {
        if (processor.initialize) {
          await processor.initialize();
          log.info({ processor: processor.name }, "processor initialized");
        }
      }

      // Initialize storage destinations
      for (const storage of storages) {
        await storage.initialize();
        log.info({ storage: storage.name }, "storage initialized");
      }

      // Initialize broadcast destinations
      for (const broadcast of broadcasts) {
        await broadcast.initialize();
        log.info({ broadcast: broadcast.name }, "broadcast initialized");
      }

      // --- State manager (cursor persistence) ---
      let state: StateManager | null = null;
      let sql: any = null;
      if (config.database && config.database.startsWith("postgres")) {
        sql = createPostgresConnection(config.database);
        state = await createStateManager(sql);
        log.info("state manager initialized");
      }

      // --- Metrics ---
      const metrics = createMetrics();

      // --- Adaptive throttle ---
      let throttle: AdaptiveThrottle | null = null;
      if (config.throttle) {
        throttle = createAdaptiveThrottle({
          initialConcurrency: config.throttle.initialConcurrency ?? 50,
          minConcurrency: config.throttle.minConcurrency,
          maxConcurrency: config.throttle.maxConcurrency,
        }, log.child({ component: "throttle" }));
        log.info({ concurrency: throttle.getConcurrency() }, "adaptive throttle enabled");
      }

      // Print human-readable banner
      if (humanOutput) {
        console.log("\n=== Jun Pipeline ===");
        console.log(`Sources:    ${sources.map(s => s.name).join(", ")}`);
        if (processors.length > 0) console.log(`Processors: ${processors.map(p => p.name).join(", ")}`);
        if (storages.length > 0) console.log(`Storage:    ${storages.map(s => s.name).join(", ")}`);
        if (broadcasts.length > 0) console.log(`Broadcast:  ${broadcasts.map(b => b.name).join(", ")}`);
        if (config.serve) console.log(`HTTP Server: port ${config.serve.port}`);
        if (state) console.log(`Cursors:    enabled (Postgres)`);
        console.log("");
      }

      // --- Signal handlers (graceful shutdown) ---
      let signalCount = 0;
      const shutdownHandler = () => {
        signalCount++;
        if (signalCount === 1) {
          log.info("shutting down gracefully...");
          if (humanOutput) console.log("\nShutting down gracefully...");
          stopped = true;
          for (const source of sources) source.stop();
        } else {
          log.warn("forced exit");
          process.exit(1);
        }
      };
      process.on("SIGINT", shutdownHandler);
      process.on("SIGTERM", shutdownHandler);

      // --- Shared write mutex ---
      let writeLock: Promise<void> = Promise.resolve();
      function acquireWriteLock(): Promise<() => void> {
        let release: () => void;
        const prev = writeLock;
        writeLock = new Promise(resolve => { release = resolve; });
        return prev.then(() => release!);
      }

      // --- HTTP server (worker thread) ---
      let httpWorker: Worker | null = null;
      let metricsInterval: Timer | null = null;
      if (config.serve) {
        const workerUrl = path.join(import.meta.dir, "..", "serve-worker.ts");
        httpWorker = new Worker(workerUrl);

        httpWorker.postMessage({
          type: "config",
          port: config.serve.port,
          hostname: config.serve.hostname ?? "127.0.0.1",
          database: config.database ?? "",
          adminToken: process.env.JUN_ADMIN_TOKEN,
          configUrl: config.configUrl,
        });

        // Handle reload messages from HTTP worker
        httpWorker.onmessage = (event: MessageEvent) => {
          if (event.data.type === "reload") {
            (async () => {
              try {
                let yamlContent = event.data.body;
                if (!yamlContent?.trim() && event.data.configUrl) {
                  const result = await fetchRemoteConfig(event.data.configUrl);
                  yamlContent = result?.content;
                }
                if (!yamlContent?.trim()) return;

                const { parsePipelineConfig } = await import("./config-parser.ts");
                const parsed = parsePipelineConfig(yamlContent);
                for (const processor of processors) {
                  if (processor.reload) {
                    const matchingNew = parsed.processors.find(p => p.name === processor.name);
                    if (matchingNew && matchingNew.reload) {
                      // Extract config from the newly parsed processor (it was initialized with the new config)
                      processor.reload((matchingNew as any)._reloadConfig);
                    }
                  }
                }
                log.info("config reloaded via HTTP worker");
              } catch (err) {
                log.error({ err }, "reload from HTTP worker failed");
              }
            })();
          }
        };

        metricsInterval = setInterval(() => {
          if (httpWorker) {
            httpWorker.postMessage({
              type: "metrics",
              snapshot: metrics.snapshot(),
              prometheus: metrics.prometheus(),
            });
          }
        }, 500);

        log.info({ port: config.serve.port }, "HTTP server worker starting");
      }

      // --- Auto-reload timer ---
      let autoReloadInterval: Timer | null = null;
      if (config.configAutoReloadMs && config.configUrl) {
        const reloadLog = log.child({ component: "auto-reload" });
        reloadLog.info({ intervalMs: config.configAutoReloadMs, configUrl: config.configUrl }, "auto-reload enabled");

        let lastEtag: string | undefined;
        autoReloadInterval = setInterval(async () => {
          try {
            const fetchResult = await fetchRemoteConfig(config.configUrl!, { etag: lastEtag });
            if (!fetchResult) {
              reloadLog.debug("config unchanged (etag match)");
              return;
            }
            lastEtag = fetchResult.etag;

            const { parsePipelineConfig } = await import("./config-parser.ts");
            const parsed = parsePipelineConfig(fetchResult.content);

            for (const processor of processors) {
              if (processor.reload) {
                const matchingNew = parsed.processors.find(p => p.name === processor.name);
                if (matchingNew) {
                  // The newly parsed processor has the updated config baked in.
                  // We call reload with the new config to swap handlers.
                  processor.reload((matchingNew as any)._reloadConfig);
                }
              }
            }
            reloadLog.info("config reloaded");
          } catch (err) {
            reloadLog.error({ err }, "auto-reload failed");
          }
        }, config.configAutoReloadMs);
      } else if (config.configAutoReloadMs && !config.configUrl) {
        log.warn("configAutoReloadMs requires configUrl — auto-reload disabled");
      }

      // --- Create write buffers per source ---
      const writeBuffers: Map<string, PipelineWriteBuffer> = new Map();
      for (const source of sources) {
        const isLive = source.name.includes("live");
        const intervalMs = config.buffer?.intervalMs ?? (isLive ? 200 : 1000);
        const maxBatchSize = config.buffer?.maxBatchSize ?? (isLive ? 50 : 500);
        const cursorKey = `${source.name}:${config.network ?? "default"}`;

        const buffer = createPipelineWriteBuffer(
          storages,
          state,
          cursorKey,
          {
            label: source.name,
            intervalMs,
            maxBatchSize,
            retries: config.buffer?.retries ?? 3,
            onFlush: (stats: PipelineFlushStats) => {
              if (isLive) {
                metrics.recordLiveFlush({
                  eventsWritten: stats.eventsWritten,
                  tablesWritten: 0,
                  flushDurationMs: stats.flushDurationMs,
                  cursorKeys: new Map(),
                  bufferSizeAfter: stats.bufferSizeAfter,
                });
              } else {
                metrics.recordBackfillFlush({
                  eventsWritten: stats.eventsWritten,
                  tablesWritten: 0,
                  flushDurationMs: stats.flushDurationMs,
                  cursorKeys: new Map(),
                  bufferSizeAfter: stats.bufferSizeAfter,
                });
                if (throttle) {
                  throttle.recordLatency(stats.flushDurationMs);
                  metrics.recordThrottleState(throttle.getConcurrency(), throttle.isPaused());
                }
              }
            },
            onBatchFlushed: (batch) => {
              // Broadcast after successful storage write
              for (const processed of batch) {
                for (const broadcast of broadcasts) {
                  try { broadcast.push(processed); } catch {}
                }
              }
            },
          },
          log.child({ component: `buffer:${source.name}` }),
          acquireWriteLock,
        );

        buffer.start();
        writeBuffers.set(source.name, buffer);
      }

      // --- Run all sources concurrently ---
      const sourcePromises = sources.map(source =>
        runSource(source, processors, writeBuffers.get(source.name)!, metrics, config, log, humanOutput),
      );
      await Promise.all(sourcePromises);

      // --- Cleanup ---
      if (autoReloadInterval) clearInterval(autoReloadInterval);
      if (metricsInterval) clearInterval(metricsInterval);
      if (httpWorker) httpWorker.terminate();

      // Stop write buffers (flushes remaining data)
      for (const buffer of writeBuffers.values()) {
        await buffer.stop();
      }

      // Shutdown storages and broadcasts
      for (const storage of storages) await storage.shutdown();
      for (const broadcast of broadcasts) await broadcast.shutdown();

      // Close SQL connection
      if (sql) await sql.close();

      // Remove signal handlers
      process.removeListener("SIGINT", shutdownHandler);
      process.removeListener("SIGTERM", shutdownHandler);

      if (humanOutput) console.log("\nPipeline stopped.");
    },

    async stop(): Promise<void> {
      stopped = true;
      for (const source of sources) {
        await source.stop();
      }
    },
  };

  return pipeline;
}

// ---------------------------------------------------------------------------
// Source runner — processes checkpoints from a single source
// ---------------------------------------------------------------------------

async function runSource(
  source: Source,
  processors: Processor[],
  buffer: PipelineWriteBuffer,
  metrics: IndexerMetrics,
  config: PipelineConfig,
  log: Logger,
  humanOutput: boolean,
): Promise<void> {
  const isLive = source.name.includes("live");

  let checkpointCount = 0;
  const startTime = performance.now();

  for await (const checkpoint of source.stream()) {
    // Process through all processors
    const processed: ProcessedCheckpoint = {
      checkpoint,
      events: [],
      balanceChanges: [],
      transactions: [],
      moveCalls: [],
    };

    for (const processor of processors) {
      const result = processor.process(checkpoint);
      processed.events.push(...result.events);
      processed.balanceChanges.push(...result.balanceChanges);
      processed.transactions.push(...result.transactions);
      processed.moveCalls.push(...result.moveCalls);
    }

    // Human output
    if (humanOutput) {
      for (const event of processed.events) {
        const shortSender = event.sender.slice(0, 10) + "..." + event.sender.slice(-4);
        console.log(`[${source.name}] EVENT  cp:${event.checkpointSeq} ${event.handlerName} sender:${shortSender} ${JSON.stringify(event.data)}`);
      }
      for (const change of processed.balanceChanges) {
        const shortAddress = change.address.slice(0, 10) + "..." + change.address.slice(-4);
        const sign = BigInt(change.amount) >= 0n ? "+" : "";
        const coinName = change.coinType.split("::").pop();
        console.log(`[${source.name}] BALANCE cp:${change.checkpointSeq} ${shortAddress} ${sign}${change.amount} ${coinName}`);
      }
      for (const tx of processed.transactions) {
        const shortSender = tx.sender.slice(0, 10) + "..." + tx.sender.slice(-4);
        const status = tx.success ? "ok" : "FAIL";
        console.log(`[${source.name}] TX     cp:${tx.checkpointSeq} ${tx.digest.slice(0, 16)}... sender:${shortSender} ${status} calls:${tx.moveCallCount}`);
      }
    }

    // Push to write buffer (handles batching, backpressure, retry, cursor persistence)
    await buffer.push(processed);

    // Update metrics
    if (isLive) {
      metrics.setLiveCursor(checkpoint.sequenceNumber);
      metrics.recordLiveCheckpoint();
    } else {
      metrics.setBackfillCursor(checkpoint.sequenceNumber);
      metrics.recordBackfillCheckpoint();
    }

    // Progress logging
    checkpointCount++;
    if (checkpointCount % 100 === 0) {
      const elapsed = ((performance.now() - startTime) / 1000).toFixed(1);
      const rate = (checkpointCount / (parseFloat(elapsed) || 1)).toFixed(0);
      if (humanOutput) {
        console.log(`[${source.name}] ${checkpointCount} checkpoints (${rate}/s, ${elapsed}s)`);
      }
      log.info({ source: source.name, checkpoints: checkpointCount, rate: `${rate}/s`, elapsed }, "progress");
    }
  }
}
