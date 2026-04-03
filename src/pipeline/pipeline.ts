/**
 * Pipeline orchestrator — connects Sources → Processors → Destinations.
 *
 * Handles buffering, cursor tracking, backpressure, throttling, and
 * gap repair between the components.
 *
 * @example
 * ```ts
 * import { createPipeline } from "jun/pipeline";
 * import { createGrpcLiveSource } from "jun/pipeline/sources/grpc-live";
 * import { createEventDecoder } from "jun/pipeline/processors/event-decoder";
 * import { createPostgresDestination } from "jun/pipeline/destinations/postgres";
 *
 * const pipeline = createPipeline()
 *   .source(createGrpcLiveSource({ url: "fullnode.testnet.sui.io:443" }))
 *   .processor(createEventDecoder({ handlers: { ... } }))
 *   .destination(createPostgresDestination({ url: "postgres://..." }))
 *   .run();
 * ```
 */
import type {
  Source,
  Processor,
  Storage,
  Broadcast,
  Pipeline,
  PipelineConfig,
  Checkpoint,
  ProcessedCheckpoint,
} from "./types.ts";
import { createLogger } from "../logger.ts";
import type { Logger } from "../logger.ts";

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

      // Print human-readable banner
      if (humanOutput) {
        console.log("\n=== Jun Pipeline ===");
        console.log(`Sources:    ${sources.map(s => s.name).join(", ")}`);
        if (processors.length > 0) console.log(`Processors: ${processors.map(p => p.name).join(", ")}`);
        if (storages.length > 0) console.log(`Storage:    ${storages.map(s => s.name).join(", ")}`);
        if (broadcasts.length > 0) console.log(`Broadcast:  ${broadcasts.map(b => b.name).join(", ")}`);
        if (config.serve) console.log(`HTTP Server: port ${config.serve.port}`);
        console.log("");
      }

      // Shared write mutex — prevents concurrent storage writes from different sources (avoids deadlocks)
      let writeLock: Promise<void> = Promise.resolve();
      function acquireWriteLock(): Promise<() => void> {
        let release: () => void;
        const prev = writeLock;
        writeLock = new Promise(resolve => { release = resolve; });
        return prev.then(() => release!);
      }

      // Run all sources concurrently
      const sourcePromises = sources.map(source => runSource(source, processors, storages, broadcasts, config, log, humanOutput, acquireWriteLock));
      await Promise.all(sourcePromises);

      // Shutdown
      for (const storage of storages) await storage.shutdown();
      for (const broadcast of broadcasts) await broadcast.shutdown();

      // Stop sources
      for (const source of sources) {
        await source.stop();
      }

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
  storages: Storage[],
  broadcasts: Broadcast[],
  config: PipelineConfig,
  log: Logger,
  humanOutput: boolean,
  acquireWriteLock?: () => Promise<() => void>,
): Promise<void> {
  const bufferIntervalMs = config.buffer?.intervalMs ?? (source.name.includes("live") ? 200 : 1000);
  const maxBatchSize = config.buffer?.maxBatchSize ?? (source.name.includes("live") ? 50 : 500);

  let batch: ProcessedCheckpoint[] = [];
  let lastFlush = performance.now();

  async function flushStorage(): Promise<void> {
    if (batch.length === 0) return;
    const toFlush = batch;
    batch = [];

    if (storages.length > 0) {
      // Acquire write lock to prevent concurrent storage writes (deadlock prevention)
      const release = acquireWriteLock ? await acquireWriteLock() : () => {};
      try {
        await Promise.all(storages.map(storage => storage.write(toFlush)));
      } finally {
        release();
      }
    }
    lastFlush = performance.now();
  }

  // Periodic storage flush timer
  const flushTimer = setInterval(async () => {
    try { await flushStorage(); } catch (err) {
      log.error({ err, source: source.name }, "periodic flush error");
    }
  }, bufferIntervalMs);

  try {
    let checkpointCount = 0;
    const startTime = performance.now();

    for await (const checkpoint of source.stream()) {
      // Process through all processors
      let processed: ProcessedCheckpoint = {
        checkpoint,
        events: [],
        balanceChanges: [],
      };

      for (const processor of processors) {
        const result = processor.process(checkpoint);
        processed.events.push(...result.events);
        processed.balanceChanges.push(...result.balanceChanges);
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
      }

      // Broadcast immediately (fire-and-forget, low latency)
      for (const broadcast of broadcasts) {
        try { broadcast.push(processed); } catch {}
      }

      // Batch for storage (flushed on interval or threshold)
      batch.push(processed);
      if (batch.length >= maxBatchSize) {
        await flushStorage();
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

    // Final storage flush
    await flushStorage();
  } finally {
    clearInterval(flushTimer);
  }
}
