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
  Destination,
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
  const destinations: Destination[] = [];
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

    destination(destination: Destination): Pipeline {
      destinations.push(destination);
      return pipeline;
    },

    configure(pipelineConfig: PipelineConfig): Pipeline {
      config = { ...config, ...pipelineConfig };
      return pipeline;
    },

    async run(): Promise<void> {
      if (sources.length === 0) throw new Error("Pipeline has no sources");
      if (destinations.length === 0) throw new Error("Pipeline has no destinations");

      const interactive = config.display !== "headless";
      const verbose = config.display === "verbose";

      // Set log level based on display mode
      if (interactive && !verbose) log.level = "silent";
      if (verbose) log.level = "debug";

      // Initialize processors
      for (const processor of processors) {
        if (processor.initialize) {
          await processor.initialize();
          log.info({ processor: processor.name }, "processor initialized");
        }
      }

      // Initialize destinations
      for (const destination of destinations) {
        await destination.initialize();
        log.info({ destination: destination.name }, "destination initialized");
      }

      // Print interactive banner
      if (interactive) {
        console.log("\n=== Jun Pipeline ===");
        console.log(`Sources:      ${sources.map(s => s.name).join(", ")}`);
        console.log(`Processors:   ${processors.length > 0 ? processors.map(p => p.name).join(", ") : "none"}`);
        console.log(`Destinations: ${destinations.map(d => d.name).join(", ")}`);
        if (config.serve) console.log(`HTTP Server:  port ${config.serve.port}`);
        console.log("");
      }

      // Run all sources concurrently
      const sourcePromises = sources.map(source => runSource(source, processors, destinations, config, log, interactive));
      await Promise.all(sourcePromises);

      // Shutdown destinations
      for (const destination of destinations) {
        await destination.shutdown();
      }

      // Stop sources
      for (const source of sources) {
        await source.stop();
      }

      if (interactive) console.log("\nPipeline stopped.");
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
  destinations: Destination[],
  config: PipelineConfig,
  log: Logger,
  interactive: boolean,
): Promise<void> {
  const bufferIntervalMs = config.buffer?.intervalMs ?? (source.name.includes("live") ? 200 : 1000);
  const maxBatchSize = config.buffer?.maxBatchSize ?? (source.name.includes("live") ? 50 : 500);

  let batch: ProcessedCheckpoint[] = [];
  let lastFlush = performance.now();

  async function flush(): Promise<void> {
    if (batch.length === 0) return;
    const toFlush = batch;
    batch = [];

    // Write to all destinations in parallel
    await Promise.all(destinations.map(destination => destination.write(toFlush)));
    lastFlush = performance.now();
  }

  // Periodic flush timer
  const flushTimer = setInterval(async () => {
    try { await flush(); } catch (err) {
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

      // Interactive output
      if (interactive) {
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

      // Add to batch
      batch.push(processed);

      // Threshold flush
      if (batch.length >= maxBatchSize) {
        await flush();
      }

      // Progress logging
      checkpointCount++;
      if (checkpointCount % 100 === 0) {
        const elapsed = ((performance.now() - startTime) / 1000).toFixed(1);
        const rate = (checkpointCount / (parseFloat(elapsed) || 1)).toFixed(0);
        if (interactive) {
          console.log(`[${source.name}] ${checkpointCount} checkpoints (${rate}/s, ${elapsed}s)`);
        }
        log.info({ source: source.name, checkpoints: checkpointCount, rate: `${rate}/s`, elapsed }, "progress");
      }
    }

    // Final flush
    await flush();
  } finally {
    clearInterval(flushTimer);
  }
}
