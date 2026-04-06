/**
 * Pipeline orchestrator — Source → Storage + Broadcast.
 *
 * Buffers checkpoints, writes to storage in batches, broadcasts after write.
 * Graceful shutdown on SIGINT/SIGTERM.
 */
import type {
  Source, Storage, Broadcast, Pipeline, PipelineConfig, ProcessedCheckpoint,
} from "./types.ts";
import { emptyProcessed } from "./types.ts";
import { createPipelineWriteBuffer, type PipelineWriteBuffer } from "./write-buffer.ts";
import { createLogger } from "../logger.ts";
import type { Logger } from "../logger.ts";

export function createPipeline(): Pipeline {
  const sources: Source[] = [];
  const storages: Storage[] = [];
  const broadcasts: Broadcast[] = [];
  let config: PipelineConfig = {};
  let stopped = false;
  const log = createLogger();

  const pipeline: Pipeline = {
    source(s: Source)       { sources.push(s); return pipeline; },
    processor()             { return pipeline; }, // no-op — Rust handles extraction
    storage(s: Storage)     { storages.push(s); return pipeline; },
    broadcast(b: Broadcast) { broadcasts.push(b); return pipeline; },
    configure(c: PipelineConfig) { config = { ...config, ...c }; return pipeline; },

    async run(): Promise<void> {
      if (sources.length === 0) throw new Error("Pipeline has no sources");

      const humanOutput = !config.quiet;

      if (config.log) {
        const level = typeof config.log === "string" ? config.log : "info";
        process.env.LOG_LEVEL = level;
        log.level = level;
      } else {
        process.env.LOG_LEVEL = "silent";
        log.level = "silent";
      }

      // Initialize
      for (const s of storages) { await s.initialize(); log.info({ storage: s.name }, "storage initialized"); }
      for (const b of broadcasts) { await b.initialize(); log.info({ broadcast: b.name }, "broadcast initialized"); }

      if (humanOutput) {
        console.log("\n=== Jun Pipeline ===");
        console.log(`Sources:    ${sources.map(s => s.name).join(", ")}`);
        if (storages.length > 0) console.log(`Storage:    ${storages.map(s => s.name).join(", ")}`);
        if (broadcasts.length > 0) console.log(`Broadcast:  ${broadcasts.map(b => b.name).join(", ")}`);
        console.log("");
      }

      // Graceful shutdown
      let signalCount = 0;
      const shutdownHandler = () => {
        signalCount++;
        if (signalCount === 1) {
          if (humanOutput) console.log("\nShutting down gracefully...");
          stopped = true;
          for (const s of sources) s.stop();
        } else {
          process.exit(1);
        }
      };
      process.on("SIGINT", shutdownHandler);
      process.on("SIGTERM", shutdownHandler);

      // Write buffers — one per source
      const writeBuffers: Map<string, PipelineWriteBuffer> = new Map();
      for (const source of sources) {
        const isLive = source.name.includes("live");
        const buffer = createPipelineWriteBuffer(
          storages,
          null, // no cursor persistence
          source.name,
          {
            label: source.name,
            intervalMs: config.buffer?.intervalMs ?? (isLive ? 200 : 1000),
            maxBatchSize: config.buffer?.maxBatchSize ?? (isLive ? 50 : 1000),
            retries: config.buffer?.retries ?? 3,
            onBatchFlushed: broadcasts.length > 0 ? (batch) => {
              for (const processed of batch) {
                for (const b of broadcasts) {
                  try { b.push(processed); } catch {}
                }
              }
            } : undefined,
          },
          log.child({ component: `buffer:${source.name}` }),
        );
        buffer.start();
        writeBuffers.set(source.name, buffer);
      }

      // Run all sources concurrently
      await Promise.all(sources.map(source =>
        runSource(source, writeBuffers.get(source.name)!, config, log, humanOutput),
      ));

      // Cleanup
      for (const buf of writeBuffers.values()) await buf.stop();
      for (const s of storages) await s.shutdown();
      for (const b of broadcasts) await b.shutdown();

      process.removeListener("SIGINT", shutdownHandler);
      process.removeListener("SIGTERM", shutdownHandler);

      if (humanOutput) console.log("\nPipeline stopped.");
    },

    async stop(): Promise<void> {
      stopped = true;
      for (const s of sources) await s.stop();
    },
  };

  return pipeline;
}

// ---------------------------------------------------------------------------
// Source runner
// ---------------------------------------------------------------------------

function progressBar(pct: number, width = 20): string {
  const filled = Math.round((pct / 100) * width);
  return "█".repeat(filled) + "░".repeat(width - filled);
}

async function runSource(
  source: Source,
  buffer: PipelineWriteBuffer,
  config: PipelineConfig,
  log: Logger,
  humanOutput: boolean,
): Promise<void> {
  let count = 0;
  const start = performance.now();

  for await (const checkpoint of source.stream()) {
    const rawBinary = (checkpoint as any)._rawBinary as Uint8Array | undefined;
    const preProcessed = (checkpoint as any)._preProcessed as ProcessedCheckpoint | undefined;

    const processed = preProcessed ?? emptyProcessed(checkpoint);
    if (rawBinary) {
      (processed as any)._rawBinary = rawBinary;
      (processed as any)._enabledProcessors = (checkpoint as any)._enabledProcessors;
      (processed as any)._balanceCoinTypes = (checkpoint as any)._balanceCoinTypes;
    }

    await buffer.push(processed);
    count++;

    // Progress bar (every 100 checkpoints)
    if (count % 100 === 0) {
      const elapsed = (performance.now() - start) / 1000;
      const rate = Math.round(count / (elapsed || 1));
      const total = config.totalCheckpoints ? Number(config.totalCheckpoints) : undefined;

      if (total) {
        const pct = Math.round((count / total) * 100);
        const remaining = Math.round((total - count) / (rate || 1));
        process.stderr.write(`\r[${source.name}] ${progressBar(pct)} ${pct}% | ${count.toLocaleString()}/${total.toLocaleString()} | ${rate}/s | ETA ${remaining}s  `);
      }
    }
  }

  // Final progress bar
  if (config.totalCheckpoints && source.name !== "live") {
    const elapsed = (performance.now() - start) / 1000;
    const rate = Math.round(count / (elapsed || 1));
    process.stderr.write(`\r[${source.name}] ${progressBar(100)} 100% | ${count.toLocaleString()} checkpoints | ${rate}/s | ${elapsed.toFixed(1)}s\n`);
  }
}
