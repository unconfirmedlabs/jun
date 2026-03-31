/**
 * Jun — Sui Event Indexer for Bun
 *
 * Main entry point. Exports `defineIndexer()` which creates an indexer
 * with `live()` and `backfill()` modes.
 */
import { SQL } from "bun";
import pMap from "p-map";
import { createGrpcClient, type GrpcClient } from "./grpc.ts";
import { cachedGetCheckpoint } from "./archive.ts";
import { createProcessor, type EventHandler, type EventProcessor } from "./processor.ts";
import { createPostgresOutput, type PostgresOutput } from "./output/postgres.ts";
import { createStateManager, type StateManager } from "./state.ts";

// ---------------------------------------------------------------------------
// Public API types
// ---------------------------------------------------------------------------

export type { FieldDefs, FieldType } from "./schema.ts";
export type { EventHandler } from "./processor.ts";

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
}

export interface Indexer {
  /** Subscribe to live checkpoints, process events, write to Postgres. Long-running. */
  live(): Promise<void>;
  /** Backfill from a starting checkpoint to the latest. Exits when done. */
  backfill(options: { from: bigint }): Promise<void>;
  /** Graceful shutdown */
  shutdown(): Promise<void>;
}

// ---------------------------------------------------------------------------
// Table name helpers
// ---------------------------------------------------------------------------

/** Convert handler name to snake_case table name */
function toTableName(handlerName: string): string {
  // Convert camelCase/PascalCase to snake_case
  return handlerName
    .replace(/([A-Z])/g, "_$1")
    .toLowerCase()
    .replace(/^_/, "");
}

// ---------------------------------------------------------------------------
// defineIndexer
// ---------------------------------------------------------------------------

export function defineIndexer(config: IndexerConfig): Indexer {
  const { network, grpcUrl, database, events, backfillConcurrency = 10 } = config;

  let shutdownRequested = false;
  let abortController = new AbortController();

  // Initialized services (set by init(), cleaned up by shutdown())
  let _sql: any = null;
  let _grpcClient: GrpcClient | null = null;

  // Build handler table mapping
  const handlerTables: Record<string, { tableName: string; fields: EventHandler["fields"] }> = {};
  for (const [name, handler] of Object.entries(events)) {
    handlerTables[name] = {
      tableName: toTableName(name),
      fields: handler.fields,
    };
  }

  /** Initialize all services */
  async function init() {
    _sql = new SQL(database);
    const state = await createStateManager(_sql);
    const output = createPostgresOutput(_sql, handlerTables);
    await output.migrate();
    const processor = createProcessor(events);
    _grpcClient = createGrpcClient({ url: grpcUrl });
    console.log(`[jun] initialized — network=${network}, events=${Object.keys(events).join(", ")}`);
    return { state, output, processor, grpcClient: _grpcClient };
  }

  // SIGINT/SIGTERM handlers
  function setupSignalHandlers() {
    const handler = () => {
      if (shutdownRequested) {
        console.log("[jun] force shutdown");
        process.exit(1);
      }
      console.log("\n[jun] shutting down gracefully...");
      shutdownRequested = true;
      abortController.abort();
    };
    process.on("SIGINT", handler);
    process.on("SIGTERM", handler);
  }

  return {
    async live(): Promise<void> {
      let { state, output, processor, grpcClient } = await init();
      setupSignalHandlers();

      const cursorKey = `live:${network}`;
      let reconnectDelay = 1000; // Start at 1s, exponential backoff

      while (!shutdownRequested) {
        try {
          console.log(`[jun] connecting to ${grpcUrl}...`);
          // Recreate gRPC client on reconnect
          grpcClient.close();
          grpcClient = createGrpcClient({ url: grpcUrl });
          _grpcClient = grpcClient;

          const stream = grpcClient.subscribeCheckpoints();
          reconnectDelay = 1000; // Reset on successful connection

          for await (const response of stream) {
            if (shutdownRequested) break;

            const seq = BigInt(response.cursor);
            const decoded = processor.process(response);

            if (decoded.length > 0) {
              await output.write(decoded);
              console.log(`[live] checkpoint ${seq}: ${decoded.length} event(s)`);
            }

            await state.setCheckpointCursor(cursorKey, seq);
          }
        } catch (err) {
          if (shutdownRequested) break;

          const msg = err instanceof Error ? err.message : String(err);
          console.error(`[jun] stream error: ${msg}`);
          console.log(`[jun] reconnecting in ${reconnectDelay / 1000}s...`);

          await sleep(reconnectDelay);
          reconnectDelay = Math.min(reconnectDelay * 2, 30000); // Cap at 30s
        }
      }

      await this.shutdown();
    },

    async backfill(options: { from: bigint }): Promise<void> {
      const { state, output, processor, grpcClient } = await init();
      setupSignalHandlers();

      const cursorKey = `backfill:${network}`;
      const from = options.from;

      // Get latest checkpoint to know where to stop
      console.log(`[jun] fetching latest checkpoint...`);
      const latest = await getLatestCheckpoint(grpcClient);
      const to = BigInt(latest);
      console.log(`[jun] backfilling from ${from} to ${to} (${to - from + 1n} checkpoints)`);

      // Check if we have a saved cursor
      const savedCursor = await state.getCheckpointCursor(cursorKey);
      const startFrom = savedCursor && savedCursor > from ? savedCursor + 1n : from;

      if (startFrom > to) {
        console.log(`[jun] backfill already complete (cursor at ${savedCursor})`);
        await this.shutdown();
        return;
      }

      if (savedCursor) {
        console.log(`[jun] resuming from checkpoint ${startFrom} (previously at ${savedCursor})`);
      }

      // Build checkpoint sequence
      const checkpoints: bigint[] = [];
      for (let i = startFrom; i <= to; i++) {
        checkpoints.push(i);
      }

      let processed = 0;
      let totalEvents = 0;
      const startTime = performance.now();

      // Track completed checkpoints for safe cursor advancement.
      // With concurrent processing, we can only advance the cursor to the
      // highest contiguous completed checkpoint to avoid gaps on resume.
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

      // Process in batches with concurrency
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

            // Progress log every 100 checkpoints
            if (processed % 100 === 0) {
              const elapsed = ((performance.now() - startTime) / 1000).toFixed(1);
              const rate = (processed / (parseFloat(elapsed) || 1)).toFixed(0);
              console.log(
                `[backfill] ${processed}/${checkpoints.length} checkpoints (${rate}/s, ${totalEvents} events, ${elapsed}s)`,
              );
            }
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            console.error(`[backfill] error at checkpoint ${seq}: ${msg}`);
            // Don't throw — skip and continue. The watermark won't advance past gaps.
          }
        },
        { concurrency: backfillConcurrency, signal: abortController.signal },
      ).catch((err) => {
        if (err.name === "AbortError") return; // Expected on shutdown
        throw err;
      });

      const elapsed = ((performance.now() - startTime) / 1000).toFixed(1);
      console.log(
        `[jun] backfill complete — ${processed} checkpoints, ${totalEvents} events in ${elapsed}s`,
      );

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
      console.log("[jun] shutdown complete");
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
  // Subscribe briefly to get the first checkpoint (which is the latest)
  const stream = client.subscribeCheckpoints();
  for await (const response of stream) {
    // break triggers the async iterator's return(), which cancels the gRPC call
    return response.cursor;
  }
  throw new Error("Failed to get latest checkpoint");
}
