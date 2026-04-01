/**
 * Jun — Sui Event Indexer for Bun
 *
 * The default export is the high-level indexer API. For building blocks,
 * import from subpaths:
 *
 * @example Define and run an indexer
 * ```ts
 * import { defineIndexer } from "jun";
 *
 * const indexer = defineIndexer({
 *   network: "mainnet",
 *   grpcUrl: "fullnode.mainnet.sui.io:443",
 *   database: process.env.DATABASE_URL!,
 *   startCheckpoint: "package:0x...",
 *   events: { ... },
 * });
 *
 * await indexer.run();
 * ```
 *
 * @example Compose building blocks
 * ```ts
 * import { createGrpcClient } from "jun/grpc";
 * import { createProcessor } from "jun/events";
 * import { createArchiveClient } from "jun/checkpoints";
 * import { createWriteBuffer, createAdaptiveThrottle } from "jun/pipeline";
 * import { createPostgresOutput } from "jun/output/postgres";
 * import { createStateManager } from "jun/cursor";
 * ```
 */

// Indexer — high-level orchestrator
export { defineIndexer } from "./src/index.ts";
export type {
  IndexerConfig,
  Indexer,
  RunOptions,
  RunMode,
} from "./src/index.ts";

// Types users always need alongside defineIndexer
export type { EventHandler, DecodedEvent } from "./src/processor.ts";
export type { FieldDefs, FieldType, PrimitiveFieldType } from "./src/schema.ts";
export type { ServeConfig, IndexerMetrics, MetricsSnapshot } from "./src/serve.ts";
export { parseIndexerConfig, loadIndexerConfig, mergeRunOptions } from "./src/indexer-config.ts";
export type { ParsedIndexerConfig } from "./src/indexer-config.ts";
