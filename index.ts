/**
 * Jun — Sui Data Pipeline for Bun
 *
 * Composable Source → Processor → Destination pipeline for indexing
 * Sui blockchain data.
 *
 * @example Pipeline API (recommended)
 * ```ts
 * import { createPipeline } from "jun";
 * import { createGrpcLiveSource } from "jun/pipeline/sources/grpc-live";
 * import { createBalanceTracker } from "jun/pipeline/processors/balance-tracker";
 * import { createPostgresDestination } from "jun/pipeline/destinations/postgres";
 *
 * const pipeline = createPipeline()
 *   .source(createGrpcLiveSource({ url: "fullnode.testnet.sui.io:443" }))
 *   .processor(createBalanceTracker({ coinTypes: ["0x2::sui::SUI"] }))
 *   .destination(createPostgresDestination({ url: process.env.DATABASE_URL! }))
 *   .run();
 * ```
 *
 * @example YAML config
 * ```bash
 * jun pipeline run config.yml
 * ```
 */

// Pipeline API
export { createPipeline } from "./src/pipeline/pipeline.ts";
export type {
  Pipeline,
  PipelineConfig,
  Source,
  Processor,
  Storage,
  Broadcast,
  Checkpoint,
  ProcessedCheckpoint,
  DecodedEvent,
  BalanceChange,
} from "./src/pipeline/types.ts";

// Config parser
export { parsePipelineConfig } from "./src/pipeline/config-parser.ts";
export type { ParsedPipelineConfig } from "./src/pipeline/config-parser.ts";

// Legacy API (defineIndexer — still functional, will be deprecated)
export { defineIndexer } from "./src/index.ts";
export type { IndexerConfig, Indexer, RunOptions, RunMode } from "./src/index.ts";

// Schema types
export type { FieldDefs, FieldType, PrimitiveFieldType } from "./src/schema.ts";
