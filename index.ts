/**
 * Jun — Sui Data Pipeline for Bun
 *
 * Composable Source → Processor → Destination pipeline for indexing
 * Sui blockchain data.
 *
 * @example Pipeline API
 * ```ts
 * import { createPipeline } from "jun";
 * import { createGrpcLiveSource } from "jun/pipeline/sources/grpc-live";
 * import { createBalanceTracker } from "jun/pipeline/processors/balance-tracker";
 * import { createSqlStorage } from "jun/pipeline/destinations/sql";
 *
 * const pipeline = createPipeline()
 *   .source(createGrpcLiveSource({ url: "fullnode.testnet.sui.io:443" }))
 *   .processor(createBalanceTracker({ coinTypes: ["0x2::sui::SUI"] }))
 *   .storage(createSqlStorage({ url: process.env.DATABASE_URL! }))
 *   .run();
 * ```
 *
 * @example YAML config
 * ```bash
 * jun run config.yml
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

// Schema types
export type { FieldDefs, FieldType, PrimitiveFieldType } from "./src/schema.ts";
