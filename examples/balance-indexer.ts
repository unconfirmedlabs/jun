/**
 * Example: Track SUI balance changes on testnet using the pipeline API.
 *
 * Usage:
 *   DATABASE_URL=postgres://... bun run examples/balance-indexer.ts
 */
import { createPipeline } from "../src/pipeline/pipeline.ts";
import { createGrpcLiveSource } from "../src/pipeline/sources/grpc.ts";
import { createArchiveSource } from "../src/pipeline/sources/archive.ts";
import { createBalanceTracker } from "../src/pipeline/processors/balance-tracker.ts";
import { createSqlStorage } from "../src/pipeline/destinations/sql.ts";

const pipeline = createPipeline()
  .source(createGrpcLiveSource({ url: "fullnode.testnet.sui.io:443" }))
  .source(createArchiveSource({
    archiveUrl: "https://checkpoints.testnet.sui.io",
    from: 100_000_000n,
    grpcUrl: "fullnode.testnet.sui.io:443",
  }))
  .processor(createBalanceTracker({ coinTypes: ["0x2::sui::SUI"] }))
  .storage(createSqlStorage({
    url: process.env.DATABASE_URL!,
    balances: true,
  }));

await pipeline.run();
