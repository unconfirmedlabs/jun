/**
 * Example: Index Sona's RecordPressedEvent from Sui testnet.
 *
 * Usage:
 *   DATABASE_URL=postgres://... bun run examples/sona.ts
 *   DATABASE_URL=postgres://... bun run examples/sona.ts --backfill 316756645
 */
import { defineIndexer } from "../src/index.ts";

const indexer = defineIndexer({
  network: "testnet",
  grpcUrl: "slc1.rpc.testnet.sui.mirai.cloud:443",
  database: process.env.DATABASE_URL!,
  events: {
    RecordPressed: {
      type: "0x10ad578f5b202fd137546f2e7bc12c319dfef98871feeb429506c4b3d62bf702::pressing::RecordPressedEvent",
      fields: {
        pressing_id: "address",
        release_id: "address",
        edition: "u16",
        record_id: "address",
        record_number: "u64",
        quantity: "u64",
        pressed_by: "address",
        paid_value: "u64",
        timestamp_ms: "u64",
      },
    },
  },
});

// Check for --backfill flag
const backfillIdx = process.argv.indexOf("--backfill");
if (backfillIdx !== -1) {
  const from = BigInt(process.argv[backfillIdx + 1] ?? "0");
  await indexer.backfill({ from });
} else {
  await indexer.live();
}
