/**
 * Example: Index Sona's RecordPressedEvent from Sui testnet.
 *
 * Usage:
 *   # Live only (original behavior)
 *   DATABASE_URL=postgres://... bun run examples/sona.ts
 *
 *   # Backfill only (original behavior)
 *   DATABASE_URL=postgres://... bun run examples/sona.ts --backfill 316756645
 *
 *   # Combined live + backfill (new run() mode)
 *   DATABASE_URL=postgres://... bun run examples/sona.ts --run
 *   DATABASE_URL=postgres://... bun run examples/sona.ts --run --live-only
 *   DATABASE_URL=postgres://... bun run examples/sona.ts --run --backfill-only
 *   DATABASE_URL=postgres://... bun run examples/sona.ts --run --repair-gaps
 */
import { defineIndexer, type RunMode } from "../src/index.ts";

const indexer = defineIndexer({
  network: "testnet",
  grpcUrl: "slc1.rpc.testnet.sui.mirai.cloud:443",
  database: process.env.DATABASE_URL!,
  // Backfill starts from the checkpoint where the pressing contract was published
  startCheckpoint: 316756645n,
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

const args = process.argv;

if (args.includes("--run")) {
  // Combined mode via run()
  let mode: RunMode = "all";
  if (args.includes("--live-only")) mode = "live-only";
  if (args.includes("--backfill-only")) mode = "backfill-only";

  const servePort = args.includes("--serve") ? 8080 : undefined;

  await indexer.run({
    mode,
    repairGaps: args.includes("--repair-gaps"),
    serve: servePort ? { port: servePort } : undefined,
  });
} else if (args.includes("--backfill")) {
  // Legacy backfill mode
  const backfillIdx = args.indexOf("--backfill");
  const from = BigInt(args[backfillIdx + 1] ?? "0");
  await indexer.backfill({ from });
} else {
  // Legacy live mode
  await indexer.live();
}
