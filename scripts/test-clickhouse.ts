/**
 * Smoke test for the ClickHouse destination.
 * Spins up the storage, writes a synthetic checkpoint, and queries back rows.
 *
 * Usage: bun scripts/test-clickhouse.ts
 */
import { createReplayClickHouseStorage } from "../src/pipeline/destinations/clickhouse.ts";
import { createClient } from "@clickhouse/client";
import type { ProcessedCheckpoint } from "../src/pipeline/types.ts";

// ---------------------------------------------------------------------------
// Synthetic checkpoint
// ---------------------------------------------------------------------------

const now = new Date();

const syntheticCheckpoint: ProcessedCheckpoint = {
  checkpoint: {
    sequenceNumber: 1000n,
    epoch: 42n,
    digest: "FakeCheckpointDigest1000",
    previousDigest: "FakeCheckpointDigest0999",
    contentDigest: "FakeContentDigest",
    timestamp: now,
    totalNetworkTransactions: 999999n,
    source: "backfill",
    transactions: [],
    epochRollingGasCostSummary: {
      computationCost: "1000000",
      storageCost: "500000",
      storageRebate: "200000",
      nonRefundableStorageFee: "50000",
    },
  },
  transactions: [
    {
      digest: "FakeTxDigest0001",
      sender: "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
      success: true,
      computationCost: "1000",
      storageCost: "500",
      storageRebate: "200",
      nonRefundableStorageFee: "50",
      moveCallCount: 2,
      checkpointSeq: 1000n,
      timestamp: now,
      epoch: 42n,
      errorKind: null,
      errorDescription: null,
      errorCommandIndex: null,
      errorAbortCode: null,
      errorModule: null,
      errorFunction: null,
      eventsDigest: "FakeEventsDigest",
      lamportVersion: "12345",
      dependencyCount: 1,
    },
    {
      digest: "FakeTxDigest0002",
      sender: "0xcafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe",
      success: false,
      computationCost: "2000",
      storageCost: "0",
      storageRebate: "0",
      nonRefundableStorageFee: null,
      moveCallCount: 1,
      checkpointSeq: 1000n,
      timestamp: now,
      epoch: 42n,
      errorKind: "MoveAbort",
      errorDescription: "assertion failed",
      errorCommandIndex: 0,
      errorAbortCode: "1",
      errorModule: "0xdeadbeef::swap",
      errorFunction: "swap_exact_input",
      eventsDigest: null,
      lamportVersion: null,
      dependencyCount: 0,
    },
  ],
  moveCalls: [
    {
      txDigest: "FakeTxDigest0001",
      callIndex: 0,
      package: "0xdee9cdb49bccb9dba3bb5726fba2e71d7ccee6d0a1a55b7d2c2eda30bee5c7f8",
      module: "clob_v2",
      function: "place_market_order",
      checkpointSeq: 1000n,
      timestamp: now,
    },
    {
      txDigest: "FakeTxDigest0001",
      callIndex: 1,
      package: "0x2",
      module: "coin",
      function: "transfer",
      checkpointSeq: 1000n,
      timestamp: now,
    },
  ],
  balanceChanges: [
    {
      txDigest: "FakeTxDigest0001",
      checkpointSeq: 1000n,
      address: "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
      coinType: "0x2::sui::SUI",
      amount: "-1000",
      timestamp: now,
    },
    {
      txDigest: "FakeTxDigest0001",
      checkpointSeq: 1000n,
      address: "0xcafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe",
      coinType: "0x2::sui::SUI",
      amount: "1000",
      timestamp: now,
    },
  ],
  objectChanges: [
    {
      txDigest: "FakeTxDigest0001",
      objectId: "0x1111111111111111111111111111111111111111111111111111111111111111",
      changeType: "MUTATED",
      objectType: "0x2::coin::Coin<0x2::sui::SUI>",
      inputVersion: "99",
      inputDigest: "FakeInputDigest",
      inputOwner: "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
      inputOwnerKind: "AddressOwner",
      outputVersion: "100",
      outputDigest: "FakeOutputDigest",
      outputOwner: "0xcafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe",
      outputOwnerKind: "AddressOwner",
      isGasObject: false,
      checkpointSeq: 1000n,
      timestamp: now,
    },
    {
      txDigest: "FakeTxDigest0001",
      objectId: "0x2222222222222222222222222222222222222222222222222222222222222222",
      changeType: "CREATED",
      objectType: "0x2::coin::Coin<0x2::sui::SUI>",
      inputVersion: null,
      inputDigest: null,
      inputOwner: null,
      inputOwnerKind: null,
      outputVersion: "1",
      outputDigest: "FakeNewObjDigest",
      outputOwner: "0xcafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe",
      outputOwnerKind: "AddressOwner",
      isGasObject: false,
      checkpointSeq: 1000n,
      timestamp: now,
    },
  ],
  dependencies: [
    {
      txDigest: "FakeTxDigest0001",
      dependsOnDigest: "PriorTxDigest9999",
      checkpointSeq: 1000n,
      timestamp: now,
    },
  ],
  inputs: [
    {
      txDigest: "FakeTxDigest0001",
      inputIndex: 0,
      kind: "IMMUTABLE_OR_OWNED",
      objectId: "0x1111111111111111111111111111111111111111111111111111111111111111",
      version: "99",
      digest: "FakeInputDigest",
      mutability: "Mutable",
      initialSharedVersion: null,
      pureBytes: null,
      amount: null,
      coinType: null,
      source: null,
      checkpointSeq: 1000n,
      timestamp: now,
    },
    {
      txDigest: "FakeTxDigest0001",
      inputIndex: 1,
      kind: "PURE",
      objectId: null,
      version: null,
      digest: null,
      mutability: null,
      initialSharedVersion: null,
      pureBytes: "deadbeef",
      amount: null,
      coinType: null,
      source: null,
      checkpointSeq: 1000n,
      timestamp: now,
    },
  ],
  commands: [
    {
      txDigest: "FakeTxDigest0001",
      commandIndex: 0,
      kind: "MoveCall",
      package: "0xdee9cdb49bccb9dba3bb5726fba2e71d7ccee6d0a1a55b7d2c2eda30bee5c7f8",
      module: "clob_v2",
      function: "place_market_order",
      typeArguments: '["0x2::sui::SUI","0x5d4b302506645c37ff133b98c4b50a5ae14841659738d6d733d59d0d217a93bf::coin::COIN"]',
      args: '{"inputs":[0,1],"results":[]}',
      checkpointSeq: 1000n,
      timestamp: now,
    },
  ],
  systemTransactions: [],
  unchangedConsensusObjects: [
    {
      txDigest: "FakeTxDigest0001",
      objectId: "0x3333333333333333333333333333333333333333333333333333333333333333",
      kind: "SharedObject",
      version: "50",
      digest: "FakeConsensusObjDigest",
      objectType: "0xdee9::clob_v2::Pool<0x2::sui::SUI,0x5d4b::coin::COIN>",
      checkpointSeq: 1000n,
      timestamp: now,
    },
  ],
  rawEvents: [
    {
      txDigest: "FakeTxDigest0001",
      eventSeq: 0,
      packageId: "0xdee9cdb49bccb9dba3bb5726fba2e71d7ccee6d0a1a55b7d2c2eda30bee5c7f8",
      module: "clob_v2",
      eventType: "0xdee9::clob_v2::OrderPlaced",
      sender: "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
      contents: "deadbeef01020304",
      checkpointSeq: 1000n,
      timestamp: now,
    },
  ],
  events: [],
};

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

const storage = createReplayClickHouseStorage({
  url: "http://localhost:8123",
  database: "jun",
});

console.log("Initializing storage (creating tables)...");
await storage.initialize();
console.log("  OK");

console.log("Writing synthetic checkpoint...");
await storage.write([syntheticCheckpoint]);
console.log("  OK");

await storage.shutdown();

// ---------------------------------------------------------------------------
// Query back and verify
// ---------------------------------------------------------------------------

const client = createClient({ url: "http://localhost:8123", database: "jun" });

async function query(label: string, sql: string): Promise<void> {
  const result = await client.query({ query: sql, format: "JSONEachRow" });
  const rows = await result.json();
  console.log(`\n${label} (${rows.length} row${rows.length === 1 ? "" : "s"}):`);
  for (const row of rows) {
    console.log(" ", JSON.stringify(row));
  }
}

await query("transactions", "SELECT digest, sender, success, epoch, error_kind FROM transactions");
await query("move_calls", "SELECT tx_digest, package, module, function FROM move_calls");
await query("balance_changes", "SELECT address, coin_type, amount FROM balance_changes ORDER BY amount");
await query("balances (computed from balance_changes)", `
  SELECT address, coin_type, sum(toInt128(amount)) AS balance
  FROM balance_changes
  GROUP BY address, coin_type
  ORDER BY balance
`);
await query("object_changes", "SELECT object_id, change_type, input_version, output_version FROM object_changes");
await query("transaction_inputs", "SELECT tx_digest, input_index, kind, object_id, version FROM transaction_inputs");
await query("commands", "SELECT tx_digest, command_index, kind, package, module, function FROM commands");
await query("unchanged_consensus_objects", "SELECT object_id, kind, version FROM unchanged_consensus_objects");
await query("raw_events", "SELECT tx_digest, event_seq, event_type FROM raw_events");
await query("checkpoints", "SELECT sequence_number, epoch, digest FROM checkpoints");

await client.close();

console.log("\nDone.");
