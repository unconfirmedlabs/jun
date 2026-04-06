import { afterEach, describe, expect, test } from "bun:test";
import { rmSync, unlinkSync } from "fs";
const TEST_DB = "/tmp/jun-pipeline-sql-test.db";
const TEST_SHARD_DIR = "/tmp/jun-pipeline-sql-shards";

function cleanupDb(path: string): void {
  try { unlinkSync(path); } catch {}
  try { unlinkSync(path + "-wal"); } catch {}
  try { unlinkSync(path + "-shm"); } catch {}
}

function runBun<T = void>(script: string): T {
  const proc = Bun.spawnSync(["bun", "-e", script], {
    stdout: "pipe",
    stderr: "pipe",
  });
  if (proc.exitCode !== 0) {
    throw new Error(new TextDecoder().decode(proc.stderr));
  }
  const stdout = new TextDecoder().decode(proc.stdout).trim();
  return (stdout ? JSON.parse(stdout) : undefined) as T;
}

describe("createSqlStorage snapshot mode", () => {
  afterEach(() => {
    cleanupDb(TEST_DB);
    try { rmSync(TEST_SHARD_DIR, { recursive: true, force: true }); } catch {}
  });

  test("repairs duplicate transactions and move calls before creating deferred indexes", async () => {
    runBun(`
      import { createSqlStorage } from "./src/pipeline/destinations/sql.ts";
      const storage = createSqlStorage({
        url: "sqlite:${TEST_DB}",
        transactions: true,
        deferIndexes: true,
      });
      await storage.initialize();
      const timestamp = new Date("2026-04-01T12:00:00Z");
      const checkpoint = (seq) => ({
        sequenceNumber: BigInt(seq),
        timestamp,
        transactions: [],
        source: "backfill",
      });
      const transaction = (seq) => ({
        digest: "0xdup",
        sender: "0x1",
        success: true,
        computationCost: "1",
        storageCost: "2",
        storageRebate: "0",
        moveCallCount: 1,
        checkpointSeq: BigInt(seq),
        timestamp,
      });
      const moveCall = (seq) => ({
        txDigest: "0xdup",
        callIndex: 0,
        package: "0x2",
        module: "coin",
        function: "transfer",
        checkpointSeq: BigInt(seq),
        timestamp,
      });
      await storage.write([{
        checkpoint: checkpoint(1),
        events: [],
        balanceChanges: [],
        transactions: [transaction(1)],
        moveCalls: [moveCall(1)],
      }]);
      await storage.write([{
        checkpoint: checkpoint(2),
        events: [],
        balanceChanges: [],
        transactions: [transaction(2)],
        moveCalls: [moveCall(2)],
      }]);
      await storage.shutdown();
    `);

    const result = runBun<{ txCount: number; moveCallCount: number }>(`
      import { Database } from "bun:sqlite";
      const db = new Database("${TEST_DB}", { readonly: true });
      const txCount = (db.query("SELECT COUNT(*) AS c FROM transactions WHERE digest = '0xdup'").get() as { c: number }).c;
      const moveCallCount = (db.query("SELECT COUNT(*) AS c FROM move_calls WHERE tx_digest = '0xdup' AND call_index = 0").get() as { c: number }).c;
      db.close();
      console.log(JSON.stringify({ txCount, moveCallCount }));
    `);

    expect(result.txCount).toBe(1);
    expect(result.moveCallCount).toBe(1);
  });

  test("repairs duplicate balance changes before materializing balances", async () => {
    runBun(`
      import { createSqlStorage } from "./src/pipeline/destinations/sql.ts";
      const storage = createSqlStorage({
        url: "sqlite:${TEST_DB}",
        balances: true,
        deferIndexes: true,
      });
      await storage.initialize();
      const timestamp = new Date("2026-04-01T12:00:00Z");
      const checkpoint = (seq) => ({
        sequenceNumber: BigInt(seq),
        timestamp,
        transactions: [],
        source: "backfill",
      });
      const balanceChange = (seq) => ({
        txDigest: "0xdup",
        checkpointSeq: BigInt(seq),
        address: "0xabc",
        coinType: "0x2::sui::SUI",
        amount: "100",
        timestamp,
      });
      await storage.write([{
        checkpoint: checkpoint(1),
        events: [],
        balanceChanges: [balanceChange(1)],
        transactions: [],
        moveCalls: [],
      }]);
      await storage.write([{
        checkpoint: checkpoint(2),
        events: [],
        balanceChanges: [balanceChange(2)],
        transactions: [],
        moveCalls: [],
      }]);
      await storage.shutdown();
    `);

    const result = runBun<{ ledgerCount: number; balanceRow: { balance: string; last_checkpoint: string } | null }>(`
      import { Database } from "bun:sqlite";
      const db = new Database("${TEST_DB}", { readonly: true });
      const ledgerCount = (db.query("SELECT COUNT(*) AS c FROM balance_changes WHERE tx_digest = '0xdup'").get() as { c: number }).c;
      const balanceRow = db.query("SELECT balance, last_checkpoint FROM balances WHERE address = '0xabc' AND coin_type = '0x2::sui::SUI'").get() as { balance: string; last_checkpoint: string } | null;
      db.close();
      console.log(JSON.stringify({ ledgerCount, balanceRow }));
    `);

    expect(result.ledgerCount).toBe(1);
    expect(result.balanceRow).not.toBeNull();
    expect(result.balanceRow!.balance).toBe("100");
    expect(result.balanceRow!.last_checkpoint).toBe("1");
  });

  test("reuses cached SQLite prepared statements for non-snapshot transaction inserts", async () => {
    const result = runBun<{ queryCalls: number; prepareCalls: number; rowCount: number }>(`
      import { Database } from "bun:sqlite";

      let queryCalls = 0;
      let prepareCalls = 0;

      const originalQuery = Database.prototype.query;
      const originalPrepare = Database.prototype.prepare;

      Database.prototype.query = function (...args) {
        queryCalls++;
        return originalQuery.apply(this, args);
      };

      Database.prototype.prepare = function (...args) {
        prepareCalls++;
        return originalPrepare.apply(this, args);
      };

      const { createSqlStorage } = await import("./src/pipeline/destinations/sql.ts");
      const storage = createSqlStorage({
        url: "sqlite:${TEST_DB}",
        transactions: true,
      });
      await storage.initialize();

      const timestamp = new Date("2026-04-01T12:00:00Z");
      const checkpoint = (seq) => ({
        sequenceNumber: BigInt(seq),
        timestamp,
        transactions: [],
        source: "backfill",
      });
      const transaction = (seq) => ({
        digest: "0xdup",
        sender: "0x1",
        success: true,
        computationCost: "1",
        storageCost: "2",
        storageRebate: "0",
        moveCallCount: 0,
        checkpointSeq: BigInt(seq),
        timestamp,
      });

      await storage.write([{
        checkpoint: checkpoint(1),
        events: [],
        balanceChanges: [],
        transactions: [transaction(1)],
        moveCalls: [],
      }]);
      await storage.write([{
        checkpoint: checkpoint(2),
        events: [],
        balanceChanges: [],
        transactions: [transaction(2)],
        moveCalls: [],
      }]);
      await storage.shutdown();

      const insertQueryCalls = queryCalls;
      const insertPrepareCalls = prepareCalls;

      const db = new Database("${TEST_DB}", { readonly: true });
      const rowCount = (db.query("SELECT COUNT(*) AS c FROM transactions WHERE digest = '0xdup'").get() as { c: number }).c;
      db.close();

      console.log(JSON.stringify({
        queryCalls: insertQueryCalls,
        prepareCalls: insertPrepareCalls,
        rowCount,
      }));
    `);

    expect(result.queryCalls).toBe(1);
    expect(result.prepareCalls).toBe(1);
    expect(result.rowCount).toBe(1);
  });

  test("merges SQLite shards into the final snapshot database", async () => {
    runBun(`
      import { mkdirSync } from "fs";
      import { Database } from "bun:sqlite";
      import { createSqlStorage } from "./src/pipeline/destinations/sql.ts";
      import { createSnapshotShardTables } from "./src/pipeline/destinations/sql-ddl.ts";

      mkdirSync("${TEST_SHARD_DIR}", { recursive: true });

      const storage = createSqlStorage({
        url: "sqlite:${TEST_DB}",
        balances: true,
        transactions: true,
        checkpoints: true,
        deferIndexes: true,
      });
      await storage.initialize();

      const enabled = {
        balances: true,
        transactions: true,
        objectChanges: false,
        dependencies: false,
        inputs: false,
        commands: false,
        systemTransactions: false,
        unchangedConsensusObjects: false,
        events: false,
        checkpoints: true,
      };

      const shard0 = new Database("${TEST_SHARD_DIR}/shard-0.db");
      createSnapshotShardTables(shard0, enabled);
      shard0.query("INSERT INTO checkpoints (sequence_number, epoch, digest, previous_digest, content_digest, sui_timestamp, total_network_transactions, rolling_computation_cost, rolling_storage_cost, rolling_storage_rebate, rolling_non_refundable_storage_fee) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").run(
        "1", "0", "cp-1", null, null, "2026-04-01T12:00:00.000Z", "10", "1", "2", "3", "4",
      );
      shard0.query("INSERT INTO transactions (digest, sender, success, computation_cost, storage_cost, storage_rebate, non_refundable_storage_fee, move_call_count, checkpoint_seq, sui_timestamp, epoch, error_kind, error_description, error_command_index, error_abort_code, error_module, error_function, events_digest, lamport_version, dependency_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").run(
        "tx-1", "0x1", 1, "1", "2", "0", null, 1, "1", "2026-04-01T12:00:00.000Z", "0", null, null, null, null, null, null, null, null, 0,
      );
      shard0.query("INSERT INTO move_calls (tx_digest, call_index, package, module, function, checkpoint_seq, sui_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)").run(
        "tx-1", 0, "0x2", "coin", "split", "1", "2026-04-01T12:00:00.000Z",
      );
      shard0.query("INSERT INTO balance_changes (tx_digest, checkpoint_seq, address, coin_type, amount, sui_timestamp) VALUES (?, ?, ?, ?, ?, ?)").run(
        "tx-1", "1", "0xaaa", "0x2::sui::SUI", "10", "2026-04-01T12:00:00.000Z",
      );
      shard0.close();

      const shard1 = new Database("${TEST_SHARD_DIR}/shard-1.db");
      createSnapshotShardTables(shard1, enabled);
      shard1.query("INSERT INTO checkpoints (sequence_number, epoch, digest, previous_digest, content_digest, sui_timestamp, total_network_transactions, rolling_computation_cost, rolling_storage_cost, rolling_storage_rebate, rolling_non_refundable_storage_fee) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").run(
        "2", "0", "cp-2", "cp-1", null, "2026-04-01T12:01:00.000Z", "20", "5", "6", "7", "8",
      );
      shard1.query("INSERT INTO transactions (digest, sender, success, computation_cost, storage_cost, storage_rebate, non_refundable_storage_fee, move_call_count, checkpoint_seq, sui_timestamp, epoch, error_kind, error_description, error_command_index, error_abort_code, error_module, error_function, events_digest, lamport_version, dependency_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").run(
        "tx-2", "0x2", 1, "3", "4", "0", null, 0, "2", "2026-04-01T12:01:00.000Z", "0", null, null, null, null, null, null, null, null, 0,
      );
      shard1.query("INSERT INTO balance_changes (tx_digest, checkpoint_seq, address, coin_type, amount, sui_timestamp) VALUES (?, ?, ?, ?, ?, ?)").run(
        "tx-2", "2", "0xaaa", "0x2::sui::SUI", "15", "2026-04-01T12:01:00.000Z",
      );
      shard1.close();

      const marker = {
        checkpoint: {
          sequenceNumber: 2n,
          timestamp: new Date("1970-01-01T00:00:00.000Z"),
          transactions: [],
          source: "backfill",
          epoch: 0n,
          digest: "",
          previousDigest: null,
          contentDigest: null,
          totalNetworkTransactions: 0n,
          epochRollingGasCostSummary: {
            computationCost: "0",
            storageCost: "0",
            storageRebate: "0",
            nonRefundableStorageFee: "0",
          },
        },
        events: [],
        balanceChanges: [],
        transactions: [],
        moveCalls: [],
        objectChanges: [],
        dependencies: [],
        inputs: [],
        commands: [],
        systemTransactions: [],
        unchangedConsensusObjects: [],
        _shardPaths: ["${TEST_SHARD_DIR}/shard-0.db", "${TEST_SHARD_DIR}/shard-1.db"],
        _shardSessionDir: "${TEST_SHARD_DIR}",
      };

      await storage.write([marker]);
      await storage.shutdown();
    `);

    const result = runBun<{
      txCount: number;
      moveCallCount: number;
      checkpointCount: number;
      balance: string | null;
      shardDirExists: boolean;
    }>(`
      import { existsSync } from "fs";
      import { Database } from "bun:sqlite";
      const db = new Database("${TEST_DB}", { readonly: true });
      const txCount = (db.query("SELECT COUNT(*) AS c FROM transactions").get() as { c: number }).c;
      const moveCallCount = (db.query("SELECT COUNT(*) AS c FROM move_calls").get() as { c: number }).c;
      const checkpointCount = (db.query("SELECT COUNT(*) AS c FROM checkpoints").get() as { c: number }).c;
      const balance = (db.query("SELECT balance FROM balances WHERE address = '0xaaa' AND coin_type = '0x2::sui::SUI'").get() as { balance: string } | null)?.balance ?? null;
      db.close();
      console.log(JSON.stringify({
        txCount,
        moveCallCount,
        checkpointCount,
        balance,
        shardDirExists: existsSync("${TEST_SHARD_DIR}"),
      }));
    `);

    expect(result.txCount).toBe(2);
    expect(result.moveCallCount).toBe(1);
    expect(result.checkpointCount).toBe(2);
    expect(result.balance).toBe("25");
    expect(result.shardDirExists).toBe(false);
  });
});
