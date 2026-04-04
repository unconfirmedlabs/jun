import { afterEach, describe, expect, test } from "bun:test";
import { unlinkSync } from "fs";
const TEST_DB = "/tmp/jun-pipeline-sql-test.db";

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
});
