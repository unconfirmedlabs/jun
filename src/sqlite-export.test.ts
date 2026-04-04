import { afterEach, describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { unlinkSync } from "fs";
import { createSqlStorage } from "./pipeline/destinations/sql.ts";
import { exportSplitSqliteDatasets } from "./sqlite-export.ts";

const SOURCE_DB = "/tmp/jun-sqlite-export-source.db";
const SPLIT_DBS = [
  "/tmp/jun-sqlite-export-source.transactions.db",
  "/tmp/jun-sqlite-export-source.balance_changes.db",
  "/tmp/jun-sqlite-export-source.balances.db",
  "/tmp/jun-sqlite-export-source.events.db",
];

function cleanupDb(path: string): void {
  try { unlinkSync(path); } catch {}
  try { unlinkSync(path + "-wal"); } catch {}
  try { unlinkSync(path + "-shm"); } catch {}
}

describe("exportSplitSqliteDatasets", () => {
  afterEach(() => {
    cleanupDb(SOURCE_DB);
    for (const path of SPLIT_DBS) cleanupDb(path);
  });

  test("exports one database per dataset from a finished SQLite snapshot", async () => {
    const storage = createSqlStorage({
      url: `sqlite:${SOURCE_DB}`,
      handlers: {
        swaps: {
          tableName: "swaps",
          fields: { pool: "string", amount: "u64" },
        },
        fills: {
          tableName: "fills",
          fields: { market: "string" },
        },
      },
      balances: true,
      transactions: true,
    });

    await storage.initialize();

    const timestamp = new Date("2026-04-04T10:00:00Z");
    await storage.write([{
      checkpoint: {
        sequenceNumber: 1n,
        timestamp,
        transactions: [],
        source: "backfill",
      },
      events: [
        {
          handlerName: "swaps",
          checkpointSeq: 1n,
          txDigest: "0xtx1",
          eventSeq: 0,
          sender: "0x1",
          timestamp,
          data: { pool: "pool-1", amount: "42" },
        },
        {
          handlerName: "fills",
          checkpointSeq: 1n,
          txDigest: "0xtx1",
          eventSeq: 1,
          sender: "0x1",
          timestamp,
          data: { market: "sui-usdc" },
        },
      ],
      balanceChanges: [
        {
          txDigest: "0xtx1",
          checkpointSeq: 1n,
          address: "0xabc",
          coinType: "0x2::sui::SUI",
          amount: "42",
          timestamp,
        },
      ],
      transactions: [
        {
          digest: "0xtx1",
          sender: "0x1",
          success: true,
          computationCost: "10",
          storageCost: "20",
          storageRebate: "5",
          moveCallCount: 1,
          checkpointSeq: 1n,
          timestamp,
        },
      ],
      moveCalls: [
        {
          txDigest: "0xtx1",
          callIndex: 0,
          package: "0x2",
          module: "coin",
          function: "transfer",
          checkpointSeq: 1n,
          timestamp,
        },
      ],
    }]);
    await storage.shutdown();

    const exports = exportSplitSqliteDatasets(SOURCE_DB);
    expect(exports.map(item => item.dataset)).toEqual([
      "transactions",
      "balance_changes",
      "balances",
      "events",
    ]);

    const txDb = new Database("/tmp/jun-sqlite-export-source.transactions.db", { readonly: true });
    expect(
      (txDb.query("SELECT COUNT(*) AS c FROM transactions").get() as { c: number }).c
    ).toBe(1);
    expect(
      (txDb.query("SELECT COUNT(*) AS c FROM move_calls").get() as { c: number }).c
    ).toBe(1);
    expect(
      (txDb.query("SELECT name FROM sqlite_master WHERE type='table' AND name='balance_changes'").get() as { name: string } | null)
    ).toBeNull();
    txDb.close();

    const balanceChangesDb = new Database("/tmp/jun-sqlite-export-source.balance_changes.db", { readonly: true });
    expect(
      (balanceChangesDb.query("SELECT COUNT(*) AS c FROM balance_changes").get() as { c: number }).c
    ).toBe(1);
    balanceChangesDb.close();

    const balancesDb = new Database("/tmp/jun-sqlite-export-source.balances.db", { readonly: true });
    expect(
      (balancesDb.query("SELECT balance AS b FROM balances WHERE address='0xabc' AND coin_type='0x2::sui::SUI'").get() as { b: string }).b
    ).toBe("42");
    balancesDb.close();

    const eventsDb = new Database("/tmp/jun-sqlite-export-source.events.db", { readonly: true });
    expect(
      (eventsDb.query("SELECT COUNT(*) AS c FROM swaps").get() as { c: number }).c
    ).toBe(1);
    expect(
      (eventsDb.query("SELECT COUNT(*) AS c FROM fills").get() as { c: number }).c
    ).toBe(1);
    expect(
      (eventsDb.query("SELECT name FROM sqlite_master WHERE type='table' AND name='transactions'").get() as { name: string } | null)
    ).toBeNull();
    eventsDb.close();
  });
});
