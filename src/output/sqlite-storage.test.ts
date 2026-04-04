import { test, expect, describe, afterEach } from "bun:test";
import { createSqliteStorageBackend } from "./sqlite-storage.ts";
import { Database } from "bun:sqlite";
import { unlinkSync } from "fs";
import type { DecodedEvent } from "../pipeline/types.ts";

const TEST_DB = "/tmp/jun-sqlite-storage-test.db";

function makeEvent(handler: string, seq: bigint, eventSeq = 0): DecodedEvent {
  return {
    handlerName: handler,
    checkpointSeq: seq,
    txDigest: `tx_${seq}_${eventSeq}`,
    eventSeq,
    sender: "0xsender",
    timestamp: new Date("2026-04-01T12:00:00Z"),
    data: { amount: "42", recipient: "0xrecipient" },
  };
}

describe("SqliteStorageBackend", () => {
  afterEach(() => {
    try { unlinkSync(TEST_DB); } catch {}
    try { unlinkSync(TEST_DB + "-wal"); } catch {}
    try { unlinkSync(TEST_DB + "-shm"); } catch {}
  });

  test("creates tables on migrate", async () => {
    const backend = createSqliteStorageBackend(TEST_DB, {
      TestEvent: { tableName: "test_event", fields: { amount: "u64", recipient: "address" } },
    });
    await backend.migrate();

    const db = new Database(TEST_DB);
    const tables = db.query("SELECT name FROM sqlite_master WHERE type='table'").all() as { name: string }[];
    expect(tables.map((t) => t.name)).toContain("test_event");
    db.close();

    await backend.shutdown();
  });

  test("writes and reads back events", async () => {
    const backend = createSqliteStorageBackend(TEST_DB, {
      TestEvent: { tableName: "test_event", fields: { amount: "u64", recipient: "address" } },
    });
    await backend.migrate();

    await backend.writeHandler("TestEvent", [
      makeEvent("TestEvent", 1n),
      makeEvent("TestEvent", 2n),
    ]);

    const db = new Database(TEST_DB);
    const rows = db.query("SELECT * FROM test_event").all() as any[];
    expect(rows.length).toBe(2);
    expect(rows[0].tx_digest).toBe("tx_1_0");
    expect(rows[0].amount).toBe("42");
    expect(rows[0].recipient).toBe("0xrecipient");
    db.close();

    await backend.shutdown();
  });

  test("idempotent on replay (ON CONFLICT DO NOTHING)", async () => {
    const backend = createSqliteStorageBackend(TEST_DB, {
      TestEvent: { tableName: "test_event", fields: { amount: "u64", recipient: "address" } },
    });
    await backend.migrate();

    const event = makeEvent("TestEvent", 1n);
    await backend.writeHandler("TestEvent", [event]);
    await backend.writeHandler("TestEvent", [event]); // duplicate

    const db = new Database(TEST_DB);
    const rows = db.query("SELECT count(*) as c FROM test_event").get() as any;
    expect(rows.c).toBe(1);
    db.close();

    await backend.shutdown();
  });

  test("multiple handlers create multiple tables", async () => {
    const backend = createSqliteStorageBackend(TEST_DB, {
      EventA: { tableName: "event_a", fields: { value: "u64" } },
      EventB: { tableName: "event_b", fields: { name: "string" } },
    });
    await backend.migrate();

    await backend.write([
      { ...makeEvent("EventA", 1n), data: { value: "100" } },
      { ...makeEvent("EventB", 2n), data: { name: "test" } },
    ]);

    const db = new Database(TEST_DB);
    const countA = (db.query("SELECT count(*) as c FROM event_a").get() as any).c;
    const countB = (db.query("SELECT count(*) as c FROM event_b").get() as any).c;
    expect(countA).toBe(1);
    expect(countB).toBe(1);
    db.close();

    await backend.shutdown();
  });

  test("unknown handler is silently ignored", async () => {
    const backend = createSqliteStorageBackend(TEST_DB, {
      TestEvent: { tableName: "test_event", fields: { amount: "u64" } },
    });
    await backend.migrate();

    // Should not throw
    await backend.writeHandler("UnknownHandler", [makeEvent("UnknownHandler", 1n)]);

    await backend.shutdown();
  });
});
