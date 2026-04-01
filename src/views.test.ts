import { test, expect, describe } from "bun:test";
import pino from "pino";
import { parseDuration, createViewManager } from "./views.ts";

const testLog = pino({ level: "silent" });

// ---------------------------------------------------------------------------
// parseDuration
// ---------------------------------------------------------------------------

describe("parseDuration", () => {
  test("parses seconds", () => {
    expect(parseDuration("30s")).toBe(30000);
    expect(parseDuration("1s")).toBe(1000);
    expect(parseDuration("120s")).toBe(120000);
  });

  test("parses minutes", () => {
    expect(parseDuration("5m")).toBe(300000);
    expect(parseDuration("1m")).toBe(60000);
  });

  test("parses hours", () => {
    expect(parseDuration("1h")).toBe(3600000);
    expect(parseDuration("24h")).toBe(86400000);
  });

  test("throws on invalid format", () => {
    expect(() => parseDuration("abc")).toThrow("Invalid duration");
    expect(() => parseDuration("30")).toThrow("Invalid duration");
    expect(() => parseDuration("30d")).toThrow("Invalid duration");
    expect(() => parseDuration("")).toThrow("Invalid duration");
    expect(() => parseDuration("5ms")).toThrow("Invalid duration");
  });
});

// ---------------------------------------------------------------------------
// createViewManager
// ---------------------------------------------------------------------------

describe("createViewManager", () => {
  function createMockSql() {
    const queries: string[] = [];
    return {
      queries,
      unsafe(query: string) {
        queries.push(query.trim());
        return Promise.resolve([]);
      },
    };
  }

  test("creates materialized views on construction", async () => {
    const sql = createMockSql();
    await createViewManager(sql, {
      daily_counts: {
        sql: "SELECT date_trunc('day', ts) AS day, count(*) FROM events GROUP BY 1",
        refresh: "60s",
      },
      top_senders: {
        sql: "SELECT sender, count(*) FROM events GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
        refresh: "5m",
      },
    }, testLog);

    expect(sql.queries.length).toBe(2);
    expect(sql.queries[0]).toContain("CREATE MATERIALIZED VIEW IF NOT EXISTS daily_counts");
    expect(sql.queries[0]).toContain("date_trunc");
    expect(sql.queries[1]).toContain("CREATE MATERIALIZED VIEW IF NOT EXISTS top_senders");
  });

  test("start() triggers initial refresh for each view", async () => {
    const sql = createMockSql();
    const manager = await createViewManager(sql, {
      view_a: { sql: "SELECT 1", refresh: "60s" },
      view_b: { sql: "SELECT 2", refresh: "5m" },
    }, testLog);

    // Clear the CREATE queries
    sql.queries.length = 0;

    manager.start();

    // Give the initial refresh a tick
    await new Promise((r) => setTimeout(r, 20));

    // Should have refreshed both views
    const refreshQueries = sql.queries.filter((q) => q.includes("REFRESH"));
    expect(refreshQueries.length).toBe(2);
    expect(refreshQueries[0]).toContain("REFRESH MATERIALIZED VIEW view_a");
    expect(refreshQueries[1]).toContain("REFRESH MATERIALIZED VIEW view_b");

    manager.stop();
  });

  test("stop() clears all timers", async () => {
    const sql = createMockSql();
    const manager = await createViewManager(sql, {
      view_a: { sql: "SELECT 1", refresh: "1s" },
    }, testLog);

    manager.start();
    await new Promise((r) => setTimeout(r, 20));

    // Clear queries and stop
    sql.queries.length = 0;
    manager.stop();

    // Wait longer than the refresh interval
    await new Promise((r) => setTimeout(r, 1200));

    // No refresh should have happened after stop
    const refreshQueries = sql.queries.filter((q) => q.includes("REFRESH"));
    expect(refreshQueries.length).toBe(0);
  });

  test("periodic refresh fires on interval", async () => {
    const sql = createMockSql();
    const manager = await createViewManager(sql, {
      fast_view: { sql: "SELECT 1", refresh: "1s" },
    }, testLog);

    sql.queries.length = 0;
    manager.start();

    // Wait for initial + one interval refresh
    await new Promise((r) => setTimeout(r, 1200));

    const refreshQueries = sql.queries.filter((q) => q.includes("REFRESH"));
    expect(refreshQueries.length).toBeGreaterThanOrEqual(2); // initial + at least 1 interval

    manager.stop();
  });

  test("refresh error does not crash the manager", async () => {
    let callCount = 0;
    const sql = {
      unsafe(query: string) {
        callCount++;
        if (query.includes("REFRESH")) {
          return Promise.reject(new Error("connection lost"));
        }
        return Promise.resolve([]);
      },
    };

    const manager = await createViewManager(sql, {
      bad_view: { sql: "SELECT 1", refresh: "1s" },
    }, testLog);

    // Should not throw
    manager.start();
    await new Promise((r) => setTimeout(r, 50));

    // Manager should still be running (timer not cleared by error)
    manager.stop();
  });

  test("CREATE failure throws (prevents startup)", async () => {
    const sql = {
      unsafe() {
        return Promise.reject(new Error("permission denied"));
      },
    };

    await expect(
      createViewManager(sql, {
        bad_view: { sql: "SELECT 1", refresh: "1s" },
      }, testLog),
    ).rejects.toThrow("permission denied");
  });

  test("empty views object creates no views", async () => {
    const sql = createMockSql();
    const manager = await createViewManager(sql, {}, testLog);

    expect(sql.queries.length).toBe(0);

    manager.start();
    manager.stop();
  });
});
