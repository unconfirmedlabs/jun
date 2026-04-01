import { test, expect, describe } from "bun:test";
import pino from "pino";
import { applyReload, type HotReloadContext } from "./hot-reload.ts";
import { createProcessor, type EventProcessor, type EventHandler } from "./processor.ts";
import type { StorageBackend } from "./output/storage.ts";

const testLog = pino({ level: "silent" });

function createMockContext(events: Record<string, EventHandler>): HotReloadContext {
  let processor = createProcessor(events);
  const ddlsRun: string[] = [];

  return {
    events: { ...events },
    getProcessor: () => processor,
    setProcessor: (p) => { processor = p; },
    output: {
      name: "mock",
      async migrate() {},
      async write() {},
      async writeHandler() {},
      async shutdown() {},
    } as StorageBackend,
    handlerTables: Object.fromEntries(
      Object.entries(events).map(([name, h]) => [
        name,
        { tableName: name.toLowerCase(), fields: h.fields },
      ]),
    ),
    sql: {
      unsafe(query: string) {
        ddlsRun.push(query);
        return Promise.resolve([]);
      },
      _ddls: ddlsRun,
    },
    log: testLog,
  };
}

describe("hot-reload", () => {
  test("adding a new handler creates table and updates processor", async () => {
    const ctx = createMockContext({
      EventA: { type: "0x1::m::EventA", fields: { value: "u64" } },
    });

    const result = await applyReload(ctx, {
      EventA: { type: "0x1::m::EventA", fields: { value: "u64" } },
      EventB: { type: "0x2::m::EventB", fields: { name: "string" } },
    });

    expect(result.added).toEqual(["EventB"]);
    expect(result.unchanged).toEqual(["EventA"]);
    expect(result.removed).toEqual([]);
    expect(result.altered).toEqual([]);

    // Table should have been created
    expect((ctx.sql as any)._ddls.some((d: string) => d.includes("CREATE TABLE"))).toBe(true);

    // Handler tables should include the new handler
    expect(ctx.handlerTables.EventB).toBeDefined();
    expect(ctx.handlerTables.EventB.fields.name).toBe("string");

    // Events should be updated
    expect(ctx.events.EventB).toBeDefined();
  });

  test("removing a handler removes from processor but preserves table", async () => {
    const ctx = createMockContext({
      EventA: { type: "0x1::m::EventA", fields: { value: "u64" } },
      EventB: { type: "0x2::m::EventB", fields: { name: "string" } },
    });

    const result = await applyReload(ctx, {
      EventA: { type: "0x1::m::EventA", fields: { value: "u64" } },
    });

    expect(result.removed).toEqual(["EventB"]);
    expect(result.unchanged).toEqual(["EventA"]);

    // Handler table removed from mapping
    expect(ctx.handlerTables.EventB).toBeUndefined();

    // Events should no longer include EventB
    expect(ctx.events.EventB).toBeUndefined();
  });

  test("altering a handler adds new columns", async () => {
    const ctx = createMockContext({
      EventA: { type: "0x1::m::EventA", fields: { value: "u64" } },
    });

    const result = await applyReload(ctx, {
      EventA: { type: "0x1::m::EventA", fields: { value: "u64", new_field: "address" } },
    });

    expect(result.altered).toEqual(["EventA"]);

    // ALTER TABLE should have been called
    expect((ctx.sql as any)._ddls.some((d: string) => d.includes("ALTER TABLE"))).toBe(true);
    expect((ctx.sql as any)._ddls.some((d: string) => d.includes("new_field"))).toBe(true);

    // Handler tables should reflect new fields
    expect(ctx.handlerTables.EventA.fields.new_field).toBe("address");
  });

  test("no changes returns all unchanged", async () => {
    const events = {
      EventA: { type: "0x1::m::EventA", fields: { value: "u64" } },
    };
    const ctx = createMockContext(events);

    const result = await applyReload(ctx, events);

    expect(result.added).toEqual([]);
    expect(result.removed).toEqual([]);
    expect(result.altered).toEqual([]);
    expect(result.unchanged).toEqual(["EventA"]);

    // No DDL should have been run
    expect((ctx.sql as any)._ddls.length).toBe(0);
  });

  test("changing event type counts as altered", async () => {
    const ctx = createMockContext({
      EventA: { type: "0x1::m::EventA", fields: { value: "u64" } },
    });

    const result = await applyReload(ctx, {
      EventA: { type: "0x1::m::EventA_v2", fields: { value: "u64" } },
    });

    expect(result.altered).toEqual(["EventA"]);
  });
});
