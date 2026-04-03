import { test, expect, describe, beforeEach, afterEach } from "bun:test";
import { parseIndexerConfig, mergeRunOptions } from "./indexer-config.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const minimalConfig = `
network: testnet
grpcUrl: fullnode.testnet.sui.io:443
database: postgres://localhost/jun
events:
  MyEvent:
    type: "0x1::module::MyEvent"
    fields:
      amount: u64
      recipient: address
`;

const fullConfig = `
network: mainnet
grpcUrl: fullnode.mainnet.sui.io:443
database: postgres://user:pass@host/db
startCheckpoint: 316756645
backfillConcurrency: 20
archiveUrl: https://checkpoints.mainnet.sui.io
mode: backfill-only
repairGaps: true
serve:
  port: 9090
  hostname: 0.0.0.0
events:
  EventA:
    type: "0x1::mod::EventA"
    fields:
      id: address
      value: u64
  EventB:
    type: "0x2::mod::EventB"
    fields:
      name: string
      active: bool
      tags: "vector<string>"
`;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("parseIndexerConfig", () => {
  test("parses minimal config with required fields only", () => {
    const { indexer, run } = parseIndexerConfig(minimalConfig);

    expect(indexer.network).toBe("testnet");
    expect(indexer.grpcUrl).toBe("fullnode.testnet.sui.io:443");
    expect(indexer.database).toBe("postgres://localhost/jun");
    expect(indexer.events.MyEvent).toBeDefined();
    expect(indexer.events.MyEvent.type).toContain("::module::MyEvent");
    expect(indexer.events.MyEvent.fields.amount).toBe("u64");
    expect(indexer.events.MyEvent.fields.recipient).toBe("address");

    // No runtime options
    expect(run.mode).toBeUndefined();
    expect(run.repairGaps).toBeUndefined();
    expect(run.serve).toBeUndefined();
  });

  test("parses full config with all optional fields", () => {
    const { indexer, run } = parseIndexerConfig(fullConfig);

    expect(indexer.network).toBe("mainnet");
    expect(indexer.backfillConcurrency).toBe(20);
    expect(indexer.archiveUrl).toBe("https://checkpoints.mainnet.sui.io");
    expect(indexer.startCheckpoint).toBe(316756645n);

    expect(run.mode).toBe("backfill-only");
    expect(run.repairGaps).toBe(true);
    expect(run.serve).toEqual({ port: 9090, hostname: "0.0.0.0" });
  });

  test("multiple event handlers", () => {
    const { indexer } = parseIndexerConfig(fullConfig);

    expect(Object.keys(indexer.events)).toEqual(["EventA", "EventB"]);
    expect(indexer.events.EventA.fields.id).toBe("address");
    expect(indexer.events.EventB.fields.name).toBe("string");
    expect(indexer.events.EventB.fields.tags).toBe("vector<string>");
  });

  test("env var substitution with $VAR syntax", () => {
    const prev = process.env.TEST_DB_URL;
    process.env.TEST_DB_URL = "postgres://envtest/db";

    try {
      const { indexer } = parseIndexerConfig(`
network: testnet
grpcUrl: fullnode.testnet.sui.io:443
database: $TEST_DB_URL
events:
  E:
    type: "0x1::m::E"
    fields:
      f: u64
`);
      expect(indexer.database).toBe("postgres://envtest/db");
    } finally {
      if (prev === undefined) delete process.env.TEST_DB_URL;
      else process.env.TEST_DB_URL = prev;
    }
  });

  test("env var substitution with ${VAR} syntax", () => {
    const prev = process.env.TEST_GRPC;
    process.env.TEST_GRPC = "my-node.example.com:443";

    try {
      const { indexer } = parseIndexerConfig(`
network: testnet
grpcUrl: "\${TEST_GRPC}"
database: postgres://localhost/db
events:
  E:
    type: "0x1::m::E"
    fields:
      f: u64
`);
      expect(indexer.grpcUrl).toBe("my-node.example.com:443");
    } finally {
      if (prev === undefined) delete process.env.TEST_GRPC;
      else process.env.TEST_GRPC = prev;
    }
  });

  test("missing env var throws", () => {
    delete process.env.DEFINITELY_NOT_SET_12345;

    expect(() => parseIndexerConfig(`
network: testnet
grpcUrl: fullnode.testnet.sui.io:443
database: $DEFINITELY_NOT_SET_12345
events:
  E:
    type: "0x1::m::E"
    fields:
      f: u64
`)).toThrow("DEFINITELY_NOT_SET_12345 is not set");
  });

  test("startCheckpoint as number becomes bigint", () => {
    const { indexer } = parseIndexerConfig(`
network: testnet
grpcUrl: fullnode.testnet.sui.io:443
database: postgres://localhost/db
startCheckpoint: 316756645
events:
  E:
    type: "0x1::m::E"
    fields:
      f: u64
`);
    expect(indexer.startCheckpoint).toBe(316756645n);
  });

  test("startCheckpoint as string stays string", () => {
    const { indexer } = parseIndexerConfig(`
network: testnet
grpcUrl: fullnode.testnet.sui.io:443
database: postgres://localhost/db
startCheckpoint: "epoch:42"
events:
  E:
    type: "0x1::m::E"
    fields:
      f: u64
`);
    expect(indexer.startCheckpoint).toBe("epoch:42");
  });

  test("startCheckpoint as package reference", () => {
    const { indexer } = parseIndexerConfig(`
network: testnet
grpcUrl: fullnode.testnet.sui.io:443
database: postgres://localhost/db
startCheckpoint: "package:0xabcd1234"
events:
  E:
    type: "0x1::m::E"
    fields:
      f: u64
`);
    expect(indexer.startCheckpoint).toBe("package:0xabcd1234");
  });

  test("serve as port number shorthand", () => {
    const { run } = parseIndexerConfig(`
network: testnet
grpcUrl: fullnode.testnet.sui.io:443
database: postgres://localhost/db
serve: 8080
events:
  E:
    type: "0x1::m::E"
    fields:
      f: u64
`);
    expect(run.serve).toEqual({ port: 8080 });
  });

  test("missing required field throws", () => {
    expect(() => parseIndexerConfig(`
grpcUrl: fullnode.testnet.sui.io:443
database: postgres://localhost/db
events:
  E:
    type: "0x1::m::E"
    fields:
      f: u64
`)).toThrow('missing required field "network"');
  });

  test("missing both events and balances throws", () => {
    expect(() => parseIndexerConfig(`
network: testnet
grpcUrl: fullnode.testnet.sui.io:443
database: postgres://localhost/db
`)).toThrow("must configure at least one of");
  });

  test("balances-only config is valid (no events required)", () => {
    const { indexer } = parseIndexerConfig(`
network: testnet
grpcUrl: fullnode.testnet.sui.io:443
database: postgres://localhost/db
balances:
  coinTypes: "*"
`);
    expect(Object.keys(indexer.events)).toEqual([]);
    expect(indexer.balances?.coinTypes).toBe("*");
  });

  test("event missing type throws", () => {
    expect(() => parseIndexerConfig(`
network: testnet
grpcUrl: fullnode.testnet.sui.io:443
database: postgres://localhost/db
events:
  BadEvent:
    fields:
      f: u64
`)).toThrow('event "BadEvent" is missing "type"');
  });

  test("event without fields is valid (auto-resolved at startup)", () => {
    const { indexer } = parseIndexerConfig(`
network: testnet
grpcUrl: fullnode.testnet.sui.io:443
database: postgres://localhost/db
events:
  MyEvent:
    type: "0x1::m::E"
`);
    expect(indexer.events.MyEvent).toBeDefined();
    expect(indexer.events.MyEvent.type).toContain("::m::E");
    expect(indexer.events.MyEvent.fields).toBeUndefined();
  });

  test("parses views from config", () => {
    const { views } = parseIndexerConfig(`
network: testnet
grpcUrl: fullnode.testnet.sui.io:443
database: postgres://localhost/db
events:
  E:
    type: "0x1::m::E"
    fields:
      f: u64
views:
  daily_counts:
    sql: "SELECT date_trunc('day', sui_timestamp) AS day, count(*) FROM e GROUP BY 1"
    refresh: 60s
  top_senders:
    sql: "SELECT sender, count(*) FROM e GROUP BY 1 ORDER BY 2 DESC"
    refresh: 5m
`);
    expect(views).toBeDefined();
    expect(Object.keys(views!)).toEqual(["daily_counts", "top_senders"]);
    expect(views!.daily_counts.refresh).toBe("60s");
    expect(views!.top_senders.sql).toContain("sender");
  });

  test("config without views has undefined views", () => {
    const { views } = parseIndexerConfig(minimalConfig);
    expect(views).toBeUndefined();
  });

  test("view missing sql throws", () => {
    expect(() => parseIndexerConfig(`
network: testnet
grpcUrl: fullnode.testnet.sui.io:443
database: postgres://localhost/db
events:
  E:
    type: "0x1::m::E"
    fields:
      f: u64
views:
  bad_view:
    refresh: 60s
`)).toThrow('view "bad_view" is missing "sql"');
  });

  test("view missing refresh throws", () => {
    expect(() => parseIndexerConfig(`
network: testnet
grpcUrl: fullnode.testnet.sui.io:443
database: postgres://localhost/db
events:
  E:
    type: "0x1::m::E"
    fields:
      f: u64
views:
  bad_view:
    sql: "SELECT 1"
`)).toThrow('view "bad_view" is missing "refresh"');
  });

  test("invalid mode throws", () => {
    expect(() => parseIndexerConfig(`
network: testnet
grpcUrl: fullnode.testnet.sui.io:443
database: postgres://localhost/db
mode: invalid-mode
events:
  E:
    type: "0x1::m::E"
    fields:
      f: u64
`)).toThrow("mode must be one of");
  });
});

describe("mergeRunOptions", () => {
  test("CLI mode overrides YAML mode", () => {
    const merged = mergeRunOptions(
      { mode: "all", repairGaps: true },
      { mode: "backfill-only" },
    );
    expect(merged.mode).toBe("backfill-only");
    expect(merged.repairGaps).toBe(true);
  });

  test("--no-serve disables YAML serve", () => {
    const merged = mergeRunOptions(
      { serve: { port: 8080 } },
      { noServe: true },
    );
    expect(merged.serve).toBeUndefined();
  });

  test("CLI --serve overrides YAML port", () => {
    const merged = mergeRunOptions(
      { serve: { port: 8080 } },
      { serve: "9090" },
    );
    expect(merged.serve).toEqual({ port: 9090 });
  });

  test("CLI repairGaps overrides YAML", () => {
    const merged = mergeRunOptions(
      { repairGaps: true },
      { repairGaps: false },
    );
    expect(merged.repairGaps).toBe(false);
  });

  test("no CLI overrides preserves YAML defaults", () => {
    const merged = mergeRunOptions(
      { mode: "all", repairGaps: true, serve: { port: 8080 } },
      {},
    );
    expect(merged.mode).toBe("all");
    expect(merged.repairGaps).toBe(true);
    expect(merged.serve).toEqual({ port: 8080 });
  });
});
