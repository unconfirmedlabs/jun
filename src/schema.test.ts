import { test, expect, describe } from "bun:test";
import { buildBcsSchema, generateDDL, formatValue, formatRow, type FieldDefs } from "./schema.ts";

describe("buildBcsSchema", () => {
  test("creates schema for primitive fields", () => {
    const fields: FieldDefs = {
      item_id: "address",
      edition: "u16",
      paid_value: "u64",
      name: "string",
      active: "bool",
    };
    const schema = buildBcsSchema(fields);
    expect(schema).toBeDefined();
  });

  test("creates schema with option and vector types", () => {
    const fields: FieldDefs = {
      owner: "address",
      label: "option<string>",
      tags: "vector<u8>",
    };
    const schema = buildBcsSchema(fields);
    expect(schema).toBeDefined();
  });

  test("BCS round-trip: serialize then parse", () => {
    const fields: FieldDefs = {
      edition: "u16",
      quantity: "u64",
      active: "bool",
      name: "string",
    };
    const schema = buildBcsSchema(fields);

    const original = {
      edition: 42,
      quantity: 1000n,
      active: true,
      name: "test-record",
    };

    const bytes = schema.serialize(original).toBytes();
    const decoded = schema.parse(bytes) as Record<string, unknown>;

    expect(decoded.edition).toBe(42);
    // @mysten/bcs returns u64 as string
    expect(decoded.quantity).toBe("1000");
    expect(decoded.active).toBe(true);
    expect(decoded.name).toBe("test-record");
  });

  test("BCS round-trip with address fields", () => {
    const fields: FieldDefs = {
      item_id: "address",
      edition: "u16",
    };
    const schema = buildBcsSchema(fields);

    const addr = "0x" + "ab".repeat(32);
    const original = {
      item_id: addr,
      edition: 1,
    };

    const bytes = schema.serialize(original).toBytes();
    const decoded = schema.parse(bytes) as Record<string, unknown>;

    expect(decoded.item_id).toBe(addr);
    expect(decoded.edition).toBe(1);
  });

  test("BCS round-trip with option field", () => {
    const fields: FieldDefs = {
      label: "option<string>",
      count: "u32",
    };
    const schema = buildBcsSchema(fields);

    // With value
    const bytes1 = schema.serialize({ label: "hello", count: 5 }).toBytes();
    const decoded1 = schema.parse(bytes1) as Record<string, unknown>;
    expect(decoded1.label).toBe("hello");
    expect(decoded1.count).toBe(5);

    // With null
    const bytes2 = schema.serialize({ label: null, count: 10 }).toBytes();
    const decoded2 = schema.parse(bytes2) as Record<string, unknown>;
    expect(decoded2.label).toBeNull();
    expect(decoded2.count).toBe(10);
  });

  test("BCS round-trip with vector field", () => {
    const fields: FieldDefs = {
      scores: "vector<u64>",
    };
    const schema = buildBcsSchema(fields);

    const original = { scores: ["100", "200", "300"] };
    const bytes = schema.serialize(original).toBytes();
    const decoded = schema.parse(bytes) as Record<string, unknown>;

    // @mysten/bcs returns u64 as strings
    expect(decoded.scores).toEqual(["100", "200", "300"]);
  });

  test("throws on unknown field type", () => {
    expect(() => {
      buildBcsSchema({ bad: "float32" as any });
    }).toThrow("Unknown field type: float32");
  });
});

describe("generateDDL", () => {
  test("generates CREATE TABLE with metadata columns", () => {
    const fields: FieldDefs = {
      item_id: "address",
      edition: "u16",
      paid_value: "u64",
    };
    const ddl = generateDDL("record_pressed", fields);

    expect(ddl).toContain("CREATE TABLE IF NOT EXISTS record_pressed");
    expect(ddl).toContain("id SERIAL PRIMARY KEY");
    expect(ddl).toContain("tx_digest TEXT NOT NULL");
    expect(ddl).toContain("event_seq INTEGER NOT NULL");
    expect(ddl).toContain("sender TEXT NOT NULL");
    expect(ddl).toContain("sui_timestamp TIMESTAMPTZ NOT NULL");
    expect(ddl).toContain("indexed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()");
    expect(ddl).toContain("item_id TEXT NOT NULL");
    expect(ddl).toContain("edition INTEGER NOT NULL");
    expect(ddl).toContain("paid_value NUMERIC NOT NULL");
    expect(ddl).toContain("UNIQUE (tx_digest, event_seq)");
  });

  test("option fields are nullable", () => {
    const ddl = generateDDL("test_table", { label: "option<string>" });
    // Should NOT have "NOT NULL" for option fields
    expect(ddl).toContain("label TEXT,");
  });

  test("vector fields use JSONB", () => {
    const ddl = generateDDL("test_table", { tags: "vector<u8>" });
    expect(ddl).toContain("tags JSONB NOT NULL");
  });

  test("bool fields use BOOLEAN", () => {
    const ddl = generateDDL("test_table", { active: "bool" });
    expect(ddl).toContain("active BOOLEAN NOT NULL");
  });

  test("all integer sizes map correctly", () => {
    const ddl = generateDDL("test_table", {
      a: "u8",
      b: "u16",
      c: "u32",
      d: "u64",
      e: "u128",
      f: "u256",
    });
    expect(ddl).toContain("a INTEGER NOT NULL");
    expect(ddl).toContain("b INTEGER NOT NULL");
    expect(ddl).toContain("c INTEGER NOT NULL");
    expect(ddl).toContain("d NUMERIC NOT NULL");
    expect(ddl).toContain("e NUMERIC NOT NULL");
    expect(ddl).toContain("f NUMERIC NOT NULL");
  });
});

describe("formatValue", () => {
  test("formats address string as-is", () => {
    const addr = "0x" + "ab".repeat(32);
    expect(formatValue(addr, "address")).toBe(addr);
  });

  test("formats u64 bigint to string", () => {
    expect(formatValue(1000000n, "u64")).toBe("1000000");
  });

  test("formats u8/u16/u32 to number", () => {
    expect(formatValue(42, "u16")).toBe(42);
    expect(formatValue(255, "u8")).toBe(255);
  });

  test("formats bool", () => {
    expect(formatValue(true, "bool")).toBe(true);
    expect(formatValue(false, "bool")).toBe(false);
  });

  test("formats null for option", () => {
    expect(formatValue(null, "option<string>")).toBeNull();
  });

  test("formats vector as JSON string", () => {
    const result = formatValue([1, 2, 3], "vector<u8>");
    expect(result).toBe("[1,2,3]");
  });
});

describe("formatRow", () => {
  test("formats all fields in a decoded struct", () => {
    const fields: FieldDefs = {
      item_id: "address",
      edition: "u16",
      paid_value: "u64",
      active: "bool",
    };
    const decoded = {
      item_id: "0x" + "ab".repeat(32),
      edition: 42,
      paid_value: 1000000n,
      active: true,
    };
    const row = formatRow(decoded, fields);

    expect(row.item_id).toBe("0x" + "ab".repeat(32));
    expect(row.edition).toBe(42);
    expect(row.paid_value).toBe("1000000");
    expect(row.active).toBe(true);
  });
});
