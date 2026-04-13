/**
 * Schema system: field DSL → BCS schema + Postgres DDL generation.
 *
 * Field types map to both @mysten/bcs types and Postgres column types.
 * Field order matters — BCS is positional, so declarations must match
 * the on-chain struct field order exactly.
 */
import { bcs } from "@mysten/bcs";
import type { BcsType } from "@mysten/bcs";

// ---------------------------------------------------------------------------
// Sui Address BCS type — 32-byte fixed array with hex string transform
// ---------------------------------------------------------------------------

const SuiAddress = bcs.fixedArray(32, bcs.u8()).transform({
  input: (val: string) => {
    const hex = val.startsWith("0x") ? val.slice(2) : val;
    return Array.from(Buffer.from(hex, "hex"));
  },
  output: (val: number[]) => {
    return "0x" + Buffer.from(val).toString("hex");
  },
});

// ---------------------------------------------------------------------------
// Field type DSL
// ---------------------------------------------------------------------------

/** Primitive field type strings */
export type PrimitiveFieldType =
  | "address"
  | "bool"
  | "u8"
  | "u16"
  | "u32"
  | "u64"
  | "u128"
  | "u256"
  | "string";

/** Compound field type strings */
export type FieldType = PrimitiveFieldType | `option<${string}>` | `vector<${string}>`;

/** A record of field names to field type strings */
export type FieldDefs = Record<string, FieldType>;

// ---------------------------------------------------------------------------
// BCS schema generation
// ---------------------------------------------------------------------------

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyBcsType = BcsType<any, any>;

/** Parse a field type string and return the corresponding BcsType */
function fieldTypeToBcs(type: FieldType): AnyBcsType {
  // option<T>
  const optionMatch = type.match(/^option<(.+)>$/);
  if (optionMatch) {
    const inner = fieldTypeToBcs(optionMatch[1] as FieldType);
    return bcs.option(inner);
  }

  // vector<T>
  const vectorMatch = type.match(/^vector<(.+)>$/);
  if (vectorMatch) {
    const inner = fieldTypeToBcs(vectorMatch[1] as FieldType);
    return bcs.vector(inner);
  }

  // Primitives
  switch (type) {
    case "address":
      return SuiAddress;
    case "bool":
      return bcs.bool();
    case "u8":
      return bcs.u8();
    case "u16":
      return bcs.u16();
    case "u32":
      return bcs.u32();
    case "u64":
      return bcs.u64();
    case "u128":
      return bcs.u128();
    case "u256":
      return bcs.u256();
    case "string":
      return bcs.string();
    default:
      throw new Error(`Unknown field type: ${type}`);
  }
}

/**
 * Build a BCS struct schema from field definitions.
 * Field order is preserved — this is critical for correct BCS decoding.
 */
export function buildBcsSchema(fields: FieldDefs): AnyBcsType {
  const schemaFields: Record<string, AnyBcsType> = {};
  for (const [name, type] of Object.entries(fields)) {
    schemaFields[name] = fieldTypeToBcs(type);
  }
  return bcs.struct("Event", schemaFields);
}

// ---------------------------------------------------------------------------
// Postgres DDL generation
// ---------------------------------------------------------------------------

/** Map a field type to a Postgres column type */
function fieldTypeToSql(type: FieldType): string {
  // option<T> — same column type as T, just nullable
  const optionMatch = type.match(/^option<(.+)>$/);
  if (optionMatch) {
    return fieldTypeToSql(optionMatch[1] as FieldType);
  }

  // vector<T> — store as JSONB
  const vectorMatch = type.match(/^vector<(.+)>$/);
  if (vectorMatch) {
    return "JSONB";
  }

  switch (type) {
    case "address":
      return "TEXT"; // 0x-prefixed hex
    case "bool":
      return "BOOLEAN";
    case "u8":
    case "u16":
    case "u32":
      return "INTEGER";
    case "u64":
    case "u128":
    case "u256":
      return "NUMERIC"; // arbitrary precision, safe for large values
    case "string":
      return "TEXT";
    default:
      throw new Error(`Unknown field type: ${type}`);
  }
}

/** Check if a field type is nullable (option<T>) */
function isNullable(type: FieldType): boolean {
  return type.startsWith("option<");
}

/**
 * Generate a CREATE TABLE statement from field definitions.
 * Every event table includes standard metadata columns.
 */
export function generateDDL(tableName: string, fields: FieldDefs): string {
  const columns: string[] = [
    "id SERIAL PRIMARY KEY",
    "tx_digest TEXT NOT NULL",
    "event_seq INTEGER NOT NULL",
    "sender TEXT NOT NULL",
    "sui_timestamp TIMESTAMPTZ NOT NULL",
    "indexed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
  ];

  for (const [name, type] of Object.entries(fields)) {
    const sqlType = fieldTypeToSql(type);
    const nullable = isNullable(type) ? "" : " NOT NULL";
    columns.push(`${name} ${sqlType}${nullable}`);
  }

  columns.push("UNIQUE (tx_digest, event_seq)");

  return `CREATE TABLE IF NOT EXISTS ${tableName} (\n  ${columns.join(",\n  ")}\n);`;
}

// ---------------------------------------------------------------------------
// BCS value formatting for Postgres
// ---------------------------------------------------------------------------

/**
 * Format a decoded BCS value for Postgres insertion.
 * - Addresses (Uint8Array) → 0x-prefixed hex string
 * - BigInt → string (Postgres NUMERIC accepts string representations)
 * - null/undefined → null
 * - Arrays → JSON stringified
 * - Primitives → as-is
 */
export function formatValue(value: unknown, type: FieldType): unknown {
  if (value === null || value === undefined) return null;

  // option<T> — unwrap and format inner value
  const optionMatch = type.match(/^option<(.+)>$/);
  if (optionMatch) {
    return formatValue(value, optionMatch[1] as FieldType);
  }

  // vector<T> — JSON stringify
  const vectorMatch = type.match(/^vector<(.+)>$/);
  if (vectorMatch) {
    if (Array.isArray(value)) {
      const innerType = vectorMatch[1] as FieldType;
      return JSON.stringify(value.map((v) => formatValue(v, innerType)));
    }
    return JSON.stringify(value);
  }

  switch (type) {
    case "address":
      // BCS decodes addresses as hex strings (from @mysten/bcs Address)
      if (typeof value === "string") return value;
      if (value instanceof Uint8Array) {
        return "0x" + Buffer.from(value).toString("hex");
      }
      return String(value);

    case "u64":
    case "u128":
    case "u256":
      // BigInt values need to be strings for Postgres NUMERIC
      return String(value);

    case "bool":
      return Boolean(value);

    case "u8":
    case "u16":
    case "u32":
      return Number(value);

    case "string":
      return String(value);

    default:
      return value;
  }
}

/**
 * Format a full decoded BCS struct for Postgres insertion.
 * Returns an object with all values properly formatted.
 */
export function formatRow(
  decoded: Record<string, unknown>,
  fields: FieldDefs,
): Record<string, unknown> {
  const row: Record<string, unknown> = {};
  for (const [name, type] of Object.entries(fields)) {
    row[name] = formatValue(decoded[name], type);
  }
  return row;
}
