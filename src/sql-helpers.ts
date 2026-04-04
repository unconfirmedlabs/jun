/**
 * Shared SQL helper functions -- used by serve.ts, serve-worker.ts, and destinations.
 */
import type { FieldDefs, FieldType } from "./schema.ts";

// ---------------------------------------------------------------------------
// Query safety
// ---------------------------------------------------------------------------

/** Strip SQL comments to prevent prefix check bypass. */
export function stripComments(sql: string): string {
  return sql
    .replace(/\/\*[\s\S]*?\*\//g, "")  // block comments
    .replace(/^--[^\n]*$/gm, "")        // line comments
    .trim();
}

/** Check if a query is read-only (SELECT, WITH, EXPLAIN). */
export function isReadOnly(sql: string): boolean {
  const upper = stripComments(sql).toUpperCase();
  return upper.startsWith("SELECT") || upper.startsWith("WITH") || upper.startsWith("EXPLAIN");
}

// ---------------------------------------------------------------------------
// SQLite DDL helpers
// ---------------------------------------------------------------------------

/** Map a jun FieldType to a SQLite column type. */
export function fieldTypeToSqlite(type: FieldType): string {
  if (type.startsWith("option<")) return fieldTypeToSqlite(type.slice(7, -1) as FieldType);
  if (type.startsWith("vector<")) return "TEXT";
  switch (type) {
    case "address": case "string": return "TEXT";
    case "bool": return "INTEGER";
    case "u8": case "u16": case "u32": return "INTEGER";
    case "u64": case "u128": case "u256": return "TEXT";
    default: return "TEXT";
  }
}

/** Generate a CREATE TABLE statement for SQLite from field definitions. */
export function generateSqliteDDL(tableName: string, fields: FieldDefs): string {
  const columns = [
    "id INTEGER PRIMARY KEY AUTOINCREMENT",
    "tx_digest TEXT NOT NULL",
    "event_seq INTEGER NOT NULL",
    "sender TEXT NOT NULL",
    "sui_timestamp TEXT NOT NULL",
    "indexed_at TEXT NOT NULL DEFAULT (datetime('now'))",
  ];
  for (const [name, type] of Object.entries(fields)) {
    const nullable = type.startsWith("option<") ? "" : " NOT NULL";
    columns.push(`${name} ${fieldTypeToSqlite(type)}${nullable}`);
  }
  columns.push("UNIQUE (tx_digest, event_seq)");
  return `CREATE TABLE IF NOT EXISTS ${tableName} (\n  ${columns.join(",\n  ")}\n);`;
}
