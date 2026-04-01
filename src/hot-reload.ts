/**
 * jun/hot-reload — Runtime config reload without downtime.
 *
 * Parses a new YAML config, diffs against the current config, and applies
 * changes: new event handlers get tables created and added to the processor,
 * removed handlers stop matching (tables preserved), changed fields get
 * ALTER TABLE ADD COLUMN.
 *
 * The swap is safe because JS is single-threaded — between checkpoints,
 * the processor reference is swapped atomically. The gRPC stream, buffers,
 * and SSE clients all stay alive.
 */
import type { EventHandler, EventProcessor } from "./processor.ts";
import { createProcessor } from "./processor.ts";
import type { StorageBackend } from "./output/storage.ts";
import { validateIdentifier } from "./output/storage.ts";
import type { FieldDefs } from "./schema.ts";
import type { Logger } from "./logger.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ReloadResult {
  added: string[];
  removed: string[];
  altered: string[];
  unchanged: string[];
}

export interface HotReloadContext {
  /** Current event handlers (mutable — hot reload updates this) */
  events: Record<string, EventHandler>;
  /** Returns the current processor */
  getProcessor(): EventProcessor;
  /** Swaps the processor */
  setProcessor(p: EventProcessor): void;
  /** Storage backend for running migrations */
  output: StorageBackend;
  /** Handler table mapping (mutable) */
  handlerTables: Record<string, { tableName: string; fields: FieldDefs }>;
  /** SQL instance for ALTER TABLE */
  sql: any;
  /** Remote config URL for reload without body */
  configUrl?: string;
  log: Logger;
}

// ---------------------------------------------------------------------------
// Table name helper (same as index.ts)
// ---------------------------------------------------------------------------

function toTableName(handlerName: string): string {
  return handlerName
    .replace(/([A-Z])/g, "_$1")
    .toLowerCase()
    .replace(/^_/, "");
}

// ---------------------------------------------------------------------------
// Diff
// ---------------------------------------------------------------------------

function diffHandlers(
  current: Record<string, EventHandler>,
  incoming: Record<string, EventHandler>,
): ReloadResult {
  const added: string[] = [];
  const removed: string[] = [];
  const altered: string[] = [];
  const unchanged: string[] = [];

  // Check for new and altered handlers
  for (const [name, handler] of Object.entries(incoming)) {
    const existing = current[name];
    if (!existing) {
      added.push(name);
    } else if (existing.type !== handler.type || JSON.stringify(existing.fields) !== JSON.stringify(handler.fields)) {
      altered.push(name);
    } else {
      unchanged.push(name);
    }
  }

  // Check for removed handlers
  for (const name of Object.keys(current)) {
    if (!incoming[name]) {
      removed.push(name);
    }
  }

  return { added, removed, altered, unchanged };
}

// ---------------------------------------------------------------------------
// Field diff for ALTER TABLE
// ---------------------------------------------------------------------------

function fieldTypeToSql(type: string): string {
  if (type.startsWith("option<")) return fieldTypeToSql(type.slice(7, -1));
  if (type.startsWith("vector<")) return "JSONB";
  switch (type) {
    case "address": case "string": return "TEXT";
    case "bool": return "BOOLEAN";
    case "u8": case "u16": case "u32": return "INTEGER";
    case "u64": case "u128": case "u256": return "NUMERIC";
    default: return "TEXT";
  }
}

async function alterTable(
  sql: any,
  tableName: string,
  oldFields: FieldDefs,
  newFields: FieldDefs,
  log: Logger,
): Promise<string[]> {
  const addedColumns: string[] = [];

  for (const [name, type] of Object.entries(newFields)) {
    if (!oldFields[name]) {
      const sqlType = fieldTypeToSql(type);
      const nullable = type.startsWith("option<") ? "" : " DEFAULT NULL";
      validateIdentifier(tableName);
    validateIdentifier(name);
    await sql.unsafe(`ALTER TABLE ${tableName} ADD COLUMN IF NOT EXISTS ${name} ${sqlType}${nullable}`);
      addedColumns.push(name);
      log.info({ table: tableName, column: name, type: sqlType }, "column added");
    }
  }

  return addedColumns;
}

// ---------------------------------------------------------------------------
// Reload
// ---------------------------------------------------------------------------

export async function applyReload(
  ctx: HotReloadContext,
  incomingEvents: Record<string, EventHandler>,
): Promise<ReloadResult> {
  const { log } = ctx;
  const reloadLog = log.child({ component: "hot-reload" });

  const diff = diffHandlers(ctx.events, incomingEvents);

  reloadLog.info({
    added: diff.added,
    removed: diff.removed,
    altered: diff.altered,
    unchanged: diff.unchanged.length,
  }, "config diff");

  // 1. Create tables for new handlers
  for (const name of diff.added) {
    const handler = incomingEvents[name]!;
    const tableName = toTableName(name);

    // Run DDL via storage backend
    const { generateDDL } = await import("./schema.ts");
    const ddl = generateDDL(tableName, handler.fields);
    await ctx.sql.unsafe(ddl);

    // Update handler tables mapping
    ctx.handlerTables[name] = { tableName, fields: handler.fields };

    reloadLog.info({ handler: name, table: tableName }, "table created");
  }

  // 2. ALTER TABLE for changed handlers (add new columns only)
  for (const name of diff.altered) {
    const oldHandler = ctx.events[name]!;
    const newHandler = incomingEvents[name]!;
    const tableName = ctx.handlerTables[name]?.tableName ?? toTableName(name);

    await alterTable(ctx.sql, tableName, oldHandler.fields, newHandler.fields, reloadLog);

    // Update handler tables mapping with new fields
    ctx.handlerTables[name] = { tableName, fields: newHandler.fields };
  }

  // 3. Remove handlers from mapping (but preserve tables)
  for (const name of diff.removed) {
    delete ctx.handlerTables[name];
    reloadLog.info({ handler: name }, "handler removed (table preserved)");
  }

  // 4. Swap the processor
  // Update the events reference
  for (const key of Object.keys(ctx.events)) delete ctx.events[key];
  Object.assign(ctx.events, incomingEvents);

  // Rebuild processor with new handlers
  const newProcessor = createProcessor(incomingEvents);
  ctx.setProcessor(newProcessor);

  if ('reloadHandlers' in ctx.output) {
    (ctx.output as any).reloadHandlers(ctx.handlerTables);
  }

  reloadLog.info("processor swapped");

  return diff;
}
