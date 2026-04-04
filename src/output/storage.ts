/**
 * StorageBackend — unified interface for event storage.
 *
 * Implemented by PostgresStorageBackend, SqliteStorageBackend, and ParquetStorageBackend.
 * Users choose one SQL backend (Postgres or SQLite) as primary, with optional Parquet
 * as a secondary best-effort output.
 */
import type { DecodedEvent } from "../pipeline/types.ts";
import type { FieldDefs } from "../schema.ts";

/** Validate a SQL identifier (table name, column name, view name). Prevents SQL injection. */
export function validateIdentifier(name: string): string {
  if (!/^[a-z_][a-z0-9_]*$/i.test(name)) {
    throw new Error(`Invalid SQL identifier: "${name}". Must be alphanumeric + underscores.`);
  }
  return name;
}

export interface StorageBackend {
  /** Backend name for logging. */
  readonly name: string;

  /** Create tables/schemas. Called once on startup. */
  migrate(): Promise<void>;

  /** Write a batch of decoded events (groups by handler internally). */
  write(events: DecodedEvent[]): Promise<void>;

  /** Write events for a single handler. Used by WriteBuffer for parallel per-table writes. */
  writeHandler(handlerName: string, events: DecodedEvent[]): Promise<void>;

  /** Graceful shutdown (close connections, flush remaining data). */
  shutdown(): Promise<void>;

  /** Reload handler table configs (for hot reload). Optional — not all backends support it. */
  reloadHandlers?(handlers: Record<string, HandlerTableConfig>): void;
}

/** Handler table config passed to storage backends. */
export interface HandlerTableConfig {
  tableName: string;
  fields: FieldDefs;
}
