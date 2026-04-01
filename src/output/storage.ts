/**
 * StorageBackend — unified interface for event storage.
 *
 * Implemented by PostgresStorageBackend, SqliteStorageBackend, and ParquetStorageBackend.
 * Users choose one SQL backend (Postgres or SQLite) as primary, with optional Parquet
 * as a secondary best-effort output.
 */
import type { DecodedEvent } from "../processor.ts";
import type { FieldDefs } from "../schema.ts";

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
}

/** Handler table config passed to storage backends. */
export interface HandlerTableConfig {
  tableName: string;
  fields: FieldDefs;
}
