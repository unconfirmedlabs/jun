/**
 * Database connection factories -- single source of truth for Postgres and SQLite setup.
 */
import { Database } from "bun:sqlite";

/**
 * Create a Postgres connection via Bun.SQL.
 */
export function createPostgresConnection(url: string) {
  return new Bun.SQL(url);
}

/**
 * Create a SQLite connection with standard pragmas.
 */
export function createSqliteConnection(path: string): Database {
  const db = new Database(path);
  db.exec("PRAGMA journal_mode = WAL");
  db.exec("PRAGMA synchronous = NORMAL");
  db.exec("PRAGMA cache_size = -64000");
  db.exec("PRAGMA mmap_size = 268435456");
  return db;
}
