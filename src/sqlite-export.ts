/**
 * SQLite export helpers for deriving dataset-specific databases from a source
 * snapshot. This is used by the CLI export path and stays out of the hot
 * ingestion/write loop.
 */
import { Database } from "bun:sqlite";
import { dirname, extname } from "path";
import { mkdirSync, unlinkSync } from "fs";

export type SqliteExportDataset = "transactions" | "balance_changes" | "balances" | "events";

export interface SqliteDatasetExport {
  dataset: SqliteExportDataset;
  path: string;
  tables: string[];
}

const CORE_TABLES = new Set(["transactions", "move_calls", "balance_changes", "balances"]);

function quoteSqliteString(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function quoteSqliteIdentifier(value: string): string {
  return `"${value.replace(/"/g, "\"\"")}"`;
}

function insertDatasetBeforeExtension(path: string, dataset: SqliteExportDataset): string {
  const ext = extname(path);
  if (!ext) return `${path}.${dataset}`;
  return `${path.slice(0, -ext.length)}.${dataset}${ext}`;
}

function cleanupSqliteFile(path: string): void {
  for (const candidate of [path, `${path}-wal`, `${path}-shm`]) {
    try {
      unlinkSync(candidate);
    } catch {}
  }
}

function listUserTables(sourcePath: string): string[] {
  const db = new Database(sourcePath, { readonly: true });
  try {
    const rows = db.query(`
      SELECT name
      FROM sqlite_master
      WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
      ORDER BY name
    `).all() as { name: string }[];
    return rows.map(row => row.name);
  } finally {
    db.close();
  }
}

function cloneTablesIntoDatabase(sourcePath: string, destPath: string, tables: string[]): void {
  mkdirSync(dirname(destPath), { recursive: true });
  cleanupSqliteFile(destPath);

  const db = new Database(destPath);
  try {
    db.exec("PRAGMA journal_mode = DELETE");
    db.exec("PRAGMA synchronous = NORMAL");
    db.exec(`ATTACH DATABASE ${quoteSqliteString(sourcePath)} AS source`);

    for (const table of tables) {
      const tableSqlRow = db.query(`
        SELECT sql
        FROM source.sqlite_master
        WHERE type = 'table' AND name = ${quoteSqliteString(table)}
      `).get() as { sql: string | null } | null;

      if (!tableSqlRow?.sql) {
        throw new Error(`Missing CREATE TABLE statement for ${table}`);
      }

      db.exec(tableSqlRow.sql);
      db.exec(`
        INSERT INTO main.${quoteSqliteIdentifier(table)}
        SELECT * FROM source.${quoteSqliteIdentifier(table)}
      `);

      const indexRows = db.query(`
        SELECT sql
        FROM source.sqlite_master
        WHERE type = 'index'
          AND tbl_name = ${quoteSqliteString(table)}
          AND sql IS NOT NULL
        ORDER BY name
      `).all() as { sql: string }[];

      for (const row of indexRows) {
        db.exec(row.sql);
      }
    }

    db.exec("DETACH DATABASE source");
    db.exec("VACUUM");
  } finally {
    db.close();
  }
}

export function deriveSplitSqliteExportPath(sourcePath: string, dataset: SqliteExportDataset): string {
  return insertDatasetBeforeExtension(sourcePath, dataset);
}

export function deriveSplitSqliteExportKey(key: string, dataset: SqliteExportDataset): string {
  return insertDatasetBeforeExtension(key, dataset);
}

export function exportSplitSqliteDatasets(sourcePath: string): SqliteDatasetExport[] {
  const sourceTables = listUserTables(sourcePath);
  const eventTables = sourceTables.filter(table => !CORE_TABLES.has(table));

  const exportPlan: Array<{ dataset: SqliteExportDataset; tables: string[] }> = [
    {
      dataset: "transactions",
      tables: sourceTables.filter(table => table === "transactions" || table === "move_calls"),
    },
    {
      dataset: "balance_changes",
      tables: sourceTables.filter(table => table === "balance_changes"),
    },
    {
      dataset: "balances",
      tables: sourceTables.filter(table => table === "balances"),
    },
    {
      dataset: "events",
      tables: eventTables,
    },
  ];

  const exports: SqliteDatasetExport[] = [];
  for (const item of exportPlan) {
    if (item.tables.length === 0) continue;

    const path = deriveSplitSqliteExportPath(sourcePath, item.dataset);
    cloneTablesIntoDatabase(sourcePath, path, item.tables);
    exports.push({
      dataset: item.dataset,
      path,
      tables: item.tables,
    });
  }

  if (exports.length === 0) {
    throw new Error(`No exportable dataset tables found in ${sourcePath}`);
  }

  return exports;
}
