#!/usr/bin/env bun
/**
 * Jun MCP server — exposes Sui checkpoint SQLite files to AI assistants.
 *
 * Usage:
 *   jun mcp --db mainnet.sqlite
 *
 * Provides:
 *   - Resource: database schema (so Claude knows the tables/columns)
 *   - Tool: query (run arbitrary SQL)
 *   - Tool: summary (quick overview of the database)
 */
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { Database } from "bun:sqlite";
import { z } from "zod";

export function createMcpServer(dbPath: string) {
  const db = new Database(dbPath, { readonly: true });
  const server = new McpServer({
    name: "jun",
    version: "0.1.0",
  });

  // ─── Resource: database schema ───────────────────────────────────────────

  server.resource("schema", `jun://schema`, async () => {
    const tables = db.query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").all() as { name: string }[];

    let schema = "";
    for (const { name } of tables) {
      const info = db.query(`PRAGMA table_info(${name})`).all() as { name: string; type: string; notnull: number; pk: number }[];
      const counts = db.query(`SELECT COUNT(*) as count FROM ${name}`).get() as { count: number };

      schema += `-- ${name} (${counts.count.toLocaleString()} rows)\n`;
      schema += `CREATE TABLE ${name} (\n`;
      schema += info.map((col) => `  ${col.name} ${col.type}${col.pk ? " PRIMARY KEY" : ""}${col.notnull ? " NOT NULL" : ""}`).join(",\n");
      schema += "\n);\n\n";
    }

    return { contents: [{ uri: `jun://schema`, text: schema, mimeType: "text/sql" }] };
  });

  // ─── Tool: query ─────────────────────────────────────────────────────────

  server.tool(
    "query",
    "Run a SQL query against the Sui checkpoint database. Use SELECT only.",
    { sql: z.string().describe("SQL query to execute (SELECT only)") },
    async ({ sql }) => {
      // Block mutations
      const normalized = sql.trim().toUpperCase();
      if (!normalized.startsWith("SELECT") && !normalized.startsWith("WITH") && !normalized.startsWith("EXPLAIN")) {
        return { content: [{ type: "text", text: "Error: Only SELECT, WITH, and EXPLAIN queries are allowed." }] };
      }

      try {
        const rows = db.query(sql).all();
        const text = rows.length === 0
          ? "No results."
          : JSON.stringify(rows, null, 2);
        return { content: [{ type: "text", text: `${rows.length} row(s)\n\n${text}` }] };
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        return { content: [{ type: "text", text: `SQL error: ${msg}` }] };
      }
    },
  );

  // ─── Tool: summary ───────────────────────────────────────────────────────

  server.tool(
    "summary",
    "Get a quick overview of the Sui checkpoint database: row counts, time range, top events.",
    {},
    async () => {
      const txCount = (db.query("SELECT COUNT(*) as c FROM transactions").get() as any).c;
      const eventCount = (db.query("SELECT COUNT(*) as c FROM events").get() as any).c;

      const timeRange = db.query("SELECT MIN(timestamp) as start, MAX(timestamp) as end FROM transactions").get() as any;
      const checkpoints = db.query("SELECT COUNT(DISTINCT checkpoint) as c FROM transactions").get() as any;

      const statusBreakdown = db.query("SELECT status, COUNT(*) as count FROM transactions GROUP BY status").all();

      const topEvents = db.query(
        "SELECT event_type, COUNT(*) as count FROM events GROUP BY event_type ORDER BY count DESC LIMIT 10",
      ).all();

      const topSenders = db.query(
        "SELECT sender, COUNT(*) as events FROM events GROUP BY sender ORDER BY events DESC LIMIT 5",
      ).all();

      // Check if balance_changes table exists
      let balanceChanges = null;
      try {
        balanceChanges = (db.query("SELECT COUNT(*) as c FROM balance_changes").get() as any).c;
      } catch {
        // table doesn't exist
      }

      const summary = {
        checkpoints: checkpoints.c,
        transactions: txCount,
        events: eventCount,
        balanceChanges,
        timeRange: { start: timeRange.start, end: timeRange.end },
        statusBreakdown,
        topEvents,
        topSenders,
      };

      return { content: [{ type: "text", text: JSON.stringify(summary, null, 2) }] };
    },
  );

  return { server, db };
}

// ─── Standalone entry point ──────────────────────────────────────────────────

if (import.meta.main) {
  const dbPath = process.argv[2];
  if (!dbPath) {
    console.error("Usage: bun src/mcp.ts <path-to-sqlite>");
    process.exit(1);
  }

  const { server } = createMcpServer(dbPath);
  const transport = new StdioServerTransport();
  await server.connect(transport);
}
