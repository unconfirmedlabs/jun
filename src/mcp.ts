#!/usr/bin/env bun
/**
 * Jun MCP server — Sui chain queries + optional SQLite checkpoint analysis.
 *
 * Usage:
 *   jun mcp                       # chain query tools only
 *   jun mcp mainnet.sqlite        # chain + SQLite analysis tools
 */
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { Database } from "bun:sqlite";
import { z } from "zod";
import { jsonReplacer } from "./cli-helpers.ts";
import { getSuiClient } from "./rpc.ts";

export interface McpServerOptions {
  dbPath?: string;
  grpcUrl?: string;
}

export function createMcpServer(options: McpServerOptions = {}) {
  const server = new McpServer({ name: "jun", version: "0.1.0" });

  // ─── Sui chain query tools ──────────────────────────────────────────────

  const sui = getSuiClient(options.grpcUrl);

  server.tool(
    "sui_info",
    "Get Sui chain overview: chain ID, epoch, latest checkpoint, gas price, server version.",
    {},
    async () => {
      try {
        const [{ response: info }, gasResult] = await Promise.all([
          sui.ledgerService.getServiceInfo({}),
          sui.core.getReferenceGasPrice(),
        ]);
        const result = {
          chain: info.chain,
          chainId: info.chainId,
          epoch: String(info.epoch),
          checkpointHeight: String(info.checkpointHeight),
          timestamp: info.timestamp?.seconds ? new Date(Number(info.timestamp.seconds) * 1000).toISOString() : null,
          referenceGasPrice: gasResult.referenceGasPrice,
          server: info.server,
        };
        return { content: [{ type: "text" as const, text: JSON.stringify(result, null, 2) }] };
      } catch (e) {
        return { content: [{ type: "text" as const, text: `Error: ${e instanceof Error ? e.message : String(e)}` }] };
      }
    },
  );

  server.tool(
    "sui_object",
    "Get a Sui object by ID. Returns type, version, owner, and JSON content.",
    { id: z.string().describe("Object ID (0x...)") },
    async ({ id }) => {
      try {
        const result = await sui.core.getObject({
          objectId: id,
          include: { json: true, previousTransaction: true },
        });
        if (!result.object) return { content: [{ type: "text" as const, text: `Object ${id} not found` }] };
        return { content: [{ type: "text" as const, text: JSON.stringify(result.object, jsonReplacer, 2) }] };
      } catch (e) {
        return { content: [{ type: "text" as const, text: `Error: ${e instanceof Error ? e.message : String(e)}` }] };
      }
    },
  );

  server.tool(
    "sui_transaction",
    "Get a Sui transaction by digest. Returns status, sender, gas, events, and balance changes.",
    { digest: z.string().describe("Transaction digest (base58)") },
    async ({ digest }) => {
      try {
        const result = await sui.core.getTransaction({
          digest,
          include: { effects: true, events: true, transaction: true, balanceChanges: true },
        });
        const tx = result.Transaction ?? result.FailedTransaction;
        if (!tx) return { content: [{ type: "text" as const, text: `Transaction ${digest} not found` }] };
        return { content: [{ type: "text" as const, text: JSON.stringify(tx, jsonReplacer, 2) }] };
      } catch (e) {
        return { content: [{ type: "text" as const, text: `Error: ${e instanceof Error ? e.message : String(e)}` }] };
      }
    },
  );

  server.tool(
    "sui_balance",
    "Get coin balance for a Sui address with human-readable formatting.",
    {
      address: z.string().describe("Sui address (0x...)"),
      coin_type: z.string().optional().describe("Coin type (default: 0x2::sui::SUI)"),
    },
    async ({ address, coin_type }) => {
      try {
        const coinType = coin_type ?? "0x2::sui::SUI";
        const [balResult, metaResult] = await Promise.all([
          sui.core.getBalance({ owner: address, coinType }),
          sui.core.getCoinMetadata({ coinType }).catch(() => ({ coinMetadata: null })),
        ]);
        const meta = metaResult.coinMetadata;
        const raw = BigInt(balResult.balance.balance);
        const decimals = meta?.decimals ?? 9;
        const symbol = meta?.symbol ?? coinType.split("::").pop() ?? coinType;
        const formatted = decimals > 0
          ? (Number(raw) / 10 ** decimals).toLocaleString(undefined, { minimumFractionDigits: Math.min(decimals, 4), maximumFractionDigits: Math.min(decimals, 4) })
          : raw.toLocaleString();
        const result = {
          address,
          coinType,
          balance: raw.toString(),
          formatted: `${formatted} ${symbol}`,
          decimals,
          symbol,
        };
        return { content: [{ type: "text" as const, text: JSON.stringify(result, null, 2) }] };
      } catch (e) {
        return { content: [{ type: "text" as const, text: `Error: ${e instanceof Error ? e.message : String(e)}` }] };
      }
    },
  );

  server.tool(
    "sui_owned_objects",
    "List objects owned by a Sui address. Supports type filtering and pagination.",
    {
      address: z.string().describe("Sui address (0x...)"),
      type: z.string().optional().describe("Filter by Move type"),
      limit: z.number().optional().default(10).describe("Max results (default: 10)"),
      cursor: z.string().optional().describe("Pagination cursor"),
    },
    async ({ address, type, limit, cursor }) => {
      try {
        const result = await sui.core.listOwnedObjects({
          owner: address,
          type,
          limit,
          cursor: cursor ?? null,
          include: { json: true },
        });
        return { content: [{ type: "text" as const, text: JSON.stringify(result, jsonReplacer, 2) }] };
      } catch (e) {
        return { content: [{ type: "text" as const, text: `Error: ${e instanceof Error ? e.message : String(e)}` }] };
      }
    },
  );

  server.tool(
    "sui_dynamic_fields",
    "List dynamic fields of a Sui object. Dynamic fields are runtime key-value pairs attached to objects.",
    {
      parent_id: z.string().describe("Parent object ID (0x...)"),
      limit: z.number().optional().describe("Max results per page"),
      cursor: z.string().optional().describe("Pagination cursor"),
    },
    async ({ parent_id, limit, cursor }) => {
      try {
        const result = await sui.listDynamicFields({
          parentId: parent_id,
          limit,
          cursor: cursor ?? null,
        });
        return { content: [{ type: "text" as const, text: JSON.stringify(result, jsonReplacer, 2) }] };
      } catch (e) {
        return { content: [{ type: "text" as const, text: `Error: ${e instanceof Error ? e.message : String(e)}` }] };
      }
    },
  );

  server.tool(
    "sui_epoch",
    "Get Sui epoch info: validators, stake, subsidy, checkpoints, timing. Omit epoch for current.",
    { epoch: z.string().optional().describe("Epoch number (omit for current)") },
    async ({ epoch }) => {
      try {
        let epochNum: bigint;
        if (epoch) {
          epochNum = BigInt(epoch);
        } else {
          const sys = await sui.core.getCurrentSystemState();
          epochNum = BigInt(sys.systemState.epoch);
        }
        const { response } = await sui.ledgerService.getEpoch({
          epoch: epochNum,
          readMask: { paths: ["*"] },
        });
        return { content: [{ type: "text" as const, text: JSON.stringify(response.epoch, jsonReplacer, 2) }] };
      } catch (e) {
        return { content: [{ type: "text" as const, text: `Error: ${e instanceof Error ? e.message : String(e)}` }] };
      }
    },
  );

  server.tool(
    "sui_move_function",
    "Get a Move function's signature: parameters, return types, visibility, type constraints.",
    {
      package_id: z.string().describe("Package ID (0x...)"),
      module: z.string().describe("Module name"),
      function_name: z.string().describe("Function name"),
    },
    async ({ package_id, module, function_name }) => {
      try {
        const result = await sui.core.getMoveFunction({
          packageId: package_id,
          moduleName: module,
          name: function_name,
        });
        return { content: [{ type: "text" as const, text: JSON.stringify(result.function, jsonReplacer, 2) }] };
      } catch (e) {
        return { content: [{ type: "text" as const, text: `Error: ${e instanceof Error ? e.message : String(e)}` }] };
      }
    },
  );

  server.tool(
    "sui_package",
    "Get Move package metadata: modules, types, functions, linkage.",
    { id: z.string().describe("Package ID (0x...)") },
    async ({ id }) => {
      try {
        const { response } = await sui.movePackageService.getPackage({ packageId: id });
        if (!response.package) return { content: [{ type: "text" as const, text: `Package ${id} not found` }] };
        // Summarize modules instead of returning raw bytecode
        const pkg = response.package;
        const summary = {
          storageId: pkg.storageId,
          originalId: pkg.originalId,
          version: String(pkg.version),
          modules: pkg.modules?.map((m: any) => ({
            name: m.name,
            datatypes: m.datatypes?.length ?? 0,
            functions: m.functions?.length ?? 0,
          })) ?? [],
          typeOrigins: pkg.typeOrigins ?? [],
          linkage: pkg.linkage ?? [],
        };
        return { content: [{ type: "text" as const, text: JSON.stringify(summary, jsonReplacer, 2) }] };
      } catch (e) {
        return { content: [{ type: "text" as const, text: `Error: ${e instanceof Error ? e.message : String(e)}` }] };
      }
    },
  );

  server.tool(
    "sui_resolve_name",
    "Resolve a SuiNS name to address, or an address to its SuiNS name.",
    {
      name: z.string().optional().describe("SuiNS name (e.g. example.sui)"),
      address: z.string().optional().describe("Address to reverse-lookup (0x...)"),
    },
    async ({ name, address }) => {
      try {
        if (name) {
          const { response } = await sui.nameService.lookupName({ name });
          const resolved = response.record?.targetAddress ?? null;
          return { content: [{ type: "text" as const, text: JSON.stringify({ name, address: resolved }, null, 2) }] };
        }
        if (address) {
          const result = await sui.core.defaultNameServiceName({ address });
          return { content: [{ type: "text" as const, text: JSON.stringify({ address, name: result ?? null }, null, 2) }] };
        }
        return { content: [{ type: "text" as const, text: "Error: provide either 'name' or 'address'" }] };
      } catch (e) {
        return { content: [{ type: "text" as const, text: `Error: ${e instanceof Error ? e.message : String(e)}` }] };
      }
    },
  );

  // ─── SQLite tools (only when dbPath provided) ───────────────────────────

  let db: Database | null = null;

  if (options.dbPath) {
    db = new Database(options.dbPath, { readonly: true });

    server.resource("schema", `jun://schema`, async () => {
      const tables = db!.query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").all() as { name: string }[];

      let schema = "";
      for (const { name } of tables) {
        const info = db!.query(`PRAGMA table_info(${name})`).all() as { name: string; type: string; notnull: number; pk: number }[];
        const counts = db!.query(`SELECT COUNT(*) as count FROM ${name}`).get() as { count: number };

        schema += `-- ${name} (${counts.count.toLocaleString()} rows)\n`;
        schema += `CREATE TABLE ${name} (\n`;
        schema += info.map((col) => `  ${col.name} ${col.type}${col.pk ? " PRIMARY KEY" : ""}${col.notnull ? " NOT NULL" : ""}`).join(",\n");
        schema += "\n);\n\n";
      }

      return { contents: [{ uri: `jun://schema`, text: schema, mimeType: "text/sql" }] };
    });

    server.tool(
      "query",
      "Run a SQL query against the Sui checkpoint database. Use SELECT only.",
      { sql: z.string().describe("SQL query to execute (SELECT only)") },
      async ({ sql }) => {
        const normalized = sql.trim().toUpperCase();
        if (!normalized.startsWith("SELECT") && !normalized.startsWith("WITH") && !normalized.startsWith("EXPLAIN")) {
          return { content: [{ type: "text" as const, text: "Error: Only SELECT, WITH, and EXPLAIN queries are allowed." }] };
        }
        try {
          const rows = db!.query(sql).all();
          const text = rows.length === 0 ? "No results." : JSON.stringify(rows, null, 2);
          return { content: [{ type: "text" as const, text: `${rows.length} row(s)\n\n${text}` }] };
        } catch (e) {
          return { content: [{ type: "text" as const, text: `SQL error: ${e instanceof Error ? e.message : String(e)}` }] };
        }
      },
    );

    server.tool(
      "summary",
      "Get a quick overview of the Sui checkpoint database: row counts, time range, top events.",
      {},
      async () => {
        const txCount = (db!.query("SELECT COUNT(*) as c FROM transactions").get() as any).c;
        const eventCount = (db!.query("SELECT COUNT(*) as c FROM events").get() as any).c;
        const timeRange = db!.query("SELECT MIN(timestamp) as start, MAX(timestamp) as end FROM transactions").get() as any;
        const checkpoints = db!.query("SELECT COUNT(DISTINCT checkpoint) as c FROM transactions").get() as any;
        const statusBreakdown = db!.query("SELECT status, COUNT(*) as count FROM transactions GROUP BY status").all();
        const topEvents = db!.query("SELECT event_type, COUNT(*) as count FROM events GROUP BY event_type ORDER BY count DESC LIMIT 10").all();
        const topSenders = db!.query("SELECT sender, COUNT(*) as events FROM events GROUP BY sender ORDER BY events DESC LIMIT 5").all();

        let balanceChanges = null;
        try {
          balanceChanges = (db!.query("SELECT COUNT(*) as c FROM balance_changes").get() as any).c;
        } catch { /* table doesn't exist */ }

        const result = { checkpoints: checkpoints.c, transactions: txCount, events: eventCount, balanceChanges, timeRange: { start: timeRange.start, end: timeRange.end }, statusBreakdown, topEvents, topSenders };
        return { content: [{ type: "text" as const, text: JSON.stringify(result, null, 2) }] };
      },
    );
  }

  return { server, db };
}

// ─── Standalone entry point ──────────────────────────────────────────────────

if (import.meta.main) {
  const dbPath = process.argv[2];
  const { server } = createMcpServer({ dbPath });
  const transport = new StdioServerTransport();
  await server.connect(transport);
}
