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

  // ─── Jun developer tools ──────────────────────────────────────────────────

  server.resource("jun_config_guide", "jun://config-guide", async () => {
    const guide = `# Jun YAML Config Reference

## Required fields

\`\`\`yaml
network: testnet                    # Network name (for cursor keys + archive URL)
grpcUrl: fullnode.testnet.sui.io:443  # Sui fullnode gRPC endpoint
database: $DATABASE_URL             # Postgres connection string (env vars supported)
events:                             # Event handlers (at least one required)
  HandlerName:
    type: "0xPKG::module::EventStruct"  # Fully qualified Move event type
    fields:                         # Field name → type (must match Move struct order!)
      field1: address
      field2: u64
\`\`\`

## Optional fields

\`\`\`yaml
startCheckpoint: 316756645          # Where to start backfill (see formats below)
backfillConcurrency: 10             # Parallel archive fetches (default: 10)
archiveUrl: https://checkpoints.testnet.sui.io  # Override archive URL
\`\`\`

## startCheckpoint formats

- Raw number: \`316756645\` → starts at this checkpoint
- Epoch: \`"epoch:1080"\` → first checkpoint of epoch 1080
- Timestamp: \`"timestamp:2026-03-28T00:00:00Z"\` → checkpoint at/after this time
- Package: \`"package:0x10ad..."\` → checkpoint where package was published
- Numeric string: \`"316756645"\` → parsed as number

## Runtime options (CLI flags override these)

\`\`\`yaml
mode: all                           # "all" | "live-only" | "backfill-only"
repairGaps: false                   # Enable periodic gap detection
serve:                              # HTTP server for /health, /status, /query, /metrics
  port: 8080
  hostname: 127.0.0.1              # Default: localhost only
\`\`\`

## Field types

| Type | Postgres | Notes |
|------|----------|-------|
| address | TEXT | 32-byte Sui address as 0x hex |
| bool | BOOLEAN | |
| u8, u16, u32 | INTEGER | |
| u64, u128, u256 | NUMERIC | Arbitrary precision for large values |
| string | TEXT | |
| option<T> | nullable T | e.g. option<address> → nullable TEXT |
| vector<T> | JSONB | JSON array, e.g. vector<u64> |

**Critical rules:**
- Field order MUST match the Move struct exactly (BCS is positional encoding)
- Only primitive types supported. Custom structs/enums → use granular events or JSONB
- Phantom type parameters don't affect BCS encoding

## Standard columns (auto-added to every event table)

| Column | Type | Source |
|--------|------|--------|
| id | SERIAL PRIMARY KEY | Auto-increment |
| tx_digest | TEXT NOT NULL | Transaction digest |
| event_seq | INTEGER NOT NULL | Index within tx events |
| sender | TEXT NOT NULL | Event sender address |
| sui_timestamp | TIMESTAMPTZ NOT NULL | Checkpoint timestamp |
| indexed_at | TIMESTAMPTZ | Server insertion time |

Unique constraint: (tx_digest, event_seq) with ON CONFLICT DO NOTHING for idempotent replay.

## Materialized views

\`\`\`yaml
views:
  daily_presses:
    sql: |
      SELECT date_trunc('day', sui_timestamp) AS day,
             count(*) AS presses
      FROM record_pressed
      GROUP BY 1
    refresh: 60s                   # Duration: "30s", "5m", "1h"
\`\`\`

Views are created on startup and refreshed on a timer. Reference event table names as snake_case of handler names (RecordPressed → record_pressed).

## Environment variable substitution

Use \`$VAR\` or \`\${VAR}\` in any string value. Resolved from process.env at load time.

\`\`\`yaml
database: $DATABASE_URL
grpcUrl: \${GRPC_ENDPOINT}
\`\`\`

## Example: complete config

\`\`\`yaml
network: testnet
grpcUrl: fullnode.testnet.sui.io:443
database: $DATABASE_URL
startCheckpoint: "package:0x10ad578f"
mode: all
repairGaps: true
serve:
  port: 8080

events:
  RecordPressed:
    type: "0x10ad578f5b202fd137546f2e7bc12c319dfef98871feeb429506c4b3d62bf702::pressing::RecordPressedEvent"
    fields:
      pressing_id: address
      release_id: address
      edition: u16
      record_id: address
      record_number: u64
      quantity: u64
      pressed_by: address
      paid_value: u64
      timestamp_ms: u64

views:
  daily_presses:
    sql: |
      SELECT date_trunc('day', sui_timestamp) AS day,
             release_id,
             count(*) AS presses
      FROM record_pressed
      GROUP BY 1, 2
    refresh: 60s
\`\`\`

Run with: \`jun run config.yml\`
Override: \`jun run config.yml --mode backfill-only --serve 9090\`
`;

    return { contents: [{ uri: "jun://config-guide", text: guide, mimeType: "text/markdown" }] };
  });

  server.resource("jun_http_api", "jun://http-api", async () => {
    const guide = `# Jun HTTP API Reference

Enable with \`serve: { port: 8080 }\` in YAML config or \`--serve 8080\` CLI flag.

## Endpoints

### GET /health
Returns \`{ "status": "ok" }\`. Use for load balancer health checks.

### GET /status
Returns indexer status as JSON:
\`\`\`json
{
  "uptime": 142,
  "live": { "cursor": "318756892", "lastFlush": { "eventsWritten": 3, "flushDurationMs": 34 } },
  "backfill": { "cursor": "316800000", "lastFlush": { "eventsWritten": 200, "flushDurationMs": 120 } },
  "throttle": { "concurrency": 10, "paused": false },
  "counters": { "liveCheckpoints": 892, "backfillCheckpoints": 43255, ... }
}
\`\`\`

### GET /query?sql=...&limit=N&timeout=N
Execute read-only SQL against indexed event tables.

**Parameters:**
- \`sql\` (required): SQL query. Only SELECT, WITH, EXPLAIN allowed.
- \`limit\` (optional): Max rows returned. Default 1000, max 10000.
- \`timeout\` (optional): Query timeout in ms. Default 5000, max 30000.

**Response:** \`{ "rows": [...], "count": N, "truncated": false }\`

**Safety:** Two layers — string prefix check + Postgres SET TRANSACTION READ ONLY.

**Examples:**
\`\`\`
GET /query?sql=SELECT * FROM record_pressed LIMIT 5
GET /query?sql=SELECT count(*) FROM record_pressed
GET /query?sql=SELECT * FROM daily_presses WHERE day >= now() - interval '7 days'
GET /query?sql=WITH recent AS (SELECT * FROM record_pressed WHERE sui_timestamp > now() - interval '1 hour') SELECT pressed_by, count(*) FROM recent GROUP BY 1&limit=20
\`\`\`

### GET /metrics
Prometheus text format. Scrape with Prometheus at this endpoint.

**Metrics exposed:**
- \`jun_uptime_seconds\` — gauge
- \`jun_live_cursor\` / \`jun_backfill_cursor\` — gauge (checkpoint sequence)
- \`jun_checkpoints_processed_total{mode="live|backfill"}\` — counter
- \`jun_events_flushed_total{buffer="live|backfill"}\` — counter
- \`jun_flushes_total{buffer="live|backfill"}\` — counter
- \`jun_flush_duration_seconds{buffer="live|backfill"}\` — gauge (last flush)
- \`jun_buffer_size{buffer="live|backfill"}\` — gauge
- \`jun_backfill_concurrency\` — gauge
- \`jun_backfill_paused\` — gauge (0 or 1)
`;

    return { contents: [{ uri: "jun://http-api", text: guide, mimeType: "text/markdown" }] };
  });

  server.tool(
    "jun_validate_config",
    "Validate a jun YAML indexer config. Returns parsed structure or validation errors.",
    { yaml_content: z.string().describe("Raw YAML config content to validate") },
    async ({ yaml_content }) => {
      try {
        const { parseIndexerConfig } = await import("./indexer-config.ts");
        const parsed = parseIndexerConfig(yaml_content);

        const eventNames = Object.keys(parsed.indexer.events);
        const viewNames = parsed.views ? Object.keys(parsed.views) : [];

        const summary = [
          `Config is valid.`,
          ``,
          `Network: ${parsed.indexer.network}`,
          `gRPC: ${parsed.indexer.grpcUrl}`,
          `Database: ${parsed.indexer.database.replace(/\/\/.*:.*@/, "//***:***@")}`,
          `Start checkpoint: ${parsed.indexer.startCheckpoint ?? "none (live-only)"}`,
          `Events: ${eventNames.join(", ")} (${eventNames.length} handler${eventNames.length === 1 ? "" : "s"})`,
        ];

        for (const name of eventNames) {
          const handler = parsed.indexer.events[name]!;
          const fieldCount = Object.keys(handler.fields).length;
          summary.push(`  ${name}: ${handler.type} (${fieldCount} fields)`);
        }

        if (viewNames.length > 0) {
          summary.push(`Views: ${viewNames.join(", ")}`);
        }

        if (parsed.run.mode) summary.push(`Mode: ${parsed.run.mode}`);
        if (parsed.run.repairGaps) summary.push(`Gap repair: enabled`);
        if (parsed.run.serve) summary.push(`Serve: port ${parsed.run.serve.port}`);

        return { content: [{ type: "text" as const, text: summary.join("\n") }] };
      } catch (err) {
        return { content: [{ type: "text" as const, text: `Validation error: ${err instanceof Error ? err.message : String(err)}` }] };
      }
    },
  );

  server.tool(
    "jun_codegen",
    "Generate jun field DSL from an on-chain Move struct. Fetches the type descriptor via gRPC and maps each field to a jun field type.",
    {
      type_name: z.string().describe("Fully qualified Move type: 0xPKG::module::StructName"),
      grpc_url: z.string().optional().describe("gRPC endpoint (default: active env)"),
    },
    async ({ type_name, grpc_url }) => {
      try {
        const { createGrpcClient } = await import("./grpc.ts");
        const { generateFieldDSL, formatCodegenResult } = await import("./codegen.ts");
        const { loadConfig } = await import("./config.ts");

        const parts = type_name.split("::");
        if (parts.length !== 3) {
          return { content: [{ type: "text" as const, text: "Error: type must be in format 0xPKG::module::StructName" }] };
        }

        const url = grpc_url ?? loadConfig().grpcUrl;
        const client = createGrpcClient({ url });
        const descriptor = await client.getDatatype(parts[0]!, parts[1]!, parts[2]!);
        client.close();

        const result = generateFieldDSL(descriptor);
        const formatted = formatCodegenResult(result);

        // Also generate YAML-ready snippet
        const yamlFields = result.fields
          .map((f) => f.type ? `      ${f.name}: ${f.type}` : `      # ${f.name}: ${f.rawType} (not a primitive — use granular events)`)
          .join("\n");

        const yamlSnippet = [
          ``,
          `YAML config snippet:`,
          ``,
          `events:`,
          `  ${result.name}:`,
          `    type: "${type_name}"`,
          `    fields:`,
          yamlFields,
        ].join("\n");

        return { content: [{ type: "text" as const, text: formatted + yamlSnippet }] };
      } catch (err) {
        return { content: [{ type: "text" as const, text: `Codegen error: ${err instanceof Error ? err.message : String(err)}` }] };
      }
    },
  );

  server.tool(
    "jun_generate_config",
    "Generate a complete jun YAML config from parameters. Provide event types and the tool will fetch field definitions from the chain.",
    {
      network: z.string().describe("Network name: mainnet, testnet, or custom"),
      grpc_url: z.string().describe("gRPC endpoint URL"),
      database: z.string().describe("Postgres connection string or $ENV_VAR"),
      event_types: z.array(z.string()).describe("Array of fully qualified Move event types (0xPKG::module::EventStruct)"),
      start_checkpoint: z.string().optional().describe("Start checkpoint (number, epoch:N, timestamp:ISO, package:0x...)"),
      mode: z.string().optional().describe("Run mode: all, live-only, backfill-only"),
      serve_port: z.number().optional().describe("HTTP server port (omit to disable)"),
    },
    async ({ network, grpc_url, database, event_types, start_checkpoint, mode, serve_port }) => {
      try {
        const { createGrpcClient } = await import("./grpc.ts");
        const { generateFieldDSL } = await import("./codegen.ts");

        const client = createGrpcClient({ url: grpc_url });
        const lines: string[] = [];

        lines.push(`network: ${network}`);
        lines.push(`grpcUrl: ${grpc_url}`);
        lines.push(`database: ${database}`);
        if (start_checkpoint) lines.push(`startCheckpoint: "${start_checkpoint}"`);
        if (mode) lines.push(`mode: ${mode}`);
        if (serve_port) lines.push(`serve:\n  port: ${serve_port}`);
        lines.push(``);
        lines.push(`events:`);

        const warnings: string[] = [];

        for (const typeName of event_types) {
          const parts = typeName.split("::");
          if (parts.length !== 3) {
            warnings.push(`Skipped ${typeName}: must be 0xPKG::module::StructName`);
            continue;
          }

          try {
            const descriptor = await client.getDatatype(parts[0]!, parts[1]!, parts[2]!);
            const result = generateFieldDSL(descriptor);

            lines.push(`  ${result.name}:`);
            lines.push(`    type: "${typeName}"`);
            lines.push(`    fields:`);

            for (const field of result.fields) {
              if (field.type) {
                lines.push(`      ${field.name}: ${field.type}`);
              } else {
                lines.push(`      # ${field.name}: ${field.rawType} (not primitive — use granular events)`);
                warnings.push(`${result.name}.${field.name} is not a primitive type (${field.rawType})`);
              }
            }
          } catch (err) {
            warnings.push(`Failed to fetch ${typeName}: ${err instanceof Error ? err.message : String(err)}`);
          }
        }

        client.close();

        let output = lines.join("\n");
        if (warnings.length > 0) {
          output += `\n\n# Warnings:\n${warnings.map((w) => `# - ${w}`).join("\n")}`;
        }

        return { content: [{ type: "text" as const, text: output }] };
      } catch (err) {
        return { content: [{ type: "text" as const, text: `Error: ${err instanceof Error ? err.message : String(err)}` }] };
      }
    },
  );

  return { server, db };
}

// ─── Standalone entry point ──────────────────────────────────────────────────

if (import.meta.main) {
  const dbPath = process.argv[2];
  const { server } = createMcpServer({ dbPath });
  const transport = new StdioServerTransport();
  await server.connect(transport);
}
