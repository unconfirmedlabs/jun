/**
 * Parse YAML pipeline config into pipeline components.
 *
 * Maps the YAML structure (sources/processors/destinations) to
 * actual Source, Processor, and Destination instances.
 */
import yaml from "js-yaml";
import type { Source, Processor, Storage, Broadcast, PipelineConfig } from "./types.ts";
import { createGrpcLiveSource } from "./sources/grpc.ts";
import { createArchiveSource } from "./sources/archive.ts";
import { createEventDecoder } from "./processors/event-decoder.ts";
import { createBalanceTracker } from "./processors/balance-tracker.ts";
import { createTransactionTracker } from "./processors/transaction-tracker.ts";
import { createSqlStorage } from "./destinations/sql.ts";
import { createSseBroadcast } from "./destinations/sse.ts";
import { createNatsBroadcast } from "./destinations/nats.ts";
import { createStdoutBroadcast } from "./destinations/stdout.ts";
import { normalizeEventType, normalizeCoinType, validateEventTypeAddress } from "../normalize.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ParsedPipelineConfig {
  sources: Source[];
  processors: Processor[];
  storages: Storage[];
  broadcasts: Broadcast[];
  pipelineConfig: PipelineConfig;
}

// ---------------------------------------------------------------------------
// Env var substitution
// ---------------------------------------------------------------------------

function substituteEnvVars(value: string): string {
  return value.replace(/\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)/g, (match, braced, bare) => {
    const variableName = braced ?? bare;
    const envValue = process.env[variableName];
    if (envValue === undefined) {
      throw new Error(`Environment variable ${variableName} is not set (referenced in config)`);
    }
    return envValue;
  });
}

function substituteDeep(obj: any): any {
  if (typeof obj === "string") return substituteEnvVars(obj);
  if (Array.isArray(obj)) return obj.map(substituteDeep);
  if (obj !== null && typeof obj === "object") {
    const result: any = {};
    for (const [key, value] of Object.entries(obj)) {
      result[key] = substituteDeep(value);
    }
    return result;
  }
  return obj;
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

export function parsePipelineConfig(yamlContent: string): ParsedPipelineConfig {
  const raw = yaml.load(yamlContent) as any;
  if (!raw || typeof raw !== "object") {
    throw new Error("Invalid config: YAML must be a mapping");
  }

  const config = substituteDeep(raw);
  const sources: Source[] = [];
  const processors: Processor[] = [];
  const storages: Storage[] = [];
  const broadcasts: Broadcast[] = [];

  // --- Sources ---
  const sourceConfig = config.sources;
  if (!sourceConfig) {
    throw new Error("Invalid config: missing 'sources' section");
  }

  if (sourceConfig.live?.grpc) {
    sources.push(createGrpcLiveSource({ url: sourceConfig.live.grpc }));
  }

  if (sourceConfig.backfill?.archive) {
    let from: bigint;
    if (typeof sourceConfig.backfill.from === "number") {
      from = BigInt(Math.floor(sourceConfig.backfill.from));
    } else if (typeof sourceConfig.backfill.from === "string") {
      // Could be "epoch:N", "timestamp:ISO", "package:0x...", or numeric string
      // For now, handle numeric strings. Resolution of epoch/timestamp/package
      // happens at runtime in the archive source or pipeline.
      from = /^\d+$/.test(sourceConfig.backfill.from)
        ? BigInt(sourceConfig.backfill.from)
        : 0n; // Will be resolved at runtime
    } else {
      from = 0n;
    }

    sources.push(createArchiveSource({
      archiveUrl: sourceConfig.backfill.archive,
      from,
      grpcUrl: sourceConfig.live?.grpc,
      concurrency: sourceConfig.backfill.concurrency,
      workers: sourceConfig.backfill.workers,
      balanceCoinTypes: config.processors?.balances?.coinTypes === "*"
        ? "*"
        : config.processors?.balances?.coinTypes?.map((coinType: string) => normalizeCoinType(coinType)),
    }));
  }

  if (sources.length === 0) {
    throw new Error("Invalid config: must define at least one source (live or backfill)");
  }

  // --- Processors ---
  const processorConfig = config.processors;

  if (processorConfig?.events) {
    const handlers: Record<string, { type: string; fields?: any; startCheckpoint?: any }> = {};
    for (const [name, handler] of Object.entries(processorConfig.events) as [string, any][]) {
      if (!handler.type) {
        throw new Error(`Invalid config: event processor "${name}" is missing "type"`);
      }
      validateEventTypeAddress(handler.type, `event "${name}"`);
      handlers[name] = {
        type: normalizeEventType(handler.type),
        fields: handler.fields,
        startCheckpoint: handler.startCheckpoint,
      };
    }
    const proc = createEventDecoder({
      handlers,
      grpcUrl: sourceConfig.live?.grpc,
    });
    // Attach reload config so auto-reload can extract it
    (proc as any)._reloadConfig = handlers;
    processors.push(proc);
  }

  if (processorConfig?.balances) {
    const coinTypes = processorConfig.balances.coinTypes;
    if (!coinTypes) {
      throw new Error("Invalid config: balances processor requires 'coinTypes'");
    }
    const normalizedCoinTypes = coinTypes === "*"
      ? "*" as const
      : (coinTypes as string[]).map(normalizeCoinType);
    const balanceConfig = { coinTypes: normalizedCoinTypes };
    const proc = createBalanceTracker(balanceConfig);
    (proc as any)._reloadConfig = balanceConfig;
    processors.push(proc);
  }

  if (processorConfig?.transactions) {
    processors.push(createTransactionTracker());
  }

  // --- Destinations ---
  const destinationConfig = config.destinations ?? {};
  const storageConfig = config.storage ?? destinationConfig;
  const broadcastConfig = config.broadcast ?? destinationConfig;

  // Collect handler table info for destinations that need it
  const handlerTables: Record<string, { tableName: string; fields: any }> = {};
  if (processorConfig?.events) {
    for (const [name, handler] of Object.entries(processorConfig.events) as [string, any][]) {
      const tableName = name.replace(/([A-Z])/g, "_$1").toLowerCase().replace(/^_/, "");
      handlerTables[name] = { tableName, fields: handler.fields ?? {} };
    }
  }

  // Storage destinations (unified SQL backend)
  if (storageConfig.postgres) {
    const url = typeof storageConfig.postgres === "string"
      ? storageConfig.postgres
      : storageConfig.postgres.url;
    storages.push(createSqlStorage({
      url,
      handlers: Object.keys(handlerTables).length > 0 ? handlerTables : undefined,
      balances: !!processorConfig?.balances,
      transactions: !!processorConfig?.transactions,
    }));
  }

  if (storageConfig.sqlite) {
    const sqlitePath = typeof storageConfig.sqlite === "string"
      ? storageConfig.sqlite
      : storageConfig.sqlite.path;
    storages.push(createSqlStorage({
      url: `sqlite:${sqlitePath}`,
      handlers: Object.keys(handlerTables).length > 0 ? handlerTables : undefined,
      balances: !!processorConfig?.balances,
      transactions: !!processorConfig?.transactions,
    }));
  }

  // Broadcast destinations
  if (broadcastConfig.sse) {
    broadcasts.push(createSseBroadcast({
      port: broadcastConfig.sse.port,
      hostname: broadcastConfig.sse.hostname,
    }));
  }

  if (broadcastConfig.nats) {
    broadcasts.push(createNatsBroadcast({
      url: broadcastConfig.nats.url,
      prefix: broadcastConfig.nats.prefix,
    }));
  }

  if (broadcastConfig.stdout) {
    broadcasts.push(createStdoutBroadcast({
      format: broadcastConfig.stdout.format,
    }));
  }

  // Default to stdout if no destinations configured
  if (storages.length === 0 && broadcasts.length === 0) {
    broadcasts.push(createStdoutBroadcast({ format: "formatted" }));
  }

  // --- Pipeline config ---
  const pipelineConfig: PipelineConfig = {};
  if (config.serve) {
    pipelineConfig.serve = {
      port: typeof config.serve === "number" ? config.serve : config.serve.port,
      hostname: config.serve?.hostname,
    };
  }
  if (config.display) pipelineConfig.display = config.display;
  if (config.configUrl) pipelineConfig.configUrl = String(config.configUrl);
  if (config.configAutoReloadMs !== undefined) {
    const val = Number(config.configAutoReloadMs);
    if (!Number.isFinite(val) || val < 100) {
      throw new Error("Invalid config: configAutoReloadMs must be a number >= 100");
    }
    pipelineConfig.configAutoReloadMs = val;
  }
  if (config.network) pipelineConfig.network = String(config.network);
  if (config.gapRepair) {
    pipelineConfig.gapRepair = {
      enabled: config.gapRepair === true || config.gapRepair?.enabled === true,
      intervalMs: typeof config.gapRepair === "object" ? config.gapRepair.intervalMs : undefined,
    };
  }
  if (config.throttle) {
    pipelineConfig.throttle = {
      initialConcurrency: config.throttle.initialConcurrency,
      minConcurrency: config.throttle.minConcurrency,
      maxConcurrency: config.throttle.maxConcurrency,
    };
  }

  // Extract database URL from postgres storage config
  if (storageConfig.postgres) {
    pipelineConfig.database = typeof storageConfig.postgres === "string"
      ? storageConfig.postgres
      : storageConfig.postgres.url;
  }

  // Infer network from gRPC URL if not explicit
  if (!pipelineConfig.network && sourceConfig.live?.grpc) {
    const grpc: string = sourceConfig.live.grpc;
    if (grpc.includes("testnet")) pipelineConfig.network = "testnet";
    else if (grpc.includes("mainnet")) pipelineConfig.network = "mainnet";
  }

  return { sources, processors, storages, broadcasts, pipelineConfig };
}
