/**
 * Parse YAML pipeline config into pipeline components.
 *
 * Maps the config structure (sources/processors/storage/broadcast) to
 * actual Source, Processor, and Destination instances.
 *
 * Supports two entry points:
 *   - parsePipelineConfig(yaml)          — for YAML strings (MCP, auto-reload, config files)
 *   - parsePipelineConfigFromObject(obj) — for pre-built config objects (CLI)
 *
 * Both paths go through normalizeConfig() which maps legacy keys to canonical.
 */
import yaml from "js-yaml";
import type { Source, Processor, Storage, Broadcast, PipelineConfig } from "./types.ts";
import { createGrpcLiveSource } from "./sources/grpc.ts";
import { createArchiveSource } from "./sources/archive.ts";
import { createEventDecoder } from "./processors/events.ts";
import { createBalanceTracker } from "./processors/balanceChanges.ts";
import { createTransactionTracker } from "./processors/transactionBlocks.ts";
import { createObjectChangeTracker } from "./processors/objectChanges.ts";
import { createDependencyTracker } from "./processors/dependencies.ts";
import { createInputTracker } from "./processors/transactionInputs.ts";
import { createCommandTracker } from "./processors/commands.ts";
import { createSystemTransactionTracker } from "./processors/systemTransactions.ts";
import { createUnchangedConsensusObjectTracker } from "./processors/unchangedConsensusObjects.ts";
import { createSqlStorage } from "./destinations/sql.ts";
import { createSseBroadcast } from "./destinations/sse.ts";
import { createNatsBroadcast } from "./destinations/nats.ts";
import { createStdoutBroadcast } from "./destinations/stdout.ts";
import { normalizeEventType, normalizeCoinType, validateEventTypeAddress } from "../normalize.ts";
import { createLogger } from "../logger.ts";
import { resolveEventHandlerFields } from "../resolve-fields.ts";
import { createGrpcClient } from "../grpc.ts";

// ---------------------------------------------------------------------------
// Canonical config schema — matches CLI flags 1:1
// ---------------------------------------------------------------------------

export interface CanonicalConfig {
  sources?: {
    grpcUrl?: string;
    archiveUrl?: string;
    epoch?: string | number;
    startCheckpoint?: string | number;
    endCheckpoint?: string | number;
    concurrency?: number;
    workers?: number;
    // Legacy keys (backwards compat)
    live?: { grpcUrl?: string; grpc?: string };
    backfill?: {
      archiveUrl?: string; archive?: string;
      epoch?: string | number;
      startCheckpoint?: string | number; from?: string | number;
      endCheckpoint?: string | number; to?: string | number;
      concurrency?: number; workers?: number;
    };
  };
  processors?: {
    transactionBlocks?: boolean;
    balances?: { coinTypes: string[] | "*" };
    events?: Record<string, { type: string; fields?: any; startCheckpoint?: any; typeParamCount?: number }>;
    checkpoints?: boolean;
    objectChanges?: boolean;
    dependencies?: boolean;
    inputs?: boolean;
    commands?: boolean;
    systemTransactions?: boolean;
    unchangedConsensusObjects?: boolean;
    // Legacy key (backwards compat)
    transactions?: boolean;
  };
  storage?: {
    sqlite?: string;
    postgres?: string;
    deferIndexes?: boolean;
    pgUnlogged?: boolean;
    // Legacy shorthands
    type?: string;
    path?: string;
    url?: string;
  };
  broadcast?: {
    stdout?: boolean | { format?: string };
    sse?: number | { port: number; hostname?: string };
    nats?: string | { url: string; prefix?: string };
  };
  // Legacy alias for storage/broadcast
  destinations?: any;
  network?: string;
  serve?: number | { port: number; hostname?: string };
  display?: any;
  configUrl?: string;
  configAutoReloadMs?: number;
  gapRepair?: boolean | { enabled?: boolean; intervalMs?: number };
  throttle?: {
    initialConcurrency?: number;
    minConcurrency?: number;
    maxConcurrency?: number;
  };
  buffer?: {
    maxBatchSize?: number;
    intervalMs?: number;
    retries?: number;
  };
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ParsedPipelineConfig {
  sources: Source[];
  processors: Processor[];
  storages: Storage[];
  broadcasts: Broadcast[];
  pipelineConfig: PipelineConfig;
  /** Resolved checkpoint range (if backfill with known bounds) */
  resolvedRange?: { start: bigint; end: bigint };
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
// Config normalization — legacy keys → canonical
// ---------------------------------------------------------------------------

/**
 * Normalize a raw config object to canonical keys.
 *
 * Handles:
 *   - sources.live.grpc / sources.backfill.archive / from / to → flat sources.*
 *   - storage.type + path/url shorthands
 *   - broadcast.sse object → port number, broadcast.nats object → url string
 *   - destinations alias → storage
 *   - processors.transactions → processors.transactionBlocks
 */
export function normalizeConfig(raw: any): CanonicalConfig {
  const config = structuredClone(raw);

  // Flatten sources.live + sources.backfill → sources.*
  if (config.sources?.live || config.sources?.backfill) {
    const live = config.sources.live;
    const backfill = config.sources.backfill;

    config.sources.grpcUrl = config.sources.grpcUrl ?? live?.grpcUrl ?? live?.grpc;
    config.sources.archiveUrl = config.sources.archiveUrl ?? backfill?.archiveUrl ?? backfill?.archive;
    config.sources.epoch = config.sources.epoch ?? backfill?.epoch;
    config.sources.startCheckpoint = config.sources.startCheckpoint ?? backfill?.startCheckpoint ?? backfill?.from;
    config.sources.endCheckpoint = config.sources.endCheckpoint ?? backfill?.endCheckpoint ?? backfill?.to;
    config.sources.concurrency = config.sources.concurrency ?? backfill?.concurrency;
    config.sources.workers = config.sources.workers ?? backfill?.workers;

    // Clean up legacy
    delete config.sources.live;
    delete config.sources.backfill;
  }

  // Normalize processors.transactions → processors.transactionBlocks
  if (config.processors?.transactions && !config.processors?.transactionBlocks) {
    config.processors.transactionBlocks = config.processors.transactions;
    delete config.processors.transactions;
  }

  // Normalize storage shorthand (type + path/url)
  if (config.storage?.type === "sqlite" && config.storage?.path) {
    config.storage.sqlite = config.storage.sqlite ?? config.storage.path;
    delete config.storage.type;
    delete config.storage.path;
  }
  if (config.storage?.type === "postgres" && config.storage?.url) {
    config.storage.postgres = config.storage.postgres ?? config.storage.url;
    delete config.storage.type;
    delete config.storage.url;
  }

  // Normalize storage.sqlite as object with path → string
  if (config.storage?.sqlite && typeof config.storage.sqlite === "object") {
    config.storage.sqlite = config.storage.sqlite.path;
  }

  // Normalize storage.postgres as object with url → string
  if (config.storage?.postgres && typeof config.storage.postgres === "object") {
    config.storage.postgres = config.storage.postgres.url;
  }

  // Support config.destinations as alias for storage/broadcast
  if (config.destinations && !config.storage) {
    config.storage = config.destinations;
    delete config.destinations;
  }

  return config;
}

// ---------------------------------------------------------------------------
// Epoch resolution
// ---------------------------------------------------------------------------

const configLog = createLogger().child({ component: "config-parser" });

/**
 * Resolve an epoch number to its checkpoint range via gRPC.
 * Validates the epoch has completed (not the current epoch).
 */
async function resolveEpochCheckpointRange(
  grpcUrl: string,
  epoch: bigint,
): Promise<{ start: bigint; end: bigint }> {
  const { createGrpcClient } = await import("../grpc.ts");
  const client = createGrpcClient({ url: grpcUrl });

  try {
    // Get current epoch from service info
    const serviceInfo = await client.getServiceInfo();
    const currentEpoch = BigInt(serviceInfo.epoch ?? "0");

    if (epoch < 0n) {
      throw new Error(`Invalid epoch: ${epoch}. Epoch must be a non-negative integer.`);
    }
    if (epoch === currentEpoch) {
      throw new Error(
        `Epoch ${epoch} is the current epoch and has not completed yet`,
      );
    }
    if (epoch > currentEpoch) {
      throw new Error(
        `Epoch ${epoch} does not exist (current epoch: ${currentEpoch})`,
      );
    }

    // Get checkpoint range for the requested epoch
    const epochInfo = await client.getEpoch(epoch);

    const first = epochInfo.firstCheckpoint;
    const last = epochInfo.lastCheckpoint;

    if (first == null || last == null) {
      throw new Error(
        `Epoch ${epoch} has not completed yet (missing checkpoint range)`,
      );
    }

    const start = BigInt(first);
    const end = BigInt(last);

    configLog.info(
      { epoch: epoch.toString(), start: start.toString(), end: end.toString() },
      "resolved epoch checkpoint range",
    );

    return { start, end };
  } finally {
    client.close();
  }
}

/**
 * Verify the archive has checkpoints for the given range by HEAD-requesting
 * the first and last checkpoint URLs.
 */
async function verifyArchiveAvailability(
  archiveUrl: string,
  start: bigint,
  end: bigint,
): Promise<void> {
  const firstUrl = `${archiveUrl}/${start}.binpb.zst`;
  const lastUrl = `${archiveUrl}/${end}.binpb.zst`;

  const [firstResp, lastResp] = await Promise.all([
    fetch(firstUrl, { method: "HEAD" }),
    fetch(lastUrl, { method: "HEAD" }),
  ]);

  if (!firstResp.ok) {
    throw new Error(
      `Archive checkpoint ${start} not available at ${firstUrl} (${firstResp.status})`,
    );
  }

  if (!lastResp.ok) {
    throw new Error(
      `Archive checkpoint ${end} not available at ${lastUrl} (${lastResp.status})`,
    );
  }

  configLog.info(
    { start: start.toString(), end: end.toString() },
    "archive availability verified",
  );
}

// ---------------------------------------------------------------------------
// Parser — from object (canonical path, no YAML dependency)
// ---------------------------------------------------------------------------

export async function parsePipelineConfigFromObject(rawConfig: any): Promise<ParsedPipelineConfig> {
  const config = normalizeConfig(rawConfig);
  const sources: Source[] = [];
  const processors: Processor[] = [];
  const storages: Storage[] = [];
  const broadcasts: Broadcast[] = [];
  let resolvedRange: { start: bigint; end: bigint } | undefined;
  const processorConfig = config.processors;

  // --- Sources ---
  const sourceConfig = config.sources;
  if (!sourceConfig) {
    throw new Error("Invalid config: missing 'sources' section");
  }

  // After normalization, all keys are canonical (flat)
  const grpcUrl = sourceConfig.grpcUrl;
  const archiveUrl = sourceConfig.archiveUrl;
  const startCheckpoint = sourceConfig.startCheckpoint;
  const endCheckpoint = sourceConfig.endCheckpoint;

  if (grpcUrl) {
    sources.push(createGrpcLiveSource({ url: grpcUrl }));
  }

  if (archiveUrl) {
    let start: bigint;
    let end: bigint | undefined;

    // Epoch-based archive source: resolve checkpoint range from epoch number
    if (sourceConfig.epoch != null) {
      if (!grpcUrl) {
        throw new Error("Epoch-based archive source requires a gRPC URL (sources.grpcUrl) to resolve the epoch's checkpoint range");
      }
      const resolved = await resolveEpochCheckpointRange(grpcUrl, BigInt(sourceConfig.epoch));
      start = resolved.start;
      end = resolved.end;
      resolvedRange = { start, end };
      await verifyArchiveAvailability(archiveUrl, start, end);
    } else {
      // Manual checkpoint range
      if (typeof startCheckpoint === "number") {
        start = BigInt(Math.floor(startCheckpoint));
      } else if (typeof startCheckpoint === "string" && /^\d+$/.test(startCheckpoint)) {
        start = BigInt(startCheckpoint);
      } else {
        start = 0n;
      }

      if (endCheckpoint != null) {
        end = typeof endCheckpoint === "number"
          ? BigInt(Math.floor(endCheckpoint))
          : BigInt(endCheckpoint);
      }

      if (end != null && start > end) {
        throw new Error(`Invalid checkpoint range: startCheckpoint (${start}) is greater than endCheckpoint (${end})`);
      }

      if (end != null) {
        resolvedRange = { start, end };
        await verifyArchiveAvailability(archiveUrl, start, end);
      }
    }

    sources.push(createArchiveSource({
      archiveUrl,
      from: start,
      to: end,
      grpcUrl,
      concurrency: sourceConfig.concurrency,
      workers: sourceConfig.workers,
      balanceCoinTypes: processorConfig?.balances?.coinTypes === "*"
        ? "*"
        : processorConfig?.balances?.coinTypes?.map((coinType: string) => normalizeCoinType(coinType)),
      unorderedDrain: config.unorderedDrain ?? false,
      enabledProcessors: {
        balances: !!processorConfig?.balances,
        transactions: !!processorConfig?.transactionBlocks,
        objectChanges: !!processorConfig?.objectChanges,
        dependencies: !!processorConfig?.dependencies,
        inputs: !!processorConfig?.inputs,
        commands: !!processorConfig?.commands,
        systemTransactions: !!processorConfig?.systemTransactions,
        unchangedConsensusObjects: !!processorConfig?.unchangedConsensusObjects,
        events: !!processorConfig?.events,
      },
    }));
  }

  if (sources.length === 0) {
    throw new Error("Invalid config: must define at least one source (live or backfill)");
  }

  // --- Processors ---
  const eventConfig = processorConfig?.events;
  if (eventConfig) {
    const handlers: Record<string, { type: string; fields?: any; startCheckpoint?: any; typeParamCount?: number }> = {};
    for (const [name, handler] of Object.entries(eventConfig) as [string, any][]) {
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

    // Eagerly resolve fields from chain so that storage DDL and the event
    // processor both see the same complete FieldDefs (incl. type_param_N).
    const needsResolution = Object.values(handlers).some(h => !h.fields);
    if (needsResolution && grpcUrl) {
      const resolveLog = createLogger().child({ component: "config-parser" });
      const resolveClient = createGrpcClient({ url: grpcUrl });
      await resolveEventHandlerFields(handlers, resolveClient, resolveLog);
      resolveClient.close();
    }

    // Write resolved fields back to eventConfig so handlerTables picks them up
    for (const [name, handler] of Object.entries(handlers)) {
      const entry = eventConfig[name];
      if (entry) {
        entry.fields = handler.fields;
        entry.typeParamCount = handler.typeParamCount;
      }
    }

    const proc = createEventDecoder({
      handlers,
      grpcUrl,
    });
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

  if (processorConfig?.transactionBlocks) {
    processors.push(createTransactionTracker());
  }

  if (processorConfig?.objectChanges) {
    processors.push(createObjectChangeTracker());
  }

  if (processorConfig?.dependencies) {
    processors.push(createDependencyTracker());
  }

  if (processorConfig?.inputs) {
    processors.push(createInputTracker());
  }

  if (processorConfig?.commands) {
    processors.push(createCommandTracker());
  }

  if (processorConfig?.systemTransactions) {
    processors.push(createSystemTransactionTracker());
  }

  if (processorConfig?.unchangedConsensusObjects) {
    processors.push(createUnchangedConsensusObjectTracker());
  }

  // --- Storage ---
  const storageConfig = config.storage ?? {};

  // Collect handler table info for destinations that need it
  const handlerTables: Record<string, { tableName: string; fields: any }> = {};
  if (processorConfig?.events) {
    for (const [name, handler] of Object.entries(processorConfig.events) as [string, any][]) {
      const tableName = name.replace(/([A-Z])/g, "_$1").toLowerCase().replace(/^_/, "");
      handlerTables[name] = { tableName, fields: handler.fields ?? {} };
    }
  }

  const extraTableFlags = {
    balances: !!processorConfig?.balances,
    transactions: !!processorConfig?.transactionBlocks,
    checkpoints: !!processorConfig?.checkpoints,
    objectChanges: !!processorConfig?.objectChanges,
    dependencies: !!processorConfig?.dependencies,
    inputs: !!processorConfig?.inputs,
    commands: !!processorConfig?.commands,
    systemTransactions: !!processorConfig?.systemTransactions,
    unchangedConsensusObjects: !!processorConfig?.unchangedConsensusObjects,
  };

  if (storageConfig.postgres) {
    storages.push(createSqlStorage({
      url: storageConfig.postgres,
      handlers: Object.keys(handlerTables).length > 0 ? handlerTables : undefined,
      ...extraTableFlags,
      deferIndexes: !!storageConfig.deferIndexes,
      pgUnlogged: !!storageConfig.pgUnlogged,
    }));
  }

  if (storageConfig.sqlite) {
    storages.push(createSqlStorage({
      url: `sqlite:${storageConfig.sqlite}`,
      handlers: Object.keys(handlerTables).length > 0 ? handlerTables : undefined,
      ...extraTableFlags,
      deferIndexes: !!storageConfig.deferIndexes,
    }));
  }

  // --- Broadcast ---
  const broadcastConfig = config.broadcast ?? {};

  if (broadcastConfig.sse) {
    const ssePort = typeof broadcastConfig.sse === "number"
      ? broadcastConfig.sse
      : (broadcastConfig.sse as { port: number; hostname?: string }).port;
    const sseHostname = typeof broadcastConfig.sse === "object"
      ? (broadcastConfig.sse as { port: number; hostname?: string }).hostname
      : undefined;
    broadcasts.push(createSseBroadcast({ port: ssePort, hostname: sseHostname }));
  }

  if (broadcastConfig.nats) {
    const natsUrl = typeof broadcastConfig.nats === "string"
      ? broadcastConfig.nats
      : (broadcastConfig.nats as { url: string; prefix?: string }).url;
    const natsPrefix = typeof broadcastConfig.nats === "object"
      ? (broadcastConfig.nats as { url: string; prefix?: string }).prefix
      : undefined;
    broadcasts.push(createNatsBroadcast({ url: natsUrl, prefix: natsPrefix }));
  }

  if (broadcastConfig.stdout) {
    const format = typeof broadcastConfig.stdout === "object"
      ? (broadcastConfig.stdout as { format?: string }).format
      : undefined;
    broadcasts.push(createStdoutBroadcast({ format }));
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
      hostname: typeof config.serve === "object" ? config.serve.hostname : undefined,
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
    pipelineConfig.database = storageConfig.postgres;
  }

  // Infer network from gRPC URL if not explicit
  if (!pipelineConfig.network && grpcUrl) {
    if (grpcUrl.includes("testnet")) pipelineConfig.network = "testnet";
    else if (grpcUrl.includes("mainnet")) pipelineConfig.network = "mainnet";
  }

  return { sources, processors, storages, broadcasts, pipelineConfig, resolvedRange };
}

// ---------------------------------------------------------------------------
// Parser — from YAML string (backwards compat for MCP, auto-reload, tests)
// ---------------------------------------------------------------------------

export async function parsePipelineConfig(yamlContent: string): Promise<ParsedPipelineConfig> {
  const raw = yaml.load(yamlContent) as any;
  if (!raw || typeof raw !== "object") {
    throw new Error("Invalid config: YAML must be a mapping");
  }

  return parsePipelineConfigFromObject(substituteDeep(raw));
}
