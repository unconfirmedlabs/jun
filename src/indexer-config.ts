/**
 * jun/indexer-config — Parse YAML config files into IndexerConfig + RunOptions.
 *
 * Supports environment variable substitution ($VAR and ${VAR}) in string values.
 * Runtime options (mode, repairGaps, serve) can be set in the YAML as defaults,
 * with CLI flags taking precedence.
 *
 * @example
 * ```yaml
 * network: testnet
 * grpcUrl: fullnode.testnet.sui.io:443
 * database: $DATABASE_URL
 * startCheckpoint: "package:0x10ad..."
 * mode: all
 * repairGaps: true
 * serve:
 *   port: 8080
 * events:
 *   MyEvent:
 *     type: "0x...::module::EventStruct"
 *     fields:
 *       field1: address
 *       field2: u64
 * ```
 */
import yaml from "js-yaml";
import type { IndexerConfig, RunOptions, RunMode } from "./index.ts";
import type { ViewDef } from "./views.ts";
import type { NATSConfig } from "./broadcast.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface BroadcastConfig {
  sse?: boolean;
  nats?: NATSConfig;
}

export interface BalancesConfig {
  coinTypes: string[] | "*";
}

export interface ParsedIndexerConfig {
  indexer: IndexerConfig;
  run: RunOptions;
  views?: Record<string, ViewDef>;
  balances?: BalancesConfig;
  broadcast?: BroadcastConfig;
}

// ---------------------------------------------------------------------------
// Env var substitution
// ---------------------------------------------------------------------------

/** Substitute $VAR and ${VAR} in a string from process.env */
function substituteEnvVars(value: string): string {
  return value.replace(/\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)/g, (match, braced, bare) => {
    const varName = braced ?? bare;
    const envValue = process.env[varName];
    if (envValue === undefined) {
      throw new Error(`Environment variable ${varName} is not set (referenced in config)`);
    }
    return envValue;
  });
}

/** Walk an object and substitute env vars in all string values */
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

/**
 * Parse a YAML string into IndexerConfig + RunOptions.
 * Env vars ($VAR, ${VAR}) are substituted in all string values.
 */
export function parseIndexerConfig(yamlContent: string): ParsedIndexerConfig {
  const raw = yaml.load(yamlContent) as any;
  if (!raw || typeof raw !== "object") {
    throw new Error("Invalid config: YAML must be a mapping");
  }

  // Substitute env vars
  const config = substituteDeep(raw);

  // Validate required fields
  const required = ["network", "grpcUrl", "database"];
  for (const field of required) {
    if (!config[field]) {
      throw new Error(`Invalid config: missing required field "${field}"`);
    }
  }

  // Events or balances must be configured (at least one)
  const hasEvents = config.events && typeof config.events === "object" && Object.keys(config.events).length > 0;
  const hasBalances = config.balances && config.balances.coinTypes;

  if (!hasEvents && !hasBalances) {
    throw new Error("Invalid config: must configure at least one of 'events' or 'balances'");
  }

  // Validate each event handler (if any)
  for (const [name, handler] of Object.entries(config.events ?? {}) as [string, any][]) {
    if (!handler.type) {
      throw new Error(`Invalid config: event "${name}" is missing "type"`);
    }
    if (!handler.fields || typeof handler.fields !== "object") {
      throw new Error(`Invalid config: event "${name}" is missing "fields"`);
    }
  }

  // Handle startCheckpoint: YAML number → bigint, string → string (for resolution)
  let startCheckpoint: bigint | string | undefined;
  if (config.startCheckpoint !== undefined) {
    if (typeof config.startCheckpoint === "number") {
      startCheckpoint = BigInt(Math.floor(config.startCheckpoint));
    } else {
      startCheckpoint = String(config.startCheckpoint);
    }
  }

  // Build IndexerConfig
  const indexer: IndexerConfig = {
    network: config.network,
    grpcUrl: config.grpcUrl,
    database: config.database,
    events: config.events ?? {},
    startCheckpoint,
    backfillConcurrency: config.backfillConcurrency,
    archiveUrl: config.archiveUrl,
    backfillWorkers: config.backfillWorkers,
  };

  // Validate indexer config fields
  if (indexer.backfillConcurrency !== undefined) {
    const val = Number(indexer.backfillConcurrency);
    if (!Number.isInteger(val) || val < 1) {
      throw new Error("Invalid config: backfillConcurrency must be an integer >= 1");
    }
  }
  if (indexer.backfillWorkers !== undefined) {
    const val = Number(indexer.backfillWorkers);
    if (!Number.isInteger(val) || val < 1) {
      throw new Error("Invalid config: backfillWorkers must be an integer >= 1");
    }
  }
  if (config.network !== "mainnet" && config.network !== "testnet" && !indexer.archiveUrl) {
    throw new Error(`Invalid config: archiveUrl is required for network "${config.network}" (only "mainnet" and "testnet" have default archive URLs)`);
  }

  // Build RunOptions from YAML defaults
  const run: RunOptions = {};

  if (config.mode) {
    const validModes = ["all", "live-only", "backfill-only"];
    if (!validModes.includes(config.mode)) {
      throw new Error(`Invalid config: mode must be one of ${validModes.join(", ")}`);
    }
    run.mode = config.mode as RunMode;
  }

  if (config.repairGaps !== undefined) {
    run.repairGaps = Boolean(config.repairGaps);
  }

  if (config.serve) {
    if (typeof config.serve === "object" && config.serve.port) {
      run.serve = {
        port: Number(config.serve.port),
        hostname: config.serve.hostname,
      };
    } else if (typeof config.serve === "number") {
      run.serve = { port: config.serve };
    }
  }

  // Parse views
  let views: Record<string, ViewDef> | undefined;
  if (config.views && typeof config.views === "object") {
    views = {};
    for (const [name, def] of Object.entries(config.views) as [string, any][]) {
      if (!def.sql) {
        throw new Error(`Invalid config: view "${name}" is missing "sql"`);
      }
      if (!def.refresh) {
        throw new Error(`Invalid config: view "${name}" is missing "refresh"`);
      }
      views[name] = { sql: def.sql, refresh: def.refresh };
    }
  }

  // Parse broadcast config
  let broadcast: BroadcastConfig | undefined;
  if (config.broadcast && typeof config.broadcast === "object") {
    broadcast = {};
    if (config.broadcast.sse !== undefined) {
      broadcast.sse = Boolean(config.broadcast.sse);
    }
    if (config.broadcast.nats) {
      if (!config.broadcast.nats.url) {
        throw new Error("Invalid config: broadcast.nats requires a url");
      }
      broadcast.nats = {
        url: config.broadcast.nats.url,
        prefix: config.broadcast.nats.prefix,
      };
    }
  }

  // Parse balances config
  let balances: BalancesConfig | undefined;
  if (config.balances) {
    const ct = config.balances.coinTypes;
    if (ct === "*") {
      balances = { coinTypes: "*" };
    } else if (Array.isArray(ct) && ct.length > 0) {
      balances = { coinTypes: ct };
    } else if (ct !== undefined) {
      throw new Error('Invalid config: balances.coinTypes must be "*" or a non-empty array of coin type strings');
    }
  }

  // Set balances on indexer config if present
  if (balances) {
    indexer.balances = balances;
  }

  return { indexer, run, views, balances, broadcast };
}

/**
 * Load and parse a YAML config file from disk.
 */
export function loadIndexerConfig(filePath: string): ParsedIndexerConfig {
  const { readFileSync } = require("fs");
  const yamlContent = readFileSync(filePath, "utf-8");
  return parseIndexerConfig(yamlContent);
}

/**
 * Merge CLI flag overrides into RunOptions parsed from YAML.
 * CLI flags take precedence over YAML defaults.
 */
export function mergeRunOptions(
  yamlOpts: RunOptions,
  cliOpts: {
    mode?: string;
    repairGaps?: boolean;
    serve?: string;
    noServe?: boolean;
  },
): RunOptions {
  const merged = { ...yamlOpts };

  if (cliOpts.mode) {
    merged.mode = cliOpts.mode as RunMode;
  }

  if (cliOpts.repairGaps !== undefined) {
    merged.repairGaps = cliOpts.repairGaps;
  }

  if (cliOpts.noServe) {
    merged.serve = undefined;
  } else if (cliOpts.serve) {
    merged.serve = { port: parseInt(cliOpts.serve, 10) };
  }

  return merged;
}
