/**
 * Jun configuration — ~/.jun/config.yml
 *
 * Manages named environments (mainnet, testnet, custom) with gRPC and archive URLs.
 * Auto-creates config with mainnet + testnet defaults on first use.
 */
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join } from "path";

// ─── Types ───────────────────────────────────────────────────────────────────

export interface EnvConfig {
  grpc_url: string;
  archive_url: string;
}

export interface JunConfig {
  active_env: string;
  cache_max_mb: number;
  envs: Record<string, EnvConfig>;
}

export interface ResolvedConfig {
  activeEnv: string;
  grpcUrl: string;
  archiveUrl: string;
  cacheMaxMb: number;
  allEnvs: Record<string, EnvConfig>;
}

// ─── Defaults ────────────────────────────────────────────────────────────────

const DEFAULT_CONFIG: JunConfig = {
  active_env: "mainnet",
  cache_max_mb: 1000,
  envs: {
    mainnet: {
      grpc_url: "https://fullnode.mainnet.sui.io",
      archive_url: "https://checkpoints.mainnet.sui.io",
    },
    testnet: {
      grpc_url: "https://fullnode.testnet.sui.io",
      archive_url: "https://checkpoints.testnet.sui.io",
    },
  },
};

// ─── Paths ───────────────────────────────────────────────────────────────────

function getConfigDir(): string {
  const home = process.env.HOME || process.env.USERPROFILE;
  if (!home) throw new Error("Cannot determine home directory");
  return join(home, ".jun");
}

function getConfigPath(): string {
  return join(getConfigDir(), "config.yml");
}

export { getConfigPath };

// ─── YAML helpers (flat key: value, no library needed) ───────────────────────

function serializeConfig(config: JunConfig): string {
  const lines: string[] = [`active_env: ${config.active_env}`, `cache_max_mb: ${config.cache_max_mb}`, "", "envs:"];
  for (const [name, env] of Object.entries(config.envs)) {
    lines.push(`  ${name}:`);
    lines.push(`    grpc_url: ${env.grpc_url}`);
    lines.push(`    archive_url: ${env.archive_url}`);
  }
  return lines.join("\n") + "\n";
}

function parseConfig(yaml: string): JunConfig {
  const config: JunConfig = { active_env: "mainnet", cache_max_mb: 1000, envs: {} };
  let currentEnv: string | null = null;

  for (const rawLine of yaml.split("\n")) {
    const line = rawLine.trimEnd();
    if (!line || line.startsWith("#")) continue;

    // Top-level: active_env
    const topMatch = line.match(/^active_env:\s*(.+)/);
    if (topMatch) {
      config.active_env = topMatch[1].trim();
      continue;
    }

    // Top-level: cache_max_mb
    const cacheMatch = line.match(/^cache_max_mb:\s*(\d+)/);
    if (cacheMatch) {
      config.cache_max_mb = parseInt(cacheMatch[1]);
      continue;
    }

    // Env name (2-space indent, ends with colon)
    const envMatch = line.match(/^  (\w[\w-]*):\s*$/);
    if (envMatch) {
      currentEnv = envMatch[1];
      config.envs[currentEnv] = { grpc_url: "", archive_url: "" };
      continue;
    }

    // Env property (4-space indent)
    if (currentEnv) {
      const propMatch = line.match(/^\s{4}(grpc_url|archive_url):\s*(.+)/);
      if (propMatch) {
        const [, key, value] = propMatch;
        (config.envs[currentEnv] as Record<string, string>)[key] = value.trim();
      }
    }
  }

  return config;
}

// ─── Public API ──────────────────────────────────────────────────────────────

function ensureConfig(): JunConfig {
  const configPath = getConfigPath();
  if (!existsSync(configPath)) {
    mkdirSync(getConfigDir(), { recursive: true });
    writeFileSync(configPath, serializeConfig(DEFAULT_CONFIG));
    return { ...DEFAULT_CONFIG };
  }
  return parseConfig(readFileSync(configPath, "utf-8"));
}

function saveConfig(config: JunConfig): void {
  mkdirSync(getConfigDir(), { recursive: true });
  writeFileSync(getConfigPath(), serializeConfig(config));
}

export function loadConfig(): ResolvedConfig {
  const config = ensureConfig();
  const env = config.envs[config.active_env];
  if (!env) {
    throw new Error(`Active environment "${config.active_env}" not found in config`);
  }
  return {
    activeEnv: config.active_env,
    grpcUrl: env.grpc_url,
    archiveUrl: env.archive_url,
    cacheMaxMb: config.cache_max_mb,
    allEnvs: config.envs,
  };
}

export function setActiveEnv(name: string): void {
  const config = ensureConfig();
  if (!config.envs[name]) {
    throw new Error(`Environment "${name}" not found. Available: ${Object.keys(config.envs).join(", ")}`);
  }
  config.active_env = name;
  saveConfig(config);
}

export function addEnv(name: string, grpcUrl: string, archiveUrl: string): void {
  const config = ensureConfig();
  config.envs[name] = { grpc_url: grpcUrl, archive_url: archiveUrl };
  saveConfig(config);
}
