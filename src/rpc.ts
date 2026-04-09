/** Shared Sui gRPC client factory. */

import { SuiGrpcClient } from "@mysten/sui/grpc";
import { loadConfig } from "./config.ts";

type SuiNetwork = "mainnet" | "testnet" | "devnet" | "localnet";

function resolveNetwork(): SuiNetwork {
  const env = process.env.JUN_NETWORK;
  if (env === "testnet" || env === "devnet" || env === "localnet") return env;
  if (env === "mainnet") return "mainnet";
  const cfg = loadConfig().activeEnv;
  if (cfg === "testnet" || cfg === "devnet" || cfg === "localnet") return cfg;
  return "mainnet";
}

const clients = new Map<string, SuiGrpcClient>();

export function getSuiClient(baseUrl?: string, network?: SuiNetwork): SuiGrpcClient {
  const url = baseUrl ?? loadConfig().grpcUrl;
  const net = network ?? resolveNetwork();
  const key = `${url}:${net}`;
  const cached = clients.get(key);
  if (cached) return cached;
  const client = new SuiGrpcClient({ baseUrl: url, network: net });
  clients.set(key, client);
  return client;
}
