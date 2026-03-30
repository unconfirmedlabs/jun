/** Shared Sui gRPC client factory. */

import { SuiGrpcClient } from "@mysten/sui/grpc";
import { loadConfig } from "./config.ts";

const clients = new Map<string, SuiGrpcClient>();

export function getSuiClient(baseUrl?: string): SuiGrpcClient {
  const url = baseUrl ?? loadConfig().grpcUrl;
  if (clients.has(url)) return clients.get(url)!;
  const client = new SuiGrpcClient({ baseUrl: url, network: "mainnet" });
  clients.set(url, client);
  return client;
}
