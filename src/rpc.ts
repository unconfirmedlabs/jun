/** Shared Sui gRPC client factory. */

import { SuiGrpcClient } from "@mysten/sui/grpc";

const DEFAULT_URL = "https://fullnode.mainnet.sui.io";

const clients = new Map<string, SuiGrpcClient>();

export function getSuiClient(baseUrl = DEFAULT_URL): SuiGrpcClient {
  if (clients.has(baseUrl)) return clients.get(baseUrl)!;
  const client = new SuiGrpcClient({ baseUrl, network: "mainnet" });
  clients.set(baseUrl, client);
  return client;
}
