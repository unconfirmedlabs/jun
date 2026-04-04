/**
 * Browser-compatible gRPC-Web checkpoint source.
 * Uses @mysten/sui SuiGrpcClient with GrpcWebFetchTransport (Fetch API).
 */
import { SuiGrpcClient } from "@mysten/sui/grpc";
import type { Source, Checkpoint } from "../types.ts";

export interface GrpcWebSourceConfig {
  network: "mainnet" | "testnet" | "devnet";
  baseUrl?: string;
}

export function createGrpcWebSource(config: GrpcWebSourceConfig): Source {
  let stopped = false;
  let client: SuiGrpcClient | null = null;

  return {
    name: "grpc-web",

    async *stream(): AsyncIterable<Checkpoint> {
      client = new SuiGrpcClient({
        network: config.network,
        baseUrl: config.baseUrl,
      });

      const call = client.subscriptionService.subscribeCheckpoints({
        readMask: {
          paths: [
            "transactions.events",
            "transactions.digest",
            "transactions.transaction",
            "transactions.balance_changes",
            "summary.timestamp",
          ],
        },
      });

      for await (const response of call.responses) {
        if (stopped) return;

        const cp = response.checkpoint;
        if (!cp) continue;

        const timestamp = cp.summary?.timestamp;
        const timestampDate = timestamp
          ? new Date(Number(timestamp.seconds) * 1000 + Math.floor((timestamp.nanos || 0) / 1_000_000))
          : new Date(0);

        yield {
          sequenceNumber: response.cursor ?? 0n,
          timestamp: timestampDate,
          transactions: cp.transactions as any,
          source: "live",
        };
      }
    },

    async stop(): Promise<void> {
      stopped = true;
    },
  };
}
