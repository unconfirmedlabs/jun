/**
 * Live gRPC checkpoint source.
 *
 * Subscribes to the Sui fullnode gRPC stream and yields checkpoints
 * as they're produced. Reconnects with exponential backoff on errors.
 */
import { createGrpcClient, type GrpcClient } from "../../grpc.ts";
import type { Source, Checkpoint } from "../types.ts";
import type { Logger } from "../../logger.ts";
import { createLogger } from "../../logger.ts";
import { parseTimestamp } from "../../timestamp.ts";

export interface GrpcLiveSourceConfig {
  /** gRPC endpoint URL (host:port) */
  url: string;
  /** Maximum reconnect delay in ms (default: 30000) */
  maxReconnectDelay?: number;
}

export function createGrpcLiveSource(config: GrpcLiveSourceConfig): Source {
  const log: Logger = createLogger().child({ component: "source:grpc-live" });
  let stopped = false;
  let currentClient: GrpcClient | null = null;

  return {
    name: "live",

    async *stream(): AsyncIterable<Checkpoint> {
      let reconnectDelay = 1000;
      const maxDelay = config.maxReconnectDelay ?? 30000;

      while (!stopped) {
        try {
          if (currentClient) currentClient.close();
          currentClient = createGrpcClient({ url: config.url });

          log.info({ url: config.url }, "connecting");
          const grpcStream = currentClient.subscribeCheckpoints();
          reconnectDelay = 1000;

          for await (const response of grpcStream) {
            if (stopped) break;

            const summary = response.checkpoint.summary;
            const timestampDate = parseTimestamp(summary?.timestamp);
            const rollingGas = summary?.epochRollingGasCostSummary;

            yield {
              sequenceNumber: BigInt(response.cursor),
              timestamp: timestampDate,
              transactions: response.checkpoint.transactions,
              source: "live",
              epoch: summary?.epoch ? BigInt(summary.epoch) : 0n,
              digest: summary?.digest ?? "",
              previousDigest: summary?.previousDigest ?? null,
              contentDigest: summary?.contentDigest ?? null,
              totalNetworkTransactions: summary?.totalNetworkTransactions
                ? BigInt(summary.totalNetworkTransactions)
                : 0n,
              epochRollingGasCostSummary: {
                computationCost: rollingGas?.computationCost ?? "0",
                storageCost: rollingGas?.storageCost ?? "0",
                storageRebate: rollingGas?.storageRebate ?? "0",
                nonRefundableStorageFee: rollingGas?.nonRefundableStorageFee ?? "0",
              },
            };
          }
        } catch (error) {
          if (stopped) break;
          log.error({ error, reconnectIn: reconnectDelay / 1000 }, "stream error");
          await new Promise(resolve => setTimeout(resolve, reconnectDelay));
          reconnectDelay = Math.min(reconnectDelay * 2, maxDelay);
        }
      }
    },

    async stop(): Promise<void> {
      stopped = true;
      if (currentClient) {
        currentClient.close();
        currentClient = null;
      }
    },
  };
}
