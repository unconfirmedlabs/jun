/**
 * Live gRPC checkpoint source.
 *
 * Subscribes to the Sui fullnode gRPC stream and yields checkpoints
 * as they're produced. Reconnects with exponential backoff on errors.
 */
import { checkpointFromGrpcResponse } from "../../checkpoint-response.ts";
import {
  createGrpcClient,
  RAW_CHECKPOINT_READ_MASK_PATHS,
  type GrpcClient,
} from "../../grpc.ts";
import {
  decodeSubscribeCheckpointsResponseNative,
  isNativeCheckpointDecoderAvailable,
} from "../../checkpoint-native-decoder.ts";
import type { Source, Checkpoint } from "../types.ts";
import type { Logger } from "../../logger.ts";
import { createLogger } from "../../logger.ts";

export interface GrpcLiveSourceConfig {
  /** gRPC endpoint URL (host:port) */
  url: string;
  /** Maximum reconnect delay in ms (default: 30000) */
  maxReconnectDelay?: number;
  /** Force JSON path instead of native raw path (needed when ObjectSet is required) */
  forceJsonPath?: boolean;
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
          reconnectDelay = 1000;

          if (isNativeCheckpointDecoderAvailable() && !config.forceJsonPath) {
            const grpcStream = currentClient.subscribeCheckpointsRaw(RAW_CHECKPOINT_READ_MASK_PATHS);
            for await (const responseBytes of grpcStream) {
              if (stopped) break;
              const response = decodeSubscribeCheckpointsResponseNative(responseBytes);
              if (!response) throw new Error("Native checkpoint decoder unavailable");
              yield checkpointFromGrpcResponse(response, "live");
            }
          } else {
            const grpcStream = currentClient.subscribeCheckpoints();
            for await (const response of grpcStream) {
              if (stopped) break;
              yield checkpointFromGrpcResponse(response, "live");
            }
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
