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
  decodeSubscribeResponseBinary,
  isNativeCheckpointDecoderAvailable,
} from "../../checkpoint-native-decoder.ts";
import { parseBinaryCheckpoint } from "../../binary-parser.ts";
import type { Source, Checkpoint } from "../types.ts";
import type { Logger } from "../../logger.ts";
import { createLogger } from "../../logger.ts";

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
          reconnectDelay = 1000;

          if (isNativeCheckpointDecoderAvailable()) {
            // Binary path: same Rust decoder as archive → guaranteed parity
            const grpcStream = currentClient.subscribeCheckpointsRaw(RAW_CHECKPOINT_READ_MASK_PATHS);
            for await (const responseBytes of grpcStream) {
              if (stopped) break;
              const binary = decodeSubscribeResponseBinary(responseBytes);
              if (!binary || binary.byteLength < 8) {
                // Fallback to JSON path
                const response = decodeSubscribeCheckpointsResponseNative(responseBytes);
                if (response) yield checkpointFromGrpcResponse(response, "live");
                continue;
              }
              // First 8 bytes = cursor (u64 LE), rest = binary checkpoint
              const view = new DataView(binary.buffer, binary.byteOffset, binary.byteLength);
              const _cursor = view.getBigUint64(0, true);
              const checkpointBinary = binary.subarray(8);
              const parsed = parseBinaryCheckpoint(checkpointBinary);
              const checkpoint: Checkpoint = {
                ...parsed.checkpoint,
                source: "live",
              };
              (checkpoint as any)._preProcessed = parsed.processed;
              yield checkpoint;
            }
          } else {
            // JS fallback: parse gRPC JSON response
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
