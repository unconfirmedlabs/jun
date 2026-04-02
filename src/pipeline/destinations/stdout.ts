/**
 * Stdout destination — prints processed data to console.
 *
 * Supports JSONL (one JSON object per line) and formatted (human-readable) modes.
 */
import type { Destination, ProcessedCheckpoint } from "../types.ts";

export interface StdoutDestinationConfig {
  /** Output format: "jsonl" for machine-readable, "formatted" for human-readable */
  format?: "jsonl" | "formatted";
}

export function createStdoutDestination(config?: StdoutDestinationConfig): Destination {
  const format = config?.format ?? "formatted";

  return {
    name: "stdout",

    async initialize(): Promise<void> {
      // Nothing to initialize
    },

    async write(batch: ProcessedCheckpoint[]): Promise<void> {
      for (const processed of batch) {
        if (format === "jsonl") {
          for (const event of processed.events) {
            console.log(JSON.stringify({
              type: "event",
              checkpoint: event.checkpointSeq.toString(),
              handler: event.handlerName,
              txDigest: event.txDigest,
              sender: event.sender,
              timestamp: event.timestamp.toISOString(),
              data: event.data,
            }));
          }
          for (const change of processed.balanceChanges) {
            console.log(JSON.stringify({
              type: "balance_change",
              checkpoint: change.checkpointSeq.toString(),
              address: change.address,
              coinType: change.coinType,
              amount: change.amount,
              timestamp: change.timestamp.toISOString(),
            }));
          }
        }
        // Formatted output is handled by the pipeline's interactive mode
      }
    },

    async shutdown(): Promise<void> {
      // Nothing to shut down
    },
  };
}
