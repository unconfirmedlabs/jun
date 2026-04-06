/**
 * Raw events processor — extracts event envelope data from gRPC checkpoints.
 *
 * For live mode only. Archive mode gets raw events from the Rust binary decoder.
 * No BCS decoding — stores the raw event envelope (packageId, module, type, sender, contents hex).
 */
import type { Processor, Checkpoint, ProcessedCheckpoint } from "../types.ts";
import { emptyProcessed } from "../types.ts";

function bytesToHex(bytes: Uint8Array): string {
  let hex = "0x";
  for (let i = 0; i < bytes.length; i++) {
    hex += bytes[i]!.toString(16).padStart(2, "0");
  }
  return hex;
}

export function createRawEventsProcessor(): Processor {
  return {
    name: "raw-events",

    process(checkpoint: Checkpoint): ProcessedCheckpoint {
      const processed = emptyProcessed(checkpoint);

      for (const tx of checkpoint.transactions) {
        const events = tx.events?.events;
        if (!events) continue;

        for (let i = 0; i < events.length; i++) {
          const event = events[i]!;
          processed.rawEvents.push({
            txDigest: tx.digest,
            eventSeq: i,
            packageId: event.packageId,
            module: event.module,
            eventType: event.eventType,
            sender: event.sender,
            contents: bytesToHex(event.contents.value),
            checkpointSeq: checkpoint.sequenceNumber,
            timestamp: checkpoint.timestamp,
          });
        }
      }

      return processed;
    },
  };
}
