/**
 * jun/events — Event matching and BCS decoding.
 *
 * @example Create a processor and decode events from a checkpoint
 * ```ts
 * import { createProcessor } from "jun/events";
 *
 * const processor = createProcessor({
 *   Transfer: {
 *     type: "0x2::coin::TransferEvent",
 *     fields: { amount: "u64", recipient: "address" },
 *   },
 * });
 *
 * const decoded = processor.process(checkpointResponse);
 * ```
 */

export { createProcessor } from "./processor.ts";
export type {
  EventProcessor,
  EventHandler,
  DecodedEvent,
} from "./processor.ts";
