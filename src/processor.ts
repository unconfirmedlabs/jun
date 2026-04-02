/**
 * Event processor: match events from checkpoints, decode BCS, batch by handler.
 *
 * Handles generic type stripping (e.g., `Event<0x1::coin::Coin>` → `Event`)
 * so that user-defined event types with generics match correctly.
 */
import type { GrpcCheckpointResponse } from "./grpc.ts";
import type { FieldDefs } from "./schema.ts";
import { buildBcsSchema, formatRow } from "./schema.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** User-defined event handler configuration */
export interface EventHandler {
  /** Fully-qualified Move event type, e.g. "0x...::module::EventStruct" */
  type: string;
  /** Start checkpoint for backfill (optional, overrides global startCheckpoint) */
  startCheckpoint?: bigint | string;
  /** Field definitions for BCS decoding. Auto-fetched from chain if not provided. */
  fields?: FieldDefs;
}

/** A decoded event ready for output */
export interface DecodedEvent {
  /** Handler name (key from the events config) */
  handlerName: string;
  /** Checkpoint sequence number */
  checkpointSeq: bigint;
  /** Transaction digest */
  txDigest: string;
  /** Event sequence number within the transaction */
  eventSeq: number;
  /** Sender address */
  sender: string;
  /** Checkpoint timestamp as Date */
  timestamp: Date;
  /** Decoded and formatted event data */
  data: Record<string, unknown>;
}

/** Internal: compiled handler with BCS schema */
interface CompiledHandler {
  name: string;
  type: string;
  strippedType: string;
  fields: FieldDefs;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  bcsSchema: any;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Strip generic type parameters from a Move type string.
 * "0x2::coin::Coin<0x2::sui::SUI>" → "0x2::coin::Coin"
 */
function stripGenerics(type: string): string {
  const idx = type.indexOf("<");
  return idx === -1 ? type : type.slice(0, idx);
}

/**
 * Parse a protobuf Timestamp to a JS Date.
 * The timestamp has `seconds` (string) and `nanos` (number).
 */
function parseTimestamp(ts: { seconds: string; nanos: number } | null | undefined): Date {
  if (!ts) return new Date(0);
  const ms = BigInt(ts.seconds) * 1000n + BigInt(Math.floor(ts.nanos / 1_000_000));
  return new Date(Number(ms));
}

// ---------------------------------------------------------------------------
// Processor
// ---------------------------------------------------------------------------

export interface EventProcessor {
  /**
   * Process a checkpoint response and return decoded events grouped by handler.
   */
  process(checkpoint: GrpcCheckpointResponse): DecodedEvent[];
}

/**
 * Create an event processor from handler definitions.
 * Compiles BCS schemas once upfront for performance.
 */
export function createProcessor(handlers: Record<string, EventHandler>): EventProcessor {
  // Compile handlers: build BCS schemas and pre-compute stripped types
  const compiled: CompiledHandler[] = Object.entries(handlers).map(([name, handler]) => ({
    name,
    type: handler.type,
    strippedType: stripGenerics(handler.type),
    fields: handler.fields!,
    bcsSchema: buildBcsSchema(handler.fields!),
  }));

  // Build lookup maps for fast matching
  // Exact match first, then stripped match
  const exactMap = new Map<string, CompiledHandler>();
  const strippedMap = new Map<string, CompiledHandler>();
  for (const handler of compiled) {
    exactMap.set(handler.type, handler);
    strippedMap.set(handler.strippedType, handler);
  }

  function matchHandler(eventType: string): CompiledHandler | undefined {
    // Try exact match first
    const exact = exactMap.get(eventType);
    if (exact) return exact;

    // Try stripping generics from the incoming event type
    const stripped = stripGenerics(eventType);
    return strippedMap.get(stripped);
  }

  return {
    process(response: GrpcCheckpointResponse): DecodedEvent[] {
      const events: DecodedEvent[] = [];
      const checkpoint = response.checkpoint;
      if (!checkpoint?.transactions) return events;

      const checkpointSeq = BigInt(response.cursor);
      const timestamp = parseTimestamp(checkpoint.summary?.timestamp);

      for (const transaction of checkpoint.transactions) {
        const txEvents = transaction.events?.events;
        if (!txEvents?.length) continue;

        for (let eventSeq = 0; eventSeq < txEvents.length; eventSeq++) {
          const event = txEvents[eventSeq]!;
          const handler = matchHandler(event.eventType);
          if (!handler) continue;

          // Decode BCS
          const value = event.contents?.value;
          if (!value || value.length === 0) continue;

          try {
            const decoded = handler.bcsSchema.parse(
              value instanceof Uint8Array ? value : new Uint8Array(value),
            );
            const formatted = formatRow(decoded, handler.fields);

            events.push({
              handlerName: handler.name,
              checkpointSeq,
              txDigest: transaction.digest,
              eventSeq,
              sender: event.sender,
              timestamp,
              data: formatted,
            });
          } catch (err) {
            console.error(
              `[jun] BCS decode error for ${handler.name} in tx ${transaction.digest}:`,
              err instanceof Error ? err.message : err,
            );
          }
        }
      }

      return events;
    },
  };
}
