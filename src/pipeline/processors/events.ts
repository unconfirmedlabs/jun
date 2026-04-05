/**
 * Event decoder processor — matches events by type, decodes BCS, returns DecodedEvent[].
 *
 * Handles generic type stripping for matching (e.g., Event<SUI> matches Event handler).
 * BCS schemas compiled once at initialization.
 * Fields auto-resolved from chain if not provided.
 */
import type { Processor, Checkpoint, ProcessedCheckpoint, DecodedEvent } from "../types.ts";
import { emptyProcessed } from "../types.ts";
import type { FieldDefs } from "../../schema.ts";
import { buildBcsSchema, formatRow } from "../../schema.ts";
import { normalizeEventType, stripGenerics, extractTypeParams } from "../../normalize.ts";
import { resolveEventHandlerFields } from "../../resolve-fields.ts";
import { createGrpcClient, type GrpcClient } from "../../grpc.ts";
import type { Logger } from "../../logger.ts";
import { createLogger } from "../../logger.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface EventHandlerConfig {
  /** Fully qualified Move event type */
  type: string;
  /** Optional: BCS field definitions. Auto-resolved from chain if not provided. */
  fields?: FieldDefs;
  /** Optional: start checkpoint for backfill */
  startCheckpoint?: bigint | string;
  /** Number of type parameters on the event struct (auto-resolved from chain). */
  typeParamCount?: number;
}

export interface EventDecoderConfig {
  /** Event handler definitions (handlerName → config) */
  handlers: Record<string, EventHandlerConfig>;
  /** gRPC URL for auto-resolving fields (required if any handler lacks fields) */
  grpcUrl?: string;
}

interface CompiledHandler {
  name: string;
  type: string;
  strippedType: string;
  fields: FieldDefs;
  bcsSchema: any;
  /** Number of type parameters from the struct descriptor (auto-resolved). */
  typeParamCount: number;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

import { parseTimestamp } from "../../timestamp.ts";

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

export function createEventDecoder(config: EventDecoderConfig): Processor {
  const log: Logger = createLogger().child({ component: "processor:event-decoder" });
  let compiled: CompiledHandler[] = [];
  let exactMap = new Map<string, CompiledHandler>();
  let strippedMap = new Map<string, CompiledHandler>();

  function compile(handlers: Record<string, EventHandlerConfig>): void {
    compiled = Object.entries(handlers).map(([name, handler]) => {
      const normalizedType = normalizeEventType(handler.type);
      // BCS schema only includes actual struct fields — not type_param_N columns
      // (phantom type params don't affect BCS encoding). Filter them out.
      const bcsFields: FieldDefs = {};
      for (const [k, v] of Object.entries(handler.fields!)) {
        if (!k.startsWith("type_param_")) bcsFields[k] = v;
      }
      return {
        name,
        type: normalizedType,
        strippedType: stripGenerics(normalizedType),
        fields: handler.fields!,
        bcsSchema: buildBcsSchema(bcsFields),
        typeParamCount: handler.typeParamCount ?? 0,
      };
    });

    exactMap = new Map();
    strippedMap = new Map();
    for (const handler of compiled) {
      exactMap.set(handler.type, handler);
      strippedMap.set(handler.strippedType, handler);
    }
  }

  function matchHandler(eventType: string): CompiledHandler | undefined {
    const normalized = normalizeEventType(eventType);
    return exactMap.get(normalized) ?? strippedMap.get(stripGenerics(normalized));
  }

  return {
    name: "event-decoder",

    async initialize(): Promise<void> {
      // Auto-resolve fields from chain for handlers that don't have them
      const needsResolution = Object.values(config.handlers).some(handler => !handler.fields);
      if (needsResolution) {
        if (!config.grpcUrl) {
          throw new Error("grpcUrl required for auto-resolving event handler fields");
        }
        const grpcClient = createGrpcClient({ url: config.grpcUrl });
        // Convert to EventHandler format for resolveEventHandlerFields
        const handlers: Record<string, { type: string; fields?: FieldDefs; typeParamCount?: number }> = {};
        for (const [name, handler] of Object.entries(config.handlers)) {
          handlers[name] = { type: handler.type, fields: handler.fields };
        }
        await resolveEventHandlerFields(handlers, grpcClient, log);
        // Write resolved fields + type param count back
        for (const [name, handler] of Object.entries(handlers)) {
          config.handlers[name]!.fields = handler.fields;
          config.handlers[name]!.typeParamCount = handler.typeParamCount;
        }
        grpcClient.close();
      }

      compile(config.handlers);
      log.info({ handlers: Object.keys(config.handlers) }, "event decoder compiled");
    },

    process(checkpoint: Checkpoint): ProcessedCheckpoint {
      const events: DecodedEvent[] = [];
      const checkpointSeq = checkpoint.sequenceNumber;
      const timestamp = checkpoint.timestamp;

      for (const transaction of checkpoint.transactions) {
        const transactionEvents = transaction.events?.events;
        if (!transactionEvents?.length) continue;

        for (let eventSeq = 0; eventSeq < transactionEvents.length; eventSeq++) {
          const event = transactionEvents[eventSeq]!;
          const handler = matchHandler(event.eventType);
          if (!handler) continue;

          const value = event.contents?.value;
          if (!value || value.length === 0) continue;

          try {
            const decoded = handler.bcsSchema.parse(
              value instanceof Uint8Array ? value : new Uint8Array(value),
            );
            const formatted = formatRow(decoded, handler.fields);

            // Merge type params into the data map as type_param_0, type_param_1, etc.
            if (handler.typeParamCount > 0) {
              // Prefer pre-parsed typeParams array (archive path). Fall back to
              // parsing from the eventType string (gRPC path where proto only
              // provides the formatted type string).
              const params = event.typeParams?.length
                ? event.typeParams
                : extractTypeParams(event.eventType);
              for (let i = 0; i < handler.typeParamCount; i++) {
                formatted[`type_param_${i}`] = params[i] ?? null;
              }
            }

            events.push({
              handlerName: handler.name,
              checkpointSeq,
              txDigest: transaction.digest,
              eventSeq,
              sender: event.sender,
              timestamp,
              data: formatted,
            });
          } catch (error) {
            log.warn({ handler: handler.name, txDigest: transaction.digest, error }, "BCS decode error");
          }
        }
      }

      return { ...emptyProcessed(checkpoint), events };
    },

    reload(newHandlers: Record<string, EventHandlerConfig>): void {
      config.handlers = newHandlers;
      compile(newHandlers);
      log.info({ handlers: Object.keys(newHandlers) }, "event decoder reloaded");
    },
  };
}
