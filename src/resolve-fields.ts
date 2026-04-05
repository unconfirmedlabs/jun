/**
 * Auto-resolve event handler fields from on-chain Move type descriptors.
 *
 * When a handler doesn't specify fields, jun fetches the struct layout
 * via gRPC and generates the BCS schema automatically.
 */
import type { GrpcClient } from "./grpc.ts";
import type { FieldDefs } from "./schema.ts";
import { generateFieldDSL } from "./codegen.ts";
import type { Logger } from "./logger.ts";

/** Event handler config — defines what to match and decode. */
export interface EventHandler {
  /** Fully-qualified Move event type */
  type: string;
  /** BCS field definitions. Auto-resolved from chain if not provided. */
  fields?: FieldDefs;
  /** Start checkpoint for backfill (optional) */
  startCheckpoint?: bigint | string;
  /** Number of type parameters on the event struct (auto-resolved). */
  typeParamCount?: number;
}

/**
 * Resolve fields for all event handlers that don't have them.
 * Mutates the handlers in place by setting .fields from chain data.
 */
export async function resolveEventHandlerFields(
  handlers: Record<string, EventHandler>,
  grpcClient: GrpcClient,
  log: Logger,
): Promise<void> {
  for (const [name, handler] of Object.entries(handlers)) {
    if (handler.fields && Object.keys(handler.fields).length > 0) {
      continue; // Already has fields
    }

    const parts = handler.type.split("::");
    if (parts.length !== 3) {
      throw new Error(`Invalid event type "${handler.type}" for handler "${name}". Expected format: 0xPKG::module::StructName`);
    }

    const [packageId, moduleName, structName] = parts;
    log.info({ handler: name, type: handler.type }, "fetching field definitions from chain");

    const descriptor = await grpcClient.getDatatype(packageId!, moduleName!, structName!);
    const result = generateFieldDSL(descriptor);

    if (!result.allPrimitive) {
      const nonPrimitive = result.fields.filter(f => !f.type);
      log.warn({
        handler: name,
        nonPrimitiveFields: nonPrimitive.map(f => `${f.name}: ${f.rawType}`),
      }, "some fields are not primitive — they will be skipped");
    }

    // Build FieldDefs from codegen result (skip non-primitive fields)
    const fields: FieldDefs = {};
    for (const field of result.fields) {
      if (field.type) {
        fields[field.name] = field.type as any;
      }
    }

    if (Object.keys(fields).length === 0) {
      throw new Error(`Handler "${name}" (${handler.type}) has no primitive fields. Cannot create BCS schema.`);
    }

    // Detect type parameters from the struct descriptor and add type_param_N columns.
    // Phantom type params don't affect BCS encoding but carry important type identity
    // (e.g., CompositionPublishedEvent<CompositionShare> — the share type is only in typeParams).
    const typeParamCount = descriptor.typeParameters?.length ?? 0;
    handler.typeParamCount = typeParamCount;
    for (let i = 0; i < typeParamCount; i++) {
      fields[`type_param_${i}`] = "string" as any;
    }

    handler.fields = fields;
    log.info({ handler: name, fields: Object.keys(fields), typeParamCount }, "fields resolved");
  }
}
