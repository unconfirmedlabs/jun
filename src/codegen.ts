/**
 * Codegen: converts a gRPC DatatypeDescriptor into jun's field DSL.
 *
 * Takes the struct layout fetched from the chain via MovePackageService.GetDatatype
 * and maps each field type to a jun field type string (address, u64, string, etc.).
 * Non-primitive types (custom structs, enums) are flagged for manual handling.
 */
import type { GrpcDatatypeDescriptor, GrpcOpenSignatureBody } from "./grpc.ts";
import { normalizeSuiAddress } from "./normalize.ts";
import { SUI_FRAMEWORK_ADDRESS, MOVE_STDLIB_ADDRESS } from "@mysten/sui/utils";

// ---------------------------------------------------------------------------
// Result type
// ---------------------------------------------------------------------------

export interface CodegenField {
  name: string;
  /** Mapped jun field type, or null if the type is non-primitive */
  type: string | null;
  /** Original type name for display in comments */
  rawType: string;
}

export interface CodegenResult {
  name: string;
  fields: CodegenField[];
  allPrimitive: boolean;
}

// ---------------------------------------------------------------------------
// Address normalization
// ---------------------------------------------------------------------------

/** Normalize the package address in a fully qualified type name */
function normalizeTypeName(typeName: string): string {
  const parts = typeName.split("::");
  if (parts.length >= 1 && parts[0]!.startsWith("0x")) {
    parts[0] = normalizeSuiAddress(parts[0]!);
  }
  return parts.join("::");
}

/** Get just the short name (last segment) from a fully qualified type name */
function shortName(typeName: string): string {
  const parts = typeName.split("::");
  return parts[parts.length - 1];
}

// ---------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------

/**
 * Map a gRPC OpenSignatureBody to a jun field type string.
 * Returns null for non-primitive types that can't be represented in the DSL.
 */
export function mapSignatureToFieldType(sig: GrpcOpenSignatureBody): { type: string | null; rawType: string } {
  const sigType = sig.type;

  switch (sigType) {
    case "ADDRESS":
      return { type: "address", rawType: "address" };
    case "BOOL":
      return { type: "bool", rawType: "bool" };
    case "U8":
      return { type: "u8", rawType: "u8" };
    case "U16":
      return { type: "u16", rawType: "u16" };
    case "U32":
      return { type: "u32", rawType: "u32" };
    case "U64":
      return { type: "u64", rawType: "u64" };
    case "U128":
      return { type: "u128", rawType: "u128" };
    case "U256":
      return { type: "u256", rawType: "u256" };

    case "VECTOR": {
      const inner = sig.typeParameterInstantiation?.[0];
      if (!inner) return { type: null, rawType: "vector<?>" };
      const mapped = mapSignatureToFieldType(inner);
      return {
        type: mapped.type ? `vector<${mapped.type}>` : null,
        rawType: `vector<${mapped.rawType}>`,
      };
    }

    case "DATATYPE": {
      const typeName = normalizeTypeName(sig.typeName || "");

      // Well-known types that map to primitives (SDK-provided canonical addresses)

      if (typeName === `${SUI_FRAMEWORK_ADDRESS}::object::ID` || typeName === `${SUI_FRAMEWORK_ADDRESS}::object::UID`) {
        return { type: "address", rawType: typeName };
      }

      if (typeName === `${MOVE_STDLIB_ADDRESS}::string::String` || typeName === `${MOVE_STDLIB_ADDRESS}::ascii::String`) {
        return { type: "string", rawType: typeName };
      }

      if (typeName === `${MOVE_STDLIB_ADDRESS}::option::Option`) {
        const inner = sig.typeParameterInstantiation?.[0];
        if (!inner) return { type: null, rawType: "Option<?>" };
        const mapped = mapSignatureToFieldType(inner);
        return {
          type: mapped.type ? `option<${mapped.type}>` : null,
          rawType: `Option<${mapped.rawType}>`,
        };
      }

      // Non-primitive datatype
      return { type: null, rawType: shortName(typeName) || typeName };
    }

    default:
      return { type: null, rawType: sigType || "unknown" };
  }
}

// ---------------------------------------------------------------------------
// Main codegen function
// ---------------------------------------------------------------------------

/**
 * Convert a DatatypeDescriptor into a CodegenResult with mapped field types.
 */
export function generateFieldDSL(descriptor: GrpcDatatypeDescriptor): CodegenResult {
  const fields: CodegenField[] = [];

  for (const field of descriptor.fields) {
    const { type, rawType } = mapSignatureToFieldType(field.type);
    fields.push({ name: field.name, type, rawType });
  }

  const allPrimitive = fields.every((f) => f.type !== null);

  return {
    name: descriptor.name,
    fields,
    allPrimitive,
  };
}

// ---------------------------------------------------------------------------
// Formatting
// ---------------------------------------------------------------------------

/**
 * Format a CodegenResult for terminal output.
 */
export function formatCodegenResult(result: CodegenResult): string {
  const lines: string[] = [];

  const nonPrimitiveCount = result.fields.filter((f) => f.type === null).length;

  if (result.allPrimitive) {
    lines.push(`${result.name} — all fields are primitive \u2713`);
  } else {
    lines.push(`${result.name} — ${nonPrimitiveCount} field${nonPrimitiveCount === 1 ? " is" : "s are"} not primitive`);
  }

  lines.push("");
  lines.push("{");

  for (const field of result.fields) {
    if (field.type !== null) {
      lines.push(`  ${field.name}: "${field.type}",`);
    } else {
      lines.push(`  // ${field.name}: ${field.rawType} — not a primitive type. Use a granular event or "json" override.`);
    }
  }

  lines.push("}");

  return lines.join("\n");
}
