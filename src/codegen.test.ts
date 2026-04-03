import { test, expect, describe } from "bun:test";
import { mapSignatureToFieldType, generateFieldDSL, formatCodegenResult } from "./codegen.ts";
import type { GrpcDatatypeDescriptor, GrpcOpenSignatureBody } from "./grpc.ts";

// ---------------------------------------------------------------------------
// Helpers to build mock proto objects
// ---------------------------------------------------------------------------

function sig(type: string, opts?: { typeName?: string; typeParameterInstantiation?: GrpcOpenSignatureBody[] }): GrpcOpenSignatureBody {
  return {
    type,
    typeName: opts?.typeName ?? "",
    typeParameterInstantiation: opts?.typeParameterInstantiation ?? [],
    typeParameter: 0,
  };
}

function field(name: string, position: number, type: GrpcOpenSignatureBody) {
  return { name, position, type };
}

function descriptor(name: string, fields: ReturnType<typeof field>[]): GrpcDatatypeDescriptor {
  return {
    typeName: `0x1234::test::${name}`,
    definingId: "0x1234",
    module: "test",
    name,
    abilities: ["COPY", "DROP"],
    typeParameters: [],
    kind: "STRUCT",
    fields,
    variants: [],
  };
}

// ---------------------------------------------------------------------------
// mapSignatureToFieldType
// ---------------------------------------------------------------------------

describe("mapSignatureToFieldType", () => {
  test("ADDRESS maps to address", () => {
    const result = mapSignatureToFieldType(sig("ADDRESS"));
    expect(result.type).toBe("address");
  });

  test("BOOL maps to bool", () => {
    const result = mapSignatureToFieldType(sig("BOOL"));
    expect(result.type).toBe("bool");
  });

  test("U8 maps to u8", () => {
    expect(mapSignatureToFieldType(sig("U8")).type).toBe("u8");
  });

  test("U16 maps to u16", () => {
    expect(mapSignatureToFieldType(sig("U16")).type).toBe("u16");
  });

  test("U32 maps to u32", () => {
    expect(mapSignatureToFieldType(sig("U32")).type).toBe("u32");
  });

  test("U64 maps to u64", () => {
    expect(mapSignatureToFieldType(sig("U64")).type).toBe("u64");
  });

  test("U128 maps to u128", () => {
    expect(mapSignatureToFieldType(sig("U128")).type).toBe("u128");
  });

  test("U256 maps to u256", () => {
    expect(mapSignatureToFieldType(sig("U256")).type).toBe("u256");
  });

  test("0x2::object::ID maps to address", () => {
    const result = mapSignatureToFieldType(
      sig("DATATYPE", { typeName: "0x0000000000000000000000000000000000000000000000000000000000000002::object::ID" }),
    );
    expect(result.type).toBe("address");
  });

  test("0x2::object::UID maps to address", () => {
    const result = mapSignatureToFieldType(
      sig("DATATYPE", { typeName: "0x0000000000000000000000000000000000000000000000000000000000000002::object::UID" }),
    );
    expect(result.type).toBe("address");
  });

  test("0x1::string::String maps to string", () => {
    const result = mapSignatureToFieldType(
      sig("DATATYPE", { typeName: "0x0000000000000000000000000000000000000000000000000000000000000001::string::String" }),
    );
    expect(result.type).toBe("string");
  });

  test("0x1::ascii::String maps to string", () => {
    const result = mapSignatureToFieldType(
      sig("DATATYPE", { typeName: "0x0000000000000000000000000000000000000000000000000000000000000001::ascii::String" }),
    );
    expect(result.type).toBe("string");
  });

  test("Option<U64> maps to option<u64>", () => {
    const result = mapSignatureToFieldType(
      sig("DATATYPE", {
        typeName: "0x0000000000000000000000000000000000000000000000000000000000000001::option::Option",
        typeParameterInstantiation: [sig("U64")],
      }),
    );
    expect(result.type).toBe("option<u64>");
  });

  test("Option<String> maps to option<string>", () => {
    const result = mapSignatureToFieldType(
      sig("DATATYPE", {
        typeName: "0x0000000000000000000000000000000000000000000000000000000000000001::option::Option",
        typeParameterInstantiation: [
          sig("DATATYPE", { typeName: "0x0000000000000000000000000000000000000000000000000000000000000001::string::String" }),
        ],
      }),
    );
    expect(result.type).toBe("option<string>");
  });

  test("vector<String> maps to vector<string>", () => {
    const result = mapSignatureToFieldType(
      sig("VECTOR", {
        typeParameterInstantiation: [
          sig("DATATYPE", { typeName: "0x0000000000000000000000000000000000000000000000000000000000000001::string::String" }),
        ],
      }),
    );
    expect(result.type).toBe("vector<string>");
  });

  test("vector<U8> maps to vector<u8>", () => {
    const result = mapSignatureToFieldType(
      sig("VECTOR", { typeParameterInstantiation: [sig("U8")] }),
    );
    expect(result.type).toBe("vector<u8>");
  });

  test("non-primitive DATATYPE (Level) returns null with short name", () => {
    const result = mapSignatureToFieldType(
      sig("DATATYPE", { typeName: "0x0000000000000000000000000000000000000000000000000000000000abcd::priority::Level" }),
    );
    expect(result.type).toBeNull();
    expect(result.rawType).toBe("Level");
  });

  test("non-primitive DATATYPE (VecSet) returns null", () => {
    const result = mapSignatureToFieldType(
      sig("DATATYPE", { typeName: "0x0000000000000000000000000000000000000000000000000000000000000002::vec_set::VecSet" }),
    );
    expect(result.type).toBeNull();
    expect(result.rawType).toBe("VecSet");
  });

  test("Option wrapping a non-primitive returns null", () => {
    const result = mapSignatureToFieldType(
      sig("DATATYPE", {
        typeName: "0x0000000000000000000000000000000000000000000000000000000000000001::option::Option",
        typeParameterInstantiation: [
          sig("DATATYPE", { typeName: "0x0000000000000000000000000000000000000000000000000000000000abcd::foo::Bar" }),
        ],
      }),
    );
    expect(result.type).toBeNull();
  });

  test("vector wrapping a non-primitive returns null", () => {
    const result = mapSignatureToFieldType(
      sig("VECTOR", {
        typeParameterInstantiation: [
          sig("DATATYPE", { typeName: "0x0000000000000000000000000000000000000000000000000000000000abcd::foo::Bar" }),
        ],
      }),
    );
    expect(result.type).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// generateFieldDSL
// ---------------------------------------------------------------------------

describe("generateFieldDSL", () => {
  test("simple event with all primitives", () => {
    const desc = descriptor("ItemCreatedEvent", [
      field("item_id", 0, sig("ADDRESS")),
      field("collection_id", 1, sig("ADDRESS")),
      field("edition", 2, sig("U16")),
      field("token_id", 3, sig("ADDRESS")),
      field("token_number", 4, sig("U64")),
      field("quantity", 5, sig("U64")),
      field("created_by", 6, sig("ADDRESS")),
      field("paid_value", 7, sig("U64")),
      field("timestamp_ms", 8, sig("U64")),
    ]);

    const result = generateFieldDSL(desc);

    expect(result.name).toBe("ItemCreatedEvent");
    expect(result.allPrimitive).toBe(true);
    expect(result.fields).toHaveLength(9);
    expect(result.fields[0]).toEqual({ name: "item_id", type: "address", rawType: "address" });
    expect(result.fields[2]).toEqual({ name: "edition", type: "u16", rawType: "u16" });
    expect(result.fields[4]).toEqual({ name: "token_number", type: "u64", rawType: "u64" });
  });

  test("event with Option<U64> field", () => {
    const desc = descriptor("TestEvent", [
      field("value", 0, sig("U64")),
      field("maybe_value", 1, sig("DATATYPE", {
        typeName: "0x0000000000000000000000000000000000000000000000000000000000000001::option::Option",
        typeParameterInstantiation: [sig("U64")],
      })),
    ]);

    const result = generateFieldDSL(desc);

    expect(result.allPrimitive).toBe(true);
    expect(result.fields[1].type).toBe("option<u64>");
  });

  test("event with vector<String> field", () => {
    const desc = descriptor("TestEvent", [
      field("titles", 0, sig("VECTOR", {
        typeParameterInstantiation: [
          sig("DATATYPE", { typeName: "0x0000000000000000000000000000000000000000000000000000000000000001::string::String" }),
        ],
      })),
    ]);

    const result = generateFieldDSL(desc);

    expect(result.allPrimitive).toBe(true);
    expect(result.fields[0].type).toBe("vector<string>");
  });

  test("event with 0x2::object::ID maps to address", () => {
    const desc = descriptor("TestEvent", [
      field("object_id", 0, sig("DATATYPE", {
        typeName: "0x0000000000000000000000000000000000000000000000000000000000000002::object::ID",
      })),
    ]);

    const result = generateFieldDSL(desc);

    expect(result.allPrimitive).toBe(true);
    expect(result.fields[0].type).toBe("address");
  });

  test("event with non-primitive fields", () => {
    const desc = descriptor("OrderPlacedEvent", [
      field("order_id", 0, sig("ADDRESS")),
      field("title", 1, sig("DATATYPE", {
        typeName: "0x0000000000000000000000000000000000000000000000000000000000000001::string::String",
      })),
      field("priority", 2, sig("DATATYPE", {
        typeName: "0x000000000000000000000000000000000000000000000000000000000000abcd::priority::Level",
      })),
      field("is_active", 3, sig("BOOL")),
      field("metadata", 4, sig("DATATYPE", {
        typeName: "0x000000000000000000000000000000000000000000000000000000000000abcd::meta::Metadata",
      })),
    ]);

    const result = generateFieldDSL(desc);

    expect(result.allPrimitive).toBe(false);
    expect(result.fields[0].type).toBe("address");
    expect(result.fields[1].type).toBe("string");
    expect(result.fields[2].type).toBeNull();
    expect(result.fields[2].rawType).toBe("Level");
    expect(result.fields[3].type).toBe("bool");
    expect(result.fields[4].type).toBeNull();
    expect(result.fields[4].rawType).toBe("Metadata");
  });
});

// ---------------------------------------------------------------------------
// formatCodegenResult
// ---------------------------------------------------------------------------

describe("formatCodegenResult", () => {
  test("all-primitive event output", () => {
    const output = formatCodegenResult({
      name: "ItemCreatedEvent",
      allPrimitive: true,
      fields: [
        { name: "item_id", type: "address", rawType: "address" },
        { name: "edition", type: "u16", rawType: "u16" },
        { name: "paid_value", type: "u64", rawType: "u64" },
      ],
    });

    expect(output).toContain("ItemCreatedEvent");
    expect(output).toContain("all fields are primitive");
    expect(output).toContain('item_id: "address"');
    expect(output).toContain('edition: "u16"');
    expect(output).toContain('paid_value: "u64"');
  });

  test("non-primitive event output shows comments", () => {
    const output = formatCodegenResult({
      name: "OrderFilledEvent",
      allPrimitive: false,
      fields: [
        { name: "order_id", type: "address", rawType: "address" },
        { name: "metadata", type: null, rawType: "OrderMeta" },
        { name: "is_complete", type: "bool", rawType: "bool" },
      ],
    });

    expect(output).toContain("1 field is not primitive");
    expect(output).toContain('order_id: "address"');
    expect(output).toContain("// metadata: OrderMeta");
    expect(output).toContain('is_complete: "bool"');
  });

  test("multiple non-primitive fields use plural", () => {
    const output = formatCodegenResult({
      name: "TestEvent",
      allPrimitive: false,
      fields: [
        { name: "a", type: null, rawType: "Foo" },
        { name: "b", type: null, rawType: "Bar" },
      ],
    });

    expect(output).toContain("2 fields are not primitive");
  });
});
