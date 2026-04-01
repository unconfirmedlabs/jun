import { test, expect, describe } from "bun:test";
import { createProcessor, type EventHandler } from "./processor.ts";
import type {
  GrpcCheckpointResponse,
  GrpcTransaction,
  GrpcEvent,
} from "./grpc.ts";
import { buildBcsSchema } from "./schema.ts";

// ---------------------------------------------------------------------------
// Test helpers — build mock gRPC objects
// ---------------------------------------------------------------------------

function makeEvent(
  type: string,
  sender: string,
  contents: Uint8Array | null,
): GrpcEvent {
  return {
    packageId: type.split("::")[0] ?? "",
    module: type.split("::")[1] ?? "",
    sender,
    eventType: type,
    contents: contents
      ? { name: type, value: contents }
      : (null as any),
  };
}

function makeTx(digest: string, events: GrpcEvent[] | null): GrpcTransaction {
  return {
    digest,
    events: events ? { events } : null,
  };
}

function makeCheckpoint(
  cursor: string,
  transactions: GrpcTransaction[],
  timestamp?: { seconds: string; nanos: number } | null,
): GrpcCheckpointResponse {
  return {
    cursor,
    checkpoint: {
      sequenceNumber: cursor,
      summary: timestamp === undefined
        ? { timestamp: { seconds: "1700000000", nanos: 0 } }
        : timestamp === null
          ? null
          : { timestamp },
      transactions,
    },
  } as GrpcCheckpointResponse;
}

// ---------------------------------------------------------------------------
// Shared test fixtures
// ---------------------------------------------------------------------------

const SIMPLE_FIELDS = {
  edition: "u16" as const,
  amount: "u64" as const,
  name: "string" as const,
};

const SIMPLE_HANDLER: EventHandler = {
  type: "0xpkg::mod::SimpleEvent",
  fields: SIMPLE_FIELDS,
};

function serializeSimple(data: { edition: number; amount: bigint | string; name: string }): Uint8Array {
  const schema = buildBcsSchema(SIMPLE_FIELDS);
  return schema.serialize(data).toBytes();
}

const SENDER_A = "0x" + "aa".repeat(32);
const SENDER_B = "0x" + "bb".repeat(32);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("createProcessor", () => {
  // 1. checkpointSeq populated from response.cursor
  describe("checkpointSeq", () => {
    test("is populated from response.cursor", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const contents = serializeSimple({ edition: 1, amount: 100n, name: "test" });
      const checkpoint = makeCheckpoint("42", [
        makeTx("tx1", [makeEvent("0xpkg::mod::SimpleEvent", SENDER_A, contents)]),
      ]);

      const results = processor.process(checkpoint);
      expect(results).toHaveLength(1);
      expect(results[0]!.checkpointSeq).toBe(42n);
    });

    test("handles large checkpoint sequence numbers", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const contents = serializeSimple({ edition: 1, amount: 100n, name: "test" });
      const checkpoint = makeCheckpoint("999999999999", [
        makeTx("tx1", [makeEvent("0xpkg::mod::SimpleEvent", SENDER_A, contents)]),
      ]);

      const results = processor.process(checkpoint);
      expect(results[0]!.checkpointSeq).toBe(999999999999n);
    });
  });

  // 2. Empty checkpoint — no transactions, null checkpoint
  describe("empty checkpoint", () => {
    test("returns empty array when checkpoint is null", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const response = {
        cursor: "10",
        checkpoint: null as any,
      };

      const results = processor.process(response);
      expect(results).toEqual([]);
    });

    test("returns empty array when transactions is undefined", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const response: GrpcCheckpointResponse = {
        cursor: "10",
        checkpoint: {
          sequenceNumber: "10",
          summary: null,
          transactions: undefined as any,
        },
      };

      const results = processor.process(response);
      expect(results).toEqual([]);
    });

    test("returns empty array when transactions is empty", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const checkpoint = makeCheckpoint("10", []);

      const results = processor.process(checkpoint);
      expect(results).toEqual([]);
    });
  });

  // 3. Transactions with no events — events is null/empty
  describe("transactions with no events", () => {
    test("skips transactions where events is null", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const checkpoint = makeCheckpoint("10", [
        makeTx("tx-no-events", null),
      ]);

      const results = processor.process(checkpoint);
      expect(results).toEqual([]);
    });

    test("skips transactions where events.events is empty array", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const checkpoint = makeCheckpoint("10", [
        makeTx("tx-empty-events", []),
      ]);

      const results = processor.process(checkpoint);
      expect(results).toEqual([]);
    });

    test("processes mix of empty and non-empty transactions", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const contents = serializeSimple({ edition: 5, amount: 200n, name: "hello" });
      const checkpoint = makeCheckpoint("10", [
        makeTx("tx-empty", null),
        makeTx("tx-with-events", [
          makeEvent("0xpkg::mod::SimpleEvent", SENDER_A, contents),
        ]),
        makeTx("tx-also-empty", []),
      ]);

      const results = processor.process(checkpoint);
      expect(results).toHaveLength(1);
      expect(results[0]!.txDigest).toBe("tx-with-events");
    });
  });

  // 4. Events that don't match any handler — unknown event type skipped
  describe("unmatched event types", () => {
    test("skips events with unknown type", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const contents = serializeSimple({ edition: 1, amount: 100n, name: "test" });
      const checkpoint = makeCheckpoint("10", [
        makeTx("tx1", [
          makeEvent("0xother::mod::UnknownEvent", SENDER_A, contents),
        ]),
      ]);

      const results = processor.process(checkpoint);
      expect(results).toEqual([]);
    });

    test("processes matched events and skips unmatched in same transaction", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const contents = serializeSimple({ edition: 3, amount: 500n, name: "match" });
      const checkpoint = makeCheckpoint("10", [
        makeTx("tx1", [
          makeEvent("0xother::mod::UnknownEvent", SENDER_A, new Uint8Array([1, 2, 3])),
          makeEvent("0xpkg::mod::SimpleEvent", SENDER_B, contents),
        ]),
      ]);

      const results = processor.process(checkpoint);
      expect(results).toHaveLength(1);
      expect(results[0]!.sender).toBe(SENDER_B);
      expect(results[0]!.eventSeq).toBe(1);
    });
  });

  // 5. Generic type stripping match
  describe("generic type stripping", () => {
    test("matches event with generics to handler without generics", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const contents = serializeSimple({ edition: 7, amount: 999n, name: "generic" });
      const checkpoint = makeCheckpoint("10", [
        makeTx("tx1", [
          makeEvent("0xpkg::mod::SimpleEvent<0x2::sui::SUI>", SENDER_A, contents),
        ]),
      ]);

      const results = processor.process(checkpoint);
      expect(results).toHaveLength(1);
      expect(results[0]!.handlerName).toBe("simple");
      expect(results[0]!.data.name).toBe("generic");
    });

    test("matches nested generics", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const contents = serializeSimple({ edition: 1, amount: 0n, name: "nested" });
      const checkpoint = makeCheckpoint("10", [
        makeTx("tx1", [
          makeEvent(
            "0xpkg::mod::SimpleEvent<0x2::coin::Coin<0x2::sui::SUI>>",
            SENDER_A,
            contents,
          ),
        ]),
      ]);

      const results = processor.process(checkpoint);
      expect(results).toHaveLength(1);
      expect(results[0]!.handlerName).toBe("simple");
    });
  });

  // 6. Exact match takes priority over stripped
  describe("exact match priority", () => {
    test("exact match wins over stripped generic match", () => {
      const exactFields = { count: "u32" as const };
      const exactHandler: EventHandler = {
        type: "0xpkg::mod::Event<0x2::sui::SUI>",
        fields: exactFields,
      };
      const strippedHandler: EventHandler = {
        type: "0xpkg::mod::Event",
        fields: SIMPLE_FIELDS,
      };

      const processor = createProcessor({
        exact: exactHandler,
        stripped: strippedHandler,
      });

      // Encode with exactFields schema (just u32 count)
      const exactSchema = buildBcsSchema(exactFields);
      const contents = exactSchema.serialize({ count: 42 }).toBytes();

      const checkpoint = makeCheckpoint("10", [
        makeTx("tx1", [
          makeEvent("0xpkg::mod::Event<0x2::sui::SUI>", SENDER_A, contents),
        ]),
      ]);

      const results = processor.process(checkpoint);
      expect(results).toHaveLength(1);
      expect(results[0]!.handlerName).toBe("exact");
      expect(results[0]!.data.count).toBe(42);
    });

    test("falls back to stripped match when no exact match", () => {
      const strippedHandler: EventHandler = {
        type: "0xpkg::mod::Event",
        fields: SIMPLE_FIELDS,
      };

      const processor = createProcessor({ stripped: strippedHandler });
      const contents = serializeSimple({ edition: 1, amount: 50n, name: "fallback" });

      const checkpoint = makeCheckpoint("10", [
        makeTx("tx1", [
          // This type has generics but no exact handler, so stripped match kicks in
          makeEvent("0xpkg::mod::Event<0xfoo::bar::Baz>", SENDER_A, contents),
        ]),
      ]);

      const results = processor.process(checkpoint);
      expect(results).toHaveLength(1);
      expect(results[0]!.handlerName).toBe("stripped");
    });
  });

  // 7. BCS decode error — malformed contents, event is skipped
  describe("BCS decode error", () => {
    test("skips event with malformed BCS data without crashing", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      // Garbage bytes that can't decode as {edition: u16, amount: u64, name: string}
      const badContents = new Uint8Array([0xff, 0x01]);

      const checkpoint = makeCheckpoint("10", [
        makeTx("tx1", [
          makeEvent("0xpkg::mod::SimpleEvent", SENDER_A, badContents),
        ]),
      ]);

      // Should not throw
      const results = processor.process(checkpoint);
      expect(results).toEqual([]);
    });

    test("processes valid events even when one has bad BCS data", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const goodContents = serializeSimple({ edition: 10, amount: 777n, name: "good" });
      const badContents = new Uint8Array([0xde, 0xad, 0xbe, 0xef]);

      const checkpoint = makeCheckpoint("10", [
        makeTx("tx1", [
          makeEvent("0xpkg::mod::SimpleEvent", SENDER_A, badContents),
          makeEvent("0xpkg::mod::SimpleEvent", SENDER_B, goodContents),
        ]),
      ]);

      const results = processor.process(checkpoint);
      expect(results).toHaveLength(1);
      expect(results[0]!.sender).toBe(SENDER_B);
      expect(results[0]!.eventSeq).toBe(1);
    });

    test("logs error to console on BCS decode failure", () => {
      const errors: any[] = [];
      const origError = console.error;
      console.error = (...args: any[]) => errors.push(args);

      try {
        const processor = createProcessor({ simple: SIMPLE_HANDLER });
        const badContents = new Uint8Array([0xff]);

        const checkpoint = makeCheckpoint("10", [
          makeTx("txBAD", [
            makeEvent("0xpkg::mod::SimpleEvent", SENDER_A, badContents),
          ]),
        ]);

        processor.process(checkpoint);
        expect(errors.length).toBeGreaterThan(0);
        expect(errors[0]![0]).toContain("[jun] BCS decode error");
        expect(errors[0]![0]).toContain("simple");
        expect(errors[0]![0]).toContain("txBAD");
      } finally {
        console.error = origError;
      }
    });
  });

  // 8. Null/missing event contents
  describe("null or missing event contents", () => {
    test("skips event when contents is null", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const checkpoint = makeCheckpoint("10", [
        makeTx("tx1", [
          makeEvent("0xpkg::mod::SimpleEvent", SENDER_A, null),
        ]),
      ]);

      const results = processor.process(checkpoint);
      expect(results).toEqual([]);
    });

    test("skips event when contents.value is empty Uint8Array", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const checkpoint = makeCheckpoint("10", [
        makeTx("tx1", [
          makeEvent("0xpkg::mod::SimpleEvent", SENDER_A, new Uint8Array(0)),
        ]),
      ]);

      const results = processor.process(checkpoint);
      expect(results).toEqual([]);
    });
  });

  // 9. Missing timestamp — fallback to Date(0)
  describe("missing timestamp", () => {
    test("falls back to Date(0) when summary is null", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const contents = serializeSimple({ edition: 1, amount: 1n, name: "t" });
      const checkpoint = makeCheckpoint(
        "10",
        [makeTx("tx1", [makeEvent("0xpkg::mod::SimpleEvent", SENDER_A, contents)])],
        null, // null summary
      );

      const results = processor.process(checkpoint);
      expect(results).toHaveLength(1);
      expect(results[0]!.timestamp).toEqual(new Date(0));
    });

    test("parses valid timestamp correctly", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const contents = serializeSimple({ edition: 1, amount: 1n, name: "t" });
      // 2023-11-14T22:13:20.000Z = 1700000000 seconds
      const checkpoint = makeCheckpoint(
        "10",
        [makeTx("tx1", [makeEvent("0xpkg::mod::SimpleEvent", SENDER_A, contents)])],
        { seconds: "1700000000", nanos: 500_000_000 },
      );

      const results = processor.process(checkpoint);
      expect(results).toHaveLength(1);
      const ts = results[0]!.timestamp;
      expect(ts.getTime()).toBe(1700000000 * 1000 + 500);
    });
  });

  // 10. Empty handlers — processor with no handlers
  describe("empty handlers", () => {
    test("returns empty array even with events present", () => {
      const processor = createProcessor({});
      const contents = serializeSimple({ edition: 1, amount: 1n, name: "t" });
      const checkpoint = makeCheckpoint("10", [
        makeTx("tx1", [
          makeEvent("0xpkg::mod::SimpleEvent", SENDER_A, contents),
          makeEvent("0xother::mod::Other", SENDER_B, contents),
        ]),
      ]);

      const results = processor.process(checkpoint);
      expect(results).toEqual([]);
    });
  });

  // 11. Multiple events in single transaction — eventSeq increments
  describe("multiple events in single transaction", () => {
    test("eventSeq increments 0, 1, 2", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const c0 = serializeSimple({ edition: 0, amount: 10n, name: "first" });
      const c1 = serializeSimple({ edition: 1, amount: 20n, name: "second" });
      const c2 = serializeSimple({ edition: 2, amount: 30n, name: "third" });

      const checkpoint = makeCheckpoint("10", [
        makeTx("tx1", [
          makeEvent("0xpkg::mod::SimpleEvent", SENDER_A, c0),
          makeEvent("0xpkg::mod::SimpleEvent", SENDER_A, c1),
          makeEvent("0xpkg::mod::SimpleEvent", SENDER_B, c2),
        ]),
      ]);

      const results = processor.process(checkpoint);
      expect(results).toHaveLength(3);
      expect(results[0]!.eventSeq).toBe(0);
      expect(results[1]!.eventSeq).toBe(1);
      expect(results[2]!.eventSeq).toBe(2);
    });

    test("eventSeq reflects original position even when some events are skipped", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const c0 = serializeSimple({ edition: 0, amount: 10n, name: "first" });
      const c2 = serializeSimple({ edition: 2, amount: 30n, name: "third" });

      const checkpoint = makeCheckpoint("10", [
        makeTx("tx1", [
          makeEvent("0xpkg::mod::SimpleEvent", SENDER_A, c0),         // seq 0 — matched
          makeEvent("0xother::unknown::Ev", SENDER_A, new Uint8Array([1])), // seq 1 — skipped
          makeEvent("0xpkg::mod::SimpleEvent", SENDER_B, c2),         // seq 2 — matched
        ]),
      ]);

      const results = processor.process(checkpoint);
      expect(results).toHaveLength(2);
      expect(results[0]!.eventSeq).toBe(0);
      expect(results[1]!.eventSeq).toBe(2);
    });
  });

  // 12. Multiple transactions — correct tx_digest per event
  describe("multiple transactions", () => {
    test("events have correct tx_digest from their respective transactions", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const c1 = serializeSimple({ edition: 1, amount: 100n, name: "tx-a" });
      const c2 = serializeSimple({ edition: 2, amount: 200n, name: "tx-b" });
      const c3 = serializeSimple({ edition: 3, amount: 300n, name: "tx-c" });

      const checkpoint = makeCheckpoint("50", [
        makeTx("digest-AAA", [
          makeEvent("0xpkg::mod::SimpleEvent", SENDER_A, c1),
        ]),
        makeTx("digest-BBB", [
          makeEvent("0xpkg::mod::SimpleEvent", SENDER_B, c2),
          makeEvent("0xpkg::mod::SimpleEvent", SENDER_B, c3),
        ]),
      ]);

      const results = processor.process(checkpoint);
      expect(results).toHaveLength(3);

      expect(results[0]!.txDigest).toBe("digest-AAA");
      expect(results[0]!.eventSeq).toBe(0);

      expect(results[1]!.txDigest).toBe("digest-BBB");
      expect(results[1]!.eventSeq).toBe(0);

      expect(results[2]!.txDigest).toBe("digest-BBB");
      expect(results[2]!.eventSeq).toBe(1);
    });

    test("eventSeq resets per transaction", () => {
      const processor = createProcessor({ simple: SIMPLE_HANDLER });
      const c = serializeSimple({ edition: 1, amount: 1n, name: "x" });

      const checkpoint = makeCheckpoint("10", [
        makeTx("tx-1", [
          makeEvent("0xpkg::mod::SimpleEvent", SENDER_A, c),
          makeEvent("0xpkg::mod::SimpleEvent", SENDER_A, c),
        ]),
        makeTx("tx-2", [
          makeEvent("0xpkg::mod::SimpleEvent", SENDER_A, c),
        ]),
      ]);

      const results = processor.process(checkpoint);
      expect(results).toHaveLength(3);

      // First tx: seq 0, 1
      expect(results[0]!.txDigest).toBe("tx-1");
      expect(results[0]!.eventSeq).toBe(0);
      expect(results[1]!.txDigest).toBe("tx-1");
      expect(results[1]!.eventSeq).toBe(1);

      // Second tx: seq resets to 0
      expect(results[2]!.txDigest).toBe("tx-2");
      expect(results[2]!.eventSeq).toBe(0);
    });
  });

  // Integration: decoded data is correctly formatted
  describe("BCS decode + format integration", () => {
    test("decodes and formats all field types correctly", () => {
      const fields = {
        owner: "address" as const,
        edition: "u16" as const,
        amount: "u64" as const,
        active: "bool" as const,
        name: "string" as const,
      };
      const handler: EventHandler = {
        type: "0xpkg::mod::RichEvent",
        fields,
      };
      const processor = createProcessor({ rich: handler });

      const schema = buildBcsSchema(fields);
      const addr = "0x" + "cd".repeat(32);
      const contents = schema
        .serialize({
          owner: addr,
          edition: 42,
          amount: 1_000_000n,
          active: true,
          name: "hello-world",
        })
        .toBytes();

      const checkpoint = makeCheckpoint("100", [
        makeTx("txRich", [
          makeEvent("0xpkg::mod::RichEvent", SENDER_A, contents),
        ]),
      ]);

      const results = processor.process(checkpoint);
      expect(results).toHaveLength(1);

      const data = results[0]!.data;
      expect(data.owner).toBe(addr);
      expect(data.edition).toBe(42);
      expect(data.amount).toBe("1000000"); // u64 formatted as string
      expect(data.active).toBe(true);
      expect(data.name).toBe("hello-world");
    });

    test("multiple handlers process different event types", () => {
      const handlerA: EventHandler = {
        type: "0xpkg::mod::EventA",
        fields: { count: "u32" as const },
      };
      const handlerB: EventHandler = {
        type: "0xpkg::mod::EventB",
        fields: { label: "string" as const },
      };
      const processor = createProcessor({ a: handlerA, b: handlerB });

      const schemaA = buildBcsSchema(handlerA.fields);
      const schemaB = buildBcsSchema(handlerB.fields);
      const contentsA = schemaA.serialize({ count: 99 }).toBytes();
      const contentsB = schemaB.serialize({ label: "tagged" }).toBytes();

      const checkpoint = makeCheckpoint("20", [
        makeTx("tx1", [
          makeEvent("0xpkg::mod::EventA", SENDER_A, contentsA),
          makeEvent("0xpkg::mod::EventB", SENDER_B, contentsB),
        ]),
      ]);

      const results = processor.process(checkpoint);
      expect(results).toHaveLength(2);

      expect(results[0]!.handlerName).toBe("a");
      expect(results[0]!.data.count).toBe(99);

      expect(results[1]!.handlerName).toBe("b");
      expect(results[1]!.data.label).toBe("tagged");
    });
  });
});
