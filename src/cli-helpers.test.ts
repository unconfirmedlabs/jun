import { test, expect, describe, spyOn, afterEach } from "bun:test";
import { formatOwner, printObject, jsonReplacer, formatBalance, cliError } from "./cli-helpers.ts";

// ---------------------------------------------------------------------------
// formatOwner
// ---------------------------------------------------------------------------

describe("formatOwner", () => {
  test("AddressOwner returns the address", () => {
    const addr = "0x" + "ab".repeat(32);
    expect(formatOwner({ $kind: "AddressOwner", AddressOwner: addr })).toBe(addr);
  });

  test("ObjectOwner returns the object address", () => {
    const addr = "0x" + "cd".repeat(32);
    expect(formatOwner({ $kind: "ObjectOwner", ObjectOwner: addr })).toBe(addr);
  });

  test("Shared returns version info", () => {
    expect(formatOwner({ $kind: "Shared", Shared: { initialSharedVersion: "42" } })).toBe("Shared (v42)");
  });

  test("Immutable returns Immutable", () => {
    expect(formatOwner({ $kind: "Immutable", Immutable: true })).toBe("Immutable");
  });

  test("ConsensusAddressOwner returns consensus info", () => {
    const addr = "0x" + "ef".repeat(32);
    expect(formatOwner({ $kind: "ConsensusAddressOwner", ConsensusAddressOwner: { owner: addr } })).toBe(`Consensus (${addr})`);
  });

  test("Unknown $kind returns unknown", () => {
    expect(formatOwner({ $kind: "SomeFutureType" })).toBe("unknown");
  });
});

// ---------------------------------------------------------------------------
// printObject
// ---------------------------------------------------------------------------

describe("printObject", () => {
  let logs: string[];
  let spy: ReturnType<typeof spyOn>;

  afterEach(() => {
    spy?.mockRestore();
  });

  function captureLogs() {
    logs = [];
    spy = spyOn(console, "log").mockImplementation((...args: unknown[]) => {
      logs.push(args.map(String).join(" "));
    });
  }

  test("prints basic object fields", () => {
    captureLogs();
    printObject({
      objectId: "0x123",
      type: "0x2::coin::Coin<0x2::sui::SUI>",
      version: "42",
      digest: "abc123",
      owner: { $kind: "Immutable", Immutable: true },
    });

    const output = logs.join("\n");
    expect(output).toContain("0x123");
    expect(output).toContain("0x2::coin::Coin<0x2::sui::SUI>");
    expect(output).toContain("42");
    expect(output).toContain("abc123");
    expect(output).toContain("Immutable");
  });

  test("prints previousTransaction when present", () => {
    captureLogs();
    printObject({
      objectId: "0x123",
      version: "1",
      digest: "d",
      owner: { $kind: "Immutable", Immutable: true },
      previousTransaction: "txdigest123",
    });
    expect(logs.join("\n")).toContain("txdigest123");
  });

  test("skips previousTransaction when null", () => {
    captureLogs();
    printObject({
      objectId: "0x123",
      version: "1",
      digest: "d",
      owner: { $kind: "Immutable", Immutable: true },
      previousTransaction: null,
    });
    expect(logs.join("\n")).not.toContain("prev tx");
  });

  test("prints BCS size when present", () => {
    captureLogs();
    printObject({
      objectId: "0x123",
      version: "1",
      digest: "d",
      owner: { $kind: "Immutable", Immutable: true },
      objectBcs: new Uint8Array(100),
    });
    expect(logs.join("\n")).toContain("100 bytes");
  });

  test("prints JSON content when present", () => {
    captureLogs();
    printObject({
      objectId: "0x123",
      version: "1",
      digest: "d",
      owner: { $kind: "Immutable", Immutable: true },
      json: { name: "hello", value: 42 },
    });
    const output = logs.join("\n");
    expect(output).toContain("content:");
    expect(output).toContain("name: hello");
    expect(output).toContain("value: 42");
  });

  test("truncates long JSON values at 120 chars", () => {
    captureLogs();
    const longValue = "x".repeat(200);
    printObject({
      objectId: "0x123",
      version: "1",
      digest: "d",
      owner: { $kind: "Immutable", Immutable: true },
      json: { data: longValue },
    });
    const output = logs.join("\n");
    expect(output).toContain("...");
    // The truncated display should be at most 120 chars for the value part
    const dataLine = logs.find(l => l.includes("data:"));
    expect(dataLine).toBeDefined();
  });

  test("skips type when not present", () => {
    captureLogs();
    printObject({
      objectId: "0x123",
      version: "1",
      digest: "d",
      owner: { $kind: "Immutable", Immutable: true },
    });
    expect(logs.join("\n")).not.toContain("type ");
  });
});

// ---------------------------------------------------------------------------
// jsonReplacer
// ---------------------------------------------------------------------------

describe("jsonReplacer", () => {
  test("converts BigInt to string", () => {
    const result = JSON.parse(JSON.stringify({ value: 123456789n }, jsonReplacer));
    expect(result.value).toBe("123456789");
  });

  test("converts Uint8Array to hex", () => {
    const result = JSON.parse(JSON.stringify({ data: new Uint8Array([0xab, 0xcd, 0xef]) }, jsonReplacer));
    expect(result.data).toBe("abcdef");
  });

  test("passes through primitives and handles mixed types", () => {
    const input = {
      epoch: 42n,
      data: new Uint8Array([1, 2, 3]),
      name: "test",
      count: 100,
    };
    const result = JSON.parse(JSON.stringify(input, jsonReplacer));
    expect(result.epoch).toBe("42");
    expect(result.data).toBe("010203");
    expect(result.name).toBe("test");
    expect(result.count).toBe(100);
  });

  test("handles empty Uint8Array", () => {
    const result = JSON.parse(JSON.stringify({ data: new Uint8Array(0) }, jsonReplacer));
    expect(result.data).toBe("");
  });

  test("converts BigInt zero", () => {
    const result = JSON.parse(JSON.stringify({ val: 0n }, jsonReplacer));
    expect(result.val).toBe("0");
  });
});

// ---------------------------------------------------------------------------
// formatBalance
// ---------------------------------------------------------------------------

describe("formatBalance", () => {
  test("formats SUI balance (9 decimals)", () => {
    const result = formatBalance(1_000_000_000n, 9, "SUI");
    expect(result).toContain("SUI");
    expect(result).toContain("1");
  });

  test("formats zero balance", () => {
    const result = formatBalance(0n, 9, "SUI");
    expect(result).toContain("0");
    expect(result).toContain("SUI");
  });

  test("formats fractional balance", () => {
    const result = formatBalance(500_000_000n, 9, "SUI");
    expect(result).toContain("0.5");
    expect(result).toContain("SUI");
  });

  test("formats with 0 decimals", () => {
    const result = formatBalance(12345n, 0, "TOKEN");
    expect(result).toContain("12,345");
    expect(result).toContain("TOKEN");
  });

  test("formats with 6 decimals (USDC-like)", () => {
    const result = formatBalance(1_500_000n, 6, "USDC");
    expect(result).toContain("1.5");
    expect(result).toContain("USDC");
  });

  test("caps fraction digits at 4", () => {
    // With 9 decimals, should show at most 4 fractional digits
    const result = formatBalance(123_456_789n, 9, "SUI");
    // 0.123456789 → should truncate to 4 decimal places
    expect(result).toMatch(/\d+\.\d{4}\s+SUI/);
  });
});

// ---------------------------------------------------------------------------
// cliError
// ---------------------------------------------------------------------------

describe("cliError", () => {
  test("formats Error instances", () => {
    const errors: string[] = [];
    const exitSpy = spyOn(process, "exit").mockImplementation(() => { throw new Error("exit"); });
    const errSpy = spyOn(console, "error").mockImplementation((...args: unknown[]) => {
      errors.push(args.map(String).join(" "));
    });

    try {
      cliError(new Error("something broke"));
    } catch { /* exit throws */ }

    expect(errors[0]).toBe("[jun] error: something broke");
    expect(exitSpy).toHaveBeenCalledWith(1);

    exitSpy.mockRestore();
    errSpy.mockRestore();
  });

  test("formats non-Error values via String()", () => {
    const errors: string[] = [];
    const exitSpy = spyOn(process, "exit").mockImplementation(() => { throw new Error("exit"); });
    const errSpy = spyOn(console, "error").mockImplementation((...args: unknown[]) => {
      errors.push(args.map(String).join(" "));
    });

    try {
      cliError("raw string error");
    } catch { /* exit throws */ }

    expect(errors[0]).toBe("[jun] error: raw string error");

    exitSpy.mockRestore();
    errSpy.mockRestore();
  });
});
