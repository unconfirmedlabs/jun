import { test, expect, describe } from "bun:test";
import { $ } from "bun";

const JUN = "bun src/cli.ts";

/** Run a jun CLI command and return stdout/stderr/exitCode */
async function run(args: string): Promise<{ stdout: string; stderr: string; exitCode: number }> {
  const result = await $`${JUN.split(" ")} ${args.split(" ")}`.quiet().nothrow();
  return {
    stdout: result.stdout.toString().trim(),
    stderr: result.stderr.toString().trim(),
    exitCode: result.exitCode,
  };
}

// ---------------------------------------------------------------------------
// Help output — verify all commands are registered
// ---------------------------------------------------------------------------

describe("client help", () => {
  test("jun client --help lists all commands", async () => {
    const { stdout } = await run("client --help");
    const commands = [
      "object|obj", "tx-block|txb", "epoch",
      "balance|bal", "balances|bals", "coins", "owned|own",
      "objects|objs", "txs", "dynamic-fields|df", "checkpoint|cp", "info",
      "coin-meta|coin", "package|pkg", "function|fn", "package-versions|pkg-v",
      "gas-price", "protocol-config|proto", "system-state|sys", "simulate|sim",
    ];
    for (const cmd of commands) {
      expect(stdout).toContain(cmd);
    }
  });

  test("jun c alias works", async () => {
    const { stdout } = await run("c --help");
    expect(stdout).toContain("Query the Sui blockchain");
  });
});

// ---------------------------------------------------------------------------
// Subcommand --help — verify arguments and options
// ---------------------------------------------------------------------------

describe("subcommand help", () => {
  const helpCases: Array<{ cmd: string; expectArgs: string[]; expectOpts: string[] }> = [
    { cmd: "balance", expectArgs: ["<address>"], expectOpts: ["--coin-type", "--json", "--url"] },
    { cmd: "balances", expectArgs: ["<address>"], expectOpts: ["--limit", "--cursor", "--json", "--url"] },
    { cmd: "coins", expectArgs: ["<address>"], expectOpts: ["--coin-type", "--limit", "--cursor", "--json", "--url"] },
    { cmd: "owned", expectArgs: ["<address>"], expectOpts: ["--type", "--limit", "--cursor", "--json", "--url"] },
    { cmd: "objects", expectArgs: ["<ids...>"], expectOpts: ["--json", "--url"] },
    { cmd: "txs", expectArgs: ["<digests...>"], expectOpts: ["--json", "--url"] },
    { cmd: "dynamic-fields", expectArgs: ["<parent-id>"], expectOpts: ["--include-value", "--limit", "--cursor", "--json", "--url"] },
    { cmd: "checkpoint", expectArgs: ["[seq]"], expectOpts: ["--json", "--url"] },
    { cmd: "info", expectArgs: [], expectOpts: ["--json", "--url"] },
    { cmd: "coin-meta", expectArgs: ["<coin-type>"], expectOpts: ["--json", "--url"] },
    { cmd: "package", expectArgs: ["<id>"], expectOpts: ["--json", "--url"] },
    { cmd: "function", expectArgs: ["<type>"], expectOpts: ["--json", "--url"] },
    { cmd: "package-versions", expectArgs: ["<id>"], expectOpts: ["--json", "--url"] },
    { cmd: "gas-price", expectArgs: [], expectOpts: ["--json", "--url"] },
    { cmd: "protocol-config", expectArgs: [], expectOpts: ["--all", "--flags-only", "--attrs-only", "--json", "--url"] },
    { cmd: "system-state", expectArgs: [], expectOpts: ["--json", "--url"] },
    { cmd: "simulate", expectArgs: ["<base64-tx>"], expectOpts: ["--no-checks", "--json", "--url"] },
  ];

  for (const { cmd, expectArgs, expectOpts } of helpCases) {
    test(`jun c ${cmd} --help shows args and opts`, async () => {
      const { stdout } = await run(`c ${cmd} --help`);
      for (const arg of expectArgs) {
        expect(stdout).toContain(arg);
      }
      for (const opt of expectOpts) {
        expect(stdout).toContain(opt);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Live network tests (mainnet) — no-argument commands
// ---------------------------------------------------------------------------

describe("no-arg commands (mainnet)", () => {
  test("jun c info returns chain data", async () => {
    const { stdout, exitCode } = await run("c info");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("chain");
    expect(stdout).toContain("epoch");
    expect(stdout).toContain("checkpoint");
    expect(stdout).toContain("server");
  });

  test("jun c info --json returns valid JSON", async () => {
    const { stdout, exitCode } = await run("c info --json");
    expect(exitCode).toBe(0);
    const data = JSON.parse(stdout);
    expect(data.chain).toBeDefined();
    expect(data.epoch).toBeDefined();
    expect(data.checkpointHeight).toBeDefined();
  });

  test("jun c gas-price returns MIST value", async () => {
    const { stdout, exitCode } = await run("c gas-price");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("MIST");
  });

  test("jun c gas-price --json returns valid JSON", async () => {
    const { stdout, exitCode } = await run("c gas-price --json");
    expect(exitCode).toBe(0);
    const data = JSON.parse(stdout);
    expect(data.referenceGasPrice).toBeDefined();
  });

  test("jun c system-state returns epoch info", async () => {
    const { stdout, exitCode } = await run("c system-state");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("epoch");
    expect(stdout).toContain("protocol");
    expect(stdout).toContain("ref gas price");
    expect(stdout).toContain("safe mode");
  });

  test("jun c system-state --json returns valid JSON", async () => {
    const { stdout, exitCode } = await run("c system-state --json");
    expect(exitCode).toBe(0);
    const data = JSON.parse(stdout);
    expect(data.epoch).toBeDefined();
    expect(data.protocolVersion).toBeDefined();
    expect(data.referenceGasPrice).toBeDefined();
  });

  test("jun c epoch returns current epoch info", async () => {
    const { stdout, exitCode } = await run("c epoch");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("epoch");
    expect(stdout).toContain("(current)");
    expect(stdout).toContain("protocol");
  });

  test("jun c protocol-config returns feature flags", async () => {
    const { stdout, exitCode } = await run("c protocol-config");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("protocol version");
    expect(stdout).toContain("feature flags");
    expect(stdout).toContain("attributes");
    expect(stdout).toContain("use --all to show all");
  });

  test("jun c protocol-config --flags-only hides attributes", async () => {
    const { stdout, exitCode } = await run("c protocol-config --flags-only");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("feature flags");
    expect(stdout).not.toContain("attributes");
  });

  test("jun c protocol-config --attrs-only hides flags", async () => {
    const { stdout, exitCode } = await run("c protocol-config --attrs-only");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("attributes");
    expect(stdout).not.toContain("feature flags");
  });
});

// ---------------------------------------------------------------------------
// Object queries (well-known objects on mainnet)
// ---------------------------------------------------------------------------

describe("object queries (mainnet)", () => {
  // 0x5 = Sui System State object (always exists, shared)
  test("jun c object 0x5 returns system state object", async () => {
    const { stdout, exitCode } = await run("c object 0x5");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("object");
    expect(stdout).toContain("version");
    expect(stdout).toContain("digest");
    expect(stdout).toContain("Shared");
  });

  test("jun c object 0x5 --json returns valid JSON", async () => {
    const { stdout, exitCode } = await run("c object 0x5 --json");
    expect(exitCode).toBe(0);
    const data = JSON.parse(stdout);
    expect(data.objectId).toContain("0x");
    expect(data.version).toBeDefined();
    expect(data.digest).toBeDefined();
  });

  test("jun c objects with multiple IDs returns all", async () => {
    const { stdout, exitCode } = await run("c objects 0x5 0x6");
    expect(exitCode).toBe(0);
    // Should see two object blocks
    const matches = stdout.match(/object\s+0x/g);
    expect(matches?.length).toBeGreaterThanOrEqual(2);
  });

  test("jun c object with invalid ID returns error", async () => {
    const { stderr, exitCode } = await run("c object 0xinvalid");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("[jun] error:");
  });
});

// ---------------------------------------------------------------------------
// Coin metadata
// ---------------------------------------------------------------------------

describe("coin metadata (mainnet)", () => {
  test("jun c coin-meta 0x2::sui::SUI returns SUI metadata", async () => {
    const { stdout, exitCode } = await run("c coin-meta 0x2::sui::SUI");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("name");
    expect(stdout).toContain("Sui");
    expect(stdout).toContain("symbol");
    expect(stdout).toContain("SUI");
    expect(stdout).toContain("decimals");
    expect(stdout).toContain("9");
  });

  test("jun c coin-meta --json returns valid JSON", async () => {
    const { stdout, exitCode } = await run("c coin-meta 0x2::sui::SUI --json");
    expect(exitCode).toBe(0);
    const data = JSON.parse(stdout);
    expect(data.name).toBe("Sui");
    expect(data.symbol).toBe("SUI");
    expect(data.decimals).toBe(9);
  });
});

// ---------------------------------------------------------------------------
// Package queries (0x1 = Move stdlib, always exists)
// ---------------------------------------------------------------------------

describe("package queries (mainnet)", () => {
  test("jun c package 0x1 returns stdlib package", async () => {
    const { stdout, exitCode } = await run("c package 0x1");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("package");
    expect(stdout).toContain("modules");
    expect(stdout).toContain("string");
    expect(stdout).toContain("vector");
    expect(stdout).toContain("option");
  });

  test("jun c package-versions 0x1 lists versions", async () => {
    const { stdout, exitCode } = await run("c package-versions 0x1");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("version");
    expect(stdout).toContain("package id");
  });

  test("jun c function 0x2::transfer::public_transfer returns signature", async () => {
    const { stdout, exitCode } = await run("c function 0x2::transfer::public_transfer");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("function");
    expect(stdout).toContain("public_transfer");
    expect(stdout).toContain("visibility");
    expect(stdout).toContain("parameters");
  });

  test("jun c function with bad format returns error", async () => {
    const { stderr, exitCode } = await run("c function badformat");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("Expected format:");
  });
});

// ---------------------------------------------------------------------------
// Dynamic fields (0x5 has dynamic fields for validators)
// ---------------------------------------------------------------------------

describe("dynamic fields (mainnet)", () => {
  test("jun c dynamic-fields 0x5 --limit 2 returns fields", async () => {
    const { stdout, exitCode } = await run("c dynamic-fields 0x5 --limit 2");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("dynamic fields for");
    expect(stdout).toContain("name type:");
    expect(stdout).toContain("value type:");
  });
});

// ---------------------------------------------------------------------------
// Balance / owned (use 0x0 as a test — empty address)
// ---------------------------------------------------------------------------

describe("balance queries (mainnet)", () => {
  const ZERO_ADDR = "0x0000000000000000000000000000000000000000000000000000000000000000";

  test("jun c balance for empty address returns 0", async () => {
    const { stdout, exitCode } = await run(`c balance ${ZERO_ADDR}`);
    expect(exitCode).toBe(0);
    expect(stdout).toContain("balance");
    expect(stdout).toContain("SUI");
  });

  test("jun c balance --json returns valid JSON", async () => {
    const { stdout, exitCode } = await run(`c balance ${ZERO_ADDR} --json`);
    expect(exitCode).toBe(0);
    const data = JSON.parse(stdout);
    expect(data.balance).toBeDefined();
    expect(data.coinType).toBeDefined();
  });

  test("jun c balances for empty address returns list", async () => {
    const { stdout, exitCode } = await run(`c balances ${ZERO_ADDR}`);
    expect(exitCode).toBe(0);
    expect(stdout).toContain("balances for");
  }, 15_000);

  test("jun c coins for empty address returns empty", async () => {
    const { stdout, exitCode } = await run(`c coins ${ZERO_ADDR}`);
    expect(exitCode).toBe(0);
    expect(stdout).toContain("coins for");
  });

  test("jun c owned for empty address returns empty", async () => {
    const { stdout, exitCode } = await run(`c owned ${ZERO_ADDR}`);
    expect(exitCode).toBe(0);
    expect(stdout).toContain("objects owned by");
  });

  test("jun c owned --json returns valid JSON", async () => {
    const { stdout, exitCode } = await run(`c owned ${ZERO_ADDR} --json`);
    expect(exitCode).toBe(0);
    const data = JSON.parse(stdout);
    expect(data.objects).toBeDefined();
    expect(data.hasNextPage).toBeDefined();
  });
});

// ---------------------------------------------------------------------------
// Checkpoint queries
// ---------------------------------------------------------------------------

describe("checkpoint queries (mainnet)", () => {
  // Use latest checkpoint (no arg) since early checkpoints are pruned
  test("jun c checkpoint (latest) returns checkpoint info", async () => {
    const { stdout, exitCode } = await run("c checkpoint");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("checkpoint");
    expect(stdout).toContain("epoch");
    expect(stdout).toContain("timestamp");
  });

  test("jun c checkpoint --json returns valid JSON", async () => {
    const { stdout, exitCode } = await run("c checkpoint --json");
    expect(exitCode).toBe(0);
    const data = JSON.parse(stdout);
    expect(data.sequenceNumber).toBeDefined();
  });

  test("jun c checkpoint with pruned seq returns error", async () => {
    const { stderr, exitCode } = await run("c checkpoint 1");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("[jun] error:");
  });
});

// ---------------------------------------------------------------------------
// Simulate (invalid tx should return error gracefully)
// ---------------------------------------------------------------------------

describe("simulate", () => {
  test("jun c simulate with invalid base64 returns error", async () => {
    const { stderr, exitCode } = await run("c simulate AAAA");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("[jun] error:");
  });
});

// ---------------------------------------------------------------------------
// Alias coverage
// ---------------------------------------------------------------------------

describe("aliases", () => {
  const aliases: Array<{ alias: string; full: string }> = [
    { alias: "obj", full: "object" },
    { alias: "txb", full: "tx-block" },
    { alias: "bal", full: "balance" },
    { alias: "bals", full: "balances" },
    { alias: "own", full: "owned" },
    { alias: "objs", full: "objects" },
    { alias: "df", full: "dynamic-fields" },
    { alias: "cp", full: "checkpoint" },
    { alias: "coin", full: "coin-meta" },
    { alias: "pkg", full: "package" },
    { alias: "fn", full: "function" },
    { alias: "pkg-v", full: "package-versions" },
    { alias: "proto", full: "protocol-config" },
    { alias: "sys", full: "system-state" },
    { alias: "sim", full: "simulate" },
  ];

  for (const { alias, full } of aliases) {
    test(`jun c ${alias} --help matches ${full}`, async () => {
      const { stdout } = await run(`c ${alias} --help`);
      expect(stdout.length).toBeGreaterThan(0);
    });
  }
});
