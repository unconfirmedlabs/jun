#!/usr/bin/env bun
/**
 * Jun CLI — Sui data pipeline and chain toolkit.
 */
import { program } from "commander";
import pMap from "p-map";
import pRetry from "p-retry";
import { createGrpcClient, type GrpcCheckpointResponse, type CheckpointEvent } from "./grpc.ts";
import { createArchiveClient } from "./archive.ts";
import { generateFieldDSL, formatCodegenResult } from "./codegen.ts";
import { createSqliteWriter, type SqliteWriter } from "./output/sqlite.ts";
import { loadConfig } from "./config.ts";
import { parseTimestampMs } from "./timestamp.ts";

const cfg = loadConfig();

// ---------------------------------------------------------------------------
// Read mask mapping
// ---------------------------------------------------------------------------

const INCLUDE_MASKS: Record<string, string[]> = {
  events: ["transactions.events", "transactions.digest", "summary.timestamp"],
  effects: ["transactions.effects", "transactions.digest", "summary.timestamp"],
  "balance-changes": ["transactions.balance_changes", "transactions.digest", "summary.timestamp"],
  objects: ["transactions.objects", "objects", "transactions.digest", "summary.timestamp"],
  transactions: ["transactions.transaction", "transactions.signatures", "transactions.digest", "summary.timestamp"],
};

const ALL_MASK = [...new Set(Object.values(INCLUDE_MASKS).flat())];

function buildReadMask(includes: string[]): string[] {
  if (includes.length === 0) return ALL_MASK;
  const paths = new Set<string>();
  for (const inc of includes) {
    const mask = INCLUDE_MASKS[inc];
    if (!mask) {
      console.error(`[jun] unknown --include value: ${inc}`);
      console.error(`[jun] valid values: ${Object.keys(INCLUDE_MASKS).join(", ")}`);
      process.exit(1);
    }
    for (const p of mask) paths.add(p);
  }
  return [...paths];
}

// ---------------------------------------------------------------------------
// Stop conditions
// ---------------------------------------------------------------------------

interface StopConditions {
  untilCheckpoint?: bigint;
  untilTimestamp?: number; // epoch ms
  deadline?: number; // epoch ms
}

function parseStopConditions(opts: { untilCheckpoint?: string; until?: string; duration?: string }): StopConditions {
  const conditions: StopConditions = {};

  if (opts.untilCheckpoint) {
    conditions.untilCheckpoint = BigInt(opts.untilCheckpoint);
  }

  if (opts.until) {
    const ts = new Date(opts.until).getTime();
    if (isNaN(ts)) {
      console.error(`[jun] invalid --until timestamp: ${opts.until}`);
      process.exit(1);
    }
    conditions.untilTimestamp = ts;
  }

  if (opts.duration) {
    const match = opts.duration.match(/^(\d+)(s|m|h)$/);
    if (!match) {
      console.error(`[jun] invalid --duration: ${opts.duration} (use e.g. 30s, 5m, 1h)`);
      process.exit(1);
    }
    const value = parseInt(match[1]);
    const unit = match[2];
    const ms = unit === "h" ? value * 3600_000 : unit === "m" ? value * 60_000 : value * 1000;
    conditions.deadline = Date.now() + ms;
  }

  return conditions;
}

/** Returns true if the stream should stop. All checks are O(1) comparisons. */
function shouldStop(stop: StopConditions, seq: string, checkpointTimestamp: number): boolean {
  if (stop.untilCheckpoint !== undefined && BigInt(seq) >= stop.untilCheckpoint) return true;
  if (stop.untilTimestamp !== undefined && checkpointTimestamp >= stop.untilTimestamp) return true;
  if (stop.deadline !== undefined && Date.now() >= stop.deadline) return true;
  return false;
}

// ---------------------------------------------------------------------------
// System transaction filter
// ---------------------------------------------------------------------------

const SYSTEM_ADDRESS = "0x0000000000000000000000000000000000000000000000000000000000000000";

function isSystemTx(tx: { digest: string; transaction?: { sender?: string }; effects?: any }): boolean {
  return tx.transaction?.sender === SYSTEM_ADDRESS || (tx as any).sender === SYSTEM_ADDRESS;
}

// ---------------------------------------------------------------------------
// Epoch → checkpoint resolution
// ---------------------------------------------------------------------------

async function resolveEpochRange(
  fromEpoch: string,
  toEpoch: string | undefined,
  grpcUrl: string,
): Promise<{ from: bigint; to: bigint }> {
  const { getSuiClient } = await import("./rpc.ts");
  const sui = getSuiClient(grpcUrl);

  const { response: fromResp } = await sui.ledgerService.getEpoch({
    epoch: fromEpoch,
    readMask: { paths: ["first_checkpoint", "last_checkpoint"] },
  });
  const from = fromResp.epoch.firstCheckpoint;
  if (!from) throw new Error(`Could not resolve epoch ${fromEpoch}`);

  if (!toEpoch || toEpoch === fromEpoch) {
    const last = fromResp.epoch.lastCheckpoint;
    if (!last) {
      // Current epoch — get latest checkpoint as upper bound
      const sysResult = await sui.core.getCurrentSystemState();
      const currentEpoch = sysResult.systemState.epoch;
      if (fromEpoch !== currentEpoch) throw new Error(`Epoch ${fromEpoch} has no last checkpoint`);
      // Use a recent checkpoint (can't know the exact latest, so fetch it)
      const { response: latestCp } = await sui.ledgerService.getCheckpoint({
        checkpointId: { oneofKind: "sequenceNumber", sequenceNumber: "0" },
        readMask: { paths: [] },
      });
      // Fallback: just use from + reasonable range
      console.error(`[jun] epoch ${fromEpoch} is current — replaying from checkpoint ${from} (use --to to set end)`);
      return { from: BigInt(from), to: BigInt(from) + 1000n };
    }
    return { from: BigInt(from), to: BigInt(last) };
  }

  const { response: toResp } = await sui.ledgerService.getEpoch({
    epoch: toEpoch,
    readMask: { paths: ["last_checkpoint"] },
  });
  const last = toResp.epoch.lastCheckpoint;
  if (!last) throw new Error(`Epoch ${toEpoch} is not yet complete (no last checkpoint)`);

  return { from: BigInt(from), to: BigInt(last) };
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

function getTimestampMs(summary: GrpcCheckpointResponse["checkpoint"]["summary"]): number {
  if (!summary?.timestamp) return Date.now();
  return parseTimestampMs(summary.timestamp);
}

function formatTimestamp(ms: number): string {
  return new Date(ms).toISOString();
}

function formatEvent(ev: CheckpointEvent, txDigest: string): string {
  const bcsLen = ev.contents?.value ? ev.contents.value.length : 0;
  return [
    `    type:   ${ev.eventType}`,
    `    sender: ${ev.sender}`,
    `    tx:     ${txDigest}`,
    `    bcs:    ${bcsLen} bytes`,
  ].join("\n");
}

function formatBalanceChange(bc: any, txDigest: string): string {
  return [
    `    owner:    ${bc.address}`,
    `    coin:     ${bc.coinType}`,
    `    amount:   ${bc.amount}`,
    `    tx:       ${txDigest}`,
  ].join("\n");
}

function formatEffect(effects: any, txDigest: string): string {
  const status = effects?.status ?? "unknown";
  const gasUsed = effects?.gasUsed;
  const gas = gasUsed ? `${gasUsed.computationCost ?? 0} compute, ${gasUsed.storageCost ?? 0} storage` : "unknown";
  return [
    `    tx:     ${txDigest}`,
    `    status: ${typeof status === "object" ? JSON.stringify(status) : status}`,
    `    gas:    ${gas}`,
  ].join("\n");
}

// ---------------------------------------------------------------------------
// Output writer
// ---------------------------------------------------------------------------

function createWriter(outputPath?: string): { write(line: string): void; close(): void } {
  if (!outputPath) {
    return { write: (line) => console.log(line), close: () => {} };
  }
  const file = Bun.file(outputPath);
  const writer = file.writer();
  return {
    write: (line) => { writer.write(line + "\n"); },
    close: () => { writer.flush(); writer.end(); },
  };
}

// ---------------------------------------------------------------------------
// Stream command
// ---------------------------------------------------------------------------

program
  .name("jun")
  .description("Sui data pipeline and chain toolkit")
  .version("0.1.0");

// ---------------------------------------------------------------------------
// Verify command
// ---------------------------------------------------------------------------

const verify = program
  .command("verify")
  .description("Cryptographically verify transactions and objects against checkpoint signatures");

verify
  .command("tx")
  .alias("transaction")
  .description("Verify a transaction's inclusion in a signed checkpoint")
  .argument("<digest>", "transaction digest (base58)")
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .option("--archive-url <url>", "checkpoint archive URL", cfg.archiveUrl)
  .option("--grpc-url <url>", "gRPC endpoint for committee fetching", cfg.grpcUrl)
  .option("--json", "output raw JSON", false)
  .action(async (digest: string, opts: { url: string; archiveUrl: string; grpcUrl: string; json: boolean }) => {
    const { verifyTransaction } = await import("./verify.ts");

    try {
      const result = await verifyTransaction(digest, {
        rpcUrl: opts.url,
        archiveUrl: opts.archiveUrl,
        grpcUrl: opts.grpcUrl,
      });

      if (opts.json) {
        console.log(JSON.stringify({ verified: true, digest, checkpoint: result.checkpointSeq, epoch: result.epoch, hasEvents: result.hasEvents, steps: result.steps }));
        return;
      }

      console.error("");
      for (const step of result.steps) {
        const detail = step.detail ? ` — ${step.detail}` : "";
        console.error(`  \u2713 ${step.label}${detail}`);
      }
      console.error("");
      console.error(`  Transaction ${digest} is cryptographically verified`);
      console.error(`  via checkpoint ${result.checkpointSeq} (epoch ${result.epoch})`);
      console.error("");
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (opts.json) { console.log(JSON.stringify({ verified: false, digest, error: msg })); process.exit(1); }
      console.error(`\n  \u2717 ${msg}\n`);
      process.exit(1);
    }
  });

verify
  .command("object")
  .alias("obj")
  .description("Verify an object's state against its last modifying transaction")
  .argument("<id>", "object ID (0x...)")
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .option("--archive-url <url>", "checkpoint archive URL", cfg.archiveUrl)
  .option("--grpc-url <url>", "gRPC endpoint for committee fetching", cfg.grpcUrl)
  .option("--json", "output raw JSON", false)
  .action(async (id: string, opts: { url: string; archiveUrl: string; grpcUrl: string; json: boolean }) => {
    const { verifyObject } = await import("./verify.ts");

    try {
      const result = await verifyObject(id, {
        rpcUrl: opts.url,
        archiveUrl: opts.archiveUrl,
        grpcUrl: opts.grpcUrl,
      });

      if (opts.json) {
        console.log(JSON.stringify({ verified: true, objectId: id, objectDigest: result.objectDigest, txDigest: result.txDigest, checkpoint: result.checkpointSeq, epoch: result.epoch, steps: result.steps }));
        return;
      }

      console.error("");
      for (const step of result.steps) {
        const detail = step.detail ? ` — ${step.detail}` : "";
        console.error(`  \u2713 ${step.label}${detail}`);
      }
      console.error("");
      if (result.objectDigest) {
        console.error(`  Object ${id} is cryptographically verified`);
        console.error(`  last modified by tx ${result.txDigest}`);
        console.error(`  via checkpoint ${result.checkpointSeq} (epoch ${result.epoch})`);
      } else {
        console.error(`  Object ${id} was deleted/wrapped`);
        console.error(`  in tx ${result.txDigest} (checkpoint ${result.checkpointSeq})`);
      }
      console.error("");
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (opts.json) { console.log(JSON.stringify({ verified: false, objectId: id, error: msg })); process.exit(1); }
      console.error(`\n  \u2717 ${msg}\n`);
      process.exit(1);
    }
  });

// ---------------------------------------------------------------------------
// Message command
// ---------------------------------------------------------------------------

const message = program
  .command("message")
  .alias("msg")
  .description("Sign and verify Sui personal messages");

message
  .command("sign")
  .description("Sign a personal message with your active Sui keypair")
  .argument("<message>", "message to sign")
  .option("--address <address>", "use a specific address from keystore (default: active)")
  .action(async (msg: string, opts: { address?: string }) => {
    const { signMessage } = await import("./message.ts");

    try {
      const result = await signMessage(msg, opts.address);
      console.log(JSON.stringify(result, null, 2));
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      console.error(`[jun] error: ${detail}`);
      process.exit(1);
    }
  });

message
  .command("verify")
  .description("Verify a Sui personal message signature")
  .argument("<message>", "original message")
  .argument("<signature>", "base64-encoded signature")
  .argument("<address>", "expected signer address (0x...)")
  .option("--json", "output raw JSON", false)
  .action(async (msg: string, signature: string, address: string, opts: { json: boolean }) => {
    const { verifyMessage } = await import("./message.ts");

    try {
      const result = await verifyMessage(msg, signature, address);
      if (opts.json) {
        console.log(JSON.stringify(result));
        return;
      }
      if (result.valid) {
        console.error(`\n  \u2713 Valid ${result.scheme} signature from ${result.address}\n`);
      } else {
        console.error(`\n  \u2717 Invalid signature\n`);
        process.exit(1);
      }
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      console.error(`[jun] error: ${detail}`);
      process.exit(1);
    }
  });

// ---------------------------------------------------------------------------
// Client command
// ---------------------------------------------------------------------------

const client = program
  .command("client")
  .alias("c")
  .description("Query the Sui blockchain");

// -- client helpers (imported from cli-helpers.ts) --
import { formatOwner, printObject, jsonReplacer, formatBalance, cliError } from "./cli-helpers.ts";

client
  .command("object")
  .alias("obj")
  .description("Get object data by ID")
  .argument("<id>", "object ID (0x...)")
  .option("--bcs", "return BCS-serialized object data", false)
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (id: string, opts: { bcs: boolean; json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);

    try {
      const result = await sui.core.getObject({
        objectId: id,
        include: { json: true, previousTransaction: true, objectBcs: opts.bcs },
      });
      const obj = result.object;
      if (!obj) throw new Error(`Object ${id} not found`);

      if (opts.json) {
        console.log(JSON.stringify(obj, null, 2));
        return;
      }

      printObject(obj);
      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

client
  .command("tx-block")
  .alias("txb")
  .description("Get transaction block data by digest")
  .argument("<digest>", "transaction digest (base58)")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (digest: string, opts: { json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);

    try {
      const result = await sui.core.getTransaction({
        digest,
        include: { effects: true, events: true, transaction: true, balanceChanges: true },
      });

      const tx = result.Transaction ?? result.FailedTransaction;
      if (!tx) throw new Error(`Transaction ${digest} not found`);

      if (opts.json) {
        console.log(JSON.stringify(tx, null, 2));
        return;
      }

      const status = tx.status?.success ? "success" : `failure (${tx.status?.error?.message ?? "unknown"})`;
      const gas = tx.effects?.gasUsed;
      const effects = tx.effects;
      const txData = tx.transaction;

      console.log(`\n  digest        ${tx.digest}`);
      console.log(`  status        ${status}`);
      if (txData?.sender) console.log(`  sender        ${txData.sender}`);
      if (tx.epoch) console.log(`  epoch         ${tx.epoch}`);
      if (tx.signatures?.length) console.log(`  signatures    ${tx.signatures.length}`);
      if (gas) {
        const total = BigInt(gas.computationCost) + BigInt(gas.storageCost) - BigInt(gas.storageRebate);
        console.log(`  gas total     ${total} MIST`);
        console.log(`  gas compute   ${gas.computationCost} MIST`);
        console.log(`  gas storage   ${gas.storageCost} MIST`);
        console.log(`  gas rebate    -${gas.storageRebate} MIST`);
        if (gas.nonRefundableStorageFee !== "0") {
          console.log(`  non-refund    ${gas.nonRefundableStorageFee} MIST`);
        }
      }

      // Transaction data
      if (txData?.commands?.length) {
        console.log(`\n  commands (${txData.commands.length}):`);
        for (const cmd of txData.commands) {
          const kind = Object.keys(cmd).find(k => k !== "$kind") ?? cmd.$kind ?? "unknown";
          console.log(`    ${kind}`);
        }
      }

      // Dependencies
      if (effects?.dependencies?.length) {
        console.log(`\n  dependencies (${effects.dependencies.length}):`);
        for (const dep of effects.dependencies.slice(0, 10)) {
          console.log(`    ${dep}`);
        }
        if (effects.dependencies.length > 10) {
          console.log(`    ... and ${effects.dependencies.length - 10} more`);
        }
      }

      // Changed objects
      if (effects?.changedObjects?.length) {
        console.log(`\n  changed objects (${effects.changedObjects.length}):`);
        for (const co of effects.changedObjects) {
          const id = co.objectId ? co.objectId.slice(0, 18) + "..." : "";
          const change = co.inputState && co.outputState
            ? `${co.inputState} → ${co.outputState}`
            : co.outputState ?? "changed";
          console.log(`    ${id}  ${change}`);
        }
      }

      // Events
      if (tx.events?.length) {
        console.log(`\n  events (${tx.events.length}):`);
        for (const ev of tx.events) {
          console.log(`    ${ev.eventType}`);
          if (ev.sender) console.log(`      sender: ${ev.sender}`);
        }
      }

      // Balance changes
      if (tx.balanceChanges?.length) {
        console.log(`\n  balance changes (${tx.balanceChanges.length}):`);
        for (const bc of tx.balanceChanges) {
          console.log(`    ${bc.address}  ${bc.amount} ${bc.coinType}`);
        }
      }

      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

client
  .command("epoch")
  .description("Get epoch info (current if no argument, or a specific epoch)")
  .argument("[epoch]", "epoch number (omit for current)")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (epochArg: string | undefined, opts: { json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);

    try {
      // Determine epoch number — if not provided, fetch current
      let epochNum: string;
      if (epochArg) {
        epochNum = epochArg;
      } else {
        const sysResult = await sui.core.getCurrentSystemState();
        epochNum = sysResult.systemState.epoch;
      }

      // Fetch full epoch data via gRPC
      const { response } = await sui.ledgerService.getEpoch({
        epoch: epochNum,
        readMask: { paths: ["*"] },
      });
      const e = response.epoch;
      const ss = e.systemState;

      if (opts.json) {
        console.log(JSON.stringify(e, jsonReplacer, 2));
        return;
      }

      const isCurrent = !e.lastCheckpoint;
      const startMs = e.start?.seconds ? Number(e.start.seconds) * 1000 : null;
      const endMs = e.end?.seconds ? Number(e.end.seconds) * 1000 : null;
      const startIso = startMs ? new Date(startMs).toISOString() : "unknown";
      const endIso = endMs ? new Date(endMs).toISOString() : null;

      const validators = ss?.validators?.activeValidators ?? [];
      const totalStake = ss?.validators?.totalStake;
      const totalStakeSui = totalStake ? (BigInt(totalStake) / 1_000_000_000n).toLocaleString() : null;
      const subsidy = ss?.stakeSubsidy;
      const storageFund = ss?.storageFund;

      console.log(`\n  epoch               ${e.epoch}${isCurrent ? " (current)" : ""}`);
      console.log(`  protocol            v${ss?.protocolVersion ?? "unknown"}`);
      console.log(`  ref gas price       ${e.referenceGasPrice} MIST`);
      console.log(`  safe mode           ${ss?.safeMode ?? false}`);

      // Time
      console.log(`\n  started             ${startIso}`);
      if (endIso) {
        const durationMs = endMs! - startMs!;
        console.log(`  ended               ${endIso}`);
        console.log(`  duration            ${(durationMs / 3_600_000).toFixed(1)}h`);
      } else if (startMs) {
        const elapsed = Date.now() - startMs;
        const durationMs = Number(ss?.parameters?.epochDurationMs ?? 86_400_000);
        const remaining = Math.max(0, durationMs - elapsed);
        console.log(`  duration            ${(durationMs / 3_600_000).toFixed(1)}h (${(remaining / 3_600_000).toFixed(1)}h remaining)`);
      }

      // Checkpoints
      console.log(`\n  first checkpoint    ${e.firstCheckpoint}`);
      if (e.lastCheckpoint) {
        const range = Number(e.lastCheckpoint) - Number(e.firstCheckpoint) + 1;
        console.log(`  last checkpoint     ${e.lastCheckpoint} (${range.toLocaleString()} total)`);
      }

      // Validators
      console.log(`\n  validators          ${validators.length}`);
      if (totalStakeSui) console.log(`  total stake         ${totalStakeSui} SUI`);

      // Subsidy
      if (subsidy) {
        const balanceSui = (BigInt(subsidy.balance) / 1_000_000_000n).toLocaleString();
        const distSui = (BigInt(subsidy.currentDistributionAmount) / 1_000_000_000n).toLocaleString();
        console.log(`\n  subsidy balance     ${balanceSui} SUI`);
        console.log(`  subsidy/epoch       ${distSui} SUI`);
        console.log(`  subsidy period      ${subsidy.stakeSubsidyPeriodLength} epochs`);
        console.log(`  subsidy counter     ${subsidy.distributionCounter}`);
      }

      // Storage fund
      if (storageFund) {
        const rebates = (BigInt(storageFund.totalObjectStorageRebates) / 1_000_000_000n).toLocaleString();
        const nonRefundable = (BigInt(storageFund.nonRefundableBalance) / 1_000_000_000n).toLocaleString();
        console.log(`\n  storage rebates     ${rebates} SUI`);
        console.log(`  non-refundable      ${nonRefundable} SUI`);
      }

      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

client
  .command("balance")
  .alias("bal")
  .description("Get coin balance for an address")
  .argument("<address>", "Sui address (0x...)")
  .option("--coin-type <type>", "coin type (default: 0x2::sui::SUI)")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (address: string, opts: { coinType?: string; json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);

    try {
      const coinType = opts.coinType ?? "0x2::sui::SUI";
      const result = await sui.core.getBalance({ owner: address, coinType });
      const bal = result.balance;

      if (opts.json) {
        console.log(JSON.stringify(bal, null, 2));
        return;
      }

      // Fetch metadata for decimals/symbol
      let decimals = 9;
      let symbol = coinType.split("::").pop() ?? coinType;
      try {
        const meta = await sui.core.getCoinMetadata({ coinType });
        if (meta.coinMetadata) {
          decimals = meta.coinMetadata.decimals;
          symbol = meta.coinMetadata.symbol;
        }
      } catch { /* metadata unavailable, use defaults */ }

      const raw = BigInt(bal.balance);

      console.log(`\n  address       ${address}`);
      console.log(`  coin type     ${coinType}`);
      console.log(`  balance       ${raw.toLocaleString()} (${formatBalance(raw, decimals, symbol)})`);
      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

client
  .command("balances")
  .alias("bals")
  .description("List all coin balances for an address")
  .argument("<address>", "Sui address (0x...)")
  .option("--limit <n>", "max results per page")
  .option("--cursor <token>", "pagination cursor from previous call")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (address: string, opts: { limit?: string; cursor?: string; json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);

    try {
      const limit = opts.limit ? parseInt(opts.limit, 10) : undefined;
      const result = await sui.core.listBalances({ owner: address, limit, cursor: opts.cursor ?? null });

      if (opts.json) {
        console.log(JSON.stringify(result, null, 2));
        return;
      }

      // Batch-fetch metadata for symbol/decimals
      const metaCache = new Map<string, { decimals: number; symbol: string }>();
      for (const bal of result.balances) {
        if (!metaCache.has(bal.coinType)) {
          try {
            const meta = await sui.core.getCoinMetadata({ coinType: bal.coinType });
            if (meta.coinMetadata) {
              metaCache.set(bal.coinType, { decimals: meta.coinMetadata.decimals, symbol: meta.coinMetadata.symbol });
            }
          } catch { /* skip */ }
        }
      }

      console.log(`\n  balances for ${address}\n`);

      if (result.balances.length === 0) {
        console.log("  (no balances)");
      } else {
        for (const bal of result.balances) {
          const raw = BigInt(bal.balance);
          const meta = metaCache.get(bal.coinType);
          const decimals = meta?.decimals ?? 9;
          const symbol = meta?.symbol ?? bal.coinType.split("::").pop() ?? "";
          console.log(`  ${bal.coinType}`);
          console.log(`    ${formatBalance(raw, decimals, symbol)} (${raw.toLocaleString()} MIST)\n`);
        }
      }

      if (result.hasNextPage && result.cursor) {
        console.log(`  cursor: ${result.cursor}  (has more pages)`);
      }
      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

client
  .command("coins")
  .description("List coin objects for an address")
  .argument("<address>", "Sui address (0x...)")
  .option("--coin-type <type>", "coin type (default: 0x2::sui::SUI)")
  .option("--limit <n>", "max results per page")
  .option("--cursor <token>", "pagination cursor from previous call")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (address: string, opts: { coinType?: string; limit?: string; cursor?: string; json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);

    try {
      const coinType = opts.coinType ?? "0x2::sui::SUI";
      const limit = opts.limit ? parseInt(opts.limit, 10) : undefined;
      const result = await sui.core.listCoins({ owner: address, coinType, limit, cursor: opts.cursor ?? null });

      if (opts.json) {
        console.log(JSON.stringify(result, null, 2));
        return;
      }

      console.log(`\n  coins for ${address} (${coinType})\n`);

      if (result.objects.length === 0) {
        console.log("  (no coins)");
      } else {
        const idW = 44;
        const verW = 10;
        console.log(`  ${"object id".padEnd(idW)} ${"version".padEnd(verW)} balance`);
        for (const coin of result.objects) {
          const short = coin.objectId.slice(0, 18) + "..." + coin.objectId.slice(-4);
          console.log(`  ${short.padEnd(idW)} ${(coin.version ?? "").toString().padEnd(verW)} ${BigInt(coin.balance ?? "0").toLocaleString()}`);
        }
      }

      if (result.hasNextPage && result.cursor) {
        console.log(`\n  cursor: ${result.cursor}  (has more pages)`);
      }
      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

client
  .command("owned")
  .alias("own")
  .description("List objects owned by an address")
  .argument("<address>", "Sui address (0x...)")
  .option("--type <move_type>", "filter by Move type")
  .option("--limit <n>", "max results per page", "10")
  .option("--cursor <token>", "pagination cursor from previous call")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (address: string, opts: { type?: string; limit: string; cursor?: string; json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);

    try {
      const limit = parseInt(opts.limit, 10);
      const result = await sui.core.listOwnedObjects({
        owner: address,
        type: opts.type,
        limit,
        cursor: opts.cursor ?? null,
        include: { json: true },
      });

      if (opts.json) {
        console.log(JSON.stringify(result, null, 2));
        return;
      }

      console.log(`\n  objects owned by ${address}${opts.type ? ` (type: ${opts.type})` : ""}\n`);

      if (result.objects.length === 0) {
        console.log("  (no objects)");
      } else {
        const idW = 44;
        const verW = 10;
        console.log(`  ${"object id".padEnd(idW)} ${"version".padEnd(verW)} type`);
        for (const obj of result.objects) {
          const short = obj.objectId.slice(0, 18) + "..." + obj.objectId.slice(-4);
          const typeStr = obj.type ?? "unknown";
          const typeTrunc = typeStr.length > 60 ? typeStr.slice(0, 57) + "..." : typeStr;
          console.log(`  ${short.padEnd(idW)} ${(obj.version ?? "").toString().padEnd(verW)} ${typeTrunc}`);
        }
      }

      const showing = result.objects.length;
      const pageInfo = result.hasNextPage ? `cursor: ${result.cursor}  (has more pages)` : "(no more pages)";
      console.log(`\n  showing ${showing}, ${pageInfo}`);
      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

client
  .command("objects")
  .alias("objs")
  .description("Get multiple objects by ID")
  .argument("<ids...>", "object IDs (0x...)")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (ids: string[], opts: { json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);

    try {
      const result = await sui.core.getObjects({
        objectIds: ids,
        include: { json: true, previousTransaction: true },
      });

      if (opts.json) {
        console.log(JSON.stringify(result.objects, null, 2));
        return;
      }

      for (const item of result.objects) {
        if (item instanceof Error) {
          console.log(`\n  error: ${item.message}`);
          continue;
        }
        printObject(item);
      }

      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

client
  .command("txs")
  .description("Get multiple transactions by digest")
  .argument("<digests...>", "transaction digests (base58)")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (digests: string[], opts: { json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);

    try {
      // Fetch each transaction individually (core.getTransaction)
      const results = await Promise.all(
        digests.map(digest => sui.core.getTransaction({
          digest,
          include: { effects: true, events: true, transaction: true, balanceChanges: true },
        }).catch(err => ({ error: err instanceof Error ? err.message : String(err), digest })))
      );

      if (opts.json) {
        console.log(JSON.stringify(results, jsonReplacer, 2));
        return;
      }

      for (const result of results) {
        if ("error" in result) {
          console.log(`\n  error for ${result.digest}: ${result.error}`);
          continue;
        }
        const tx = result.Transaction ?? result.FailedTransaction;
        if (!tx) continue;

        const status = tx.status?.success ? "success" : `failure (${tx.status?.error?.message ?? "unknown"})`;
        const gas = tx.effects?.gasUsed;
        const txData = tx.transaction;

        console.log(`\n  digest        ${tx.digest}`);
        console.log(`  status        ${status}`);
        if (txData?.sender) console.log(`  sender        ${txData.sender}`);
        if (tx.epoch) console.log(`  epoch         ${tx.epoch}`);
        if (gas) {
          const total = BigInt(gas.computationCost) + BigInt(gas.storageCost) - BigInt(gas.storageRebate);
          console.log(`  gas total     ${total} MIST`);
        }

        if (tx.events?.length) {
          console.log(`  events        ${tx.events.length}`);
        }
        if (tx.balanceChanges?.length) {
          console.log(`  bal changes   ${tx.balanceChanges.length}`);
        }
      }

      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

client
  .command("dynamic-fields")
  .alias("df")
  .description("List dynamic fields of an object")
  .argument("<parent-id>", "parent object ID (0x...)")
  .option("--include-value", "include field values", false)
  .option("--limit <n>", "max results per page")
  .option("--cursor <token>", "pagination cursor from previous call")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (parentId: string, opts: { includeValue: boolean; limit?: string; cursor?: string; json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);

    try {
      const limit = opts.limit ? parseInt(opts.limit, 10) : undefined;
      const result = await sui.listDynamicFields({
        parentId,
        limit,
        cursor: opts.cursor ?? null,
        include: { value: opts.includeValue },
      });

      if (opts.json) {
        console.log(JSON.stringify(result, jsonReplacer, 2));
        return;
      }

      console.log(`\n  dynamic fields for ${parentId}\n`);

      if (result.dynamicFields.length === 0) {
        console.log("  (no dynamic fields)");
      } else {
        for (const df of result.dynamicFields) {
          const kind = df.$kind === "DynamicObject" ? "Object" : "Field";
          const fieldId = df.fieldId.slice(0, 18) + "..." + df.fieldId.slice(-4);
          console.log(`  ${kind.padEnd(8)} ${fieldId}`);
          console.log(`    name type:  ${df.name.type}`);
          console.log(`    value type: ${df.valueType}`);
          const val = df.value as { type: string; bcs: Uint8Array } | undefined;
          if (opts.includeValue && val) {
            console.log(`    value bcs:  ${Buffer.from(val.bcs).toString("hex").slice(0, 64)}${val.bcs.length > 32 ? "..." : ""}`);
          }
          console.log("");
        }
      }

      if (result.hasNextPage && result.cursor) {
        console.log(`  cursor: ${result.cursor}  (has more pages)`);
      }
      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

client
  .command("checkpoint")
  .alias("cp")
  .description("Get checkpoint info by sequence number")
  .argument("[seq]", "checkpoint sequence number (omit for latest)")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (seqArg: string | undefined, opts: { json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);

    try {
      let seq: bigint;
      if (seqArg) {
        seq = BigInt(seqArg);
      } else {
        // Get latest checkpoint from service info
        const { response: info } = await sui.ledgerService.getServiceInfo({});
        seq = info.checkpointHeight ?? 0n;
      }

      const { response } = await sui.ledgerService.getCheckpoint({
        checkpointId: { oneofKind: "sequenceNumber", sequenceNumber: seq },
        readMask: { paths: ["*"] },
      });
      const cp = response.checkpoint;
      if (!cp) throw new Error(`Checkpoint ${seq} not found`);

      if (opts.json) {
        console.log(JSON.stringify(cp, jsonReplacer, 2));
        return;
      }

      const summary = cp.summary;
      const ts = summary?.timestamp?.seconds
        ? new Date(Number(summary.timestamp.seconds) * 1000).toISOString()
        : null;
      const txCount = cp.transactions?.length ?? 0;

      console.log(`\n  checkpoint    ${cp.sequenceNumber}`);
      if (summary?.contentDigest) console.log(`  digest        ${summary.contentDigest}`);
      if (summary?.epoch !== undefined) console.log(`  epoch         ${summary.epoch}`);
      if (ts) console.log(`  timestamp     ${ts}`);
      console.log(`  tx count      ${txCount}`);
      if (summary?.totalNetworkTransactions !== undefined) {
        console.log(`  total txs     ${BigInt(summary.totalNetworkTransactions).toLocaleString()}`);
      }
      if (summary?.previousDigest) console.log(`  prev digest   ${summary.previousDigest}`);
      if (summary?.epochRollingGasCostSummary) {
        const g = summary.epochRollingGasCostSummary;
        console.log(`  gas compute   ${g.computationCost}`);
        console.log(`  gas storage   ${g.storageCost}`);
        console.log(`  gas rebate    ${g.storageRebate}`);
      }
      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

client
  .command("info")
  .description("Get chain info from the gRPC endpoint")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (opts: { json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);

    try {
      const { response } = await sui.ledgerService.getServiceInfo({});

      if (opts.json) {
        console.log(JSON.stringify(response, jsonReplacer, 2));
        return;
      }

      const ts = response.timestamp?.seconds
        ? new Date(Number(response.timestamp.seconds) * 1000).toISOString()
        : null;

      console.log(`\n  chain           ${response.chain ?? "unknown"}`);
      if (response.chainId) console.log(`  chain id        ${response.chainId}`);
      if (response.epoch !== undefined) console.log(`  epoch           ${response.epoch}`);
      if (response.checkpointHeight !== undefined) console.log(`  checkpoint      ${BigInt(response.checkpointHeight).toLocaleString()}`);
      if (ts) console.log(`  timestamp       ${ts}`);
      if (response.lowestAvailableCheckpoint !== undefined) console.log(`  lowest cp       ${response.lowestAvailableCheckpoint}`);
      if (response.lowestAvailableCheckpointObjects !== undefined) console.log(`  lowest cp objs  ${response.lowestAvailableCheckpointObjects}`);
      if (response.server) console.log(`  server          ${response.server}`);
      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

client
  .command("coin-meta")
  .alias("coin")
  .description("Get coin metadata (name, symbol, decimals)")
  .argument("<coin-type>", "coin type (e.g. 0x2::sui::SUI)")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (coinType: string, opts: { json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);

    try {
      const result = await sui.core.getCoinMetadata({ coinType });
      const meta = result.coinMetadata;
      if (!meta) throw new Error(`No metadata found for coin type ${coinType}`);

      if (opts.json) {
        console.log(JSON.stringify(meta, null, 2));
        return;
      }

      console.log(`\n  coin type     ${coinType}`);
      console.log(`  name          ${meta.name}`);
      console.log(`  symbol        ${meta.symbol}`);
      console.log(`  decimals      ${meta.decimals}`);
      if (meta.description) console.log(`  description   ${meta.description}`);
      if (meta.iconUrl) console.log(`  icon url      ${meta.iconUrl}`);
      if (meta.id) console.log(`  metadata id   ${meta.id}`);
      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

client
  .command("package")
  .alias("pkg")
  .description("Get Move package metadata")
  .argument("<id>", "package object ID (0x...)")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (id: string, opts: { json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);

    try {
      const { response } = await sui.movePackageService.getPackage({ packageId: id });
      const pkg = response.package;
      if (!pkg) throw new Error(`Package ${id} not found`);

      if (opts.json) {
        console.log(JSON.stringify(pkg, jsonReplacer, 2));
        return;
      }

      console.log(`\n  package       ${pkg.storageId}`);
      if (pkg.originalId) console.log(`  original id   ${pkg.originalId}`);
      if (pkg.version !== undefined) console.log(`  version       ${pkg.version}`);

      if (pkg.modules?.length) {
        console.log(`\n  modules (${pkg.modules.length}):`);
        for (const mod of pkg.modules) {
          const dtCount = mod.datatypes?.length ?? 0;
          const fnCount = mod.functions?.length ?? 0;
          console.log(`    ${mod.name ?? "?"}    ${dtCount} type${dtCount !== 1 ? "s" : ""}, ${fnCount} function${fnCount !== 1 ? "s" : ""}`);
        }
      }

      if (pkg.typeOrigins?.length) {
        const shown = pkg.typeOrigins.slice(0, 20);
        console.log(`\n  type origins (${pkg.typeOrigins.length}):`);
        for (const to of shown) {
          console.log(`    ${to.moduleName}::${to.datatypeName} → ${to.packageId}`);
        }
        if (pkg.typeOrigins.length > 20) {
          console.log(`    ... and ${pkg.typeOrigins.length - 20} more`);
        }
      }

      if (pkg.linkage?.length) {
        console.log(`\n  linkage (${pkg.linkage.length}):`);
        for (const link of pkg.linkage) {
          console.log(`    ${link.originalId} → ${link.upgradedId} v${link.upgradedVersion}`);
        }
      }

      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

client
  .command("function")
  .alias("fn")
  .description("Get Move function signature")
  .argument("<type>", "fully qualified name (0xPKG::module::function)")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (typeArg: string, opts: { json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);

    try {
      const parts = typeArg.split("::");
      if (parts.length !== 3) throw new Error("Expected format: 0xPKG::module::function_name");
      const [packageId, moduleName, name] = parts as [string, string, string];

      const result = await sui.core.getMoveFunction({ packageId, moduleName, name });
      const fn = result.function;

      if (opts.json) {
        console.log(JSON.stringify(fn, null, 2));
        return;
      }

      const formatSigBody = (body: any): string => {
        if (!body) return "unknown";
        const kind = body.$kind ?? Object.keys(body).find((k: string) => k !== "$kind" && k !== "reference") ?? "";
        if (["u8", "u16", "u32", "u64", "u128", "u256", "bool", "address", "unknown"].includes(kind)) return kind;
        if (kind === "vector") return `vector<${formatSigBody(body.vector)}>`;
        if (kind === "datatype") {
          const dt = body.datatype;
          const typeParams = dt.typeParameters?.length
            ? `<${dt.typeParameters.map(formatSigBody).join(", ")}>`
            : "";
          return `${dt.typeName}${typeParams}`;
        }
        if (kind === "typeParameter") return `T${body.index ?? body.typeParameter ?? ""}`;
        return kind;
      };

      const formatSig = (sig: any): string => {
        const ref = sig.reference === "mutable" ? "&mut " : sig.reference === "immutable" ? "&" : "";
        return ref + formatSigBody(sig.body);
      };

      console.log(`\n  function      ${fn.name}`);
      console.log(`  package       ${fn.packageId}`);
      console.log(`  module        ${fn.moduleName}`);
      console.log(`  visibility    ${fn.visibility}`);
      console.log(`  is entry      ${fn.isEntry}`);

      if (fn.typeParameters?.length) {
        console.log(`\n  type params (${fn.typeParameters.length}):`);
        for (let i = 0; i < fn.typeParameters.length; i++) {
          const tp = fn.typeParameters[i]!;
          const constraints = tp.constraints?.length ? tp.constraints.join(" + ") : "none";
          console.log(`    T${i}: [${constraints}]`);
        }
      }

      if (fn.parameters?.length) {
        console.log(`\n  parameters (${fn.parameters.length}):`);
        for (const param of fn.parameters) {
          console.log(`    ${formatSig(param)}`);
        }
      }

      if (fn.returns?.length) {
        console.log(`\n  returns (${fn.returns.length}):`);
        for (const ret of fn.returns) {
          console.log(`    ${formatSig(ret)}`);
        }
      } else {
        console.log(`\n  returns       (none)`);
      }

      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

client
  .command("package-versions")
  .alias("pkg-v")
  .description("List all versions of a Move package")
  .argument("<id>", "package object ID (any version)")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (id: string, opts: { json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);

    try {
      const { response } = await sui.movePackageService.listPackageVersions({ packageId: id });

      if (opts.json) {
        console.log(JSON.stringify(response.versions, jsonReplacer, 2));
        return;
      }

      console.log(`\n  versions of package ${id}\n`);

      if (!response.versions?.length) {
        console.log("  (no versions found)");
      } else {
        const verW = 10;
        console.log(`  ${"version".padEnd(verW)} package id`);
        for (const ver of response.versions) {
          console.log(`  ${String(ver.version ?? "?").padEnd(verW)} ${ver.packageId ?? "?"}`);
        }
      }

      if (response.nextPageToken?.length) {
        console.log(`\n  (has more pages)`);
      }
      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

client
  .command("gas-price")
  .description("Get the current reference gas price")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (opts: { json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);

    try {
      const result = await sui.core.getReferenceGasPrice();

      if (opts.json) {
        console.log(JSON.stringify(result, null, 2));
        return;
      }

      console.log(`\n  reference gas price    ${result.referenceGasPrice} MIST`);
      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

client
  .command("protocol-config")
  .alias("proto")
  .description("Get protocol configuration (feature flags and attributes)")
  .option("--all", "show all entries (default: first 20 per section)", false)
  .option("--flags-only", "show only feature flags", false)
  .option("--attrs-only", "show only attributes", false)
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (opts: { all: boolean; flagsOnly: boolean; attrsOnly: boolean; json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);

    try {
      const result = await sui.core.getProtocolConfig();
      const config = result.protocolConfig;

      if (opts.json) {
        console.log(JSON.stringify(config, null, 2));
        return;
      }

      console.log(`\n  protocol version    ${config.protocolVersion}`);

      if (!opts.attrsOnly) {
        const flags = Object.entries(config.featureFlags).sort(([a], [b]) => a.localeCompare(b));
        const showFlags = opts.all ? flags : flags.slice(0, 20);
        console.log(`\n  feature flags (${flags.length}):`);
        for (const [key, val] of showFlags) {
          console.log(`    ${key.padEnd(50)} ${val}`);
        }
        if (!opts.all && flags.length > 20) {
          console.log(`    ... and ${flags.length - 20} more (use --all to show all)`);
        }
      }

      if (!opts.flagsOnly) {
        const attrs = Object.entries(config.attributes).sort(([a], [b]) => a.localeCompare(b));
        const showAttrs = opts.all ? attrs : attrs.slice(0, 20);
        console.log(`\n  attributes (${attrs.length}):`);
        for (const [key, val] of showAttrs) {
          console.log(`    ${key.padEnd(50)} ${val ?? "null"}`);
        }
        if (!opts.all && attrs.length > 20) {
          console.log(`    ... and ${attrs.length - 20} more (use --all to show all)`);
        }
      }

      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

client
  .command("system-state")
  .alias("sys")
  .description("Get current system state snapshot")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (opts: { json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);

    try {
      const result = await sui.core.getCurrentSystemState();
      const ss = result.systemState;

      if (opts.json) {
        console.log(JSON.stringify(ss, jsonReplacer, 2));
        return;
      }

      console.log(`\n  epoch               ${ss.epoch}`);
      console.log(`  protocol            v${ss.protocolVersion}`);
      console.log(`  ref gas price       ${ss.referenceGasPrice} MIST`);
      console.log(`  safe mode           ${ss.safeMode}`);

      const startMs = Number(ss.epochStartTimestampMs);
      if (startMs) {
        console.log(`  epoch start         ${new Date(startMs).toISOString()}`);
        const durationMs = Number(ss.parameters?.epochDurationMs ?? 86_400_000);
        const remaining = Math.max(0, durationMs - (Date.now() - startMs));
        console.log(`  epoch duration      ${(durationMs / 3_600_000).toFixed(1)}h (${(remaining / 3_600_000).toFixed(1)}h remaining)`);
      }

      if (ss.storageFund) {
        const rebates = (BigInt(ss.storageFund.totalObjectStorageRebates) / 1_000_000_000n).toLocaleString();
        const nonRefundable = (BigInt(ss.storageFund.nonRefundableBalance) / 1_000_000_000n).toLocaleString();
        console.log(`\n  storage rebates     ${rebates} SUI`);
        console.log(`  non-refundable      ${nonRefundable} SUI`);
      }

      if (ss.stakeSubsidy) {
        const sub = ss.stakeSubsidy;
        const balanceSui = (BigInt(sub.balance) / 1_000_000_000n).toLocaleString();
        const distSui = (BigInt(sub.currentDistributionAmount) / 1_000_000_000n).toLocaleString();
        console.log(`\n  subsidy balance     ${balanceSui} SUI`);
        console.log(`  subsidy/epoch       ${distSui} SUI`);
      }

      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

client
  .command("ping")
  .description("Measure gRPC endpoint latency")
  .option("--count <n>", "number of pings", "5")
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (opts: { count: string; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);
    const isHayabusa = opts.url.includes("hayabusa");

    try {
      const count = parseInt(opts.count, 10);

      const measure = async (fn: () => Promise<void>) => {
        const times: number[] = [];
        for (let i = 0; i < count; i++) {
          const start = performance.now();
          await fn();
          times.push(performance.now() - start);
        }
        return times;
      };

      const printStats = (times: number[]) => {
        const sorted = [...times].sort((a, b) => a - b);
        for (let i = 0; i < times.length; i++) {
          console.log(`  ${(i + 1).toString().padStart(2)}   ${times[i]!.toFixed(1)} ms`);
        }
        console.log(`\n  min       ${sorted[0]!.toFixed(1)} ms`);
        console.log(`  max       ${sorted[sorted.length - 1]!.toFixed(1)} ms`);
        console.log(`  avg       ${(times.reduce((a, b) => a + b, 0) / times.length).toFixed(1)} ms`);
        console.log(`  median    ${sorted[Math.floor(sorted.length / 2)]!.toFixed(1)} ms`);
      };

      console.log(`\n  pinging ${opts.url} ...\n`);

      // GetServiceInfo — not cacheable, measures real round-trip
      const uncachedTimes = await measure(() => sui.ledgerService.getServiceInfo({}));

      console.log("  ── latency (GetServiceInfo) ──\n");
      printStats(uncachedTimes);

      // Hayabusa: test cached path via GetTransaction (tier 1, always cached)
      if (isHayabusa) {
        // Get a tx digest from a recent checkpoint
        const { response: info } = await sui.ledgerService.getServiceInfo({});
        const cpSeq = (info.checkpointHeight ?? 0n) - 10n;
        const { response: cpResp } = await sui.ledgerService.getCheckpoint({
          checkpointId: { oneofKind: "sequenceNumber", sequenceNumber: cpSeq },
          readMask: { paths: ["transactions.digest"] },
        });
        const digest = cpResp.checkpoint?.transactions?.[0]?.digest;

        if (digest) {
          // Warm the cache
          await sui.core.getTransaction({ digest });

          console.log(`\n  ── cached (GetTransaction via hayabusa L1/L2) ──\n`);
          const cachedTimes = await measure(() => sui.core.getTransaction({ digest }));
          printStats(cachedTimes);
        }
      }

      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

client
  .command("simulate")
  .alias("sim")
  .description("Simulate a transaction (dry-run, read-only)")
  .argument("<base64-tx>", "base64-encoded transaction bytes")
  .option("--no-checks", "disable validation checks")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (base64Tx: string, opts: { checks: boolean; json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);

    try {
      const txBytes = Uint8Array.from(atob(base64Tx), c => c.charCodeAt(0));
      const result = await sui.core.simulateTransaction({
        transaction: txBytes,
        include: { effects: true, events: true, balanceChanges: true, transaction: true },
        checksEnabled: opts.checks,
      });

      if (opts.json) {
        console.log(JSON.stringify(result, jsonReplacer, 2));
        return;
      }

      const tx = result.Transaction ?? result.FailedTransaction;
      const simStatus = result.$kind === "FailedTransaction" ? "FAILED" : "SUCCESS";

      console.log(`\n  ── SIMULATED (dry-run) ──\n`);
      console.log(`  status        ${simStatus}`);

      if (tx) {
        if (tx.transaction?.sender) console.log(`  sender        ${tx.transaction.sender}`);

        const gas = tx.effects?.gasUsed;
        if (gas) {
          const total = BigInt(gas.computationCost) + BigInt(gas.storageCost) - BigInt(gas.storageRebate);
          console.log(`  gas total     ${total} MIST`);
          console.log(`  gas compute   ${gas.computationCost} MIST`);
          console.log(`  gas storage   ${gas.storageCost} MIST`);
          console.log(`  gas rebate    -${gas.storageRebate} MIST`);
        }

        if (tx.effects?.changedObjects?.length) {
          console.log(`\n  changed objects (${tx.effects.changedObjects.length}):`);
          for (const co of tx.effects.changedObjects) {
            const id = co.objectId ? co.objectId.slice(0, 18) + "..." : "";
            const change = co.inputState && co.outputState
              ? `${co.inputState} → ${co.outputState}`
              : co.outputState ?? "changed";
            console.log(`    ${id}  ${change}`);
          }
        }

        if (tx.events?.length) {
          console.log(`\n  events (${tx.events.length}):`);
          for (const ev of tx.events) {
            console.log(`    ${ev.eventType}`);
          }
        }

        if (tx.balanceChanges?.length) {
          console.log(`\n  balance changes (${tx.balanceChanges.length}):`);
          for (const bc of tx.balanceChanges) {
            console.log(`    ${bc.address}  ${bc.amount} ${bc.coinType}`);
          }
        }
      }

      console.log("");
    } catch (err) {
      cliError(err);
    }
  });

// ---------------------------------------------------------------------------
// Name service command
// ---------------------------------------------------------------------------

const ns = program
  .command("ns")
  .description("SuiNS name service lookups");

ns
  .command("resolve-address")
  .description("Resolve a SuiNS name to an address")
  .argument("<name>", "SuiNS name (e.g. example.sui)")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (name: string, opts: { json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);
    try {
      const { response } = await sui.nameService.lookupName({ name });
      const address = response.record?.targetAddress;
      if (!address) throw new Error(`Name "${name}" not found`);
      if (opts.json) { console.log(JSON.stringify({ name, address })); return; }
      console.log(`\n  ${name} \u2192 ${address}\n`);
    } catch (err) {
      cliError(err);
    }
  });

ns
  .command("resolve-name")
  .description("Resolve an address to its SuiNS name")
  .argument("<address>", "Sui address (0x...)")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (address: string, opts: { json: boolean; url: string }) => {
    const { getSuiClient } = await import("./rpc.ts");
    const sui = getSuiClient(opts.url);
    try {
      const { response } = await sui.nameService.reverseLookupName({ address });
      const name = response.record?.name;
      if (!name) throw new Error(`No SuiNS name for ${address}`);
      if (opts.json) { console.log(JSON.stringify({ address, name })); return; }
      console.log(`\n  ${address} \u2192 ${name}\n`);
    } catch (err) {
      cliError(err);
    }
  });

// ---------------------------------------------------------------------------
// Config command
// ---------------------------------------------------------------------------

const configCmd = program
  .command("config")
  .description("Manage jun configuration (~/.jun/config.yml)");

configCmd
  .command("show")
  .description("Show current configuration")
  .action(async () => {
    const config = loadConfig();

    console.log(`\n  active env      ${config.activeEnv}`);
    console.log(`  grpc_url        ${config.grpcUrl}`);
    console.log(`  archive_url     ${config.archiveUrl}`);

    const otherEnvs = Object.keys(config.allEnvs).filter(e => e !== config.activeEnv);
    if (otherEnvs.length) {
      console.log(`\n  other envs:`);
      for (const name of otherEnvs) {
        console.log(`    ${name}: ${config.allEnvs[name].grpc_url}`);
      }
    }
    console.log("");
  });

configCmd
  .command("set")
  .description("Switch active environment")
  .argument("<env>", "environment name (e.g. mainnet, testnet)")
  .action(async (env: string) => {
    const { setActiveEnv } = await import("./config.ts");
    try {
      setActiveEnv(env);
      const config = loadConfig();
      console.log(`\n  Switched to ${env}`);
      console.log(`  grpc_url    ${config.grpcUrl}`);
      console.log(`  archive_url ${config.archiveUrl}\n`);
    } catch (err) {
      cliError(err);
    }
  });

configCmd
  .command("add")
  .description("Add a custom environment")
  .argument("<name>", "environment name")
  .requiredOption("--grpc-url <url>", "gRPC endpoint URL")
  .option("--archive-url <url>", "checkpoint archive URL", "")
  .action(async (name: string, opts: { grpcUrl: string; archiveUrl: string }) => {
    const { addEnv } = await import("./config.ts");
    addEnv(name, opts.grpcUrl, opts.archiveUrl);
    console.log(`\n  Added environment "${name}"`);
    console.log(`  grpc_url    ${opts.grpcUrl}`);
    if (opts.archiveUrl) console.log(`  archive_url ${opts.archiveUrl}`);
    console.log(`\n  Switch to it: jun config set ${name}\n`);
  });

configCmd
  .command("path")
  .description("Print config file path")
  .action(async () => {
    const { getConfigPath } = await import("./config.ts");
    console.log(getConfigPath());
  });

// ---------------------------------------------------------------------------
// Codegen command
// ---------------------------------------------------------------------------

program
  .command("codegen")
  .description("Generate jun field DSL from an on-chain Sui struct")
  .argument("<type>", "fully qualified type (e.g. 0xPACKAGE::module::StructName)")
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (typeArg: string, opts: { url: string }) => {
    // Parse the fully qualified type: 0xPACKAGE::module::StructName
    const parts = typeArg.split("::");
    if (parts.length !== 3) {
      console.error(`[jun] invalid type format: ${typeArg}`);
      console.error(`[jun] expected: 0xPACKAGE::module::StructName`);
      process.exit(1);
    }

    const [packageId, moduleName, structName] = parts;

    const client = createGrpcClient({ url: opts.url });

    try {
      const descriptor = await client.getDatatype(packageId, moduleName, structName);
      const result = generateFieldDSL(descriptor);
      console.log(formatCodegenResult(result));
    } catch (err) {
      cliError(err);
    } finally {
      client.close();
    }
  });

// ---------------------------------------------------------------------------
// MCP command
// ---------------------------------------------------------------------------

program
  .command("mcp")
  .description("Start MCP server for Sui chain queries and optional SQLite analysis")
  .argument("[db]", "path to SQLite database file (optional)")
  .option("--url <url>", "gRPC endpoint", cfg.grpcUrl)
  .action(async (dbPath: string | undefined, opts: { url: string }) => {
    const { createMcpServer } = await import("./mcp.ts");
    const { StdioServerTransport } = await import("@modelcontextprotocol/sdk/server/stdio.js");

    const { server } = createMcpServer({ dbPath, grpcUrl: opts.url });
    const transport = new StdioServerTransport();
    await server.connect(transport);
  });

// ---------------------------------------------------------------------------
// Chat command
// ---------------------------------------------------------------------------

program
  .command("chat")
  .description("Open an AI chat with Sui chain query tools, optionally streaming checkpoint data")
  .option("--live", "stream live checkpoints (grows in real-time)", false)
  .option("--url <url>", "gRPC endpoint for live streaming", cfg.grpcUrl)
  .option("--from-checkpoint <checkpoint>", "start checkpoint for replay")
  .option("--to <checkpoint>", "end checkpoint for replay")
  .option("--count <n>", "number of checkpoints for replay", "100")
  .option("--archive-url <url>", "checkpoint archive URL", cfg.archiveUrl)
  .option("--concurrency <n>", "concurrent checkpoint fetches for replay", "32")
  .option("--verify", "cryptographically verify each checkpoint", false)
  .option("--verify-url <url>", "gRPC URL for committee fetching", cfg.grpcUrl)
  .action(async (opts: {
    live: boolean;
    url: string;
    from?: string;
    to?: string;
    count: string;
    archiveUrl: string;
    concurrency: string;
    verify: boolean;
    verifyUrl: string;
  }) => {
    const { tmpdir } = await import("os");
    const { join } = await import("path");
    const { spawn } = await import("child_process");
    const { unlinkSync, writeFileSync } = await import("fs");

    const mcpConfigPath = join(tmpdir(), `jun-mcp-${Date.now()}.json`);
    const junDir = join(import.meta.dir, "..");
    const chainOnly = !opts.live && !opts.fromCheckpoint;

    let dbPath: string | undefined;
    let sqliteWriter: { close(): void } | undefined;
    let description: string;

    if (chainOnly) {
      description = "Sui chain query tools";
    } else if (opts.live) {
      const { createSqliteWriter } = await import("./output/sqlite.ts");
      dbPath = join(tmpdir(), `jun-chat-${Date.now()}.sqlite`);
      sqliteWriter = createSqliteWriter({ path: dbPath, showEvents: true, showBalanceChanges: opts.live });
      // ── Live mode: stream in background, launch chat immediately ──
      const { createGrpcClient } = await import("./grpc.ts");
      const readMask = buildReadMask([]);
      const client = createGrpcClient({ url: opts.url, readMask });

      description = `live stream from ${opts.url} (data growing in real-time)`;
      console.error(`[jun] streaming live to ${dbPath}`);

      let checkpointCount = 0;

      // Stream in background
      (async () => {
        try {
          for await (const response of client.subscribeCheckpoints()) {
            const { checkpoint } = response;
            const seq = response.cursor;
            const tsMs = getTimestampMs(checkpoint.summary);
            const ts = formatTimestamp(tsMs);

            const txData = (checkpoint.transactions ?? []).filter((tx: any) => !isSystemTx(tx)).map((tx: any) => ({
              digest: tx.digest,
              sender: tx.transaction?.sender,
              status: tx.effects?.status?.success ? "success" : tx.effects?.status ? "failure" : null,
              gasComputation: tx.effects?.gasUsed?.computationCost ? parseInt(tx.effects.gasUsed.computationCost) : null,
              gasStorage: tx.effects?.gasUsed?.storageCost ? parseInt(tx.effects.gasUsed.storageCost) : null,
              gasRebate: tx.effects?.gasUsed?.storageRebate ? parseInt(tx.effects.gasUsed.storageRebate) : null,
              events: tx.events?.events ?? [],
              balanceChanges: tx.balanceChanges ?? [],
            }));
            sqliteWriter.writeCheckpoint({ seq, timestamp: ts, transactions: txData });
            checkpointCount++;
          }
        } catch {
          // Stream ended (Claude exited, cleanup will happen)
        }
      })();

      // Wait a moment for some data to arrive
      await Bun.sleep(3000);
      console.error(`[jun] ${checkpointCount} checkpoints buffered, starting chat...`);

    } else if (opts.fromCheckpoint) {
      // ── Replay mode: stream into DB in background, launch chat immediately ──
      const { createSqliteWriter } = await import("./output/sqlite.ts");
      dbPath = join(tmpdir(), `jun-chat-${Date.now()}.sqlite`);
      sqliteWriter = createSqliteWriter({ path: dbPath, showEvents: true, showBalanceChanges: false });
      const { createArchiveClient } = await import("./archive.ts");
      const from = BigInt(opts.fromCheckpoint!);
      const to = opts.toCheckpoint ? BigInt(opts.toCheckpoint) : from + BigInt(opts.count) - 1n;
      const concurrency = parseInt(opts.concurrency);
      const total = Number(to - from) + 1;

      description = `${total.toLocaleString()} checkpoints (${from} → ${to}, loading in background)`;
      console.error(`[jun] replaying ${description}...`);

      const archive = createArchiveClient({
        archiveUrl: opts.archiveUrl,
        verify: opts.verify,
        grpcUrl: opts.verify ? opts.verifyUrl : undefined,
      });

      const seqs: bigint[] = [];
      for (let s = from; s <= to; s++) seqs.push(s);

      const pMap = (await import("p-map")).default;
      const pRetry = (await import("p-retry")).default;

      let processed = 0;
      const startTime = performance.now();

      // Stream in background — Claude starts while data is still loading
      (async () => {
        try {
          await pMap(seqs, async (seq) => {
            const response = await pRetry(() => archive.fetchCheckpoint(seq), { retries: 3, minTimeout: 1000 });
            const { checkpoint } = response;
            const tsMs = getTimestampMs(checkpoint.summary);
            const ts = formatTimestamp(tsMs);

            const txData = (checkpoint.transactions ?? []).filter((tx: any) => !isSystemTx(tx)).map((tx: any) => ({
              digest: tx.digest,
              sender: tx.transaction?.sender,
              status: tx.effects?.status?.success ? "success" : tx.effects?.status ? "failure" : null,
              gasComputation: tx.effects?.gasUsed?.computationCost ? parseInt(tx.effects.gasUsed.computationCost) : null,
              gasStorage: tx.effects?.gasUsed?.storageCost ? parseInt(tx.effects.gasUsed.storageCost) : null,
              gasRebate: tx.effects?.gasUsed?.storageRebate ? parseInt(tx.effects.gasUsed.storageRebate) : null,
              events: tx.events?.events ?? [],
              balanceChanges: [],
            }));
            sqliteWriter.writeCheckpoint({ seq: response.cursor, timestamp: ts, transactions: txData });
            processed++;
          }, { concurrency });

          const elapsed = ((performance.now() - startTime) / 1000).toFixed(1);
          console.error(`\n[jun] replay complete — ${processed.toLocaleString()} checkpoints in ${elapsed}s`);
        } catch {
          // Replay ended (Claude exited, cleanup will happen)
        }
      })();

      // Wait for initial buffer before launching Claude
      const bufferTarget = Math.min(total, 50);
      while (processed < bufferTarget) await Bun.sleep(100);
      console.error(`[jun] ${processed}/${total} checkpoints loaded, starting chat (loading continues in background)...`);
    }

    // Write temp MCP config
    const mcpArgs = [join(junDir, "src", "mcp.ts")];
    if (dbPath) mcpArgs.push(dbPath);
    const mcpConfig = {
      mcpServers: {
        jun: { command: "bun", args: mcpArgs },
      },
    };
    writeFileSync(mcpConfigPath, JSON.stringify(mcpConfig));

    // Build system prompt
    let systemPrompt: string;
    if (chainOnly) {
      systemPrompt = [
        `You have access to Sui blockchain query tools via the "jun" MCP server.`,
        `Available tools: sui_info, sui_object, sui_transaction, sui_balance, sui_owned_objects, sui_dynamic_fields, sui_epoch, sui_move_function, sui_package, sui_resolve_name.`,
        `Start by calling "sui_info" to see the current chain state.`,
      ].join("\n");
    } else {
      const liveNote = opts.live
        ? "The database is being written to in REAL-TIME by a live gRPC stream. New checkpoints arrive every ~250ms. Run queries multiple times to see fresh data."
        : "The database is being populated from the archive in the background. New checkpoints are being added as they download. Run queries multiple times to see more data.";

      systemPrompt = [
        `You have access to a Sui blockchain SQLite database: ${description}.`,
        liveNote,
        `Use the "jun" MCP server tools to analyze the data.`,
        `Start by calling the "summary" tool to get an overview, then use "query" for specific SQL queries.`,
        `Tables: transactions (digest, checkpoint, timestamp, sender, status, gas_computation, gas_storage, gas_rebate), events (tx_digest, event_seq, event_type, package_id, module, sender, bcs), balance_changes (tx_digest, checkpoint, timestamp, owner, coin_type, amount).`,
      ].join("\n");
    }

    // Spawn claude
    console.error(`[jun] starting chat...`);
    console.error("");

    const claude = spawn("claude", [
      "--mcp-config", mcpConfigPath,
      "--system-prompt", systemPrompt,
    ], {
      stdio: "inherit",
    });

    claude.on("close", () => {
      sqliteWriter?.close();
      if (dbPath) try { unlinkSync(dbPath); } catch {}
      try { unlinkSync(mcpConfigPath); } catch {}
    });
  });

// ---------------------------------------------------------------------------
// Move decompiler
// ---------------------------------------------------------------------------

const moveCmd = program
  .command("move")
  .description("Move bytecode tools");

moveCmd
  .command("decompile")
  .description("Decompile a published Sui Move package")
  .argument("<packageId>", "package object ID (0x...)")
  .option("-m, --module <name...>", "specific module(s) to decompile (all if omitted)")
  .option("--network <network>", "network to use", "mainnet")
  .option("--output <dir>", "output directory for .move files")
  .action(async (packageId: string, opts: { module?: string[]; network: string; output?: string }) => {
    const { fetchPackageModules } = await import("./package-reader.ts");
    const { decompile } = await import("./decompiler-native.ts");
    const network = opts.network as "mainnet" | "testnet" | "devnet" | "localnet";

    try {
      const modules = await fetchPackageModules(packageId, network);

      // Filter to specified modules if any
      const filterSet = opts.module ? new Set(opts.module) : null;
      const targets = filterSet
        ? modules.filter((m) => filterSet.has(m.name))
        : modules;

      if (targets.length === 0) {
        const available = modules.map((m) => m.name).join(", ");
        if (filterSet) {
          const missing = [...filterSet].filter((n) => !modules.some((m) => m.name === n));
          throw new Error(`Module(s) not found: ${missing.join(", ")}. Available: ${available}`);
        }
        throw new Error("Package contains no modules");
      }

      if (!filterSet) {
        console.error(`[jun] decompiling ${targets.length} module${targets.length !== 1 ? "s" : ""} from ${packageId}`);
      }

      for (const mod of targets) {
        const source = await decompile(mod.bytes);

        if (opts.output) {
          const { mkdirSync, writeFileSync } = await import("fs");
          mkdirSync(opts.output, { recursive: true });
          const filePath = `${opts.output}/${mod.name}.move`;
          writeFileSync(filePath, source);
          console.error(`[jun] wrote ${filePath}`);
        } else if (targets.length > 1) {
          console.log(`// ===== ${mod.name} =====`);
          console.log(source);
          console.log("");
        } else {
          console.log(source);
        }
      }
    } catch (err) {
      cliError(err);
    }
  });

// ---------------------------------------------------------------------------
// pipeline — composable Source → Processor → Destination
// ---------------------------------------------------------------------------

const pipelineCmd = program
  .command("pipeline")
  .description("Composable data pipeline (Source → Processor → Destination)");

// Shared pipeline options applied to both `run` and `snapshot`
function addPipelineOptions(cmd: any) {
  return cmd
    .option("--config-url <url>", "fetch config from S3 or HTTPS URL")
    .option("--quiet", "suppress human-readable output to stdout")
    .option("--log [level]", "enable JSON logs to stderr (default: info)")
    .option("--grpc-url <url>", "gRPC endpoint URL")
    .option("--archive-url <url>", "archive base URL for backfill")
    .option("--epoch <number>", "backfill a specific completed epoch")
    .option("--start-checkpoint <seq>", "backfill starting checkpoint (inclusive)")
    .option("--end-checkpoint <seq>", "backfill ending checkpoint (inclusive)")
    .option("--concurrency <n>", "archive fetch concurrency (default: 200)")
    .option("--workers <n>", "checkpoint decoder workers (default: CPU count)")
    .option("--batch-size <n>", "write buffer batch size in checkpoints (default: 500)")
    .option("-y, --yes", "skip confirmation prompt")
    .option("--network <network>", "network name (mainnet, testnet, devnet)")
    .option("--bcs-provider <provider>", "JS BCS runtime: mysten (default) or unconfirmed")
    .option("--bcs-decoder <decoder>", "checkpoint decoder: native (default, Rust FFI) or js")
    .option("--transaction-blocks", "enable transaction block indexing")
    .option("--coin-type <type...>", "coin types to track balances for (repeatable, or \"*\" for all)")
    .option("--event-type <type...>", "Move event types to index (repeatable)")
    .option("--checkpoints", "index checkpoint summaries (one row per checkpoint)")
    .option("--object-changes", "index per-object state changes from effects")
    .option("--dependencies", "index transaction dependencies from effects")
    .option("--inputs", "index programmable transaction inputs")
    .option("--commands", "index all PTB commands (superset of move_calls)")
    .option("--system-transactions", "index non-programmable (system) transactions")
    .option("--unchanged-consensus-objects", "index read-only consensus object refs")
    .option("--everything", "enable all indexers (transaction-blocks, object-changes, dependencies, inputs, commands, system-transactions, unchanged-consensus-objects, checkpoints, and coin-type '*')")
    .option("--sqlite <path>", "write to SQLite database at path")
    .option("--postgres <url>", "write to Postgres database at URL")
    .option("--sqlite-export <s3url>", "after pipeline: VACUUM + upload to S3 (e.g. s3://bucket/key.db)")
    .option("--sqlite-export-split-datasets", "export SQLite as one database per dataset before upload")
    .option("--stdout", "broadcast events to stdout as JSONL")
    .option("--sse <port>", "broadcast events via SSE on port")
    .option("--nats <url>", "broadcast events to NATS server");
}

interface PipelineOpts {
    configUrl?: string;
    quiet?: boolean;
    log?: string | boolean;
    grpcUrl?: string;
    archiveUrl?: string;
    epoch?: string;
    startCheckpoint?: string;
    endCheckpoint?: string;
    concurrency?: string;
    workers?: string;
    batchSize?: string;
    yes?: boolean;
    network?: string;
    transactionBlocks?: boolean;
    coinType?: string[];
    eventType?: string[];
    checkpoints?: boolean;
    objectChanges?: boolean;
    dependencies?: boolean;
    inputs?: boolean;
    commands?: boolean;
    systemTransactions?: boolean;
    unchangedConsensusObjects?: boolean;
    everything?: boolean;
    bcsProvider?: string;
    bcsDecoder?: string;
    sqlite?: string;
    postgres?: string;
    sqliteExport?: string;
    sqliteExportSplitDatasets?: boolean;
    stdout?: boolean;
    sse?: string;
    nats?: string;
}

async function runPipeline(configFile: string | undefined, opts: PipelineOpts, backfillOnly: boolean) {
    // Set BCS provider before any dynamic imports that trigger bcs-provider.ts
    if (opts.bcsProvider) {
      process.env.JUN_BCS_PROVIDER = opts.bcsProvider;
    }
    if (opts.bcsDecoder) {
      process.env.JUN_BCS_DECODER = opts.bcsDecoder;
    }

    try {
      const { resolve } = await import("path");
      const { parsePipelineConfigFromObject } = await import("./pipeline/config-parser.ts");
      const { createPipeline } = await import("./pipeline/pipeline.ts");

      // Load base config from file/URL, or start empty for pure CLI flags
      let baseConfig: Record<string, any> = {};
      const configUrl = opts.configUrl ?? process.env.JUN_CONFIG_URL;

      if (configUrl) {
        const { fetchRemoteConfig } = await import("./remote-config.ts");
        const result = await fetchRemoteConfig(configUrl);
        const yaml = await import("js-yaml");
        baseConfig = (yaml.load(result!.content) as Record<string, any>) ?? {};
      } else if (configFile) {
        const { readFileSync } = require("fs");
        const yaml = await import("js-yaml");
        baseConfig = (yaml.load(readFileSync(resolve(configFile), "utf-8")) as Record<string, any>) ?? {};
      } else if (opts.epoch || opts.startCheckpoint) {
        // Generate config entirely from CLI flags — baseConfig stays empty
      } else {
        console.error("[jun] error: provide a config file, --config-url, or --epoch/--start-checkpoint flags");
        process.exit(1);
      }

      // Set log level before creating components
      if (opts.log) {
        const level = typeof opts.log === "string" ? opts.log : "info";
        process.env.LOG_LEVEL = level;
      } else {
        process.env.LOG_LEVEL = "silent";
      }

      // CLI flags override config file — build with canonical keys directly
      const networkDefaults: Record<string, string> = {
        mainnet: "https://checkpoints.mainnet.sui.io",
        testnet: "https://checkpoints.testnet.sui.io",
        devnet: "https://checkpoints.devnet.sui.io",
      };
      const net = opts.network ?? baseConfig.network ?? "mainnet";
      // Resolve grpcUrl: CLI flag > canonical key > legacy key > default
      const grpcUrl = opts.grpcUrl
        ?? baseConfig.sources?.grpcUrl
        ?? baseConfig.sources?.live?.grpcUrl
        ?? baseConfig.sources?.live?.grpc
        ?? "hayabusa.mainnet.unconfirmed.cloud:443";
      const archiveUrl = opts.archiveUrl
        ?? baseConfig.sources?.archiveUrl
        ?? baseConfig.sources?.backfill?.archiveUrl
        ?? baseConfig.sources?.backfill?.archive
        ?? networkDefaults[net];

      // Source overrides — use canonical keys
      if (opts.epoch || opts.startCheckpoint) {
        baseConfig.sources = baseConfig.sources ?? {};
        baseConfig.sources.grpcUrl = grpcUrl;
        baseConfig.sources.archiveUrl = archiveUrl;

        if (opts.epoch) {
          baseConfig.sources.epoch = opts.epoch;
        } else {
          if (opts.startCheckpoint) baseConfig.sources.startCheckpoint = opts.startCheckpoint;
          if (opts.endCheckpoint) baseConfig.sources.endCheckpoint = opts.endCheckpoint;
        }
        if (opts.concurrency) baseConfig.sources.concurrency = parseInt(opts.concurrency);
        if (opts.workers) baseConfig.sources.workers = parseInt(opts.workers);
      }

      // --everything: convenience flag that enables every indexer + all coin types
      if (opts.everything) {
        opts.transactionBlocks = true;
        opts.checkpoints = true;
        opts.objectChanges = true;
        opts.dependencies = true;
        opts.inputs = true;
        opts.commands = true;
        opts.systemTransactions = true;
        opts.unchangedConsensusObjects = true;
        if (!opts.coinType) opts.coinType = ["*"];
      }

      // Processor overrides
      const anyProcessorFlag =
        opts.transactionBlocks || opts.coinType || opts.eventType ||
        opts.checkpoints || opts.objectChanges || opts.dependencies ||
        opts.inputs || opts.commands || opts.systemTransactions ||
        opts.unchangedConsensusObjects;

      if (anyProcessorFlag) {
        baseConfig.processors = baseConfig.processors ?? {};

        if (opts.transactionBlocks) baseConfig.processors.transactionBlocks = true;
        if (opts.checkpoints) baseConfig.processors.checkpoints = true;
        if (opts.objectChanges) baseConfig.processors.objectChanges = true;
        if (opts.dependencies) baseConfig.processors.dependencies = true;
        if (opts.inputs) baseConfig.processors.inputs = true;
        if (opts.commands) baseConfig.processors.commands = true;
        if (opts.systemTransactions) baseConfig.processors.systemTransactions = true;
        if (opts.unchangedConsensusObjects) baseConfig.processors.unchangedConsensusObjects = true;

        if (opts.coinType) {
          // "*" means track all coin types. Shell may expand * to filenames,
          // so also check if any element equals "*"
          const isAll = opts.coinType.includes("*") || opts.coinType.some((t: string) => t === "*");
          const coinTypes = isAll ? "*" : opts.coinType;
          baseConfig.processors.balances = { coinTypes };
          console.error(`[jun] balance tracking: ${isAll ? "all coin types" : coinTypes.join(", ")}`);
        }

        if (opts.eventType) {
          baseConfig.processors.events = baseConfig.processors.events ?? {};
          for (const eventType of opts.eventType) {
            // Auto-generate handler name from event type: 0x...::module::Event → module_event
            const parts = eventType.split("::");
            const name = parts.length >= 3
              ? `${parts[parts.length - 2]}_${parts[parts.length - 1]}`.toLowerCase()
              : eventType.replace(/[^a-zA-Z0-9]/g, "_").toLowerCase();
            baseConfig.processors.events[name] = { type: eventType };
          }
        }
      }

      // Storage overrides
      if (opts.sqlite) {
        baseConfig.storage = baseConfig.storage ?? {};
        baseConfig.storage.sqlite = opts.sqlite;
      }
      if (opts.postgres) {
        baseConfig.storage = baseConfig.storage ?? {};
        baseConfig.storage.postgres = opts.postgres;
      }
      // Snapshot optimizations: defer indexes + UNLOGGED tables for bulk loading
      if (baseConfig.storage && backfillOnly) {
        baseConfig.storage.deferIndexes = true;
        if (baseConfig.storage.postgres) {
          baseConfig.storage.pgUnlogged = true;
        }
      }

      // S3 export validation — check creds before pipeline starts
      let s3ExportConfig: { bucket: string; key: string } | null = null;
      if (opts.sqliteExportSplitDatasets && !opts.sqliteExport) {
        console.error("[jun] error: --sqlite-export-split-datasets requires --sqlite-export <s3url>");
        process.exit(1);
      }
      if (opts.sqliteExport) {
        if (!opts.sqliteExport.startsWith("s3://")) {
          console.error("[jun] error: --sqlite-export must be an S3 URL (s3://bucket/key.db.zst)");
          process.exit(1);
        }
        if (!opts.sqlite) {
          console.error("[jun] error: --sqlite-export requires --sqlite <path>");
          process.exit(1);
        }

        const s3Path = opts.sqliteExport.slice(5); // strip "s3://"
        const slashIdx = s3Path.indexOf("/");
        if (slashIdx < 1) {
          console.error("[jun] error: invalid S3 URL format. Expected: s3://bucket/key.db.zst");
          process.exit(1);
        }
        const s3Bucket = s3Path.slice(0, slashIdx);
        const s3Key = s3Path.slice(slashIdx + 1);

        const s3Endpoint = process.env.S3_ENDPOINT;
        const s3AccessKey = process.env.AWS_ACCESS_KEY_ID;
        const s3SecretKey = process.env.AWS_SECRET_ACCESS_KEY;

        if (!s3Endpoint || !s3AccessKey || !s3SecretKey) {
          console.error("[jun] error: --sqlite-export requires env vars: S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY");
          process.exit(1);
        }

        // Validate S3 access with a probe write
        const s3Client = new Bun.S3Client({
          endpoint: s3Endpoint,
          bucket: s3Bucket,
          accessKeyId: s3AccessKey,
          secretAccessKey: s3SecretKey,
        });
        try {
          const probe = s3Client.file(`${s3Key}.probe`);
          await probe.write("ok");
          await probe.delete();
          console.error("[jun] S3 access verified");
        } catch (e) {
          console.error("[jun] error: S3 access failed — check S3_ENDPOINT and credentials");
          console.error(e instanceof Error ? e.message : e);
          process.exit(1);
        }

        s3ExportConfig = { bucket: s3Bucket, key: s3Key };
      }

      // Broadcast overrides
      if (opts.stdout) {
        baseConfig.broadcast = baseConfig.broadcast ?? {};
        baseConfig.broadcast.stdout = true;
      }
      if (opts.sse) {
        baseConfig.broadcast = baseConfig.broadcast ?? {};
        baseConfig.broadcast.sse = { port: parseInt(opts.sse) };
      }
      if (opts.nats) {
        baseConfig.broadcast = baseConfig.broadcast ?? {};
        baseConfig.broadcast.nats = { url: opts.nats };
      }

      // Pass config object directly to parser — no YAML roundtrip
      const { sources, processors, storages, broadcasts, pipelineConfig, resolvedRange } = await parsePipelineConfigFromObject(baseConfig);

      const pipeline = createPipeline();

      pipelineConfig.quiet = opts.quiet ?? false;
      pipelineConfig.log = opts.log ?? false;
      if (opts.batchSize) {
        pipelineConfig.buffer = pipelineConfig.buffer ?? {};
        pipelineConfig.buffer.maxBatchSize = parseInt(opts.batchSize);
      }

      // Set total checkpoints for progress bar
      if (resolvedRange) {
        pipelineConfig.totalCheckpoints = resolvedRange.end - resolvedRange.start + 1n;
      }

      pipeline.configure(pipelineConfig);

      // If backfill-only (--epoch or --start-checkpoint without --grpc-url), skip live sources
      const filteredSources = backfillOnly
        ? sources.filter((s) => s.name !== "live")
        : sources;
      for (const source of filteredSources) pipeline.source(source);
      for (const processor of processors) pipeline.processor(processor);
      for (const storage of storages) pipeline.storage(storage);
      for (const broadcast of broadcasts) pipeline.broadcast(broadcast);

      // Print summary and confirm (use canonical keys from baseConfig)
      const enabledProcessors: string[] = [];
      if (baseConfig.processors?.transactionBlocks) enabledProcessors.push("transaction-blocks");
      if (baseConfig.processors?.balances) enabledProcessors.push("balance-changes");
      if (baseConfig.processors?.events) enabledProcessors.push(`events (${Object.keys(baseConfig.processors.events).length} handlers)`);
      if (baseConfig.processors?.checkpoints) enabledProcessors.push("checkpoints");
      if (baseConfig.processors?.objectChanges) enabledProcessors.push("object-changes");
      if (baseConfig.processors?.dependencies) enabledProcessors.push("dependencies");
      if (baseConfig.processors?.inputs) enabledProcessors.push("inputs");
      if (baseConfig.processors?.commands) enabledProcessors.push("commands");
      if (baseConfig.processors?.systemTransactions) enabledProcessors.push("system-transactions");
      if (baseConfig.processors?.unchangedConsensusObjects) enabledProcessors.push("unchanged-consensus-objects");

      console.error("");
      console.error("  Pipeline Summary");
      console.error("  ────────────────");
      console.error(`  mode            ${backfillOnly ? "snapshot (exit after backfill)" : "continuous (backfill + live)"}`);
      if (baseConfig.sources?.epoch) console.error(`  epoch           ${baseConfig.sources.epoch}`);
      if (resolvedRange) {
        const count = resolvedRange.end - resolvedRange.start + 1n;
        console.error(`  checkpoints     ${resolvedRange.start} → ${resolvedRange.end} (${count.toLocaleString()})`);
      } else if (baseConfig.sources?.startCheckpoint && baseConfig.sources?.endCheckpoint) {
        const count = BigInt(baseConfig.sources.endCheckpoint) - BigInt(baseConfig.sources.startCheckpoint) + 1n;
        console.error(`  checkpoints     ${baseConfig.sources.startCheckpoint} → ${baseConfig.sources.endCheckpoint} (${count.toLocaleString()})`);
      }
      if (enabledProcessors.length > 0) console.error(`  processors      ${enabledProcessors.join(", ")}`);
      if (opts.sqlite) console.error(`  sqlite          ${opts.sqlite}`);
      if (opts.postgres) console.error(`  postgres        ${opts.postgres}`);
      if (opts.sqliteExport) console.error(`  export          ${opts.sqliteExport}`);
      if (opts.sqliteExportSplitDatasets) console.error(`  export split    datasets`);
      if (opts.stdout) console.error(`  broadcast       stdout`);
      if (baseConfig.sources?.concurrency) console.error(`  concurrency     ${baseConfig.sources.concurrency}`);
      if (baseConfig.sources?.workers) console.error(`  workers         ${baseConfig.sources.workers}`);
      if (baseConfig.sources?.archiveUrl || baseConfig.sources?.epoch || baseConfig.sources?.startCheckpoint) {
        const decoderLabel = process.env.JUN_BCS_DECODER === "js"
          ? `JS (${process.env.JUN_BCS_PROVIDER === "unconfirmed" ? "@unconfirmed/bcs" : "@mysten/bcs"})`
          : "native (Rust FFI)";
        console.error(`  decoder         ${decoderLabel}`);
      }
      console.error("");

      if (!opts.yes) {
        process.stderr.write("  Proceed? [Y/n] ");
        const reader = Bun.stdin.stream().getReader();
        const { value } = await reader.read();
        reader.releaseLock();
        const answer = new TextDecoder().decode(value).trim().toLowerCase();
        if (answer && answer !== "y" && answer !== "yes") {
          console.error("  Aborted.");
          process.exit(0);
        }
      }

      const pipelineStart = performance.now();
      await pipeline.run();
      const pipelineElapsed = ((performance.now() - pipelineStart) / 1000).toFixed(1);
      console.error(`[jun] pipeline completed in ${pipelineElapsed}s`);

      // Post-pipeline: S3 export (VACUUM already done by worker during shutdown)
      if (s3ExportConfig && opts.sqlite) {
        const dbPath = resolve(opts.sqlite);
        const s3Client = new Bun.S3Client({
          endpoint: process.env.S3_ENDPOINT!,
          bucket: s3ExportConfig.bucket,
          accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
          secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
        });

        if (opts.sqliteExportSplitDatasets) {
          const { exportSplitSqliteDatasets, deriveSplitSqliteExportKey } = await import("./sqlite-export.ts");
          const datasetExports = exportSplitSqliteDatasets(dbPath);

          try {
            for (const datasetExport of datasetExports) {
              const dbSize = Bun.file(datasetExport.path).size;
              const uploadKey = deriveSplitSqliteExportKey(s3ExportConfig.key, datasetExport.dataset);
              console.error(`[jun] ${datasetExport.dataset} export size: ${(dbSize / 1024 / 1024).toFixed(1)} MB`);
              console.error(`[jun] uploading ${datasetExport.dataset} to s3://${s3ExportConfig.bucket}/${uploadKey}...`);
              const s3File = s3Client.file(uploadKey);
              await s3File.write(Bun.file(datasetExport.path));
            }
            console.error("[jun] split dataset exports uploaded successfully");
          } finally {
            const { unlinkSync } = await import("fs");
            for (const datasetExport of datasetExports) {
              for (const candidate of [datasetExport.path, `${datasetExport.path}-wal`, `${datasetExport.path}-shm`]) {
                try {
                  unlinkSync(candidate);
                } catch {}
              }
            }
          }
        } else {
          const dbSize = Bun.file(dbPath).size;
          console.error(`[jun] database size: ${(dbSize / 1024 / 1024).toFixed(1)} MB`);
          console.error(`[jun] uploading to s3://${s3ExportConfig.bucket}/${s3ExportConfig.key}...`);
          const s3File = s3Client.file(s3ExportConfig.key);
          await s3File.write(Bun.file(dbPath));
          console.error(`[jun] uploaded successfully`);
        }
      }

      // Snapshot mode: exit explicitly (workers/timers may keep event loop alive)
      if (backfillOnly) {
        process.exit(0);
      }
    } catch (err) {
      cliError(err);
    }
}

addPipelineOptions(pipelineCmd.command("run [config]").description("Run a continuous pipeline (backfill + live)"))
  .action(async (configFile: string | undefined, opts: PipelineOpts) => {
    await runPipeline(configFile, opts, false);
  });

addPipelineOptions(pipelineCmd.command("snapshot [config]").description("Backfill a range or epoch, write to storage, exit"))
  .action(async (configFile: string | undefined, opts: PipelineOpts) => {
    await runPipeline(configFile, opts, true);
  });

program.parse();
