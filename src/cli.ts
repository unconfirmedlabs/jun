#!/usr/bin/env bun
/**
 * Jun CLI — Sui checkpoint stream explorer.
 *
 * Usage:
 *   jun stream                                    # stream all checkpoint data
 *   jun stream --include events                   # only events
 *   jun stream --include events --filter marketplace::ItemListed
 *   jun stream --jsonl                             # JSON lines output
 *   jun stream --jsonl --output events.jsonl       # write to file
 *   jun stream --until-checkpoint 318200000       # stop at checkpoint
 *   jun stream --duration 30s                     # stream for 30 seconds
 *   jun stream --until "2026-03-28T00:00:00Z"     # stop at timestamp
 */
import { program } from "commander";
import pMap from "p-map";
import pRetry from "p-retry";
import { createGrpcClient, type GrpcCheckpointResponse, type GrpcEvent } from "./grpc.ts";
import { createArchiveClient } from "./archive.ts";
import { generateFieldDSL, formatCodegenResult } from "./codegen.ts";
import { createSqliteWriter, type SqliteWriter } from "./output/sqlite.ts";

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
// Formatting helpers
// ---------------------------------------------------------------------------

function getTimestampMs(summary: GrpcCheckpointResponse["checkpoint"]["summary"]): number {
  if (!summary?.timestamp) return Date.now();
  return Number(BigInt(summary.timestamp.seconds) * 1000n + BigInt(Math.floor(summary.timestamp.nanos / 1_000_000)));
}

function formatTimestamp(ms: number): string {
  return new Date(ms).toISOString();
}

function formatEvent(ev: GrpcEvent, txDigest: string): string {
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
  .description("Sui checkpoint stream explorer")
  .version("0.1.0");

program
  .command("stream")
  .description("Stream live checkpoints")
  .option("--url <url>", "gRPC endpoint URL", "hayabusa.mainnet.unconfirmed.cloud:443")
  .option("--include <types...>", "data to include: events, effects, balance-changes, objects, transactions (default: all)")
  .option("--filter <type>", "only show events matching this type substring")
  .option("--jsonl", "output as JSON lines instead of formatted text", false)
  .option("--output <path>", "write output to file (implies --jsonl)")
  .option("--until-checkpoint <seq>", "stop after reaching this checkpoint")
  .option("--until <timestamp>", "stop after this ISO timestamp (e.g. 2026-03-28T00:00:00Z)")
  .option("--duration <time>", "stream for this duration then stop (e.g. 30s, 5m, 1h)")
  .action(async (opts: {
    url: string;
    include?: string[];
    filter?: string;
    jsonl: boolean;
    output?: string;
    untilCheckpoint?: string;
    until?: string;
    duration?: string;
  }) => {
    const includes = opts.include ?? [];
    const readMask = buildReadMask(includes);
    const showEvents = includes.length === 0 || includes.includes("events");
    const showEffects = includes.length === 0 || includes.includes("effects");
    const showBalanceChanges = includes.length === 0 || includes.includes("balance-changes");
    const isSqlite = opts.output?.endsWith(".sqlite") ?? false;
    const useJson = (opts.jsonl || !!opts.output) && !isSqlite;
    const stop = parseStopConditions(opts);
    const jsonWriter = !isSqlite ? createWriter(opts.output) : null;
    const sqliteWriter = isSqlite
      ? createSqliteWriter({ path: opts.output!, showEvents, showBalanceChanges })
      : null;

    const client = createGrpcClient({ url: opts.url, readMask });

    let stopped = false;
    let checkpointCount = 0;
    const shutdown = () => {
      if (stopped) process.exit(1);
      stopped = true;
      console.error(`\n[jun] stopping... (${checkpointCount} checkpoints streamed)`);
      jsonWriter?.close();
      sqliteWriter?.close();
      client.close();
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);

    console.error(`[jun] streaming from ${opts.url}`);
    if (includes.length > 0) console.error(`[jun] include: ${includes.join(", ")}`);
    else console.error(`[jun] include: all`);
    if (opts.filter) console.error(`[jun] filter: ${opts.filter}`);
    if (stop.untilCheckpoint) console.error(`[jun] until checkpoint: ${stop.untilCheckpoint}`);
    if (stop.untilTimestamp) console.error(`[jun] until: ${formatTimestamp(stop.untilTimestamp)}`);
    if (stop.deadline) console.error(`[jun] duration: ${opts.duration}`);
    if (opts.output) console.error(`[jun] output: ${opts.output}`);
    console.error("");

    try {
      for await (const response of client.subscribeCheckpoints()) {
        if (stopped) break;

        const { checkpoint } = response;
        const seq = response.cursor;
        const tsMs = getTimestampMs(checkpoint.summary);
        const ts = formatTimestamp(tsMs);
        const txCount = checkpoint.transactions?.length ?? 0;

        // Check stop conditions (O(1) comparisons)
        if (shouldStop(stop, seq, tsMs)) {
          console.error(`[jun] stop condition reached at checkpoint ${seq}`);
          break;
        }

        checkpointCount++;

        // Collect data from transactions
        const events: Array<{ event: GrpcEvent; txDigest: string }> = [];
        const balanceChanges: Array<{ change: any; txDigest: string }> = [];
        const effects: Array<{ effects: any; txDigest: string }> = [];

        for (const tx of checkpoint.transactions ?? []) {
          const txDigest = tx.digest;

          if (showEvents && tx.events?.events?.length) {
            for (const ev of tx.events.events) {
              events.push({ event: ev, txDigest });
            }
          }

          if (showBalanceChanges) {
            const bcs = (tx as any).balanceChanges ?? [];
            for (const bc of bcs) {
              balanceChanges.push({ change: bc, txDigest });
            }
          }

          if (showEffects) {
            const eff = (tx as any).effects;
            if (eff && Object.keys(eff).length > 0) {
              effects.push({ effects: eff, txDigest });
            }
          }
        }

        // Apply event filter
        let filteredEvents = events;
        if (opts.filter) {
          filteredEvents = events.filter((e) => e.event.eventType.includes(opts.filter!));
        }

        // Skip empty checkpoints when filtering
        const hasData = filteredEvents.length > 0 || balanceChanges.length > 0 || effects.length > 0;
        if (!hasData && (opts.filter || includes.length > 0)) continue;
        if (txCount === 0) continue;

        // SQLite output — write full checkpoint data
        if (sqliteWriter) {
          const txData = (checkpoint.transactions ?? []).map((tx: any) => ({
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

          // Log progress periodically
          if (checkpointCount % 100 === 0) {
            console.error(`[jun] ${checkpointCount} checkpoints → ${opts.output}`);
          }
          continue;
        }

        if (useJson) {
          const record: any = { checkpoint: seq, timestamp: ts, txCount };
          if (filteredEvents.length > 0) {
            record.events = filteredEvents.map((e) => ({
              type: e.event.eventType,
              sender: e.event.sender,
              tx: e.txDigest,
              bcsBytes: e.event.contents?.value?.length ?? 0,
            }));
          }
          if (balanceChanges.length > 0) {
            record.balanceChanges = balanceChanges.map((bc) => ({
              ...bc.change,
              tx: bc.txDigest,
            }));
          }
          if (effects.length > 0) {
            record.effects = effects.map((e) => ({
              status: e.effects.status,
              tx: e.txDigest,
            }));
          }
          writer.write(JSON.stringify(record));
        } else {
          console.log(`── checkpoint ${seq} ── ${ts} ── ${txCount} tx(s) ──`);

          if (filteredEvents.length > 0) {
            console.log(`  events (${filteredEvents.length}):`);
            for (const { event, txDigest } of filteredEvents) {
              console.log(formatEvent(event, txDigest));
              console.log("");
            }
          }

          if (effects.length > 0) {
            console.log(`  effects (${effects.length}):`);
            for (const { effects: eff, txDigest } of effects) {
              console.log(formatEffect(eff, txDigest));
              console.log("");
            }
          }

          if (balanceChanges.length > 0) {
            console.log(`  balance changes (${balanceChanges.length}):`);
            for (const { change, txDigest } of balanceChanges) {
              console.log(formatBalanceChange(change, txDigest));
              console.log("");
            }
          }
        }
      }
    } catch (err) {
      if (!stopped) {
        const msg = err instanceof Error ? err.message : String(err);
        console.error(`[jun] error: ${msg}`);
        process.exit(1);
      }
    }

    jsonWriter?.close();
    sqliteWriter?.close();
    client.close();
    console.error(`[jun] done (${checkpointCount} checkpoints streamed)`);
  });

// ---------------------------------------------------------------------------
// Fetch command
// ---------------------------------------------------------------------------

program
  .command("fetch")
  .description("Fetch historical checkpoints by range")
  .requiredOption("--from <checkpoint>", "start checkpoint (inclusive)")
  .option("--to <checkpoint>", "end checkpoint (inclusive)")
  .option("--count <n>", "number of checkpoints to fetch (alternative to --to)")
  .option("--url <url>", "gRPC endpoint URL", "hayabusa.mainnet.unconfirmed.cloud:443")
  .option("--include <types...>", "data to include: events, effects, balance-changes, objects, transactions (default: all)")
  .option("--filter <type>", "only show events matching this type substring")
  .option("--jsonl", "output as JSON lines instead of formatted text", false)
  .option("--output <path>", "write output to file (.sqlite or .jsonl)")
  .option("--concurrency <n>", "concurrent checkpoint fetches", "16")
  .action(async (opts: {
    from: string;
    to?: string;
    count?: string;
    url: string;
    include?: string[];
    filter?: string;
    jsonl: boolean;
    output?: string;
    concurrency: string;
  }) => {
    const from = BigInt(opts.from);
    const concurrency = parseInt(opts.concurrency);

    let to: bigint;
    if (opts.to) {
      to = BigInt(opts.to);
    } else if (opts.count) {
      to = from + BigInt(opts.count) - 1n;
    } else {
      console.error("[jun] either --to or --count is required");
      process.exit(1);
    }

    if (to < from) {
      console.error(`[jun] --to (${to}) must be >= --from (${from})`);
      process.exit(1);
    }

    const total = Number(to - from) + 1;
    const includes = opts.include ?? [];
    const readMask = buildReadMask(includes);
    const showEvents = includes.length === 0 || includes.includes("events");
    const showEffects = includes.length === 0 || includes.includes("effects");
    const showBalanceChanges = includes.length === 0 || includes.includes("balance-changes");
    const isSqlite = opts.output?.endsWith(".sqlite") ?? false;
    const useJson = (opts.jsonl || !!opts.output) && !isSqlite;
    const jsonWriter = !isSqlite ? createWriter(opts.output) : null;
    const sqliteWriter = isSqlite
      ? createSqliteWriter({ path: opts.output!, showEvents, showBalanceChanges })
      : null;

    const client = createGrpcClient({ url: opts.url, readMask });

    let stopped = false;
    const shutdown = () => {
      if (stopped) process.exit(1);
      stopped = true;
      console.error("\n[jun] stopping...");
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);

    console.error(`[jun] fetching checkpoints ${from} → ${to} (${total.toLocaleString()} checkpoints, concurrency ${concurrency})`);
    if (includes.length > 0) console.error(`[jun] include: ${includes.join(", ")}`);
    if (opts.filter) console.error(`[jun] filter: ${opts.filter}`);
    if (opts.output) console.error(`[jun] output: ${opts.output}`);
    console.error("");

    const startTime = performance.now();
    let processed = 0;
    let totalEvents = 0;
    let lastLogTime = startTime;

    // Build array of checkpoint sequence numbers
    const seqs: bigint[] = [];
    for (let s = from; s <= to; s++) seqs.push(s);

    try {
      await pMap(seqs, async (seq) => {
        if (stopped) throw new pRetry.AbortError("stopped");

        const response = await pRetry(
          () => client.getCheckpoint(seq),
          {
            retries: 5,
            minTimeout: 1000,
            factor: 2,
            onFailedAttempt: (err) => {
              if (stopped) throw new pRetry.AbortError("stopped");
              const inner = err.cause ?? err;
              const msg = inner?.details ?? inner?.message ?? String(inner);
              console.error(`[jun] checkpoint ${seq} attempt ${err.attemptNumber} failed: ${msg}`);
            },
          },
        );

        const { checkpoint } = response;
        const tsMs = getTimestampMs(checkpoint.summary);
        const ts = formatTimestamp(tsMs);
        const txCount = checkpoint.transactions?.length ?? 0;

        // Collect data
        const events: Array<{ event: GrpcEvent; txDigest: string }> = [];
        const balanceChanges: Array<{ change: any; txDigest: string }> = [];
        const effects: Array<{ effects: any; txDigest: string }> = [];

        for (const tx of checkpoint.transactions ?? []) {
          const txDigest = tx.digest;
          if (showEvents && tx.events?.events?.length) {
            for (const ev of tx.events.events) {
              events.push({ event: ev, txDigest });
            }
          }
          if (showBalanceChanges) {
            for (const bc of (tx as any).balanceChanges ?? []) {
              balanceChanges.push({ change: bc, txDigest });
            }
          }
          if (showEffects) {
            const eff = (tx as any).effects;
            if (eff && Object.keys(eff).length > 0) {
              effects.push({ effects: eff, txDigest });
            }
          }
        }

        let filteredEvents = events;
        if (opts.filter) {
          filteredEvents = events.filter((e) => e.event.eventType.includes(opts.filter!));
        }

        totalEvents += filteredEvents.length;

        // SQLite output
        if (sqliteWriter) {
          const txData = (checkpoint.transactions ?? []).map((tx: any) => ({
            digest: tx.digest,
            sender: tx.transaction?.sender,
            status: tx.effects?.status?.success ? "success" : tx.effects?.status ? "failure" : null,
            gasComputation: tx.effects?.gasUsed?.computationCost ? parseInt(tx.effects.gasUsed.computationCost) : null,
            gasStorage: tx.effects?.gasUsed?.storageCost ? parseInt(tx.effects.gasUsed.storageCost) : null,
            gasRebate: tx.effects?.gasUsed?.storageRebate ? parseInt(tx.effects.gasUsed.storageRebate) : null,
            events: tx.events?.events ?? [],
            balanceChanges: tx.balanceChanges ?? [],
          }));
          sqliteWriter.writeCheckpoint({ seq: response.cursor, timestamp: ts, transactions: txData });
        }

        // JSONL output
        if (useJson) {
          const hasData = filteredEvents.length > 0 || balanceChanges.length > 0 || effects.length > 0;
          if (hasData || (!opts.filter && includes.length === 0)) {
            const record: any = { checkpoint: response.cursor, timestamp: ts, txCount };
            if (filteredEvents.length > 0) {
              record.events = filteredEvents.map((e) => ({
                type: e.event.eventType,
                sender: e.event.sender,
                tx: e.txDigest,
                bcsBytes: e.event.contents?.value?.length ?? 0,
              }));
            }
            if (balanceChanges.length > 0) {
              record.balanceChanges = balanceChanges.map((bc) => ({ ...bc.change, tx: bc.txDigest }));
            }
            if (effects.length > 0) {
              record.effects = effects.map((e) => ({ status: e.effects.status, tx: e.txDigest }));
            }
            jsonWriter!.write(JSON.stringify(record));
          }
        }

        // Progress logging
        processed++;
        const now = performance.now();
        if (now - lastLogTime >= 3000 || processed === total) {
          const elapsed = (now - startTime) / 1000;
          const rate = Math.round(processed / elapsed);
          const eta = total > processed ? Math.round((total - processed) / rate) : 0;
          const pct = Math.round((processed / total) * 100);
          console.error(`[jun] ${processed.toLocaleString()}/${total.toLocaleString()} (${pct}%) ${rate} cp/s, ${totalEvents} events, ETA ${eta}s`);
          lastLogTime = now;
        }
      }, { concurrency, signal: stopped ? AbortSignal.abort() : undefined });
    } catch (err) {
      if (!stopped) {
        const msg = err instanceof Error ? err.message : String(err);
        console.error(`[jun] error: ${msg}`);
      }
    }

    jsonWriter?.close();
    sqliteWriter?.close();
    client.close();

    const elapsed = ((performance.now() - startTime) / 1000).toFixed(1);
    console.error(`[jun] done — ${processed.toLocaleString()} checkpoints, ${totalEvents} events in ${elapsed}s`);
  });

// ---------------------------------------------------------------------------
// Replay command
// ---------------------------------------------------------------------------

program
  .command("replay")
  .description("Replay historical checkpoints from the archive (genesis to present)")
  .requiredOption("--from <checkpoint>", "start checkpoint (inclusive)")
  .option("--to <checkpoint>", "end checkpoint (inclusive)")
  .option("--count <n>", "number of checkpoints to replay (alternative to --to)")
  .option("--archive-url <url>", "checkpoint archive URL", "https://checkpoints.mainnet.sui.io")
  .option("--include <types...>", "data to include: events (default: all)")
  .option("--filter <type>", "only show events matching this type substring")
  .option("--jsonl", "output as JSON lines instead of formatted text", false)
  .option("--output <path>", "write output to file (.sqlite or .jsonl)")
  .option("--concurrency <n>", "concurrent checkpoint fetches", "16")
  .option("--verify", "cryptographically verify each checkpoint signature", false)
  .option("--verify-url <url>", "gRPC URL for fetching validator committees (for --verify)", "hayabusa.mainnet.unconfirmed.cloud:443")
  .action(async (opts: {
    from: string;
    to?: string;
    count?: string;
    archiveUrl: string;
    include?: string[];
    filter?: string;
    jsonl: boolean;
    output?: string;
    concurrency: string;
    verify: boolean;
    verifyUrl: string;
  }) => {
    const from = BigInt(opts.from);
    const concurrency = parseInt(opts.concurrency);

    let to: bigint;
    if (opts.to) {
      to = BigInt(opts.to);
    } else if (opts.count) {
      to = from + BigInt(opts.count) - 1n;
    } else {
      console.error("[jun] either --to or --count is required");
      process.exit(1);
    }

    if (to < from) {
      console.error(`[jun] --to (${to}) must be >= --from (${from})`);
      process.exit(1);
    }

    const total = Number(to - from) + 1;
    const includes = opts.include ?? [];
    const showEvents = includes.length === 0 || includes.includes("events");
    const isSqlite = opts.output?.endsWith(".sqlite") ?? false;
    const useJson = (opts.jsonl || !!opts.output) && !isSqlite;
    const jsonWriter = !isSqlite ? createWriter(opts.output) : null;
    const sqliteWriter = isSqlite
      ? createSqliteWriter({ path: opts.output!, showEvents, showEffects: false, showBalanceChanges: false })
      : null;

    const archive = createArchiveClient({
      archiveUrl: opts.archiveUrl,
      verify: opts.verify,
      grpcUrl: opts.verify ? opts.verifyUrl : undefined,
    });

    let stopped = false;
    const shutdown = () => {
      if (stopped) process.exit(1);
      stopped = true;
      console.error("\n[jun] stopping...");
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);

    console.error(`[jun] replaying checkpoints ${from} → ${to} (${total.toLocaleString()} checkpoints, concurrency ${concurrency})`);
    console.error(`[jun] archive: ${opts.archiveUrl}`);
    if (opts.filter) console.error(`[jun] filter: ${opts.filter}`);
    if (opts.output) console.error(`[jun] output: ${opts.output}`);
    console.error("");

    const startTime = performance.now();
    let processed = 0;
    let totalEvents = 0;
    let lastLogTime = startTime;

    const seqs: bigint[] = [];
    for (let s = from; s <= to; s++) seqs.push(s);

    try {
      await pMap(seqs, async (seq) => {
        if (stopped) throw new pRetry.AbortError("stopped");

        const response = await pRetry(
          () => archive.fetchCheckpoint(seq),
          {
            retries: 5,
            minTimeout: 1000,
            factor: 2,
            onFailedAttempt: (err) => {
              if (stopped) throw new pRetry.AbortError("stopped");
              const inner = err.cause ?? err;
              const msg = inner?.message ?? String(inner);
              console.error(`[jun] checkpoint ${seq} attempt ${err.attemptNumber} failed: ${msg}`);
            },
          },
        );

        const { checkpoint } = response;
        const tsMs = getTimestampMs(checkpoint.summary);
        const ts = formatTimestamp(tsMs);

        // Collect events
        const events: Array<{ event: GrpcEvent; txDigest: string }> = [];
        if (showEvents) {
          for (const tx of checkpoint.transactions ?? []) {
            for (const ev of tx.events?.events ?? []) {
              events.push({ event: ev, txDigest: tx.digest });
            }
          }
        }

        let filteredEvents = events;
        if (opts.filter) {
          filteredEvents = events.filter((e) => e.event.eventType.includes(opts.filter!));
        }
        totalEvents += filteredEvents.length;

        // SQLite output
        if (sqliteWriter) {
          const txData = (checkpoint.transactions ?? []).map((tx: any) => ({
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
        }

        // JSONL output
        if (useJson && filteredEvents.length > 0) {
          const record: any = {
            checkpoint: response.cursor,
            timestamp: ts,
            events: filteredEvents.map((e) => ({
              type: e.event.eventType,
              sender: e.event.sender,
              tx: e.txDigest,
              bcsBytes: e.event.contents?.value?.length ?? 0,
            })),
          };
          jsonWriter!.write(JSON.stringify(record));
        }

        // Text output
        if (!useJson && !sqliteWriter && filteredEvents.length > 0) {
          console.log(`── checkpoint ${response.cursor} ── ${ts} ── ${filteredEvents.length} event(s) ──`);
          for (const { event, txDigest } of filteredEvents) {
            console.log(formatEvent(event, txDigest));
            console.log("");
          }
        }

        // Progress
        processed++;
        const now = performance.now();
        if (now - lastLogTime >= 3000 || processed === total) {
          const elapsed = (now - startTime) / 1000;
          const rate = Math.round(processed / elapsed);
          const eta = total > processed ? Math.round((total - processed) / rate) : 0;
          const pct = Math.round((processed / total) * 100);
          console.error(`[jun] ${processed.toLocaleString()}/${total.toLocaleString()} (${pct}%) ${rate} cp/s, ${totalEvents} events, ETA ${eta}s`);
          lastLogTime = now;
        }
      }, { concurrency });
    } catch (err) {
      if (!stopped) {
        const msg = err instanceof Error ? err.message : String(err);
        console.error(`[jun] error: ${msg}`);
      }
    }

    jsonWriter?.close();
    sqliteWriter?.close();

    const elapsed = ((performance.now() - startTime) / 1000).toFixed(1);
    console.error(`[jun] done — ${processed.toLocaleString()} checkpoints, ${totalEvents} events in ${elapsed}s`);
  });

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
  .option("--url <url>", "JSON-RPC endpoint", "https://fullnode.mainnet.sui.io:443")
  .option("--archive-url <url>", "checkpoint archive URL", "https://checkpoints.mainnet.sui.io")
  .option("--grpc-url <url>", "gRPC endpoint for committee fetching", "hayabusa.mainnet.unconfirmed.cloud:443")
  .action(async (digest: string, opts: { url: string; archiveUrl: string; grpcUrl: string }) => {
    const { verifyTransaction } = await import("./verify.ts");

    try {
      const result = await verifyTransaction(digest, {
        rpcUrl: opts.url,
        archiveUrl: opts.archiveUrl,
        grpcUrl: opts.grpcUrl,
      });

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
      console.error(`\n  \u2717 ${msg}\n`);
      process.exit(1);
    }
  });

verify
  .command("object")
  .alias("obj")
  .description("Verify an object's state against its last modifying transaction")
  .argument("<id>", "object ID (0x...)")
  .option("--url <url>", "JSON-RPC endpoint", "https://fullnode.mainnet.sui.io:443")
  .option("--archive-url <url>", "checkpoint archive URL", "https://checkpoints.mainnet.sui.io")
  .option("--grpc-url <url>", "gRPC endpoint for committee fetching", "hayabusa.mainnet.unconfirmed.cloud:443")
  .action(async (id: string, opts: { url: string; archiveUrl: string; grpcUrl: string }) => {
    const { verifyObject } = await import("./verify.ts");

    try {
      const result = await verifyObject(id, {
        rpcUrl: opts.url,
        archiveUrl: opts.archiveUrl,
        grpcUrl: opts.grpcUrl,
      });

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
  .action(async (msg: string, signature: string, address: string) => {
    const { verifyMessage } = await import("./message.ts");

    try {
      const result = await verifyMessage(msg, signature, address);
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
  .description("Query Sui objects and transactions");

client
  .command("object")
  .alias("obj")
  .description("Get object data by ID")
  .argument("<id>", "object ID (0x...)")
  .option("--bcs", "return BCS-serialized object data", false)
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "JSON-RPC endpoint", "https://fullnode.mainnet.sui.io:443")
  .action(async (id: string, opts: { bcs: boolean; json: boolean; url: string }) => {
    const { jsonRpc } = await import("./rpc.ts");

    try {
      const options = opts.bcs
        ? { showBcs: true, showType: true, showOwner: true, showPreviousTransaction: true, showStorageRebate: true }
        : { showType: true, showOwner: true, showPreviousTransaction: true, showContent: true, showStorageRebate: true };

      const result = (await jsonRpc("sui_getObject", [id, options], opts.url)) as {
        data?: {
          objectId: string;
          version: string;
          digest: string;
          type?: string;
          owner?: { AddressOwner?: string; ObjectOwner?: string; Shared?: { initial_shared_version: string }; Immutable?: boolean };
          previousTransaction?: string;
          storageRebate?: string;
          content?: { type: string; fields: Record<string, unknown> };
          bcs?: { bcsBytes: string; type: string };
        };
        error?: { code: string; message?: string };
      };

      if (result?.error) throw new Error(result.error.message ?? result.error.code);
      if (!result?.data) throw new Error(`Object ${id} not found`);

      const obj = result.data;

      if (opts.json) {
        console.log(JSON.stringify(obj, null, 2));
        return;
      }

      const ownerStr = obj.owner
        ? obj.owner.AddressOwner ?? obj.owner.ObjectOwner
          ?? (obj.owner.Shared ? `Shared (v${obj.owner.Shared.initial_shared_version})` : null)
          ?? (obj.owner.Immutable ? "Immutable" : "unknown")
        : "unknown";

      console.log(`\n  object    ${obj.objectId}`);
      if (obj.type) console.log(`  type      ${obj.type}`);
      console.log(`  version   ${obj.version}`);
      console.log(`  digest    ${obj.digest}`);
      console.log(`  owner     ${ownerStr}`);
      if (obj.previousTransaction) console.log(`  prev tx   ${obj.previousTransaction}`);
      if (obj.storageRebate) console.log(`  rebate    ${obj.storageRebate} MIST`);

      if (obj.content?.fields && !opts.bcs) {
        console.log(`\n  content (${obj.content.type}):`);
        for (const [key, val] of Object.entries(obj.content.fields)) {
          const display = typeof val === "object" ? JSON.stringify(val) : String(val);
          const truncated = display.length > 80 ? display.slice(0, 77) + "..." : display;
          console.log(`    ${key}: ${truncated}`);
        }
      }

      if (obj.bcs && opts.bcs) {
        console.log(`\n  bcs (${obj.bcs.bcsBytes.length} chars base64, type: ${obj.bcs.type})`);
      }

      console.log("");
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`[jun] error: ${msg}`);
      process.exit(1);
    }
  });

client
  .command("tx-block")
  .alias("txb")
  .description("Get transaction block data by digest")
  .argument("<digest>", "transaction digest (base58)")
  .option("--json", "output raw JSON", false)
  .option("--url <url>", "JSON-RPC endpoint", "https://fullnode.mainnet.sui.io:443")
  .action(async (digest: string, opts: { json: boolean; url: string }) => {
    const { jsonRpc } = await import("./rpc.ts");

    try {
      const result = (await jsonRpc("sui_getTransactionBlock", [digest, {
        showInput: true,
        showEffects: true,
        showEvents: true,
        showObjectChanges: true,
        showBalanceChanges: true,
      }], opts.url)) as {
        digest: string;
        checkpoint?: string;
        timestampMs?: string;
        transaction?: { data?: { sender?: string; transaction?: { kind?: string } }; txSignatures?: string[] };
        effects?: { status?: { status: string }; gasUsed?: { computationCost: string; storageCost: string; storageRebate: string; nonRefundableStorageFee: string } };
        events?: Array<{ type: string; sender: string; parsedJson?: Record<string, unknown> }>;
        objectChanges?: Array<{ type: string; objectType?: string; objectId?: string; sender?: string }>;
        balanceChanges?: Array<{ owner: { AddressOwner?: string }; coinType: string; amount: string }>;
      };

      if (!result) throw new Error(`Transaction ${digest} not found`);

      if (opts.json) {
        console.log(JSON.stringify(result, null, 2));
        return;
      }

      const status = result.effects?.status?.status ?? "unknown";
      const gas = result.effects?.gasUsed;
      const sender = result.transaction?.data?.sender ?? "unknown";
      const ts = result.timestampMs ? new Date(Number(result.timestampMs)).toISOString() : null;

      console.log(`\n  digest      ${result.digest}`);
      console.log(`  status      ${status}`);
      console.log(`  sender      ${sender}`);
      if (result.checkpoint) console.log(`  checkpoint  ${result.checkpoint}`);
      if (ts) console.log(`  timestamp   ${ts}`);
      if (gas) {
        const total = BigInt(gas.computationCost) + BigInt(gas.storageCost) - BigInt(gas.storageRebate);
        console.log(`  gas         ${total} MIST (${gas.computationCost} compute, ${gas.storageCost} storage, -${gas.storageRebate} rebate)`);
      }

      if (result.events?.length) {
        console.log(`\n  events (${result.events.length}):`);
        for (const ev of result.events) {
          console.log(`    ${ev.type}`);
        }
      }

      if (result.objectChanges?.length) {
        console.log(`\n  object changes (${result.objectChanges.length}):`);
        for (const oc of result.objectChanges) {
          const id = oc.objectId ? oc.objectId.slice(0, 18) + "..." : "";
          const typ = oc.objectType ?? "";
          console.log(`    ${oc.type.padEnd(10)} ${id}  ${typ}`);
        }
      }

      if (result.balanceChanges?.length) {
        console.log(`\n  balance changes (${result.balanceChanges.length}):`);
        for (const bc of result.balanceChanges) {
          const owner = bc.owner.AddressOwner?.slice(0, 18) + "..." ?? "unknown";
          console.log(`    ${owner}  ${bc.amount} ${bc.coinType}`);
        }
      }

      console.log("");
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`[jun] error: ${msg}`);
      process.exit(1);
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
  .option("--url <url>", "JSON-RPC endpoint", "https://fullnode.mainnet.sui.io:443")
  .action(async (name: string, opts: { url: string }) => {
    const { jsonRpc } = await import("./rpc.ts");
    try {
      const address = (await jsonRpc("suix_resolveNameServiceAddress", [name], opts.url)) as string | null;
      if (!address) throw new Error(`Name "${name}" not found`);
      console.log(`\n  ${name} \u2192 ${address}\n`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`[jun] error: ${msg}`);
      process.exit(1);
    }
  });

ns
  .command("resolve-name")
  .description("Resolve an address to its SuiNS name")
  .argument("<address>", "Sui address (0x...)")
  .option("--url <url>", "JSON-RPC endpoint", "https://fullnode.mainnet.sui.io:443")
  .action(async (address: string, opts: { url: string }) => {
    const { jsonRpc } = await import("./rpc.ts");
    try {
      const result = (await jsonRpc("suix_resolveNameServiceNames", [address], opts.url)) as { data?: string[] };
      if (!result?.data?.length) throw new Error(`No SuiNS name for ${address}`);
      for (const name of result.data) {
        console.log(`\n  ${address} \u2192 ${name}\n`);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`[jun] error: ${msg}`);
      process.exit(1);
    }
  });

// ---------------------------------------------------------------------------
// Codegen command
// ---------------------------------------------------------------------------

program
  .command("codegen")
  .description("Generate jun field DSL from an on-chain Sui struct")
  .argument("<type>", "fully qualified type (e.g. 0xPACKAGE::module::StructName)")
  .option("--url <url>", "gRPC endpoint URL", "hayabusa.mainnet.unconfirmed.cloud:443")
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
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`[jun] error: ${msg}`);
      process.exit(1);
    } finally {
      client.close();
    }
  });

// ---------------------------------------------------------------------------
// MCP command
// ---------------------------------------------------------------------------

program
  .command("mcp")
  .description("Start MCP server for AI-assisted analysis of a SQLite database")
  .argument("<db>", "path to SQLite database file")
  .action(async (dbPath: string) => {
    const { createMcpServer } = await import("./mcp.ts");
    const { StdioServerTransport } = await import("@modelcontextprotocol/sdk/server/stdio.js");

    const { server } = createMcpServer(dbPath);
    const transport = new StdioServerTransport();
    await server.connect(transport);
  });

// ---------------------------------------------------------------------------
// Chat command
// ---------------------------------------------------------------------------

program
  .command("chat")
  .description("Stream or replay checkpoints and open an AI chat to analyze the data live")
  .option("--live", "stream live checkpoints (grows in real-time)", false)
  .option("--url <url>", "gRPC endpoint for live streaming", "hayabusa.mainnet.unconfirmed.cloud:443")
  .option("--from <checkpoint>", "start checkpoint for replay")
  .option("--to <checkpoint>", "end checkpoint for replay")
  .option("--count <n>", "number of checkpoints for replay", "100")
  .option("--archive-url <url>", "checkpoint archive URL", "https://checkpoints.mainnet.sui.io")
  .option("--concurrency <n>", "concurrent checkpoint fetches for replay", "32")
  .option("--verify", "cryptographically verify each checkpoint", false)
  .option("--verify-url <url>", "gRPC URL for committee fetching", "hayabusa.mainnet.unconfirmed.cloud:443")
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
    if (!opts.live && !opts.from) {
      console.error("[jun] either --live or --from is required");
      console.error("  jun chat --live                         # stream live data");
      console.error("  jun chat --from 259063382 --count 100   # replay historical");
      process.exit(1);
    }

    const { createSqliteWriter } = await import("./output/sqlite.ts");
    const { tmpdir } = await import("os");
    const { join } = await import("path");
    const { spawn } = await import("child_process");
    const { unlinkSync, writeFileSync } = await import("fs");

    const dbPath = join(tmpdir(), `jun-chat-${Date.now()}.sqlite`);
    const mcpConfigPath = join(tmpdir(), `jun-mcp-${Date.now()}.json`);
    const sqliteWriter = createSqliteWriter({ path: dbPath, showEvents: true, showBalanceChanges: opts.live });

    let description: string;

    if (opts.live) {
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

            const txData = (checkpoint.transactions ?? []).map((tx: any) => ({
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

    } else {
      // ── Replay mode: fetch all data first, then launch chat ──
      const { createArchiveClient } = await import("./archive.ts");
      const from = BigInt(opts.from!);
      const to = opts.to ? BigInt(opts.to) : from + BigInt(opts.count) - 1n;
      const concurrency = parseInt(opts.concurrency);
      const total = Number(to - from) + 1;

      description = `${total.toLocaleString()} checkpoints (${from} → ${to})`;
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

      await pMap(seqs, async (seq) => {
        const response = await pRetry(() => archive.fetchCheckpoint(seq), { retries: 3, minTimeout: 1000 });
        const { checkpoint } = response;
        const tsMs = getTimestampMs(checkpoint.summary);
        const ts = formatTimestamp(tsMs);

        const txData = (checkpoint.transactions ?? []).map((tx: any) => ({
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
        if (processed % 50 === 0 || processed === total) {
          const rate = Math.round(processed / ((performance.now() - startTime) / 1000));
          console.error(`[jun] ${processed}/${total} (${rate} cp/s)`);
        }
      }, { concurrency });

      const elapsed = ((performance.now() - startTime) / 1000).toFixed(1);
      console.error(`[jun] replay done in ${elapsed}s`);
    }

    // Write temp MCP config
    const junDir = join(import.meta.dir, "..");
    const mcpConfig = {
      mcpServers: {
        jun: {
          command: "bun",
          args: [join(junDir, "src", "mcp.ts"), dbPath],
        },
      },
    };
    writeFileSync(mcpConfigPath, JSON.stringify(mcpConfig));

    // Build system prompt
    const liveNote = opts.live
      ? "The database is being written to in REAL-TIME by a live gRPC stream. New checkpoints arrive every ~250ms. Run queries multiple times to see fresh data."
      : "";

    const systemPrompt = [
      `You have access to a Sui blockchain SQLite database: ${description}.`,
      liveNote,
      `Use the "jun" MCP server tools to analyze the data.`,
      `Start by calling the "summary" tool to get an overview, then use "query" for specific SQL queries.`,
      `Tables: transactions (digest, checkpoint, timestamp, sender, status, gas_computation, gas_storage, gas_rebate), events (tx_digest, event_seq, event_type, package_id, module, sender, bcs), balance_changes (tx_digest, checkpoint, timestamp, owner, coin_type, amount).`,
    ].filter(Boolean).join("\n");

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
      sqliteWriter.close();
      try { unlinkSync(dbPath); } catch {}
      try { unlinkSync(mcpConfigPath); } catch {}
    });
  });

program.parse();
