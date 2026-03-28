#!/usr/bin/env bun
/**
 * Jun CLI — Sui checkpoint stream explorer.
 *
 * Usage:
 *   jun stream                                    # stream all checkpoint data
 *   jun stream --include events                   # only events
 *   jun stream --include events --filter pressing::Record
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
  .option("--url <url>", "gRPC endpoint URL", "fullnode.mainnet.sui.io:443")
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
      ? createSqliteWriter({ path: opts.output!, showEvents, showEffects, showBalanceChanges })
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
            events: tx.events?.events ?? [],
            effects: tx.effects,
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
  .option("--url <url>", "gRPC endpoint URL", "fullnode.mainnet.sui.io:443")
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
      ? createSqliteWriter({ path: opts.output!, showEvents, showEffects, showBalanceChanges })
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
            events: tx.events?.events ?? [],
            effects: tx.effects,
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

    const archive = createArchiveClient({ archiveUrl: opts.archiveUrl });

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
          const txData = (checkpoint.transactions ?? []).map((tx) => ({
            digest: tx.digest,
            sender: undefined,
            events: tx.events?.events ?? [],
            effects: undefined,
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
// Codegen command
// ---------------------------------------------------------------------------

program
  .command("codegen")
  .description("Generate jun field DSL from an on-chain Sui struct")
  .argument("<type>", "fully qualified type (e.g. 0xPACKAGE::module::StructName)")
  .option("--url <url>", "gRPC endpoint URL", "fullnode.mainnet.sui.io:443")
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

program.parse();
