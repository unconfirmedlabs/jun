#!/usr/bin/env bun
/**
 * Jun CLI — debugging and exploration tool for Sui checkpoint streams.
 *
 * Usage:
 *   jun stream                              # stream live checkpoints with events
 *   jun stream --filter pressing::Record    # only show matching event types
 *   jun stream --json                       # output as JSON lines
 */
import { program } from "commander";
import { createGrpcClient, type GrpcCheckpointResponse, type GrpcEvent } from "./grpc.ts";

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

function formatTimestamp(summary: GrpcCheckpointResponse["checkpoint"]["summary"]): string {
  if (!summary?.timestamp) return "unknown";
  const ms = BigInt(summary.timestamp.seconds) * 1000n + BigInt(Math.floor(summary.timestamp.nanos / 1_000_000));
  return new Date(Number(ms)).toISOString();
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

function getAllEvents(checkpoint: GrpcCheckpointResponse["checkpoint"]): Array<{ event: GrpcEvent; txDigest: string }> {
  const results: Array<{ event: GrpcEvent; txDigest: string }> = [];
  for (const tx of checkpoint.transactions) {
    if (!tx.events?.events?.length) continue;
    for (const ev of tx.events.events) {
      results.push({ event: ev, txDigest: tx.digest });
    }
  }
  return results;
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
  .description("Stream live checkpoints and print events")
  .option("--url <url>", "gRPC endpoint URL", "slc1.rpc.testnet.sui.mirai.cloud:443")
  .option("--filter <type>", "only show events matching this type substring")
  .option("--json", "output as JSON lines instead of formatted text", false)
  .action(async (opts: { url: string; filter?: string; json: boolean }) => {
    const client = createGrpcClient({ url: opts.url });

    // Graceful shutdown on Ctrl+C
    let stopped = false;
    const shutdown = () => {
      if (stopped) process.exit(1);
      stopped = true;
      console.error("\n[jun] stopping...");
      client.close();
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);

    console.error(`[jun] streaming from ${opts.url}...`);
    if (opts.filter) console.error(`[jun] filter: ${opts.filter}`);
    console.error("");

    try {
      for await (const response of client.subscribeCheckpoints()) {
        if (stopped) break;

        const { checkpoint } = response;
        let events = getAllEvents(checkpoint);

        // Skip checkpoints with no events
        if (events.length === 0) continue;

        // Apply filter
        if (opts.filter) {
          events = events.filter((e) => e.event.eventType.includes(opts.filter!));
          if (events.length === 0) continue;
        }

        const seq = response.cursor;
        const ts = formatTimestamp(checkpoint.summary);

        if (opts.json) {
          // JSON lines mode — one line per checkpoint
          const record = {
            checkpoint: seq,
            timestamp: ts,
            events: events.map((e) => ({
              type: e.event.eventType,
              sender: e.event.sender,
              tx: e.txDigest,
              bcsBytes: e.event.contents?.value ? e.event.contents.value.length : 0,
            })),
          };
          console.log(JSON.stringify(record));
        } else {
          // Formatted text mode
          console.log(`── checkpoint ${seq} ── ${ts} ── ${events.length} event(s) ──`);
          for (const { event, txDigest } of events) {
            console.log(formatEvent(event, txDigest));
            console.log("");
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

    client.close();
    console.error("[jun] done");
  });

program.parse();
