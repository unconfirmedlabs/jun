/**
 * jun/broadcast — Pluggable broadcast system for checkpoints, transactions, and events.
 *
 * Supports SSE (default, zero-dependency) and NATS (opt-in, for fan-out).
 * Broadcasts full data, not summaries. Consumers filter via SSE query params
 * or NATS subject subscriptions.
 *
 * @example YAML config
 * ```yaml
 * broadcast:
 *   sse: true
 *   nats:
 *     url: nats://localhost:4222
 *     prefix: jun
 * ```
 */
import { connect, type NatsConnection, StringCodec } from "nats";
import type { Logger } from "./logger.ts";
import type { GrpcCheckpointResponse, GrpcEvent } from "./grpc.ts";
import type { DecodedEvent } from "./processor.ts";

// ---------------------------------------------------------------------------
// Broadcast data types
// ---------------------------------------------------------------------------

export interface CheckpointBroadcast {
  seq: string;
  timestamp: string | null;
  txCount: number;
  source: "live" | "backfill";
}

export interface TransactionBroadcast {
  checkpoint: string;
  digest: string;
  sender: string | null;
  eventCount: number;
  timestamp: string | null;
  source: "live" | "backfill";
}

export interface RawEventBroadcast {
  checkpoint: string;
  txDigest: string;
  eventType: string;
  sender: string;
  packageId: string;
  module: string;
  bcs: string; // base64
  timestamp: string | null;
  source: "live" | "backfill";
}

// ---------------------------------------------------------------------------
// Broadcast target interface
// ---------------------------------------------------------------------------

export interface BroadcastTarget {
  publishCheckpoint(data: CheckpointBroadcast): void;
  publishTransaction(data: TransactionBroadcast): void;
  publishRawEvent(data: RawEventBroadcast): void;
  publishDecodedEvent(data: DecodedEvent, source: "live" | "backfill"): void;
  shutdown(): Promise<void>;
}

// ---------------------------------------------------------------------------
// SSE client types
// ---------------------------------------------------------------------------

export type SSEStreamType = "checkpoints" | "transactions" | "events" | "broadcast/events";

export interface SSEClient {
  controller: ReadableStreamDefaultController<string>;
  stream: SSEStreamType;
  source?: string;
  handler?: string;      // events only
  sender?: string;       // transactions only
  eventType?: string;    // broadcast/events only
}

// ---------------------------------------------------------------------------
// Broadcast manager
// ---------------------------------------------------------------------------

export interface BroadcastManager {
  /** Broadcast full checkpoint data (checkpoints + transactions + raw events). */
  broadcast(response: GrpcCheckpointResponse, source: "live" | "backfill"): void;
  /** Broadcast decoded handler-matched events (after Postgres write). */
  broadcastDecodedEvents(events: DecodedEvent[], source: "live" | "backfill"): void;
  /** Register an SSE client. Returns cleanup function. */
  addSSEClient(client: SSEClient): () => void;
  /** Number of connected SSE clients. */
  sseClientCount(): number;
  /** Shut down all targets. */
  shutdown(): Promise<void>;
}

/** Helper: parse checkpoint timestamp to ISO string */
function parseTimestampISO(ts: { seconds: string; nanos: number } | null | undefined): string | null {
  if (!ts) return null;
  const ms = Number(BigInt(ts.seconds) * 1000n + BigInt(Math.floor(ts.nanos / 1_000_000)));
  return new Date(ms).toISOString();
}

/** Helper: Uint8Array to base64 */
function toBase64(data: Uint8Array): string {
  return Buffer.from(data).toString("base64");
}

export function createBroadcastManager(
  targets: BroadcastTarget[],
  log: Logger,
): BroadcastManager {
  const broadcastLog = log.child({ component: "broadcast" });
  const sseClients = new Set<SSEClient>();
  const MAX_SSE_CLIENTS = 100;

  function sendToSSE(client: SSEClient, data: string): boolean {
    try {
      if (client.controller.desiredSize !== null && client.controller.desiredSize <= 0) {
        sseClients.delete(client);
        return false;
      }
      client.controller.enqueue(data);
      return true;
    } catch {
      sseClients.delete(client);
      return false;
    }
  }

  return {
    broadcast(response: GrpcCheckpointResponse, source: "live" | "backfill"): void {
      const hasTargets = targets.length > 0;
      const hasClients = sseClients.size > 0;
      if (!hasTargets && !hasClients) return;

      const cp = response.checkpoint;
      const timestamp = parseTimestampISO(cp.summary?.timestamp);

      // Checkpoint broadcast
      const checkpointData: CheckpointBroadcast = {
        seq: response.cursor,
        timestamp,
        txCount: cp.transactions.length,
        source,
      };

      const checkpointMsg = `data: ${JSON.stringify(checkpointData)}\n\n`;

      for (const target of targets) {
        try {
          target.publishCheckpoint(checkpointData);
        } catch (err) {
          broadcastLog.warn({ err }, "broadcast target publishCheckpoint failed");
        }
      }

      // Transaction + raw event broadcasts
      const txMessages: { sender: string | null; formatted: string }[] = [];
      const eventMessages: { eventType: string; sender: string; formatted: string }[] = [];

      for (const tx of cp.transactions) {
        const txSender = (tx as any).transaction?.sender ?? null;
        const events = tx.events?.events ?? [];

        const txData: TransactionBroadcast = {
          checkpoint: response.cursor,
          digest: tx.digest,
          sender: txSender,
          eventCount: events.length,
          timestamp,
          source,
        };

        txMessages.push({ sender: txSender, formatted: `data: ${JSON.stringify(txData)}\n\n` });

        for (const target of targets) {
          try {
            target.publishTransaction(txData);
          } catch (err) {
            broadcastLog.warn({ err }, "broadcast target publishTransaction failed");
          }
        }

        // Raw events
        for (const ev of events) {
          const evData: RawEventBroadcast = {
            checkpoint: response.cursor,
            txDigest: tx.digest,
            eventType: ev.eventType,
            sender: ev.sender,
            packageId: ev.packageId,
            module: ev.module,
            bcs: ev.contents?.value ? toBase64(
              ev.contents.value instanceof Uint8Array ? ev.contents.value : new Uint8Array(ev.contents.value),
            ) : "",
            timestamp,
            source,
          };

          eventMessages.push({
            eventType: ev.eventType,
            sender: ev.sender,
            formatted: `data: ${JSON.stringify(evData)}\n\n`,
          });

          for (const target of targets) {
            try {
              target.publishRawEvent(evData);
            } catch (err) {
              broadcastLog.warn({ err }, "broadcast target publishRawEvent failed");
            }
          }
        }
      }

      // SSE distribution
      for (const client of sseClients) {
        if (client.source && client.source !== source) continue;

        if (client.stream === "checkpoints") {
          sendToSSE(client, checkpointMsg);
        } else if (client.stream === "transactions") {
          for (const msg of txMessages) {
            if (client.sender && client.sender !== msg.sender) continue;
            sendToSSE(client, msg.formatted);
          }
        } else if (client.stream === "broadcast/events") {
          for (const msg of eventMessages) {
            if (client.eventType && !msg.eventType.includes(client.eventType)) continue;
            if (client.sender && client.sender !== msg.sender) continue;
            sendToSSE(client, msg.formatted);
          }
        }
      }
    },

    broadcastDecodedEvents(events: DecodedEvent[], source: "live" | "backfill"): void {
      const hasTargets = targets.length > 0;
      const hasClients = sseClients.size > 0;
      if (!hasTargets && !hasClients) return;

      const messages: { handlerName: string; formatted: string }[] = [];
      for (const event of events) {
        const obj = {
          handlerName: event.handlerName,
          checkpointSeq: event.checkpointSeq.toString(),
          txDigest: event.txDigest,
          eventSeq: event.eventSeq,
          sender: event.sender,
          timestamp: event.timestamp.toISOString(),
          source,
          data: event.data,
        };
        messages.push({ handlerName: event.handlerName, formatted: `data: ${JSON.stringify(obj)}\n\n` });

        for (const target of targets) {
          try {
            target.publishDecodedEvent(event, source);
          } catch (err) {
            broadcastLog.warn({ err }, "broadcast target publishDecodedEvent failed");
          }
        }
      }

      // SSE distribution to /stream/events clients
      for (const client of sseClients) {
        if (client.stream !== "events") continue;
        if (client.source && client.source !== source) continue;

        for (const msg of messages) {
          if (client.handler && client.handler !== msg.handlerName) continue;
          sendToSSE(client, msg.formatted);
        }
      }
    },

    addSSEClient(client: SSEClient): () => void {
      if (sseClients.size >= MAX_SSE_CLIENTS) {
        throw new Error(`max SSE clients reached (${MAX_SSE_CLIENTS})`);
      }
      sseClients.add(client);
      broadcastLog.debug({ stream: client.stream, clients: sseClients.size }, "SSE client connected");
      return () => {
        sseClients.delete(client);
        broadcastLog.debug({ stream: client.stream, clients: sseClients.size }, "SSE client disconnected");
      };
    },

    sseClientCount(): number {
      return sseClients.size;
    },

    async shutdown(): Promise<void> {
      for (const target of targets) {
        await target.shutdown();
      }
      broadcastLog.info("broadcast targets shut down");
    },
  };
}

// ---------------------------------------------------------------------------
// NATS broadcast target
// ---------------------------------------------------------------------------

export interface NATSConfig {
  url: string;
  prefix?: string;
}

export async function createNATSTarget(config: NATSConfig, log: Logger): Promise<BroadcastTarget> {
  const natsLog = log.child({ component: "nats" });
  const prefix = config.prefix ?? "jun";
  const sc = StringCodec();

  const nc = await connect({
    servers: config.url,
    maxReconnectAttempts: -1,  // unlimited reconnection
    reconnectTimeWait: 2000,   // 2s between reconnects
  });
  natsLog.info({ url: config.url, prefix }, "connected");

  return {
    publishCheckpoint(data: CheckpointBroadcast): void {
      nc.publish(`${prefix}.checkpoints`, sc.encode(JSON.stringify(data)));
    },
    publishTransaction(data: TransactionBroadcast): void {
      nc.publish(`${prefix}.transactions`, sc.encode(JSON.stringify(data)));
    },
    publishRawEvent(data: RawEventBroadcast): void {
      nc.publish(`${prefix}.events.raw`, sc.encode(JSON.stringify(data)));
    },
    publishDecodedEvent(event: DecodedEvent, source: "live" | "backfill"): void {
      const obj = {
        handlerName: event.handlerName,
        checkpointSeq: event.checkpointSeq.toString(),
        txDigest: event.txDigest,
        eventSeq: event.eventSeq,
        sender: event.sender,
        timestamp: event.timestamp.toISOString(),
        source,
        data: event.data,
      };
      nc.publish(`${prefix}.events.decoded.${event.handlerName}`, sc.encode(JSON.stringify(obj)));
    },
    async shutdown(): Promise<void> {
      await nc.drain();
      natsLog.info("disconnected");
    },
  };
}
