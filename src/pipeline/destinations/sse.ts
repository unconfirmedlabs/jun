/**
 * SSE destination — broadcasts processed data to connected Server-Sent Events clients.
 *
 * Runs on the HTTP server worker thread. Pushes checkpoint summaries,
 * transaction summaries, decoded events, and raw events to filtered clients.
 */
import type { Destination, ProcessedCheckpoint } from "../types.ts";
import type { Logger } from "../../logger.ts";
import { createLogger } from "../../logger.ts";

export interface SseDestinationConfig {
  /** Port for the HTTP server */
  port: number;
  /** Hostname to bind (default: "127.0.0.1") */
  hostname?: string;
}

interface SseClient {
  controller: ReadableStreamDefaultController<string>;
  stream: "checkpoints" | "transactions" | "events" | "broadcast/events";
  source?: string;
  handler?: string;
  sender?: string;
  eventType?: string;
}

const MAX_SSE_CLIENTS = 100;

export function createSseDestination(config: SseDestinationConfig): Destination {
  const log: Logger = createLogger().child({ component: "destination:sse" });
  let server: ReturnType<typeof Bun.serve> | null = null;
  const clients = new Set<SseClient>();

  function sendToClient(client: SseClient, data: string): boolean {
    try {
      if ((client.controller.desiredSize ?? 1) <= 0) {
        clients.delete(client);
        return false;
      }
      client.controller.enqueue(data);
      return true;
    } catch {
      clients.delete(client);
      return false;
    }
  }

  function handleStream(
    streamType: SseClient["stream"],
    url: URL,
  ): Response {
    const source = url.searchParams.get("source") ?? undefined;
    const handler = url.searchParams.get("handler") ?? undefined;
    const sender = url.searchParams.get("sender") ?? undefined;
    const eventType = url.searchParams.get("type") ?? undefined;

    if (source && source !== "live" && source !== "backfill") {
      return Response.json({ error: 'source must be "live" or "backfill"' }, { status: 400 });
    }

    if (clients.size >= MAX_SSE_CLIENTS) {
      return Response.json({ error: "too many SSE clients" }, { status: 503 });
    }

    let cleanup: (() => void) | null = null;

    const readable = new ReadableStream<string>({
      start(controller) {
        const client: SseClient = { controller, stream: streamType, handler, source, sender, eventType };
        clients.add(client);
        cleanup = () => clients.delete(client);
        controller.enqueue(`data: ${JSON.stringify({ type: "connected", stream: streamType })}\n\n`);
      },
      cancel() { cleanup?.(); },
    });

    return new Response(readable, {
      headers: { "content-type": "text/event-stream", "cache-control": "no-cache", "connection": "keep-alive" },
    });
  }

  return {
    name: "sse",

    async initialize(): Promise<void> {
      server = Bun.serve({
        port: config.port,
        hostname: config.hostname ?? "127.0.0.1",
        fetch(request) {
          const url = new URL(request.url);
          if (url.pathname === "/stream/checkpoints") return handleStream("checkpoints", url);
          if (url.pathname === "/stream/transactions") return handleStream("transactions", url);
          if (url.pathname === "/stream/events") return handleStream("events", url);
          if (url.pathname === "/broadcast/events") return handleStream("broadcast/events", url);
          return Response.json({ error: "not found" }, { status: 404 });
        },
      });

      log.info({ port: server.port, hostname: config.hostname ?? "127.0.0.1" }, "SSE server started");
    },

    async write(batch: ProcessedCheckpoint[]): Promise<void> {
      if (clients.size === 0) return;

      for (const processed of batch) {
        const checkpoint = processed.checkpoint;

        // Checkpoint summary
        const checkpointMessage = `data: ${JSON.stringify({
          seq: checkpoint.sequenceNumber.toString(),
          timestamp: checkpoint.timestamp.toISOString(),
          txCount: checkpoint.transactions.length,
          source: checkpoint.source,
        })}\n\n`;

        // Decoded events
        const eventMessages = processed.events.map(event => ({
          handlerName: event.handlerName,
          formatted: `data: ${JSON.stringify({
            handlerName: event.handlerName,
            checkpointSeq: event.checkpointSeq.toString(),
            txDigest: event.txDigest,
            eventSeq: event.eventSeq,
            sender: event.sender,
            timestamp: event.timestamp.toISOString(),
            source: checkpoint.source,
            data: event.data,
          })}\n\n`,
        }));

        for (const client of clients) {
          if (client.source && client.source !== checkpoint.source) continue;

          if (client.stream === "checkpoints") {
            sendToClient(client, checkpointMessage);
          } else if (client.stream === "events") {
            for (const message of eventMessages) {
              if (client.handler && client.handler !== message.handlerName) continue;
              sendToClient(client, message.formatted);
            }
          }
        }
      }
    },

    async shutdown(): Promise<void> {
      if (server) {
        await server.stop();
        server = null;
      }
      log.info("SSE server stopped");
    },
  };
}
