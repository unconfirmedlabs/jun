/**
 * NATS destination — publishes processed data to NATS subjects.
 *
 * Subject hierarchy:
 *   {prefix}.checkpoints
 *   {prefix}.transactions
 *   {prefix}.events.decoded.{handlerName}
 *   {prefix}.balances
 */
import { connect, type NatsConnection, StringCodec } from "nats";
import type { Broadcast, ProcessedCheckpoint } from "../types.ts";
import type { Logger } from "../../logger.ts";
import { createLogger } from "../../logger.ts";

export interface NatsDestinationConfig {
  /** NATS server URL */
  url: string;
  /** Subject prefix (default: "jun") */
  prefix?: string;
}

export function createNatsBroadcast(config: NatsDestinationConfig): Broadcast {
  const log: Logger = createLogger().child({ component: "destination:nats" });
  const prefix = config.prefix ?? "jun";
  const stringCodec = StringCodec();
  let connection: NatsConnection | null = null;

  return {
    name: "nats",

    async initialize(): Promise<void> {
      connection = await connect({
        servers: config.url,
        maxReconnectAttempts: -1,
        reconnectTimeWait: 2000,
      });
      log.info({ url: config.url, prefix }, "NATS connected");
    },

    push(processed: ProcessedCheckpoint): void {
      if (!connection) return;

      const checkpoint = processed.checkpoint;

      try {
        connection.publish(
          `${prefix}.checkpoints`,
          stringCodec.encode(JSON.stringify({
            seq: checkpoint.sequenceNumber.toString(),
            timestamp: checkpoint.timestamp.toISOString(),
            txCount: checkpoint.transactions.length,
            source: checkpoint.source,
          })),
        );
      } catch (error) {
        log.warn({ error }, "NATS checkpoint publish failed");
      }

      for (const event of processed.events) {
        try {
          connection.publish(
            `${prefix}.events.decoded.${event.handlerName}`,
            stringCodec.encode(JSON.stringify({
              handlerName: event.handlerName,
              checkpointSeq: event.checkpointSeq.toString(),
              txDigest: event.txDigest,
              eventSeq: event.eventSeq,
              sender: event.sender,
              timestamp: event.timestamp.toISOString(),
              data: event.data,
            })),
          );
        } catch (error) {
          log.warn({ error }, "NATS event publish failed");
        }
      }

      for (const change of processed.balanceChanges) {
        try {
          connection.publish(
            `${prefix}.balances`,
            stringCodec.encode(JSON.stringify({
              checkpointSeq: change.checkpointSeq.toString(),
              address: change.address,
              coinType: change.coinType,
              amount: change.amount,
              timestamp: change.timestamp.toISOString(),
            })),
          );
        } catch (error) {
          log.warn({ error }, "NATS balance publish failed");
        }
      }
    },

    async shutdown(): Promise<void> {
      if (connection) {
        await connection.drain();
        connection = null;
      }
      log.info("NATS disconnected");
    },
  };
}
