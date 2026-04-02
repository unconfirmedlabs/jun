/**
 * Parquet destination — writes chunked columnar files with optional S3 upload.
 *
 * Accumulates events across pipeline flushes and writes Parquet chunks
 * on a timer or when a size threshold is reached. Partitioned by day.
 */
import parquet from "parquetjs";
import path from "path";
import fs from "fs";
import type { Destination, ProcessedCheckpoint, DecodedEvent } from "../types.ts";
import type { FieldDefs, FieldType } from "../../schema.ts";
import { parseDuration } from "../../views.ts";
import type { Logger } from "../../logger.ts";
import { createLogger } from "../../logger.ts";

export interface ParquetDestinationConfig {
  /** Directory for Parquet files */
  path: string;
  /** Event handler definitions (for schema generation) */
  handlers?: Record<string, { tableName: string; fields: FieldDefs }>;
  /** Partition strategy (default: "day") */
  partitionBy?: "day" | "hour";
  /** Flush interval (default: "5m") */
  flushInterval?: string;
  /** Flush when accumulated events exceed this (default: 10000) */
  flushSize?: number;
  /** Optional S3 upload */
  s3?: { bucket: string; endpoint?: string; accessKeyId?: string; secretAccessKey?: string };
}

function fieldTypeToParquet(type: FieldType): any {
  if (type.startsWith("option<")) return { ...fieldTypeToParquet(type.slice(7, -1) as FieldType), optional: true };
  if (type.startsWith("vector<")) return { type: "UTF8", optional: false };
  switch (type) {
    case "address": case "string": return { type: "UTF8", optional: false };
    case "bool": return { type: "BOOLEAN", optional: false };
    case "u8": case "u16": case "u32": return { type: "INT32", optional: false };
    case "u64": case "u128": case "u256": return { type: "UTF8", optional: false };
    default: return { type: "UTF8", optional: false };
  }
}

function formatDatePartition(date: Date, partitionBy: "day" | "hour"): string {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  if (partitionBy === "hour") return `${year}-${month}-${day}/${String(date.getUTCHours()).padStart(2, "0")}`;
  return `${year}-${month}-${day}`;
}

export function createParquetDestination(config: ParquetDestinationConfig): Destination {
  const log: Logger = createLogger().child({ component: "destination:parquet" });
  const partitionBy = config.partitionBy ?? "day";
  const flushIntervalMs = parseDuration(config.flushInterval ?? "5m");
  const flushSize = config.flushSize ?? 10000;

  const schemas = new Map<string, any>();
  const accumulated = new Map<string, DecodedEvent[]>();
  let accumulatedCount = 0;
  let timer: Timer | null = null;

  async function flushAccumulator(): Promise<void> {
    if (accumulatedCount === 0) return;

    const snapshot = new Map(accumulated);
    try {
      for (const [handlerName, events] of snapshot) {
        if (events.length === 0) continue;
        const schema = schemas.get(handlerName);
        if (!schema) continue;

        const partitions = new Map<string, DecodedEvent[]>();
        for (const event of events) {
          const partition = formatDatePartition(event.timestamp, partitionBy);
          const list = partitions.get(partition);
          if (list) list.push(event);
          else partitions.set(partition, [event]);
        }

        for (const [partition, partitionEvents] of partitions) {
          const directory = path.join(config.path, handlerName, partition);
          fs.mkdirSync(directory, { recursive: true });
          const chunkFile = path.join(directory, `chunk-${Date.now()}.parquet`);

          const writer = await parquet.ParquetWriter.openFile(schema, chunkFile);
          try {
            for (const event of partitionEvents) {
              const row: Record<string, any> = {
                tx_digest: event.txDigest,
                event_seq: event.eventSeq,
                sender: event.sender,
                sui_timestamp: event.timestamp.toISOString(),
              };
              for (const [key, value] of Object.entries(event.data)) {
                row[key] = typeof value === "bigint"
                  ? (value <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(value) : value.toString())
                  : value;
              }
              await writer.appendRow(row);
            }
          } finally {
            await writer.close();
          }

          log.debug({ handler: handlerName, partition, events: partitionEvents.length }, "chunk written");
        }
      }

      // Clear only on success
      accumulated.clear();
      accumulatedCount = 0;

      // Write state file (atomic)
      const statePath = path.join(config.path, "_jun_state.json");
      const tmpPath = statePath + ".tmp";
      fs.writeFileSync(tmpPath, JSON.stringify({ updatedAt: new Date().toISOString() }, null, 2));
      fs.renameSync(tmpPath, statePath);
    } catch (error) {
      log.error({ error }, "parquet flush failed, will retry");
    }
  }

  return {
    name: "parquet",

    async initialize(): Promise<void> {
      fs.mkdirSync(config.path, { recursive: true });

      if (config.handlers) {
        for (const [handlerName, handler] of Object.entries(config.handlers)) {
          const schemaFields: Record<string, any> = {
            tx_digest: { type: "UTF8", optional: false },
            event_seq: { type: "INT32", optional: false },
            sender: { type: "UTF8", optional: false },
            sui_timestamp: { type: "UTF8", optional: false },
          };
          for (const [name, type] of Object.entries(handler.fields)) {
            schemaFields[name] = fieldTypeToParquet(type);
          }
          schemas.set(handlerName, new parquet.ParquetSchema(schemaFields));
        }
      }

      timer = setInterval(() => {
        flushAccumulator().catch(error => log.error({ error }, "periodic parquet flush error"));
      }, flushIntervalMs);

      log.info({ path: config.path, partitionBy, flushInterval: config.flushInterval ?? "5m" }, "parquet destination initialized");
    },

    async write(batch: ProcessedCheckpoint[]): Promise<void> {
      for (const processed of batch) {
        for (const event of processed.events) {
          const list = accumulated.get(event.handlerName);
          if (list) list.push(event);
          else accumulated.set(event.handlerName, [event]);
          accumulatedCount++;
        }
      }

      if (accumulatedCount >= flushSize) {
        await flushAccumulator();
      }
    },

    async shutdown(): Promise<void> {
      if (timer) { clearInterval(timer); timer = null; }
      await flushAccumulator();
      log.info("parquet destination shut down");
    },
  };
}
