/**
 * Parquet storage backend — columnar analytics output with optional S3/R2 upload.
 *
 * Accumulates events across buffer flushes and writes chunked Parquet files
 * on a timer interval or event count threshold. Files are partitioned by day.
 *
 * Directory structure:
 *   data/
 *     _jun_state.json
 *     record_pressed/
 *       2026-04-01/
 *         chunk-1711929600000.parquet
 *         chunk-1711929900000.parquet
 */
import parquet from "parquetjs";
import path from "path";
import fs from "fs";
import type { DecodedEvent } from "../processor.ts";
import type { FieldDefs, FieldType } from "../schema.ts";
import type { StorageBackend, HandlerTableConfig } from "./storage.ts";
import type { Logger } from "../logger.ts";
import { parseDuration } from "../views.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ParquetConfig {
  /** Directory for Parquet files */
  path: string;
  /** Partition strategy (default: "day") */
  partitionBy?: "day" | "hour";
  /** Flush interval as duration string (default: "5m") */
  flushInterval?: string;
  /** Flush when accumulated event count exceeds this (default: 10000) */
  flushSize?: number;
  /** Optional S3/R2 upload config */
  s3?: {
    bucket: string;
    endpoint?: string;
    accessKeyId?: string;
    secretAccessKey?: string;
  };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function fieldTypeToParquet(type: FieldType): any {
  if (type.startsWith("option<")) {
    return { ...fieldTypeToParquet(type.slice(7, -1) as FieldType), optional: true };
  }
  if (type.startsWith("vector<")) {
    return { type: "UTF8", optional: false }; // JSON string
  }
  switch (type) {
    case "address": case "string": return { type: "UTF8", optional: false };
    case "bool": return { type: "BOOLEAN", optional: false };
    case "u8": case "u16": case "u32": return { type: "INT32", optional: false };
    case "u64": return { type: "UTF8", optional: false };
    case "u128": case "u256": return { type: "UTF8", optional: false }; // too large for INT64
    default: return { type: "UTF8", optional: false };
  }
}

function buildParquetSchema(fields: FieldDefs): any {
  const schema: Record<string, any> = {
    tx_digest: { type: "UTF8", optional: false },
    event_seq: { type: "INT32", optional: false },
    sender: { type: "UTF8", optional: false },
    sui_timestamp: { type: "UTF8", optional: false },
  };
  for (const [name, type] of Object.entries(fields)) {
    schema[name] = fieldTypeToParquet(type);
  }
  return new parquet.ParquetSchema(schema);
}

function formatDatePartition(date: Date, partitionBy: "day" | "hour"): string {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, "0");
  const d = String(date.getUTCDate()).padStart(2, "0");
  if (partitionBy === "hour") {
    const h = String(date.getUTCHours()).padStart(2, "0");
    return `${y}-${m}-${d}/${h}`;
  }
  return `${y}-${m}-${d}`;
}

function toTableName(handlerName: string): string {
  return handlerName.replace(/([A-Z])/g, "_$1").toLowerCase().replace(/^_/, "");
}

// ---------------------------------------------------------------------------
// State file
// ---------------------------------------------------------------------------

interface ParquetState {
  cursor: string;
  updatedAt: string;
}

function readState(statePath: string): ParquetState | null {
  try {
    return JSON.parse(fs.readFileSync(statePath, "utf-8"));
  } catch {
    return null;
  }
}

function writeState(statePath: string, cursor: bigint): void {
  const state: ParquetState = {
    cursor: cursor.toString(),
    updatedAt: new Date().toISOString(),
  };
  // Atomic write: write to temp file then rename (prevents corruption on crash)
  const tmpPath = statePath + ".tmp";
  fs.writeFileSync(tmpPath, JSON.stringify(state, null, 2));
  fs.renameSync(tmpPath, statePath);
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

export function createParquetStorageBackend(
  config: ParquetConfig,
  handlers: Record<string, HandlerTableConfig>,
  log: Logger,
): StorageBackend {
  const parquetLog = log.child({ component: "parquet" });
  const basePath = config.path;
  const partitionBy = config.partitionBy ?? "day";
  const flushIntervalMs = parseDuration(config.flushInterval ?? "5m");
  const flushSize = config.flushSize ?? 10000;
  const statePath = path.join(basePath, "_jun_state.json");

  // Pre-build schemas per handler
  const schemas = new Map<string, any>();
  for (const [handlerName, handlerConfig] of Object.entries(handlers)) {
    schemas.set(handlerName, buildParquetSchema(handlerConfig.fields));
  }

  // Accumulator
  const accumulated = new Map<string, DecodedEvent[]>();
  let accumulatedCount = 0;
  let maxCheckpointSeq = 0n;
  let timer: Timer | null = null;

  async function flushAccumulator(): Promise<void> {
    if (accumulatedCount === 0) return;

    const snapshot = new Map(accumulated);
    const snapshotSeq = maxCheckpointSeq;
    // Don't clear yet — only clear on successful write

    try {
      for (const [handlerName, events] of snapshot) {
        if (events.length === 0) continue;

        const schema = schemas.get(handlerName);
        if (!schema) continue;

        // Group by partition
        const partitions = new Map<string, DecodedEvent[]>();
        for (const event of events) {
          const partition = formatDatePartition(event.timestamp, partitionBy);
          const list = partitions.get(partition);
          if (list) list.push(event);
          else partitions.set(partition, [event]);
        }

        const tableName = toTableName(handlerName);

        for (const [partition, partEvents] of partitions) {
          const dir = path.join(basePath, tableName, partition);
          fs.mkdirSync(dir, { recursive: true });

          const chunkFile = path.join(dir, `chunk-${Date.now()}.parquet`);

          const writer = await parquet.ParquetWriter.openFile(schema, chunkFile);
          try {
            for (const event of partEvents) {
              const row: Record<string, any> = {
                tx_digest: event.txDigest,
                event_seq: event.eventSeq,
                sender: event.sender,
                sui_timestamp: event.timestamp.toISOString(),
              };
              for (const [key, val] of Object.entries(event.data)) {
                // BigInt: u64 fits in Number, u128/u256 stored as UTF8 string (see schema mapping)
                if (typeof val === "bigint") {
                  row[key] = val <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(val) : val.toString();
                } else {
                  row[key] = val;
                }
              }
              await writer.appendRow(row);
            }
          } finally {
            await writer.close();
          }

          parquetLog.debug({ handler: handlerName, partition, events: partEvents.length, file: chunkFile }, "chunk written");

          // S3 upload if configured
          if (config.s3) {
            try {
              const s3Key = `${tableName}/${partition}/${path.basename(chunkFile)}`;
              const s3Client = new Bun.S3Client({
                endpoint: config.s3.endpoint,
                credentials: {
                  accessKeyId: config.s3.accessKeyId ?? "",
                  secretAccessKey: config.s3.secretAccessKey ?? "",
                },
              });
              const s3File = s3Client.file(`${config.s3.bucket}/${s3Key}`);
              await Bun.write(s3File, Bun.file(chunkFile));
              parquetLog.debug({ key: s3Key }, "uploaded to S3");
            } catch (err) {
              parquetLog.error({ err }, "S3 upload failed");
            }
          }
        }
      }

      // Only clear on success
      accumulated.clear();
      accumulatedCount = 0;

      // Update state file
      if (snapshotSeq > 0n) {
        writeState(statePath, snapshotSeq);
      }

      parquetLog.info({ cursor: snapshotSeq.toString() }, "parquet flush complete");
    } catch (err) {
      parquetLog.error({ err }, "parquet flush failed, will retry");
      // Don't clear — data stays in accumulator for next flush attempt
      return;
    }
  }

  return {
    name: "parquet",

    async migrate(): Promise<void> {
      fs.mkdirSync(basePath, { recursive: true });

      // Start the periodic flush timer
      timer = setInterval(() => {
        flushAccumulator().catch((err) => {
          parquetLog.error({ err }, "periodic flush error");
        });
      }, flushIntervalMs);

      parquetLog.info({ path: basePath, partitionBy, flushInterval: config.flushInterval ?? "5m", flushSize }, "initialized");
    },

    async writeHandler(handlerName: string, events: DecodedEvent[]): Promise<void> {
      if (events.length === 0) return;

      const list = accumulated.get(handlerName);
      if (list) {
        list.push(...events);
      } else {
        accumulated.set(handlerName, [...events]);
      }
      accumulatedCount += events.length;

      // Track max checkpoint seq for state file
      for (const event of events) {
        if (event.checkpointSeq > maxCheckpointSeq) {
          maxCheckpointSeq = event.checkpointSeq;
        }
      }

      // Flush if accumulated count exceeds threshold
      if (accumulatedCount >= flushSize) {
        await flushAccumulator();
      }
    },

    async write(events: DecodedEvent[]): Promise<void> {
      if (events.length === 0) return;

      const grouped = new Map<string, DecodedEvent[]>();
      for (const event of events) {
        const list = grouped.get(event.handlerName);
        if (list) list.push(event);
        else grouped.set(event.handlerName, [event]);
      }

      for (const [handlerName, handlerEvents] of grouped) {
        await this.writeHandler(handlerName, handlerEvents);
      }
    },

    async shutdown(): Promise<void> {
      if (timer) {
        clearInterval(timer);
        timer = null;
      }
      await flushAccumulator();
      parquetLog.info("shutdown complete");
    },
  };
}
