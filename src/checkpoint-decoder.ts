/**
 * Checkpoint decode worker — Rust FFI binary decode from cached files.
 *
 * Each worker processes a range of checkpoints: readFileSync → Rust FFI → parseBinaryCheckpoint
 * → postMessage(parsed). Fully decoded ProcessedCheckpoint arrives on the main thread with no
 * further JS parsing required.
 *
 * In decode-write-range mode, workers decode AND write directly to ClickHouse or Postgres,
 * sending only progress events back to the pool (no heavy postMessage per checkpoint).
 */
/// <reference lib="webworker" />
import {
  decodeArchiveCheckpointBinary,
  decodeArchiveCheckpointBinarySelective,
  isNativeCheckpointDecoderAvailable,
} from "./checkpoint-native-decoder.ts";
import { parseBinaryCheckpoint } from "./binary-parser.ts";
import type { ProcessedCheckpoint } from "./pipeline/types.ts";

declare var self: Worker;

interface DecodeCachedRangeMessage {
  type: "decode-cached-range";
  from: string;
  to: string;
  cacheDir: string;
  workerIndex: number;
  extractMask?: number;
}

interface DecodeWriteRangeMessage {
  type: "decode-write-range";
  from: string;
  to: string;
  cacheDir: string;
  workerIndex: number;
  extractMask?: number;
  batchSize: number;
  backend: "clickhouse" | "postgres";
  // ClickHouse
  clickhouseUrl?: string;
  clickhouseDatabase?: string;
  clickhouseUsername?: string;
  clickhousePassword?: string;
  // Postgres
  postgresUrl?: string;
  // Balance filtering (null = all)
  balanceCoinTypes?: string[] | null;
}

async function handleDecodeCachedRange(msg: DecodeCachedRangeMessage): Promise<void> {
  const { readFileSync } = await import("fs");
  const { join } = await import("path");

  const from = BigInt(msg.from);
  const to = BigInt(msg.to);
  const useSelective = msg.extractMask !== undefined && msg.extractMask !== 0x7FF;
  let processed = 0;
  const startedAt = performance.now();

  if (!isNativeCheckpointDecoderAvailable()) {
    postMessage({ type: "error", error: "Native checkpoint decoder not available" });
    postMessage({ type: "done" });
    return;
  }

  for (let seq = from; seq <= to; seq++) {
    const cachePath = join(msg.cacheDir, `${seq}.binpb.zst`);
    try {
      const compressed = new Uint8Array(readFileSync(cachePath));
      const binary = useSelective
        ? decodeArchiveCheckpointBinarySelective(compressed, msg.extractMask!)
        : decodeArchiveCheckpointBinary(compressed);

      if (binary) {
        // Parse the binary here in the worker thread — no JS parsing on the main thread.
        const parsed = parseBinaryCheckpoint(binary);
        postMessage({ seq: seq.toString(), parsed });
        processed++;
        if (processed % 5000 === 0) {
          const elapsed = (performance.now() - startedAt) / 1000;
          console.error(
            `[decode-worker ${msg.workerIndex}] ${processed} cp ${Math.round(processed / elapsed)} cp/s`,
          );
        }
      }
    } catch (err) {
      postMessage({
        type: "error",
        seq: seq.toString(),
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  postMessage({ type: "done" });
}

async function handleDecodeWriteRange(msg: DecodeWriteRangeMessage): Promise<void> {
  const { readFileSync } = await import("fs");
  const { join } = await import("path");

  const from = BigInt(msg.from);
  const to = BigInt(msg.to);
  const useSelective = msg.extractMask !== undefined && msg.extractMask !== 0x7FF;
  const allowedCoinTypes =
    Array.isArray(msg.balanceCoinTypes) && msg.balanceCoinTypes.length > 0
      ? new Set(msg.balanceCoinTypes)
      : null;

  if (!isNativeCheckpointDecoderAvailable()) {
    postMessage({ type: "write-error", error: "Native checkpoint decoder not available" });
    postMessage({ type: "done", decoded: 0 });
    return;
  }

  // Set up backend lazily
  if (msg.backend === "clickhouse") {
    const { createClient } = await import("@clickhouse/client");
    const { buildTableRows, TABLES } = await import("./pipeline/destinations/clickhouse.ts");

    const client = createClient({
      url: msg.clickhouseUrl ?? process.env.JUN_CLICKHOUSE_URL,
      database: msg.clickhouseDatabase ?? process.env.JUN_CLICKHOUSE_DATABASE,
      username: msg.clickhouseUsername ?? process.env.JUN_CLICKHOUSE_USERNAME ?? "default",
      password: msg.clickhousePassword ?? process.env.JUN_CLICKHOUSE_PASSWORD ?? "",
      compression: { request: false },
    });

    const chSettings = {
      insert_deduplicate: 0 as const,
      optimize_on_insert: 0 as const,
      async_insert: 1 as const,
      wait_for_async_insert: 0 as const,
    };

    let batch: ProcessedCheckpoint[] = [];
    let totalDecoded = 0;

    async function flushBatch(): Promise<void> {
      if (batch.length === 0) return;
      const tableRows = buildTableRows(batch);
      await Promise.all(
        TABLES.map((table, i) => {
          const rows = tableRows[i]!;
          if (rows.length === 0) return Promise.resolve();
          return client.insert({
            table: table.name,
            values: rows,
            format: "JSONEachRow",
            clickhouse_settings: chSettings,
          });
        }),
      );
      batch = [];
    }

    try {
      for (let seq = from; seq <= to; seq++) {
        const cachePath = join(msg.cacheDir, `${seq}.binpb.zst`);
        try {
          const compressed = new Uint8Array(readFileSync(cachePath));
          const binary = useSelective
            ? decodeArchiveCheckpointBinarySelective(compressed, msg.extractMask!)
            : decodeArchiveCheckpointBinary(compressed);

          if (binary) {
            const parsed = parseBinaryCheckpoint(binary);
            let finalProcessed: ProcessedCheckpoint = parsed.processed;
            if (allowedCoinTypes !== null) {
              finalProcessed = {
                ...parsed.processed,
                balanceChanges: parsed.processed.balanceChanges.filter(bc => allowedCoinTypes.has(bc.coinType)),
              };
            }
            batch.push(finalProcessed);
            totalDecoded++;

            if (batch.length >= msg.batchSize) {
              await flushBatch();
              postMessage({ type: "write-progress", decoded: totalDecoded });
            }
          }
        } catch (err) {
          postMessage({
            type: "write-error",
            seq: seq.toString(),
            error: err instanceof Error ? err.message : String(err),
          });
        }
      }

      await flushBatch();
    } catch (err) {
      postMessage({ type: "write-error", error: err instanceof Error ? err.message : String(err) });
    } finally {
      await client.close();
      postMessage({ type: "done", decoded: totalDecoded });
    }
  } else {
    // postgres backend
    const { createPostgresConnection } = await import("./db.ts");
    const { collectTableRows, insertUnnest, PG_COLUMN_TYPES } = await import("./pipeline/destinations/per-table-postgres.ts");
    const { TABLES } = await import("./pipeline/destinations/per-table-sqlite.ts");

    if (!msg.postgresUrl) {
      postMessage({ type: "write-error", error: "postgresUrl is required for postgres backend" });
      postMessage({ type: "done", decoded: 0 });
      return;
    }

    const db = createPostgresConnection(msg.postgresUrl);

    let batch: ProcessedCheckpoint[] = [];
    let totalDecoded = 0;

    async function flushBatch(): Promise<void> {
      if (batch.length === 0) return;
      const tableRows = collectTableRows(TABLES, batch);
      await db.begin(async (tx) => {
        for (let i = 0; i < TABLES.length; i++) {
          const tableDef = TABLES[i]!;
          const pgTypes = PG_COLUMN_TYPES[tableDef.name];
          if (!pgTypes) throw new Error(`No PG_COLUMN_TYPES for table ${tableDef.name}`);
          await insertUnnest(
            tx as ReturnType<typeof createPostgresConnection>,
            tableDef.name,
            tableDef.columns,
            pgTypes,
            tableRows[i]!,
          );
        }
      });
      batch = [];
    }

    try {
      for (let seq = from; seq <= to; seq++) {
        const cachePath = join(msg.cacheDir, `${seq}.binpb.zst`);
        try {
          const compressed = new Uint8Array(readFileSync(cachePath));
          const binary = useSelective
            ? decodeArchiveCheckpointBinarySelective(compressed, msg.extractMask!)
            : decodeArchiveCheckpointBinary(compressed);

          if (binary) {
            const parsed = parseBinaryCheckpoint(binary);
            let finalProcessed: ProcessedCheckpoint = parsed.processed;
            if (allowedCoinTypes !== null) {
              finalProcessed = {
                ...parsed.processed,
                balanceChanges: parsed.processed.balanceChanges.filter(bc => allowedCoinTypes.has(bc.coinType)),
              };
            }
            batch.push(finalProcessed);
            totalDecoded++;

            if (batch.length >= msg.batchSize) {
              await flushBatch();
              postMessage({ type: "write-progress", decoded: totalDecoded });
            }
          }
        } catch (err) {
          postMessage({
            type: "write-error",
            seq: seq.toString(),
            error: err instanceof Error ? err.message : String(err),
          });
        }
      }

      await flushBatch();
    } catch (err) {
      postMessage({ type: "write-error", error: err instanceof Error ? err.message : String(err) });
    } finally {
      await db.end?.();
      postMessage({ type: "done", decoded: totalDecoded });
    }
  }
}

self.onmessage = async (event: MessageEvent) => {
  const msg = event.data;
  if (msg && typeof msg === "object") {
    if (msg.type === "decode-cached-range") {
      await handleDecodeCachedRange(msg as DecodeCachedRangeMessage);
    } else if (msg.type === "decode-write-range") {
      await handleDecodeWriteRange(msg as DecodeWriteRangeMessage);
    }
  }
};
