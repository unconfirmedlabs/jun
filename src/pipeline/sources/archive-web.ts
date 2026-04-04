/**
 * Browser-compatible archive checkpoint source.
 * Uses fetch() + fzstd (pure JS zstd) + protobufjs for decompression and parsing.
 * No Node/Bun-specific APIs — runs in any browser.
 */
import { decompressSync } from "fzstd";
import type { Source, Checkpoint } from "../types.ts";

export interface ArchiveWebSourceConfig {
  /** Archive base URL (e.g. https://checkpoints.mainnet.sui.io) */
  archiveUrl: string;
  /** Starting checkpoint sequence number */
  startCheckpoint: bigint;
  /** End checkpoint (optional — defaults to fetching until error) */
  endCheckpoint?: bigint;
  /** Concurrency for parallel fetches (default: 4) */
  concurrency?: number;
  /** Callback for progress reporting */
  onProgress?: (current: bigint, total?: bigint) => void;
}

const DEFAULT_ARCHIVE_URLS: Record<string, string> = {
  mainnet: "https://checkpoints.mainnet.sui.io",
  testnet: "https://checkpoints.testnet.sui.io",
};

export function createArchiveWebSource(config: ArchiveWebSourceConfig): Source {
  let stopped = false;

  return {
    name: "archive-web",

    async *stream(): AsyncIterable<Checkpoint> {
      const concurrency = config.concurrency ?? 4;
      let cursor = config.startCheckpoint;
      const end = config.endCheckpoint;

      while (!stopped) {
        if (end !== undefined && cursor > end) break;

        // Fetch a batch of checkpoints in parallel
        const batch: bigint[] = [];
        for (let i = 0; i < concurrency && (end === undefined || cursor + BigInt(i) <= end); i++) {
          batch.push(cursor + BigInt(i));
        }

        const results = await Promise.allSettled(
          batch.map((seq) => fetchAndDecodeCheckpoint(config.archiveUrl, seq))
        );

        for (let i = 0; i < results.length; i++) {
          if (stopped) return;
          const result = results[i]!;
          if (result.status === "rejected") {
            // If we hit a 404, we've reached the tip
            return;
          }
          const checkpoint = result.value;
          config.onProgress?.(batch[i]!, end);
          yield checkpoint;
        }

        cursor += BigInt(batch.length);
      }
    },

    async stop(): Promise<void> {
      stopped = true;
    },
  };
}

async function fetchAndDecodeCheckpoint(
  archiveUrl: string,
  seq: bigint,
): Promise<Checkpoint> {
  const url = `${archiveUrl}/${seq}.binpb.zst`;
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to fetch checkpoint ${seq}: ${response.status}`);
  }

  const compressed = new Uint8Array(await response.arrayBuffer());
  const decompressed = decompressSync(compressed);

  // TODO: Parse protobuf checkpoint from decompressed bytes
  // For now, this is a placeholder that needs jun's proto parser
  // to be made browser-compatible (it already has a pure JS path)
  throw new Error("Protobuf parsing not yet implemented for browser - needs proto-parser integration");
}
