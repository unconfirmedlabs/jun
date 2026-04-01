/**
 * jun/checkpoints — Historical checkpoint fetching and local caching.
 *
 * @example Fetch a checkpoint from the public archive
 * ```ts
 * import { createArchiveClient } from "jun/checkpoints";
 *
 * const archive = createArchiveClient({ archiveUrl: "https://checkpoints.mainnet.sui.io" });
 * const checkpoint = await archive.fetchCheckpoint(316756645n);
 * ```
 *
 * @example Cache-through fetch (local cache → fallback)
 * ```ts
 * import { cachedGetCheckpoint } from "jun/checkpoints";
 *
 * const checkpoint = await cachedGetCheckpoint(seq, () => grpcClient.getCheckpoint(seq));
 * ```
 */

// Archive client
export {
  createArchiveClient,
  cachedGetCheckpoint,
  decodeCompressedCheckpoint,
  fetchRawCheckpoint,
  getCommittee,
  getCheckpointType,
} from "./archive.ts";
export type {
  ArchiveClient,
  ArchiveClientOptions,
  RawCheckpoint,
} from "./archive.ts";

// Local checkpoint cache
export {
  cacheGet,
  cachePut,
  cacheStats,
  cacheClear,
} from "./cache.ts";
