/**
 * jun/checkpoints — Historical checkpoint fetching and decoding.
 *
 * @example Fetch a checkpoint from the public archive
 * ```ts
 * import { createArchiveClient } from "jun/checkpoints";
 *
 * const archive = createArchiveClient({ archiveUrl: "https://checkpoints.mainnet.sui.io" });
 * const checkpoint = await archive.fetchCheckpoint(316756645n);
 * ```
 */

export {
  createArchiveClient,
  decodeCompressedCheckpoint,
  fetchCompressed,
  fetchRawCheckpoint,
  getCommittee,
  getCheckpointType,
} from "./archive.ts";
export type {
  ArchiveClient,
  ArchiveClientOptions,
  RawCheckpoint,
} from "./archive.ts";
