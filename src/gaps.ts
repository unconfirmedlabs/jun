/**
 * Gap detection and repair for checkpoint processing.
 *
 * Uses the `processed_checkpoints` table to find missing sequences,
 * then fetches and processes them from the archive.
 */
import pMap from "p-map";
import type { Logger } from "./logger.ts";
import type { EventProcessor } from "./processor.ts";
import type { WriteBuffer } from "./buffer.ts";
import type { ArchiveClient } from "./archive.ts";
import type { StateManager } from "./state.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface GapDetector {
  /** Find missing checkpoint sequences in a range. Returns gap sequences. */
  findGaps(from: bigint, to: bigint): Promise<bigint[]>;

  /** Repair gaps by fetching from archive, processing, and writing via buffer. */
  repairGaps(
    gaps: bigint[],
    processor: EventProcessor,
    buffer: WriteBuffer,
    archiveClient: ArchiveClient,
    cursorKey: string,
  ): Promise<number>;

  /** Start periodic gap detection + repair loop. Returns a stop function. */
  startPeriodicRepair(
    getProcessor: (() => EventProcessor) | EventProcessor,
    buffer: WriteBuffer,
    archiveClient: ArchiveClient,
  ): () => void;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

export function createGapDetector(
  sql: any,
  state: StateManager,
  network: string,
  log: Logger,
  intervalMs = 300_000, // 5 minutes
): GapDetector {
  return {
    async findGaps(from: bigint, to: bigint): Promise<bigint[]> {
      if (from > to) return [];

      const gaps: bigint[] = [];
      const batchSize = 10_000n;

      // Query in batches to avoid generating huge series
      let cursor = from;
      while (cursor <= to) {
        const batchEnd = cursor + batchSize - 1n < to ? cursor + batchSize - 1n : to;

        const rows = await sql.unsafe(
          `SELECT s.seq::NUMERIC AS seq
           FROM generate_series($1::NUMERIC, $2::NUMERIC) AS s(seq)
           WHERE NOT EXISTS (
             SELECT 1 FROM processed_checkpoints pc WHERE pc.checkpoint_seq = s.seq
           )`,
          [cursor.toString(), batchEnd.toString()],
        );

        for (const row of rows) {
          gaps.push(BigInt(row.seq));
        }

        cursor = batchEnd + 1n;
      }

      return gaps;
    },

    async repairGaps(
      gaps: bigint[],
      processor: EventProcessor,
      buffer: WriteBuffer,
      archiveClient: ArchiveClient,
      cursorKey: string,
    ): Promise<number> {
      if (gaps.length === 0) return 0;

      let repaired = 0;

      await pMap(
        gaps,
        async (seq) => {
          try {
            const response = await archiveClient.fetchCheckpoint(seq);
            const decoded = processor.process(response);
            await buffer.push(decoded, cursorKey, seq);
            repaired++;
          } catch (err) {
            log.error({ checkpoint: seq.toString(), err }, "failed to repair checkpoint");
          }
        },
        { concurrency: 5 },
      );

      return repaired;
    },

    startPeriodicRepair(
      getProcessor: (() => EventProcessor) | EventProcessor,
      buffer: WriteBuffer,
      archiveClient: ArchiveClient,
    ): () => void {
      // Support both direct processor and getter function (for hot reload)
      const resolveProcessor = typeof getProcessor === "function" && !("process" in getProcessor)
        ? getProcessor as () => EventProcessor
        : () => getProcessor as EventProcessor;
      let stopped = false;

      const run = async () => {
        if (stopped) return;

        try {
          // Determine the range from backfill and live cursors
          const backfillCursor = await state.getCheckpointCursor(`backfill:${network}`);
          const liveCursor = await state.getCheckpointCursor(`live:${network}`);

          if (!backfillCursor) return; // No backfill has run yet

          // Scan from start of backfill range to the lower of the two cursors
          const to = liveCursor && liveCursor < backfillCursor ? liveCursor : backfillCursor;

          // Find the earliest processed checkpoint as the lower bound
          const earliest = await sql.unsafe(
            `SELECT MIN(checkpoint_seq)::NUMERIC AS min_seq FROM processed_checkpoints`,
          );
          const from = earliest[0]?.min_seq ? BigInt(earliest[0].min_seq) : backfillCursor;

          if (from >= to) return;

          log.debug({ from: from.toString(), to: to.toString() }, "scanning for gaps");

          const gaps = await this.findGaps(from, to);
          if (gaps.length === 0) {
            log.debug("no gaps found");
            return;
          }

          log.info({ count: gaps.length, from: from.toString(), to: to.toString() }, "gaps found");
          const repaired = await this.repairGaps(
            gaps,
            resolveProcessor(),
            buffer,
            archiveClient,
            `gaps:${network}`,
          );
          if (repaired > 0) {
            log.info({ repaired }, "gaps repaired");
          }
        } catch (err) {
          log.error({ err }, "periodic repair error");
        }
      };

      const timer = setInterval(run, intervalMs);

      // Run once immediately on start
      run();

      return () => {
        stopped = true;
        clearInterval(timer);
      };
    },
  };
}
