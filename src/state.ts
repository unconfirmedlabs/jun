/**
 * State management: checkpoint cursor tracking with Bun.sql.
 *
 * Maintains an `indexer_checkpoints` table for tracking the last processed
 * checkpoint per cursor key. Live and backfill use separate keys so they
 * can run concurrently without interfering.
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface StateManager {
  /** Get the last processed checkpoint for a cursor key. Returns null if no cursor exists. */
  getCheckpointCursor(key: string): Promise<bigint | null>;

  /** Set the checkpoint cursor for a key. Upserts. */
  setCheckpointCursor(key: string, seq: bigint): Promise<void>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

/**
 * Initialize state management. Auto-creates the `indexer_checkpoints` table.
 */
export async function createStateManager(sql: any): Promise<StateManager> {
  // Auto-create the state table
  await sql`
    CREATE TABLE IF NOT EXISTS indexer_checkpoints (
      key TEXT PRIMARY KEY,
      checkpoint_seq NUMERIC NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `;

  return {
    async getCheckpointCursor(key: string): Promise<bigint | null> {
      const rows = await sql`
        SELECT checkpoint_seq FROM indexer_checkpoints WHERE key = ${key}
      `;
      if (rows.length === 0) return null;
      return BigInt(rows[0].checkpoint_seq);
    },

    async setCheckpointCursor(key: string, seq: bigint): Promise<void> {
      await sql`
        INSERT INTO indexer_checkpoints (key, checkpoint_seq, updated_at)
        VALUES (${key}, ${seq.toString()}, NOW())
        ON CONFLICT (key)
        DO UPDATE SET checkpoint_seq = ${seq.toString()}, updated_at = NOW()
      `;
    },
  };
}
