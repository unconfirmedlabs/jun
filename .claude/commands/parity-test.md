# Data Parity Test

**Model: Use Sonnet for this task. Opus is unnecessary for running tests.**

Compare two decoder paths by running the same checkpoint range and diffing SQLite output.

## Purpose

Verify that `JUN_DIRECT_BINARY=1` (direct binary write) produces identical data to the default path (struct-based extraction + binary write).

## Setup

Server: `ssh claude` — Bun at `$HOME/.bun/bin/bun`.

Epoch 1086 checkpoints cached at `~/.jun/cache/checkpoints/`.

## Step 1: Run both paths

Use a 100K checkpoint range (takes ~5 min each with SQLite write):

```bash
# Old path
ssh claude "export PATH=\$HOME/.bun/bin:\$PATH && cd ~/jun && rm -f /tmp/parity-old.db && bun run src/cli.ts pipeline snapshot --start-checkpoint 260650000 --end-checkpoint 260749999 --everything --sqlite /tmp/parity-old.db --workers 8 --concurrency 500 --quiet --yes 2>&1 | tr '\r' '\n' | grep -E '(checkpoints|pipeline completed)' | tail -3"

# Direct path
ssh claude "export PATH=\$HOME/.bun/bin:\$PATH && export JUN_DIRECT_BINARY=1 && cd ~/jun && rm -f /tmp/parity-new.db && bun run src/cli.ts pipeline snapshot --start-checkpoint 260650000 --end-checkpoint 260749999 --everything --sqlite /tmp/parity-new.db --workers 8 --concurrency 500 --quiet --yes 2>&1 | tr '\r' '\n' | grep -E '(checkpoints|pipeline completed)' | tail -3"
```

## Step 2: Compare row counts

```bash
ssh claude "export PATH=\$HOME/.bun/bin:\$PATH && bun -e \"
const { Database } = require('bun:sqlite');
const oldDb = new Database('/tmp/parity-old.db', { readonly: true });
const newDb = new Database('/tmp/parity-new.db', { readonly: true });
const tables = oldDb.query(\\\"SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence' ORDER BY name\\\").all().map(r => r.name);
for (const table of tables) {
  const oldCount = oldDb.query('SELECT COUNT(*) as c FROM ' + table).get().c;
  const newCount = newDb.query('SELECT COUNT(*) as c FROM ' + table).get().c;
  const match = oldCount === newCount ? '✓' : '✗';
  console.log(table.padEnd(35), 'old=' + String(oldCount).padEnd(10), 'new=' + String(newCount).padEnd(10), match);
}
\""
```

## Step 3: Compare content hashes per table

Compare sorted content. Do ONE table at a time to avoid OOM (tables can have millions of rows):

```bash
ssh claude "export PATH=\$HOME/.bun/bin:\$PATH && bun -e \"
const { Database } = require('bun:sqlite');
const crypto = require('crypto');
const oldDb = new Database('/tmp/parity-old.db', { readonly: true });
const newDb = new Database('/tmp/parity-new.db', { readonly: true });

const TABLE = 'transactions';  // Change this for each table
const ORDER = 'digest';        // Change sort key per table

const oldHash = crypto.createHash('sha256');
const newHash = crypto.createHash('sha256');
for (const row of oldDb.query('SELECT * FROM ' + TABLE + ' ORDER BY ' + ORDER).all()) oldHash.update(JSON.stringify(row));
for (const row of newDb.query('SELECT * FROM ' + TABLE + ' ORDER BY ' + ORDER).all()) newHash.update(JSON.stringify(row));
const oh = oldHash.digest('hex').slice(0, 16);
const nh = newHash.digest('hex').slice(0, 16);
console.log(TABLE, oh === nh ? '✓ MATCH' : '✗ MISMATCH', oh, nh);
\""
```

### Sort keys per table

| Table | ORDER BY |
|-------|----------|
| transactions | digest |
| move_calls | tx_digest, call_index |
| balance_changes | checkpoint_seq, address, coin_type |
| object_changes | tx_digest, object_id |
| transaction_dependencies | tx_digest, depends_on_digest |
| commands | tx_digest, command_index |
| transaction_inputs | tx_digest, input_index |
| system_transactions | tx_digest |
| unchanged_consensus_objects | tx_digest, object_id |
| checkpoints | sequence_number |

## Step 4: Debug mismatches

For a mismatched table, find the first differing row:

```bash
ssh claude "export PATH=\$HOME/.bun/bin:\$PATH && bun -e \"
const { Database } = require('bun:sqlite');
const oldDb = new Database('/tmp/parity-old.db', { readonly: true });
const newDb = new Database('/tmp/parity-new.db', { readonly: true });
const TABLE = 'balance_changes';
const ORDER = 'checkpoint_seq, address, coin_type';
const oldRows = oldDb.query('SELECT * FROM ' + TABLE + ' ORDER BY ' + ORDER + ' LIMIT 100').all();
const newRows = newDb.query('SELECT * FROM ' + TABLE + ' ORDER BY ' + ORDER + ' LIMIT 100').all();
for (let i = 0; i < Math.min(oldRows.length, newRows.length); i++) {
  if (JSON.stringify(oldRows[i]) !== JSON.stringify(newRows[i])) {
    console.log('First diff at row', i);
    console.log('OLD:', JSON.stringify(oldRows[i]));
    console.log('NEW:', JSON.stringify(newRows[i]));
    break;
  }
}
\""
```

## Known Issues

- **balance_changes**: The direct path (`direct.rs`) reimplements balance aggregation and has a known mismatch. The fix is to reuse the existing `compute_balance_changes` logic or adopt `sui_types::balance_change::derive_balance_changes()`.
- **Row ordering**: Snapshot mode uses unordered drain, so `rowid` order differs between runs. Always sort by deterministic keys when comparing.
- **OOM on large comparisons**: Loading all rows of a 6M-row table into JS can crash Bun. Compare one table at a time, or use streaming hash for large tables.
