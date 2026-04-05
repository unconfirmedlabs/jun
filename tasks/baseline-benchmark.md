# Baseline Benchmark — `baseline/mysten-bcs-full-decode`

Reference numbers for the pure Mysten BCS + full-checkpoint-decode baseline.
Future optimization branches (fast JS paths, `@unconfirmed/bcs`, Zig) will be
compared against these.

## Setup

- **Branch:** `baseline/mysten-bcs-full-decode`
- **Commit:** `3720675` (phase 5)
- **Pipeline:** `jun pipeline snapshot ... --batch-size 10`
- **Range:** checkpoints `180000000–180000099` (100 checkpoints)
- **Range archive source:** `https://checkpoints.mainnet.sui.io`
- **Runs per config:** 5
- **Note on `--batch-size 10`:** there is a pre-existing drain race in
  jun on fast hosts when the per-batch payload is large — the main thread
  can exit before the SQLite worker finishes the deferred-index / VACUUM
  shutdown path. `--batch-size 10` sidesteps it by flushing eagerly
  during processing. Not specific to this branch; `main` exhibits it too
  with `--everything`-sized payloads. Tracked separately.

## Hosts

| Host | CPUs | RAM | Disk | Notes |
|---|---|---|---|---|
| **server** (`ssh claude`) | 32 | 249 GiB | local SSD | Ubuntu 24.04, x86_64, Linux 6.8 |
| **local** (darwin-arm64) | 10 | 16 GiB | local SSD | over 4G hotspot (network-bound) |

## Row counts (100 checkpoints, `--everything`)

Verified identical between `main` and `baseline` on the four overlapping
tables. The seven new tables are baseline-only.

| Table | Rows | Δ vs main |
|---|---|---|
| `checkpoints` | 100 | **NEW** |
| `transactions` | 1003 | same (now +10 cols) |
| `move_calls` | 1612 | same |
| `balance_changes` | 547 | same |
| `balances` | 547 | same |
| `commands` | 2221 | **NEW** (superset of `move_calls`) |
| `object_changes` | 4607 | **NEW** |
| `transaction_dependencies` | 3975 | **NEW** |
| `transaction_inputs` | 4818 | **NEW** |
| `system_transactions` | 432 | **NEW** (~4/cp — multiple ConsensusCommitPrologueV4 per checkpoint for each sub-dag) |
| `unchanged_consensus_objects` | 932 | **NEW** |

## Throughput — server (32c / 249 GiB)

5 runs per config, `--concurrency 50 --workers 8 --batch-size 10`.

| Config | Min | Max | Avg | vs main |
|---|---|---|---|---|
| `main` + `--transaction-blocks --coin-type '*'` | 545 cp/s | 640 cp/s | **597 cp/s** | 1.00× |
| `baseline` + `--transaction-blocks --coin-type '*'` (apples-to-apples) | 232 cp/s | 245 cp/s | **240 cp/s** | 0.40× |
| `baseline` + `--everything` (9 processors, 11 tables) | 210 cp/s | 229 cp/s | **221 cp/s** | 0.37× |

**Observations:**

1. **Main is ~2.5× faster than baseline on apples-to-apples work.** That's
   the cost of removing Zig + JS fast paths in favor of
   `protobufjs`+`@mysten/sui/bcs` end-to-end. Every future optimization
   branch has a 2.5× headroom to recover.
2. **Extra tables are nearly free.** Going from `--transaction-blocks`
   alone to `--everything` (adding six processors + six tables + ~3× the
   write volume) costs only **~8%** throughput (240 → 221 cp/s). Decode is
   CPU-bound; writes amortize well inside one SQLite transaction.
3. **The `main` path does minimal decode work.** It skips extraction of
   `changed_objects`, `dependencies`, inputs, commands beyond MoveCall,
   system tx kinds, and unchanged consensus objects. Baseline pays for
   fully parsing `TransactionEffects.V2` and the full `TransactionData`
   kind enum on every transaction.

## Throughput — local (darwin-arm64 / 4G network)

| Config | Avg | vs main |
|---|---|---|
| `main` + `--transaction-blocks --coin-type '*'` | 87 cp/s | 1.00× |
| `baseline` + `--everything` | 82 cp/s | 0.94× |

Local is bandwidth-limited (4G hotspot) so both branches stall on HTTP
fetches, which flattens the delta to ~6%. The server numbers are the
correct baseline reference.

## Database size

`/tmp/b.db` at `--everything` for 100 checkpoints: **~9 MB**. Scales to
~90 MB per 1000 checkpoints, ~90 GB per epoch of ~1M checkpoints.

## Next steps

1. Re-enable `fastParseEvents` and `fastParseEffects` in archive.ts → measure.
2. Swap `@mysten/bcs` → `@unconfirmed/bcs` → measure.
3. Re-enable Zig FFI decoder → measure.
4. Stack all three → compare against baseline end-to-end.

## Known bugs uncovered during benchmarking

1. **Drain race on fast hosts.** With default `--batch-size 500` and
   `--everything`, the SQLite writer worker can be torn down before
   deferred indexes + VACUUM complete, leaving 0 rows in the DB. The
   pipeline reports success, no errors are logged. Workaround:
   `--batch-size 10`. Pre-existing — `main` also fails with the same
   symptoms on an equivalently-sized payload. Root cause is somewhere
   in `pipeline.ts` shutdown or `writers/sqlite.ts` close() — drain()
   resolves before the actual on-disk commit lands. Needs a separate
   fix.
