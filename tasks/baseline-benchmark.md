# Baseline Benchmark — `baseline/mysten-bcs-full-decode`

Reference numbers for the pure Mysten BCS + full-checkpoint-decode baseline.
Future optimization branches (fast JS paths, `@unconfirmed/bcs`, Zig) will be
compared against these.

## Setup

- **Branch:** `baseline/mysten-bcs-full-decode`
- **Commit:** `3720675` (phase 5)
- **Host:** local (darwin-arm64, M-series)
- **Network:** `hayabusa.mainnet.unconfirmed.cloud:443` + `checkpoints.mainnet.sui.io`
- **Pipeline:** `jun pipeline snapshot ... --everything`
- **Range:** checkpoints `180000000–180000099` (100 checkpoints)

## Row counts

| Table | Rows | Notes |
|---|---|---|
| `checkpoints` | 100 | 1 row per checkpoint |
| `transactions` | 1003 | +10 new columns over main (epoch, error fields, events_digest, …) |
| `move_calls` | 1612 | Identical to main — subset of `commands` |
| `commands` | 2221 | Superset of `move_calls`; includes TransferObjects, SplitCoins, MergeCoins, Upgrade, … |
| `balance_changes` | 547 | Identical to main |
| `balances` | — | Materialized on shutdown |
| `object_changes` | 4607 | **NEW** — per-object state changes |
| `transaction_dependencies` | 3975 | **NEW** — causal edges |
| `transaction_inputs` | 4818 | **NEW** — all PTB input variants |
| `system_transactions` | 432 | **NEW** — ~4 per checkpoint (ConsensusCommitPrologueV4 per sub-dag) |
| `unchanged_consensus_objects` | 932 | **NEW** — read-only consensus refs |

**Row-count parity verified** against `main` on identical range for the four
overlapping tables (transactions=1003, move_calls=1612, balance_changes=547).

## Throughput

| Branch | Throughput | Total time | Work done |
|---|---|---|---|
| `main` (Zig + fast paths) | **87 cp/s** | 1.2s | transactions + move_calls + balance_changes |
| `baseline/mysten-bcs-full-decode` | **82 cp/s** | 1.3s | all of the above **+** checkpoints, object_changes, dependencies, inputs, commands, system_transactions, unchanged_consensus_objects |

Baseline is **~6% slower** while producing **~6× more data** per checkpoint.
That's a very different ratio from the naive expectation that removing Zig
would make everything 2–3× slower. Two reasons:

1. The Zig fast path only accelerated a subset of the work (event + effects
   BCS decoding). The rest — HTTP fetches, zstd decompress, protobufjs proto
   decode, worker thread plumbing — is identical on both branches.
2. With `--everything`, the baseline writes ~20k rows per checkpoint batch
   (vs ~3.2k for main's `--transaction-blocks --coin-type '*'`). SQL write
   time dominates, not decoding.

## Smaller run (10 checkpoints, `180000000–180000009`)

| Branch | Throughput | Total time |
|---|---|---|
| `main` | 25 cp/s | 0.4s |
| `baseline` (`--everything`) | 11 cp/s | 0.9s |

For small batches where write-amortization doesn't help, the baseline is
~2.3× slower as expected from removing the Zig fast paths.

## Database size

- `/tmp/jun-baseline-bench.db`: **8.6 MB** for 100 checkpoints with
  `--everything`. Scales to ~86 MB per 1000 checkpoints.

## Next steps

1. Re-enable `fastParseEvents` and `fastParseEffects` in archive.ts → measure.
2. Swap `@mysten/bcs` → `@unconfirmed/bcs` → measure.
3. Re-enable Zig FFI decoder → measure.
4. Compose all three → compare against baseline end-to-end.
