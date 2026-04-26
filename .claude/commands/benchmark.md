# Benchmark Pipeline Throughput

**Model: Use Sonnet for this task. Opus is unnecessary for running benchmarks.**

Run checkpoint decode benchmarks on the production server. All commands use SSH.

## Server Access

```bash
ssh claude  # Production server (32 vCPU AMD 9950X3D, 16 physical cores)
```

Bun is at `$HOME/.bun/bin/bun`. Cargo at `$HOME/.cargo/bin/cargo`. Always export PATH.

## Update Server Code

```bash
ssh claude "export PATH=\$HOME/.bun/bin:\$HOME/.cargo/bin:\$PATH && cd ~/jun && git fetch origin && git reset --hard origin/baseline/mysten-bcs-full-decode"
```

## Build Rust Decoder (required after Rust changes)

```bash
ssh claude "export PATH=\$HOME/.bun/bin:\$HOME/.cargo/bin:\$PATH && cd ~/jun/native/rust-decoder && cargo build --release 2>&1 | tail -5"
```

## Run Benchmark

Epoch 1086 checkpoints are pre-cached at `~/.jun/cache/checkpoints/` (334K files, 2.7GB).

Standard benchmark command:
```bash
ssh claude "export PATH=\$HOME/.bun/bin:\$PATH && cd ~/jun && bun run src/cli.ts pipeline snapshot --epoch 1086 --everything --stdout --workers 8 --concurrency 500 --quiet --yes 2>&1 | tr '\r' '\n' | grep -E '(checkpoints \||pipeline completed)' | tail -5"
```

### Key flags
- `--workers N` — decoder worker threads (sweet spot: 8-16 on this server)
- `--everything` — enable all processors
- `--stdout` — JSONL broadcast (adds ~0% overhead)
- `--quiet --yes` — suppress prompts

### Environment variables
- `JUN_DIRECT_BINARY=1` — use direct binary write path (new, faster)
- `JUN_BCS_DECODER=js` — force JS decoder instead of Rust FFI

### Typical results (epoch 1086, all cached)
| Config | cp/s |
|--------|------|
| 8 workers, old path | 5,334 |
| 8 workers, direct binary | 6,069 |
| 16 workers, old path | 5,360 |
| 4 workers | 3,422 |

## Run with SQLite Write

```bash
ssh claude "export PATH=\$HOME/.bun/bin:\$PATH && cd ~/jun && rm -f /tmp/bench.db && bun run src/cli.ts pipeline snapshot --epoch 1086 --everything --sqlite /tmp/bench.db --workers 8 --concurrency 500 --quiet --yes 2>&1 | tr '\r' '\n' | grep -E '(checkpoints \||pipeline completed)' | tail -5"
```

## Profiling (Rust decode breakdown)

Use 1 worker to see per-phase timing:
```bash
ssh claude "export PATH=\$HOME/.bun/bin:\$PATH && cd ~/jun && bun run src/cli.ts pipeline snapshot --epoch 1086 --everything --stdout --workers 1 --concurrency 500 --quiet --yes 2>&1 | tr '\r' '\n' | grep 'rust-profile' | head -5"
```

Output shows: zstd, proto, bcs_effects, bcs_tx_data, bcs_events, extract, balance, binary_write — each with % and µs per checkpoint.
