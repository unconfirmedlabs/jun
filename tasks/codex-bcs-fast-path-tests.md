# Codex Task: BCS Fast Path Parity Tests

## Goal

Write a Rust test suite that verifies the custom BCS reader (`bcs_reader.rs`) produces identical output to the full sui-types deserialization path (`extract.rs`). The BCS fast path currently crashes with a 54TB allocation on real checkpoint data — these tests will help identify and fix the parsing bugs.

## Context

The decoder has two paths:
1. **Full path**: `bcs::from_bytes::<TransactionEffects>(bytes)` → full Rust struct → extract fields → write to binary
2. **Fast path**: `bcs_reader::parse_effects(bytes)` → read BCS fields directly → write to binary

Both paths should produce byte-identical binary output for the same input checkpoint. The fast path is gated by `JUN_BCS_FAST_PATH=1` (default off).

The crash happens on real mainnet checkpoint data — likely a field offset miscalculation where the BCS reader reads a garbage ULEB128 length and tries to allocate 54TB.

## Test Strategy

### Test 1: Round-trip parity on real checkpoints

Use cached checkpoint files from `~/.jun/cache/checkpoints/` (or download a few if not cached).

```rust
#[test]
fn test_effects_parity() {
    // Load a real checkpoint's compressed bytes
    let compressed = std::fs::read("test_data/260650000.binpb.zst").unwrap();
    let mut decoder = zstd::Decoder::new(&compressed[..]).unwrap();
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).unwrap();
    
    let parsed = proto::parse_checkpoint(&decompressed).unwrap();
    
    for ptx in &parsed.transactions {
        let Some(effects_bcs) = ptx.effects_bcs else { continue };
        
        // Full path: deserialize to struct
        let full = bcs::from_bytes::<TransactionEffects>(effects_bcs).unwrap();
        
        // Fast path: custom reader
        let fast = bcs_reader::parse_effects(effects_bcs);
        
        // Both should succeed
        assert!(fast.is_ok(), "Fast path failed for tx {}: {:?}", ptx.digest, fast.err());
        let fast = fast.unwrap();
        
        // Compare key fields
        assert_eq!(fast.digest, full.transaction_digest().inner(), "digest mismatch");
        assert_eq!(fast.epoch, full.executed_epoch(), "epoch mismatch");
        assert_eq!(fast.status.is_success(), matches!(full.status(), ExecutionStatus::Success), "status mismatch");
        assert_eq!(fast.gas.computation_cost, full.gas_cost_summary().computation_cost, "gas mismatch");
        assert_eq!(fast.lamport_version, full.lamport_version().value(), "lamport mismatch");
        assert_eq!(fast.dependency_count, full.dependencies().len(), "dependency count mismatch");
    }
}
```

### Test 2: Binary output parity

The ultimate test — both paths should produce identical binary output bytes.

```rust
#[test]
fn test_binary_output_parity() {
    let compressed = std::fs::read("test_data/260650000.binpb.zst").unwrap();
    let mut decoder = zstd::Decoder::new(&compressed[..]).unwrap();
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).unwrap();
    
    // Full path output
    let mut full_output = vec![0u8; 64 * 1024 * 1024];
    std::env::set_var("JUN_BCS_FAST_PATH", "0");
    let (full_len, _) = crate::direct::extract_and_write_binary(&decompressed, &mut full_output).unwrap();
    
    // Fast path output
    let mut fast_output = vec![0u8; 64 * 1024 * 1024];
    std::env::set_var("JUN_BCS_FAST_PATH", "1");
    let (fast_len, _) = crate::direct::extract_and_write_binary(&decompressed, &mut fast_output).unwrap();
    
    assert_eq!(full_len, fast_len, "output length mismatch: full={} fast={}", full_len, fast_len);
    
    if full_output[..full_len] != fast_output[..fast_len] {
        // Find first difference
        for i in 0..full_len {
            if full_output[i] != fast_output[i] {
                panic!("Binary output differs at byte {}: full=0x{:02x} fast=0x{:02x}", i, full_output[i], fast_output[i]);
            }
        }
    }
}
```

### Test 3: TransactionData parity

```rust
#[test]
fn test_tx_data_parity() {
    // Same pattern as effects but for TransactionData
    let compressed = std::fs::read("test_data/260650000.binpb.zst").unwrap();
    // ... decompress, parse proto ...
    
    for ptx in &parsed.transactions {
        let Some(tx_bcs) = ptx.transaction_bcs else { continue };
        
        let full = crate::extract::decode_transaction_data_pub(tx_bcs);
        let fast = bcs_reader::parse_tx_data(tx_bcs);
        
        match (full, fast) {
            (Some(full_tx), Ok(fast_tx)) => {
                // Compare sender
                assert_eq!(
                    fast_tx.sender,
                    full_tx.sender().as_ref(),
                    "sender mismatch for tx {}",
                    ptx.digest,
                );
                // Compare command count
                assert_eq!(
                    fast_tx.commands.len(),
                    full_tx.kind().iter_commands().count(),
                    "command count mismatch",
                );
            }
            (Some(_), Err(e)) => {
                panic!("Fast path failed but full path succeeded for tx {}: {}", ptx.digest, e);
            }
            _ => {} // Both fail = ok
        }
    }
}
```

### Test 4: Edge case — system transactions

System transactions (ConsensusCommitPrologue, ChangeEpoch, etc.) have different TransactionKind variants. Test that the fast path handles or gracefully falls back for these.

### Test 5: Edge case — failed transactions

Transactions with `ExecutionStatus::Failure` have error details (MoveAbort with location/code). Test that the fast path extracts error fields correctly.

### Test 6: Stress test — multiple checkpoints

Run through 100 consecutive checkpoints and verify no panics/crashes:

```rust
#[test]
fn test_100_checkpoints_no_crash() {
    for seq in 260650000..260650100 {
        let path = format!("test_data/{}.binpb.zst", seq);
        if !std::path::Path::new(&path).exists() { continue; }
        
        let compressed = std::fs::read(&path).unwrap();
        let mut decoder = zstd::Decoder::new(&compressed[..]).unwrap();
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();
        
        let mut output = vec![0u8; 64 * 1024 * 1024];
        let result = crate::direct::extract_and_write_binary(&decompressed, &mut output);
        assert!(result.is_ok(), "Checkpoint {} failed: {:?}", seq, result.err());
    }
}
```

## Test Data

Copy a few cached checkpoints into a `test_data/` directory:
```bash
mkdir -p native/rust-decoder/test_data
for seq in 260650000 260650001 260650050 260650099; do
  cp ~/.jun/cache/checkpoints/${seq}.binpb.zst native/rust-decoder/test_data/
done
```

Or download them:
```bash
for seq in 260650000 260650001 260650050 260650099; do
  curl -o native/rust-decoder/test_data/${seq}.binpb.zst https://checkpoints.mainnet.sui.io/${seq}.binpb.zst
done
```

## Files

- `native/rust-decoder/src/bcs_reader.rs` — the custom BCS parser to test
- `native/rust-decoder/src/direct.rs` — integration point (ParsedEffects::Fast vs Full)
- `native/rust-decoder/src/extract.rs` — full path reference implementation
- `native/rust-decoder/src/proto.rs` — proto parser to get BCS byte slices

Put tests in `native/rust-decoder/tests/bcs_parity.rs` (integration test) or in `bcs_reader.rs` itself.

## Debugging the Crash

The crash is: `memory allocation of 54095568350752 bytes failed` — this means the BCS reader read a ULEB128 value that decoded to ~54TB, then tried to allocate a Vec of that size.

To find the bug:
1. Run the parity test on checkpoint 260650000
2. It will either panic at a specific field or produce wrong output
3. The field where it diverges tells you exactly which BCS offset calculation is wrong
4. Common causes:
   - Off-by-one in enum variant tag reading (ULEB128 vs fixed u8)
   - Missing field in a struct (BCS is positional — skipping a field shifts everything)
   - Wrong type size (reading u32 where u64 is expected)
   - Not handling Option<T> correctly (0x00 = None, 0x01 + value = Some)

## Important Notes

- The env var `JUN_BCS_FAST_PATH` controls the toggle. Default is "0" (off). Set to "1" to enable.
- The thread_local caches the env var value, so it can't be changed mid-process. For tests, you may need to call `parse_effects` and `parse_tx_data` directly rather than going through the env var.
- `extract_and_write_binary` is the top-level function in `direct.rs` — it's `pub` and callable from integration tests.
- The proto parser is also `pub` — `proto::parse_checkpoint(decompressed)` gives you the BCS byte slices.
