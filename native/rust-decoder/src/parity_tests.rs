use std::collections::BTreeSet;
use std::io::Read;
use std::path::{Path, PathBuf};

use sui_types::effects::{TransactionEffects, TransactionEffectsAPI};
use sui_types::execution_status::{ExecutionErrorKind, ExecutionStatus};
use sui_types::transaction::{
    Command, SenderSignedData, TransactionData, TransactionDataAPI, TransactionKind,
};

use crate::{bcs_reader, direct, extract, proto};

const PRIMARY_SEQUENCE: u64 = 260650000;
const STRESS_COUNT: usize = 100;

fn fixture_roots() -> Vec<PathBuf> {
    let mut roots = vec![Path::new(env!("CARGO_MANIFEST_DIR")).join("test-data")];
    if let Some(home) = std::env::var_os("HOME") {
        roots.push(PathBuf::from(home).join(".jun/cache/checkpoints"));
    }
    roots
}

fn fixture_path(sequence: u64) -> PathBuf {
    let file_name = format!("{sequence}.binpb.zst");
    for root in fixture_roots() {
        let candidate = root.join(&file_name);
        if candidate.exists() {
            return candidate;
        }
    }

    panic!(
        "missing checkpoint fixture {file_name}; add it to {}/test-data or ~/.jun/cache/checkpoints",
        env!("CARGO_MANIFEST_DIR")
    );
}

fn available_sequences() -> Vec<u64> {
    let mut sequences = BTreeSet::new();
    for root in fixture_roots() {
        let Ok(entries) = std::fs::read_dir(root) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            let Some(seq) = name.strip_suffix(".binpb.zst") else {
                continue;
            };
            let Ok(seq) = seq.parse::<u64>() else {
                continue;
            };
            sequences.insert(seq);
        }
    }
    sequences.into_iter().collect()
}

fn consecutive_sequences(limit: usize) -> Vec<u64> {
    let sequences = available_sequences();
    assert!(
        !sequences.is_empty(),
        "no checkpoint fixtures found in {}/test-data or ~/.jun/cache/checkpoints",
        env!("CARGO_MANIFEST_DIR")
    );

    for start in &sequences {
        let candidate = (*start..(*start + limit as u64)).collect::<Vec<_>>();
        if candidate
            .iter()
            .all(|seq| sequences.binary_search(seq).is_ok())
        {
            return candidate;
        }
    }

    panic!("need {limit} consecutive checkpoint fixtures for the stress test");
}

fn decompress_checkpoint(path: &Path) -> Vec<u8> {
    let compressed = std::fs::read(path).unwrap_or_else(|err| {
        panic!("failed to read fixture {}: {err}", path.display());
    });
    let mut decoder = zstd::Decoder::new(compressed.as_slice()).unwrap_or_else(|err| {
        panic!("failed to open zstd fixture {}: {err}", path.display());
    });
    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .unwrap_or_else(|err| {
            panic!("failed to decompress fixture {}: {err}", path.display());
        });
    decompressed
}

fn load_primary_checkpoint() -> (u64, Vec<u8>) {
    let path = fixture_path(PRIMARY_SEQUENCE);
    (PRIMARY_SEQUENCE, decompress_checkpoint(&path))
}

fn output_diff_message(left: &[u8], right: &[u8]) -> String {
    let diff_index = left
        .iter()
        .zip(right)
        .position(|(lhs, rhs)| lhs != rhs)
        .unwrap_or_else(|| left.len().min(right.len()));

    let start = diff_index.saturating_sub(8);
    let end = (diff_index + 8).min(left.len().min(right.len()));
    format!(
        "binary output differs at byte {diff_index}; left={:02x?}; right={:02x?}",
        &left[start..end],
        &right[start..end]
    )
}

fn execution_error_kind_name(error: &ExecutionErrorKind) -> String {
    let debug = format!("{error:?}");
    debug
        .split(['(', '{'])
        .next()
        .unwrap_or(debug.as_str())
        .trim()
        .to_string()
}

fn format_hex_address(bytes: &[u8; 32]) -> String {
    let mut out = String::with_capacity(66);
    out.push_str("0x");
    for byte in bytes {
        out.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
        out.push(char::from(b"0123456789abcdef"[(byte & 0x0f) as usize]));
    }
    out
}

fn full_move_call_count(kind: &TransactionKind) -> usize {
    match kind {
        TransactionKind::ProgrammableTransaction(pt)
        | TransactionKind::ProgrammableSystemTransaction(pt) => pt
            .commands
            .iter()
            .filter(|command| matches!(command, Command::MoveCall(_)))
            .count(),
        _ => 0,
    }
}

#[test]
fn test_effects_parity_on_real_checkpoint() {
    let (sequence, decompressed) = load_primary_checkpoint();
    let parsed = proto::parse_checkpoint(&decompressed).unwrap();

    let mut checked = 0usize;
    for tx in &parsed.transactions {
        let Some(effects_bcs) = tx.effects_bcs else {
            continue;
        };

        let full = bcs::from_bytes::<TransactionEffects>(effects_bcs).unwrap_or_else(|err| {
            panic!(
                "full effects decode failed for tx {} in checkpoint {}: {err}",
                tx.digest, sequence
            )
        });
        let fast = bcs_reader::parse_effects(effects_bcs).unwrap_or_else(|err| {
            panic!(
                "fast effects decode failed for tx {} at index {} in checkpoint {}: {err}; full status={:?}",
                tx.digest,
                checked,
                sequence,
                full.status()
            )
        });

        let gas = full.gas_cost_summary();
        assert_eq!(
            fast.digest,
            *full.transaction_digest().inner(),
            "digest mismatch for tx {}",
            tx.digest
        );
        assert_eq!(
            fast.epoch,
            full.executed_epoch(),
            "epoch mismatch for tx {}",
            tx.digest
        );
        assert_eq!(
            fast.status.is_success(),
            matches!(full.status(), ExecutionStatus::Success),
            "status mismatch for tx {}",
            tx.digest
        );
        assert_eq!(
            fast.gas_used.computation_cost, gas.computation_cost,
            "computation gas mismatch for tx {}",
            tx.digest
        );
        assert_eq!(
            fast.gas_used.storage_cost, gas.storage_cost,
            "storage gas mismatch for tx {}",
            tx.digest
        );
        assert_eq!(
            fast.gas_used.storage_rebate, gas.storage_rebate,
            "storage rebate mismatch for tx {}",
            tx.digest
        );
        assert_eq!(
            fast.gas_used.non_refundable_storage_fee, gas.non_refundable_storage_fee,
            "non-refundable storage fee mismatch for tx {}",
            tx.digest
        );
        assert_eq!(
            fast.events_digest,
            full.events_digest().map(|digest| digest.into_inner()),
            "events digest mismatch for tx {}",
            tx.digest
        );
        assert_eq!(
            fast.lamport_version,
            full.lamport_version().value(),
            "lamport version mismatch for tx {}",
            tx.digest
        );
        assert_eq!(
            fast.dependency_count,
            full.dependencies().len(),
            "dependency count mismatch for tx {}",
            tx.digest
        );
        assert_eq!(
            fast.dependencies.len(),
            full.dependencies().len(),
            "dependency vector length mismatch for tx {}",
            tx.digest
        );
        for (index, (fast_dep, full_dep)) in fast
            .dependencies
            .iter()
            .zip(full.dependencies())
            .enumerate()
        {
            assert_eq!(
                fast_dep,
                full_dep.inner(),
                "dependency {} mismatch for tx {}",
                index,
                tx.digest
            );
        }

        checked += 1;
    }

    assert!(
        checked > 0,
        "no effects payloads found in checkpoint {sequence}"
    );
}

#[test]
fn test_tx_data_parity_on_real_checkpoint() {
    let (sequence, decompressed) = load_primary_checkpoint();
    let parsed = proto::parse_checkpoint(&decompressed).unwrap();

    let mut checked = 0usize;
    for tx in &parsed.transactions {
        let Some(tx_bcs) = tx.transaction_bcs else {
            continue;
        };
        let Some(full) = extract::decode_transaction_data_pub(tx_bcs) else {
            continue;
        };

        let expected_kind = match full.kind() {
            TransactionKind::ProgrammableTransaction(_) => bcs_reader::ProgrammableKind::User,
            TransactionKind::ProgrammableSystemTransaction(_) => {
                bcs_reader::ProgrammableKind::System
            }
            _ => continue,
        };

        let fast = bcs_reader::parse_tx_data(tx_bcs).unwrap_or_else(|err| {
            panic!(
                "fast tx-data decode failed for tx {} at tx-data index {} in checkpoint {}: {err}; raw_tx={} sender_signed={}",
                tx.digest,
                checked,
                sequence,
                bcs::from_bytes::<TransactionData>(tx_bcs).is_ok(),
                bcs::from_bytes::<SenderSignedData>(tx_bcs).is_ok(),
            )
        });

        assert_eq!(
            fast.sender.as_slice(),
            full.sender().as_ref(),
            "sender mismatch for tx {}",
            tx.digest
        );
        assert!(matches!(
            (fast.kind, expected_kind),
            (
                bcs_reader::ProgrammableKind::User,
                bcs_reader::ProgrammableKind::User
            ) | (
                bcs_reader::ProgrammableKind::System,
                bcs_reader::ProgrammableKind::System
            )
        ));
        assert_eq!(
            fast.move_call_count,
            full_move_call_count(full.kind()),
            "move call count mismatch for tx {}",
            tx.digest
        );

        let expected = match full.kind() {
            TransactionKind::ProgrammableTransaction(pt)
            | TransactionKind::ProgrammableSystemTransaction(pt) => pt,
            _ => unreachable!(),
        };
        assert_eq!(
            fast.inputs.len(),
            expected.inputs.len(),
            "input count mismatch for tx {}",
            tx.digest
        );
        assert_eq!(
            fast.commands.len(),
            expected.commands.len(),
            "command count mismatch for tx {}",
            tx.digest
        );

        checked += 1;
    }

    assert!(
        checked > 0,
        "no programmable transaction payloads found in checkpoint {sequence}"
    );
}

#[test]
fn test_failed_transactions_match_full_decode() {
    let sequences = consecutive_sequences(8);
    let mut checked = 0usize;

    for sequence in sequences {
        let decompressed = decompress_checkpoint(&fixture_path(sequence));
        let parsed = proto::parse_checkpoint(&decompressed).unwrap();

        for tx in &parsed.transactions {
            let Some(effects_bcs) = tx.effects_bcs else {
                continue;
            };

            let full = bcs::from_bytes::<TransactionEffects>(effects_bcs).unwrap();
            let ExecutionStatus::Failure(full_failure) = full.status() else {
                continue;
            };

            let fast = bcs_reader::parse_effects(effects_bcs).unwrap_or_else(|err| {
                panic!(
                    "fast failed-effects decode failed for tx {} in checkpoint {}: {err}; full error={:?}",
                    tx.digest,
                    sequence,
                    full_failure.error
                )
            });

            let fast_failure = match fast.status {
                bcs_reader::StatusSummary::Failure(failure) => failure,
                bcs_reader::StatusSummary::Success => {
                    panic!(
                        "expected failure status for tx {} in checkpoint {}",
                        tx.digest, sequence
                    )
                }
            };

            assert_eq!(
                fast_failure.kind_name,
                execution_error_kind_name(&full_failure.error),
                "error kind mismatch for tx {}",
                tx.digest
            );
            assert_eq!(
                fast_failure.command,
                full_failure.command.map(|index| index as u64),
                "error command mismatch for tx {}",
                tx.digest
            );

            match (&full_failure.error, fast_failure.move_abort) {
                (ExecutionErrorKind::MoveAbort(location, code), Some(details)) => {
                    assert_eq!(
                        details.abort_code, *code,
                        "abort code mismatch for tx {}",
                        tx.digest
                    );
                    assert_eq!(
                        format!(
                            "{}::{}",
                            format_hex_address(&details.module_address),
                            details.module_name
                        ),
                        location.module.to_string(),
                        "abort module mismatch for tx {}",
                        tx.digest
                    );
                    assert_eq!(
                        details.function_name,
                        location.function_name.as_deref(),
                        "abort function mismatch for tx {}",
                        tx.digest
                    );
                }
                (ExecutionErrorKind::MoveAbort(_, _), None) => {
                    panic!("missing move abort details for tx {}", tx.digest)
                }
                (_, Some(_)) => panic!("unexpected move abort details for tx {}", tx.digest),
                (_, None) => {}
            }

            checked += 1;
        }
    }

    assert!(
        checked > 0,
        "no failed transactions found in sampled checkpoints"
    );
}

#[test]
fn test_system_transactions_fall_back_cleanly() {
    let sequences = consecutive_sequences(16);

    for sequence in sequences {
        let decompressed = decompress_checkpoint(&fixture_path(sequence));
        let parsed = proto::parse_checkpoint(&decompressed).unwrap();

        let unsupported = parsed
            .transactions
            .iter()
            .filter_map(|tx| tx.transaction_bcs.map(|bcs| (tx.digest, bcs)))
            .find_map(|(digest, tx_bcs)| {
                let full = extract::decode_transaction_data_pub(tx_bcs)?;
                match full.kind() {
                    TransactionKind::ProgrammableTransaction(_)
                    | TransactionKind::ProgrammableSystemTransaction(_) => None,
                    _ => Some((digest, tx_bcs)),
                }
            });

        let Some((digest, tx_bcs)) = unsupported else {
            continue;
        };

        assert!(
            bcs_reader::parse_tx_data(tx_bcs).is_err(),
            "unsupported system tx {} unexpectedly parsed through fast path",
            digest
        );

        let mut full_output = vec![0u8; 64 * 1024 * 1024];
        let (full_len, _) =
            extract::extract_checkpoint_binary(&decompressed, &mut full_output).unwrap();

        let mut fast_output = vec![0u8; 64 * 1024 * 1024];
        let (fast_len, _) =
            direct::extract_and_write_binary_with_fast_path(&decompressed, &mut fast_output, true)
                .unwrap();

        assert_eq!(
            full_len, fast_len,
            "binary length mismatch for checkpoint {sequence}"
        );
        assert_eq!(
            &fast_output[..fast_len],
            &full_output[..full_len],
            "{}",
            output_diff_message(&fast_output[..fast_len], &full_output[..full_len])
        );
        return;
    }

    panic!("no unsupported system transactions found in sampled checkpoints");
}

#[test]
fn test_binary_output_parity_on_real_checkpoint() {
    let (sequence, decompressed) = load_primary_checkpoint();

    let mut extract_output = vec![0u8; 64 * 1024 * 1024];
    let (extract_len, _) =
        extract::extract_checkpoint_binary(&decompressed, &mut extract_output).unwrap();

    let mut direct_full_output = vec![0u8; 64 * 1024 * 1024];
    let (direct_full_len, _) = direct::extract_and_write_binary_with_fast_path(
        &decompressed,
        &mut direct_full_output,
        false,
    )
    .unwrap();
    assert_eq!(
        extract_len, direct_full_len,
        "baseline fused-writer length mismatch for checkpoint {sequence}"
    );
    assert_eq!(
        &direct_full_output[..direct_full_len],
        &extract_output[..extract_len],
        "{}",
        output_diff_message(
            &direct_full_output[..direct_full_len],
            &extract_output[..extract_len]
        )
    );

    let mut fast_output = vec![0u8; 64 * 1024 * 1024];
    let (fast_len, _) =
        direct::extract_and_write_binary_with_fast_path(&decompressed, &mut fast_output, true)
            .unwrap();

    assert_eq!(
        extract_len, fast_len,
        "fast-path length mismatch for checkpoint {sequence}"
    );
    assert_eq!(
        &fast_output[..fast_len],
        &extract_output[..extract_len],
        "{}",
        output_diff_message(&fast_output[..fast_len], &extract_output[..extract_len])
    );
}

#[test]
fn test_100_checkpoints_no_crash() {
    let sequences = consecutive_sequences(STRESS_COUNT);
    let mut output = vec![0u8; 64 * 1024 * 1024];

    for sequence in sequences {
        let decompressed = decompress_checkpoint(&fixture_path(sequence));
        let result =
            direct::extract_and_write_binary_with_fast_path(&decompressed, &mut output, true);
        assert!(
            result.is_ok(),
            "checkpoint {} failed through fast path: {:?}",
            sequence,
            result.err()
        );
    }
}
