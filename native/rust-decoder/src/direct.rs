//! Direct binary write — fused extraction + binary output, no intermediate String allocations.
//!
//! Same binary format as binary.rs, but writes fields directly to the output buffer
//! instead of building record structs with owned Strings.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use sui_types::balance::Balance;
use sui_types::base_types::{ObjectID, SequenceNumber};
use sui_types::coin::Coin;
use sui_types::effects::{
    IDOperation, TransactionEffects, TransactionEffectsAPI, TransactionEvents,
    UnchangedConsensusKind,
};
use sui_types::event::Event;
use sui_types::execution_status::ExecutionFailure;
use sui_types::execution_status::{ExecutionErrorKind, ExecutionStatus};
use sui_types::messages_checkpoint::CheckpointSummary;
use sui_types::object::{Object, Owner};
use sui_types::storage::ObjectKey;
use sui_types::transaction::{
    Argument, CallArg, Command, ObjectArg, Reservation, SharedObjectMutability, TransactionData,
    TransactionDataAPI, TransactionKind, WithdrawFrom,
};
use sui_types::TypeTag;

use crate::bcs_reader::{
    self, AccumulatorOperation, AccumulatorValueSummary, ChangedObject, EffectsExtract,
    IdOperation as FastIdOperation, MoveCallInfo, ObjectInSummary, ObjectOutSummary, OwnerSummary,
    ProgrammableKind, StatusSummary, TxDataExtract, UnchangedConsensusKindSummary,
};
use crate::binary::BinaryWriter;
use crate::extract::ExtractProfile;
use crate::proto;

fn use_bcs_fast_path() -> bool {
    BCS_FAST_PATH.with(|v| *v)
}

thread_local! {
    static BCS_FAST_PATH: bool = std::env::var("JUN_BCS_FAST_PATH").map(|v| v != "0").unwrap_or(false);
}

/// Pre-parsed transaction data for a single executed transaction.
enum ParsedEffects<'a> {
    Fast(EffectsExtract<'a>),
    Full(TransactionEffects),
}

enum ParsedTxData<'a> {
    Fast(TxDataExtract<'a>),
    Full(TransactionData),
}

struct ParsedTx<'a> {
    effects: Option<ParsedEffects<'a>>,
    tx_data: Option<ParsedTxData<'a>>,
    events: Option<TransactionEvents>,
    proto_digest: &'a str,
    transaction_bcs: Option<&'a [u8]>,
}

/// Fused extract + binary write. Same output format as extract_checkpoint_binary,
/// but eliminates ~90% of intermediate String allocations.
pub fn extract_and_write_binary(
    decompressed: &[u8],
    output: &mut [u8],
) -> Result<(usize, ExtractProfile), Box<dyn std::error::Error>> {
    use std::time::Instant;
    let mut profile = ExtractProfile::default();

    // Proto parse
    let t0 = Instant::now();
    let parsed = proto::parse_checkpoint(decompressed).map_err(|e| format!("proto parse: {e}"))?;
    profile.proto_parse_ns = t0.elapsed().as_nanos() as u64;

    // Decode summary
    let summary_bcs = parsed.summary_bcs;
    let summary = summary_bcs.and_then(|b| bcs::from_bytes::<CheckpointSummary>(b).ok());

    // BCS decode all transactions
    let _t_bcs = Instant::now();
    let mut txs: Vec<ParsedTx<'_>> = Vec::with_capacity(parsed.transactions.len());

    for ptx in &parsed.transactions {
        let t1 = Instant::now();
        let effects = ptx.effects_bcs.and_then(|bytes| {
            if use_bcs_fast_path() {
                bcs_reader::parse_effects(bytes)
                    .map(ParsedEffects::Fast)
                    .or_else(|_| {
                        bcs::from_bytes::<TransactionEffects>(bytes)
                            .map(ParsedEffects::Full)
                            .map_err(|_| "effects")
                    })
                    .ok()
            } else {
                bcs::from_bytes::<TransactionEffects>(bytes)
                    .map(ParsedEffects::Full)
                    .ok()
            }
        });
        profile.bcs_effects_ns += t1.elapsed().as_nanos() as u64;

        let t2 = Instant::now();
        let tx_data = ptx.transaction_bcs.and_then(|bytes| {
            if use_bcs_fast_path() {
                bcs_reader::parse_tx_data(bytes)
                    .map(ParsedTxData::Fast)
                    .or_else(|_| {
                        crate::extract::decode_transaction_data_pub(bytes)
                            .map(ParsedTxData::Full)
                            .ok_or("tx")
                    })
                    .ok()
            } else {
                crate::extract::decode_transaction_data_pub(bytes)
                    .map(ParsedTxData::Full)
            }
        });
        profile.bcs_tx_data_ns += t2.elapsed().as_nanos() as u64;

        let t3 = Instant::now();
        let events = ptx
            .events_bcs
            .and_then(|bytes| bcs::from_bytes::<TransactionEvents>(bytes).ok());
        profile.bcs_events_ns += t3.elapsed().as_nanos() as u64;

        txs.push(ParsedTx {
            effects,
            tx_data,
            events,
            proto_digest: ptx.digest,
            transaction_bcs: ptx.transaction_bcs,
        });
    }

    profile.tx_count = txs.len();

    let t_extract = Instant::now();
    let mut w = BinaryWriter::new(output);

    // Reserve header: 10 × u32
    let header_pos = w.position();
    for _ in 0..10 {
        w.write_u32(0);
    }

    // ── Checkpoint summary ──
    if let Some(ref s) = summary {
        w.write_u64_dec(s.sequence_number);
        w.write_u64_dec(s.epoch);
        write_iso_timestamp(&mut w, s.timestamp_ms);
        w.write_empty_str(); // digest placeholder
        match &s.previous_digest {
            Some(d) => w.write_hex(d.inner()),
            None => w.write_null(),
        }
        w.write_hex(s.content_digest.inner());
        w.write_u64_dec(s.network_total_transactions);
        w.write_u64_dec(s.epoch_rolling_gas_cost_summary.computation_cost);
        w.write_u64_dec(s.epoch_rolling_gas_cost_summary.storage_cost);
        w.write_u64_dec(s.epoch_rolling_gas_cost_summary.storage_rebate);
        w.write_u64_dec(s.epoch_rolling_gas_cost_summary.non_refundable_storage_fee);
    } else {
        // Default summary
        w.write_str("0"); // seq
        w.write_str("0"); // epoch
        write_iso_timestamp(&mut w, 0); // timestamp
        w.write_empty_str(); // digest
        w.write_null(); // previous_digest
        w.write_null(); // content_digest
        w.write_str("0"); // total_network_transactions
        w.write_str("0");
        w.write_str("0");
        w.write_str("0");
        w.write_str("0"); // gas
    }

    // ── Transactions ──
    let mut tx_count = 0u32;
    for tx in &txs {
        let Some(effects) = &tx.effects else { continue };
        write_tx_digest(&mut w, tx);
        write_tx_sender(&mut w, tx);

        match effects {
            ParsedEffects::Fast(fx) => {
                w.write_bool(fx.status.is_success());
                w.write_u64_dec(fx.gas_used.computation_cost);
                w.write_u64_dec(fx.gas_used.storage_cost);
                w.write_u64_dec(fx.gas_used.storage_rebate);
                w.write_u64_dec(fx.gas_used.non_refundable_storage_fee);
            }
            ParsedEffects::Full(fx) => {
                let gas = fx.gas_cost_summary();
                w.write_bool(matches!(fx.status(), ExecutionStatus::Success));
                w.write_u64_dec(gas.computation_cost);
                w.write_u64_dec(gas.storage_cost);
                w.write_u64_dec(gas.storage_rebate);
                w.write_u64_dec(gas.non_refundable_storage_fee);
            }
        }
        write_checkpoint_context(&mut w, &summary);
        let mc_count = tx.tx_data.as_ref().map(tx_move_call_count).unwrap_or(0);
        w.write_usize_as_u32(mc_count);
        match effects {
            ParsedEffects::Fast(fx) => w.write_u64_dec(fx.epoch),
            ParsedEffects::Full(fx) => w.write_u64_dec(fx.executed_epoch()),
        }

        // Error fields
        match effects {
            ParsedEffects::Fast(fx) => match &fx.status {
                StatusSummary::Success => {
                    w.write_null();
                    w.write_null();
                    w.write_u8(0xFF);
                    w.write_null();
                    w.write_null();
                    w.write_null();
                }
                StatusSummary::Failure(failure) => {
                    w.write_str(failure.kind_name);
                    w.write_null();
                    w.write_u8(failure.command.map(|i| i as u8).unwrap_or(0xFF));
                    if let Some(move_abort) = &failure.move_abort {
                        w.write_u64_dec(move_abort.abort_code);
                        let len_pos = w.begin_string();
                        write_canonical_address_contents_no_prefix(
                            &mut w,
                            &move_abort.module_address,
                        );
                        w.write_raw_str("::");
                        w.write_raw_str(move_abort.module_name);
                        w.finish_string(len_pos);
                        match move_abort.function_name {
                            Some(name) => w.write_str(name),
                            None => w.write_null(),
                        }
                    } else {
                        w.write_null();
                        w.write_null();
                        w.write_null();
                    }
                }
            },
            ParsedEffects::Full(fx) => match fx.status() {
                ExecutionStatus::Success => {
                    w.write_null(); // error_kind
                    w.write_null(); // error_description
                    w.write_u8(0xFF); // error_command_index
                    w.write_null(); // error_abort_code
                    w.write_null(); // error_module
                    w.write_null(); // error_function
                }
                ExecutionStatus::Failure(ExecutionFailure { error, command }) => {
                    w.write_str(&execution_error_kind_name(error));
                    w.write_null(); // error_description
                    w.write_u8(command.map(|i| i as u8).unwrap_or(0xFF));
                    match error {
                        ExecutionErrorKind::MoveAbort(location, code) => {
                            w.write_u64_dec(*code);
                            let len_pos = w.begin_string();
                            write_canonical_address_contents_no_prefix(
                                &mut w,
                                location.module.address().into_bytes().as_ref(),
                            );
                            w.write_raw_str("::");
                            w.write_raw_str(location.module.name().as_str());
                            w.finish_string(len_pos);
                            match &location.function_name {
                                Some(name) => w.write_str(name),
                                None => w.write_null(),
                            }
                        }
                        _ => {
                            w.write_null();
                            w.write_null();
                            w.write_null();
                        }
                    }
                }
            },
        }

        match effects {
            ParsedEffects::Fast(fx) => match fx.events_digest {
                Some(digest) => w.write_base58(&digest),
                None => w.write_null(),
            },
            ParsedEffects::Full(fx) => match fx.events_digest() {
                Some(digest) => w.write_base58(&digest.into_inner()),
                None => w.write_null(),
            },
        }

        match effects {
            ParsedEffects::Fast(fx) => w.write_u64_dec(fx.lamport_version),
            ParsedEffects::Full(fx) => w.write_u64_dec(fx.lamport_version().value()),
        }

        match effects {
            ParsedEffects::Fast(fx) => w.write_usize_as_u32(fx.dependency_count),
            ParsedEffects::Full(fx) => w.write_usize_as_u32(fx.dependencies().len()),
        }

        tx_count += 1;
    }

    // Build ObjectSet from proto objects (shared by object_changes + balance_changes)
    let mut object_set = sui_types::full_checkpoint_content::ObjectSet::default();
    for obj in &parsed.objects {
        if let Ok(decoded) = bcs::from_bytes::<Object>(obj.bcs) {
            object_set.insert(decoded);
        }
    }

    // ── Object changes ──
    let mut oc_count = 0u32;
    for tx in &txs {
        let Some(effects) = &tx.effects else { continue };
        match effects {
            ParsedEffects::Fast(fx) => {
                write_object_changes_fast(&mut w, tx, fx, &summary, &object_set, &mut oc_count);
            }
            ParsedEffects::Full(fx) => {
                write_object_changes_full(&mut w, tx, fx, &summary, &object_set, &mut oc_count);
            }
        }
    }

    // ── Dependencies ──
    let mut dep_count = 0u32;
    for tx in &txs {
        let Some(effects) = &tx.effects else { continue };
        match effects {
            ParsedEffects::Fast(fx) => {
                for dep in &fx.dependencies {
                    write_tx_digest(&mut w, tx);
                    w.write_base58(dep);
                    write_checkpoint_context(&mut w, &summary);
                    dep_count += 1;
                }
            }
            ParsedEffects::Full(fx) => {
                for dep in fx.dependencies() {
                    write_tx_digest(&mut w, tx);
                    w.write_base58(dep.inner());
                    write_checkpoint_context(&mut w, &summary);
                    dep_count += 1;
                }
            }
        }
    }

    // ── Commands ──
    let mut cmd_count = 0u32;
    let mut sys_count = 0u32;
    let mut mc_count = 0u32;
    let mut input_count = 0u32;

    // Commands + Move calls must be written in separate sections, but we need to
    // process them together. Buffer move calls, then write after commands.
    enum McPackage {
        Bytes([u8; 32]),
    }
    struct McEntry<'a> {
        digest_idx: usize,
        call_index: usize,
        pkg: McPackage,
        module: Cow<'a, str>,
        func: Cow<'a, str>,
    }
    let mut move_call_buf: Vec<McEntry<'_>> = Vec::new();

    for (tx_idx, tx) in txs.iter().enumerate() {
        let Some(td) = &tx.tx_data else { continue };
        match td {
            ParsedTxData::Fast(fast) => {
                let mut call_index = 0usize;
                for (command_index, command_bcs) in fast.commands.iter().enumerate() {
                    write_tx_digest(&mut w, tx);
                    w.write_usize_as_u32(command_index);
                    let move_call = bcs_reader::write_command_fields_from_bcs(command_bcs, &mut w)?;
                    write_checkpoint_context(&mut w, &summary);
                    cmd_count += 1;

                    if let Some(MoveCallInfo {
                        package,
                        module,
                        function,
                    }) = move_call
                    {
                        move_call_buf.push(McEntry {
                            digest_idx: tx_idx,
                            call_index,
                            pkg: McPackage::Bytes(package),
                            module: Cow::Borrowed(module),
                            func: Cow::Borrowed(function),
                        });
                        call_index += 1;
                    }
                }
            }
            ParsedTxData::Full(td) => {
                let programmable = match td.kind() {
                    TransactionKind::ProgrammableTransaction(pt)
                    | TransactionKind::ProgrammableSystemTransaction(pt) => pt,
                    _ => continue,
                };

                let mut call_index = 0usize;
                for (command_index, command) in programmable.commands.iter().enumerate() {
                    match command {
                        Command::MoveCall(mc) => {
                            write_tx_digest(&mut w, tx);
                            w.write_usize_as_u32(command_index);
                            w.write_str("MoveCall");
                            w.write_0x_hex(mc.package.as_ref());
                            w.write_str(mc.module.as_str());
                            w.write_str(mc.function.as_str());
                            if mc.type_arguments.is_empty() {
                                w.write_str("[]");
                            } else {
                                let len_pos = w.begin_string();
                                w.write_raw_byte(b'[');
                                for (i, ty) in mc.type_arguments.iter().enumerate() {
                                    if i > 0 {
                                        w.write_raw_byte(b',');
                                    }
                                    w.write_raw_byte(b'"');
                                    let display = ty.to_canonical_display(true);
                                    w.write_display(&display);
                                    w.write_raw_byte(b'"');
                                }
                                w.write_raw_byte(b']');
                                w.finish_string(len_pos);
                            }
                            w.write_null();
                            write_checkpoint_context(&mut w, &summary);
                            cmd_count += 1;

                            move_call_buf.push(McEntry {
                                digest_idx: tx_idx,
                                call_index,
                                pkg: McPackage::Bytes(mc.package.into_bytes()),
                                module: Cow::Owned(mc.module.clone()),
                                func: Cow::Owned(mc.function.clone()),
                            });
                            call_index += 1;
                        }
                        _ => {
                            let (kind, args_json) = format_non_move_command(command);
                            write_tx_digest(&mut w, tx);
                            w.write_usize_as_u32(command_index);
                            w.write_str(kind);
                            w.write_null();
                            w.write_null();
                            w.write_null();
                            w.write_null();
                            w.write_str(&args_json);
                            write_checkpoint_context(&mut w, &summary);
                            cmd_count += 1;
                        }
                    }
                }
            }
        }
    }

    // ── System transactions ──
    for tx in &txs {
        let Some(td) = &tx.tx_data else { continue };
        match td {
            ParsedTxData::Fast(fast) => {
                if matches!(fast.kind, ProgrammableKind::System) {
                    if let Some(full) = tx
                        .transaction_bcs
                        .and_then(crate::extract::decode_transaction_data_pub)
                    {
                        if let Some((kind_name, data)) = format_system_transaction(full.kind()) {
                            write_tx_digest(&mut w, tx);
                            w.write_str(kind_name);
                            w.write_str(&data);
                            write_checkpoint_context(&mut w, &summary);
                            sys_count += 1;
                        }
                    }
                }
            }
            ParsedTxData::Full(td) => {
                if let Some((kind_name, data)) = format_system_transaction(td.kind()) {
                    write_tx_digest(&mut w, tx);
                    w.write_str(kind_name);
                    w.write_str(&data);
                    write_checkpoint_context(&mut w, &summary);
                    sys_count += 1;
                }
            }
        }
    }

    // ── Move calls ──
    for mc in &move_call_buf {
        let tx = &txs[mc.digest_idx];
        write_tx_digest(&mut w, tx);
        w.write_usize_as_u32(mc.call_index);
        match &mc.pkg {
            McPackage::Bytes(bytes) => w.write_0x_hex(bytes),
        }
        w.write_str(mc.module.as_ref());
        w.write_str(mc.func.as_ref());
        write_checkpoint_context(&mut w, &summary);
        mc_count += 1;
    }

    // ── Inputs ──
    for tx in &txs {
        let Some(td) = &tx.tx_data else { continue };
        match td {
            ParsedTxData::Fast(fast) => {
                for (input_index, input_bcs) in fast.inputs.iter().enumerate() {
                    write_tx_digest(&mut w, tx);
                    w.write_usize_as_u32(input_index);
                    bcs_reader::write_input_fields_from_bcs(input_bcs, &mut w)?;
                    write_checkpoint_context(&mut w, &summary);
                    input_count += 1;
                }
            }
            ParsedTxData::Full(td) => {
                let programmable = match td.kind() {
                    TransactionKind::ProgrammableTransaction(pt)
                    | TransactionKind::ProgrammableSystemTransaction(pt) => pt,
                    _ => continue,
                };

                for (input_index, input) in programmable.inputs.iter().enumerate() {
                    write_tx_digest(&mut w, tx);
                    w.write_usize_as_u32(input_index);
                    write_input_fields(&mut w, input);
                    write_checkpoint_context(&mut w, &summary);
                    input_count += 1;
                }
            }
        }
    }

    // ── Unchanged consensus objects ──
    let mut uco_count = 0u32;
    for tx in &txs {
        let Some(effects) = &tx.effects else { continue };
        match effects {
            ParsedEffects::Fast(fx) => {
                for entry in &fx.unchanged_consensus_objects {
                    let object = bcs_reader::parse_unchanged_consensus_object(entry)?;
                    write_tx_digest(&mut w, tx);
                    w.write_0x_hex(&object.id);
                    match object.kind {
                        UnchangedConsensusKindSummary::ReadOnlyRoot { version, digest } => {
                            w.write_str("READ_ONLY_ROOT");
                            w.write_u64_dec(version);
                            w.write_base58(&digest);
                        }
                        UnchangedConsensusKindSummary::MutateConsensusStreamEnded(version) => {
                            w.write_str("MUTATE_CONSENSUS_STREAM_ENDED");
                            w.write_u64_dec(version);
                            w.write_null();
                        }
                        UnchangedConsensusKindSummary::ReadConsensusStreamEnded(version) => {
                            w.write_str("READ_CONSENSUS_STREAM_ENDED");
                            w.write_u64_dec(version);
                            w.write_null();
                        }
                        UnchangedConsensusKindSummary::Cancelled(version) => {
                            w.write_str("CANCELED");
                            w.write_u64_dec(version);
                            w.write_null();
                        }
                        UnchangedConsensusKindSummary::PerEpochConfig => {
                            w.write_str("PER_EPOCH_CONFIG");
                            w.write_null();
                            w.write_null();
                        }
                    }
                    w.write_null();
                    write_checkpoint_context(&mut w, &summary);
                    uco_count += 1;
                }
            }
            ParsedEffects::Full(fx) => {
                for (id, kind) in fx.unchanged_consensus_objects() {
                    write_tx_digest(&mut w, tx);
                    w.write_0x_hex(id.as_ref());
                    match kind {
                        UnchangedConsensusKind::ReadOnlyRoot((version, digest)) => {
                            w.write_str("READ_ONLY_ROOT");
                            w.write_u64_dec(version.value());
                            w.write_base58(digest.inner());
                        }
                        UnchangedConsensusKind::MutateConsensusStreamEnded(version) => {
                            w.write_str("MUTATE_CONSENSUS_STREAM_ENDED");
                            w.write_u64_dec(version.value());
                            w.write_null();
                        }
                        UnchangedConsensusKind::ReadConsensusStreamEnded(version) => {
                            w.write_str("READ_CONSENSUS_STREAM_ENDED");
                            w.write_u64_dec(version.value());
                            w.write_null();
                        }
                        UnchangedConsensusKind::Cancelled(version) => {
                            w.write_str("CANCELED");
                            w.write_u64_dec(version.value());
                            w.write_null();
                        }
                        UnchangedConsensusKind::PerEpochConfig => {
                            w.write_str("PER_EPOCH_CONFIG");
                            w.write_null();
                            w.write_null();
                        }
                    }
                    w.write_null();
                    write_checkpoint_context(&mut w, &summary);
                    uco_count += 1;
                }
            }
        }
    }

    // ── Events ──
    let mut event_count = 0u32;
    for tx in &txs {
        let Some(events) = &tx.events else { continue };
        for (event_seq, event) in events.data.iter().enumerate() {
            write_event(&mut w, tx, event_seq, event, &summary);
            event_count += 1;
        }
    }

    // ── Balance changes — derive_balance_changes_2 per transaction ──
    profile.extract_records_ns = t_extract.elapsed().as_nanos() as u64;

    let t_balance = Instant::now();
    let mut bal_count = 0u32;
    for tx in &txs {
        let Some(effects) = &tx.effects else { continue };
        match effects {
            ParsedEffects::Fast(fx) => {
                for (address, coin_type, amount) in derive_balance_changes_fast(fx, &object_set) {
                    write_tx_digest(&mut w, tx);
                    write_checkpoint_seq(&mut w, &summary);
                    w.write_0x_hex(&address);
                    w.write_str(&coin_type);
                    w.write_i128_dec(amount);
                    write_timestamp(&mut w, &summary);
                    bal_count += 1;
                }
            }
            ParsedEffects::Full(fx) => {
                for bc in sui_types::balance_change::derive_balance_changes_2(fx, &object_set) {
                    write_tx_digest(&mut w, tx);
                    write_checkpoint_seq(&mut w, &summary);
                    w.write_0x_hex(bc.address.as_ref());
                    let display = bc.coin_type.to_canonical_display(true);
                    w.write_display(&display);
                    w.write_i128_dec(bc.amount);
                    write_timestamp(&mut w, &summary);
                    bal_count += 1;
                }
            }
        }
    }

    profile.balance_changes_ns = t_balance.elapsed().as_nanos() as u64;

    // Patch header
    w.write_u32_at(header_pos, tx_count);
    w.write_u32_at(header_pos + 4, oc_count);
    w.write_u32_at(header_pos + 8, dep_count);
    w.write_u32_at(header_pos + 12, cmd_count);
    w.write_u32_at(header_pos + 16, sys_count);
    w.write_u32_at(header_pos + 20, mc_count);
    w.write_u32_at(header_pos + 24, input_count);
    w.write_u32_at(header_pos + 28, uco_count);
    w.write_u32_at(header_pos + 32, event_count);
    w.write_u32_at(header_pos + 36, bal_count);

    Ok((w.position(), profile))
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn write_checkpoint_context(w: &mut BinaryWriter, summary: &Option<CheckpointSummary>) {
    if let Some(s) = summary {
        w.write_u64_dec(s.sequence_number);
        write_iso_timestamp(w, s.timestamp_ms);
    } else {
        w.write_str("0");
        write_iso_timestamp(w, 0);
    }
}

fn write_iso_timestamp(w: &mut BinaryWriter, ms: u64) {
    let ms = ms as i64;
    let secs = ms.div_euclid(1_000);
    let millis = ms.rem_euclid(1_000);
    // Write directly using chrono-like formatting
    let dt_secs = secs;
    let days = dt_secs.div_euclid(86400);
    let time_secs = dt_secs.rem_euclid(86400);
    let hours = time_secs / 3600;
    let mins = (time_secs % 3600) / 60;
    let secs_of_min = time_secs % 60;

    // Convert days since epoch to Y-M-D
    let (year, month, day) = days_to_ymd(days as i32 + 719468);

    let mut buf = [0u8; 28];
    let n = write_iso_to_buf(
        &mut buf,
        year,
        month,
        day,
        hours as u32,
        mins as u32,
        secs_of_min as u32,
        millis as u32,
    );
    w.write_str(unsafe { std::str::from_utf8_unchecked(&buf[..n]) });
}

fn write_iso_to_buf(
    buf: &mut [u8; 28],
    y: i32,
    m: u32,
    d: u32,
    h: u32,
    min: u32,
    s: u32,
    ms: u32,
) -> usize {
    // YYYY-MM-DDTHH:MM:SS.mmmZ
    let y = y as u32;
    buf[0] = b'0' + (y / 1000) as u8;
    buf[1] = b'0' + ((y / 100) % 10) as u8;
    buf[2] = b'0' + ((y / 10) % 10) as u8;
    buf[3] = b'0' + (y % 10) as u8;
    buf[4] = b'-';
    buf[5] = b'0' + (m / 10) as u8;
    buf[6] = b'0' + (m % 10) as u8;
    buf[7] = b'-';
    buf[8] = b'0' + (d / 10) as u8;
    buf[9] = b'0' + (d % 10) as u8;
    buf[10] = b'T';
    buf[11] = b'0' + (h / 10) as u8;
    buf[12] = b'0' + (h % 10) as u8;
    buf[13] = b':';
    buf[14] = b'0' + (min / 10) as u8;
    buf[15] = b'0' + (min % 10) as u8;
    buf[16] = b':';
    buf[17] = b'0' + (s / 10) as u8;
    buf[18] = b'0' + (s % 10) as u8;
    buf[19] = b'.';
    buf[20] = b'0' + (ms / 100) as u8;
    buf[21] = b'0' + ((ms / 10) % 10) as u8;
    buf[22] = b'0' + (ms % 10) as u8;
    buf[23] = b'Z';
    24
}

fn days_to_ymd(days: i32) -> (i32, u32, u32) {
    // Algorithm from https://howardhinnant.github.io/date_algorithms.html
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = (days - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i32 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

fn write_tx_digest(w: &mut BinaryWriter, tx: &ParsedTx<'_>) {
    match tx.effects.as_ref() {
        Some(ParsedEffects::Fast(fx)) => w.write_base58(&fx.digest),
        Some(ParsedEffects::Full(fx)) => w.write_base58(fx.transaction_digest().inner()),
        None if !tx.proto_digest.is_empty() => w.write_str(tx.proto_digest),
        None => w.write_empty_str(),
    }
}

fn write_tx_sender(w: &mut BinaryWriter, tx: &ParsedTx<'_>) {
    match tx.tx_data.as_ref() {
        Some(ParsedTxData::Fast(td)) => w.write_0x_hex(&td.sender),
        Some(ParsedTxData::Full(td)) => w.write_0x_hex(td.sender().as_ref()),
        None => w.write_empty_str(),
    }
}

fn tx_move_call_count(tx_data: &ParsedTxData<'_>) -> usize {
    match tx_data {
        ParsedTxData::Fast(td) => td.move_call_count,
        ParsedTxData::Full(td) => count_move_calls(td.kind()),
    }
}

fn write_canonical_address_contents_no_prefix(w: &mut BinaryWriter, bytes: &[u8]) {
    w.write_raw_hex_contents(bytes);
}

fn write_object_changes_full(
    w: &mut BinaryWriter,
    tx: &ParsedTx<'_>,
    effects: &TransactionEffects,
    summary: &Option<CheckpointSummary>,
    object_set: &sui_types::full_checkpoint_content::ObjectSet,
    count: &mut u32,
) {
    let old_metadata: HashMap<ObjectID, Owner> = effects
        .old_object_metadata()
        .into_iter()
        .map(|((id, _, _), owner)| (id, owner))
        .collect();

    let mut new_metadata: HashMap<ObjectID, Owner> = HashMap::new();
    for ((id, _, _), owner) in effects.created() {
        new_metadata.insert(id, owner);
    }
    for ((id, _, _), owner) in effects.mutated() {
        new_metadata.insert(id, owner);
    }
    for ((id, _, _), owner) in effects.unwrapped() {
        new_metadata.insert(id, owner);
    }

    let created_ids: HashSet<ObjectID> = effects.created().into_iter().map(|(o, _)| o.0).collect();
    let mutated_ids: HashSet<ObjectID> = effects.mutated().into_iter().map(|(o, _)| o.0).collect();
    let unwrapped_ids: HashSet<ObjectID> =
        effects.unwrapped().into_iter().map(|(o, _)| o.0).collect();
    let deleted_ids: HashSet<ObjectID> = effects
        .deleted()
        .into_iter()
        .map(|o| o.0)
        .chain(effects.unwrapped_then_deleted().into_iter().map(|o| o.0))
        .collect();
    let wrapped_ids: HashSet<ObjectID> = effects.wrapped().into_iter().map(|o| o.0).collect();
    let published_ids: HashSet<ObjectID> = effects.published_packages().into_iter().collect();

    let gas_oid = effects.gas_object().0 .0;
    let gas_oid = if gas_oid == ObjectID::ZERO {
        None
    } else {
        Some(gas_oid)
    };

    for change in effects.object_changes() {
        write_tx_digest(w, tx);
        w.write_0x_hex(change.id.as_ref());

        // change_type
        let ct = derive_change_type_str(
            &change,
            &created_ids,
            &mutated_ids,
            &unwrapped_ids,
            &deleted_ids,
            &wrapped_ids,
            &published_ids,
        );
        w.write_str(ct);

        match change
            .output_version
            .and_then(|v| object_set.get(&sui_types::storage::ObjectKey(change.id, v)))
            .or_else(|| {
                change
                    .input_version
                    .and_then(|v| object_set.get(&sui_types::storage::ObjectKey(change.id, v)))
            })
            .and_then(|obj| obj.type_().map(|t| t.to_canonical_string(true)))
        {
            Some(display) => w.write_str(&display),
            None => w.write_null(),
        }

        // input_version, input_digest
        w.write_opt_u64_dec(change.input_version.map(|v| v.value()));
        match change.input_digest {
            Some(d) => w.write_base58(d.inner()),
            None => w.write_null(),
        }

        // input_owner, input_owner_kind
        write_owner_fields_full(w, old_metadata.get(&change.id));
        // output_version, output_digest
        w.write_opt_u64_dec(change.output_version.map(|v| v.value()));
        match change.output_digest {
            Some(d) => w.write_base58(d.inner()),
            None => w.write_null(),
        }
        // output_owner, output_owner_kind
        write_owner_fields_full(w, new_metadata.get(&change.id));

        w.write_bool(gas_oid.as_ref().is_some_and(|id| *id == change.id));
        write_checkpoint_context(w, summary);
        *count += 1;
    }
}

fn write_object_changes_fast(
    w: &mut BinaryWriter,
    tx: &ParsedTx<'_>,
    effects: &EffectsExtract<'_>,
    summary: &Option<CheckpointSummary>,
    object_set: &sui_types::full_checkpoint_content::ObjectSet,
    count: &mut u32,
) {
    for (index, entry) in effects.changed_objects.iter().enumerate() {
        let change = match bcs_reader::parse_changed_object(entry) {
            Ok(change) => change,
            Err(_) => continue,
        };
        if matches!(change.output, ObjectOutSummary::AccumulatorWrite(_)) {
            continue;
        }

        write_tx_digest(w, tx);
        w.write_0x_hex(&change.id);
        w.write_str(derive_change_type_fast(&change));

        let object_id = ObjectID::new(change.id);
        let output_version = change_output_version(&change, effects.lamport_version);
        let input_version = change_input_version(&change);
        match output_version
            .map(SequenceNumber::from_u64)
            .and_then(|v| object_set.get(&ObjectKey(object_id, v)))
            .or_else(|| {
                input_version
                    .map(SequenceNumber::from_u64)
                    .and_then(|v| object_set.get(&ObjectKey(object_id, v)))
            })
            .and_then(|obj| obj.type_().map(|t| t.to_canonical_string(true)))
        {
            Some(display) => w.write_str(&display),
            None => w.write_null(),
        }

        match change.input {
            ObjectInSummary::NotExist => {
                w.write_null();
                w.write_null();
                write_owner_fields_fast(w, None);
            }
            ObjectInSummary::Exist {
                version,
                digest,
                owner,
            } => {
                w.write_u64_dec(version);
                w.write_base58(&digest);
                write_owner_fields_fast(w, Some(&owner));
            }
        }

        match change.output {
            ObjectOutSummary::NotExist => {
                w.write_null();
                w.write_null();
                write_owner_fields_fast(w, None);
            }
            ObjectOutSummary::ObjectWrite { digest, owner } => {
                w.write_u64_dec(effects.lamport_version);
                w.write_base58(&digest);
                write_owner_fields_fast(w, Some(&owner));
            }
            ObjectOutSummary::PackageWrite { version, digest } => {
                w.write_u64_dec(version);
                w.write_base58(&digest);
                write_owner_fields_fast(w, Some(&OwnerSummary::Immutable));
            }
            ObjectOutSummary::AccumulatorWrite(_) => unreachable!(),
        }

        w.write_bool(effects.gas_object_index == Some(index as u32));
        write_checkpoint_context(w, summary);
        *count += 1;
    }
}

fn write_owner_fields_full(w: &mut BinaryWriter, owner: Option<&Owner>) {
    match owner {
        Some(Owner::AddressOwner(addr)) => {
            w.write_0x_hex(addr.as_ref());
            w.write_str("ADDRESS");
        }
        Some(Owner::ObjectOwner(addr)) => {
            w.write_0x_hex(addr.as_ref());
            w.write_str("OBJECT");
        }
        Some(Owner::Shared { .. }) => {
            w.write_null();
            w.write_str("SHARED");
        }
        Some(Owner::Immutable) => {
            w.write_null();
            w.write_str("IMMUTABLE");
        }
        Some(Owner::ConsensusAddressOwner { owner, .. }) => {
            w.write_0x_hex(owner.as_ref());
            w.write_str("CONSENSUS_ADDRESS");
        }
        None => {
            w.write_null();
            w.write_null();
        }
    }
}

fn write_owner_fields_fast(w: &mut BinaryWriter, owner: Option<&OwnerSummary>) {
    match owner {
        Some(OwnerSummary::Address(addr)) => {
            w.write_0x_hex(addr);
            w.write_str("ADDRESS");
        }
        Some(OwnerSummary::Object(addr)) => {
            w.write_0x_hex(addr);
            w.write_str("OBJECT");
        }
        Some(OwnerSummary::Shared) => {
            w.write_null();
            w.write_str("SHARED");
        }
        Some(OwnerSummary::Immutable) => {
            w.write_null();
            w.write_str("IMMUTABLE");
        }
        Some(OwnerSummary::ConsensusAddress(owner)) => {
            w.write_0x_hex(owner);
            w.write_str("CONSENSUS_ADDRESS");
        }
        None => {
            w.write_null();
            w.write_null();
        }
    }
}

fn change_input_version(change: &ChangedObject<'_>) -> Option<u64> {
    match change.input {
        ObjectInSummary::Exist { version, .. } => Some(version),
        ObjectInSummary::NotExist => None,
    }
}

fn change_output_version(change: &ChangedObject<'_>, lamport_version: u64) -> Option<u64> {
    match change.output {
        ObjectOutSummary::NotExist | ObjectOutSummary::AccumulatorWrite(_) => None,
        ObjectOutSummary::ObjectWrite { .. } => Some(lamport_version),
        ObjectOutSummary::PackageWrite { version, .. } => Some(version),
    }
}

fn derive_change_type_fast(change: &ChangedObject<'_>) -> &'static str {
    if matches!(change.output, ObjectOutSummary::PackageWrite { .. }) {
        return "PACKAGE_WRITE";
    }
    match (
        matches!(change.input, ObjectInSummary::Exist { .. }),
        matches!(change.output, ObjectOutSummary::NotExist),
        change.id_operation,
    ) {
        (false, false, FastIdOperation::Created) => "CREATED",
        (false, false, FastIdOperation::None) => "UNWRAPPED",
        (true, false, _) => "MUTATED",
        (true, true, FastIdOperation::Deleted) => "DELETED",
        (true, true, _) => "WRAPPED",
        (false, true, FastIdOperation::Deleted) => "DELETED",
        _ => "UNKNOWN",
    }
}

fn write_input_fields(w: &mut BinaryWriter, input: &CallArg) {
    match input {
        CallArg::Pure(bytes) => {
            w.write_str("PURE");
            w.write_null(); // object_id
            w.write_null(); // version
            w.write_null(); // digest
            w.write_null(); // mutability
            w.write_null(); // initial_shared_version
            w.write_hex(bytes); // pure_bytes
            w.write_null(); // amount
            w.write_null(); // coin_type
            w.write_null(); // source
        }
        CallArg::Object(obj) => {
            match obj {
                ObjectArg::ImmOrOwnedObject((id, version, digest)) => {
                    w.write_str("IMMUTABLE_OR_OWNED");
                    w.write_0x_hex(id.as_ref());
                    w.write_u64_dec(version.value());
                    w.write_base58(digest.inner());
                    w.write_null(); // mutability
                    w.write_null(); // initial_shared_version
                }
                ObjectArg::SharedObject {
                    id,
                    initial_shared_version,
                    mutability,
                } => {
                    w.write_str("SHARED");
                    w.write_0x_hex(id.as_ref());
                    w.write_null(); // version
                    w.write_null(); // digest
                    w.write_str(shared_mutability_name(mutability));
                    w.write_u64_dec(initial_shared_version.value());
                }
                ObjectArg::Receiving((id, version, digest)) => {
                    w.write_str("RECEIVING");
                    w.write_0x_hex(id.as_ref());
                    w.write_u64_dec(version.value());
                    w.write_base58(digest.inner());
                    w.write_null(); // mutability
                    w.write_null(); // initial_shared_version
                }
            }
            w.write_null(); // pure_bytes
            w.write_null(); // amount
            w.write_null(); // coin_type
            w.write_null(); // source
        }
        CallArg::FundsWithdrawal(withdrawal) => {
            w.write_str("FUNDS_WITHDRAWAL");
            w.write_null(); // object_id
            w.write_null(); // version
            w.write_null(); // digest
            w.write_null(); // mutability
            w.write_null(); // initial_shared_version
            w.write_null(); // pure_bytes
            match withdrawal.reservation {
                Reservation::MaxAmountU64(v) => w.write_u64_dec(v),
            }
            let type_tag = withdrawal.type_arg.to_type_tag();
            let display = type_tag.to_canonical_display(true);
            w.write_display(&display);
            match withdrawal.withdraw_from {
                WithdrawFrom::Sender => w.write_str("SENDER"),
                WithdrawFrom::Sponsor => w.write_str("SPONSOR"),
            }
        }
    }
}

fn write_event(
    w: &mut BinaryWriter,
    tx: &ParsedTx<'_>,
    event_seq: usize,
    event: &Event,
    summary: &Option<CheckpointSummary>,
) {
    let event_type = TypeTag::Struct(Box::new(event.type_.clone()));
    // Binary format order: handler_name, checkpoint_seq, tx_digest, event_seq, sender, timestamp, data
    let handler = event_type.to_canonical_display(true);
    w.write_display(&handler); // handler_name
    write_checkpoint_seq(w, summary); // checkpoint_seq only
    write_tx_digest(w, tx); // tx_digest
    w.write_usize_as_u32(event_seq);
    w.write_0x_hex(event.sender.as_ref());
    write_timestamp(w, summary); // timestamp only

    let len_pos = w.begin_string();
    w.write_raw_str("{\"packageId\":\"");
    w.write_raw_str("0x");
    w.write_raw_hex_contents(event.package_id.as_ref());
    w.write_raw_str("\",\"module\":\"");
    w.write_raw_str(event.transaction_module.as_str());
    w.write_raw_str("\",\"eventType\":\"");
    let display = event_type.to_canonical_display(true);
    w.write_display(&display);
    w.write_raw_str("\",\"contents\":\"");
    w.write_raw_str("0x");
    w.write_raw_hex_contents(&event.contents);
    w.write_raw_str("\"}");
    w.finish_string(len_pos);
}

fn write_checkpoint_seq(w: &mut BinaryWriter, summary: &Option<CheckpointSummary>) {
    if let Some(s) = summary {
        w.write_u64_dec(s.sequence_number);
    } else {
        w.write_str("0");
    }
}

fn write_timestamp(w: &mut BinaryWriter, summary: &Option<CheckpointSummary>) {
    write_iso_timestamp(w, summary.as_ref().map(|s| s.timestamp_ms).unwrap_or(0));
}

fn derive_balance_changes_fast(
    effects: &EffectsExtract<'_>,
    object_set: &sui_types::full_checkpoint_content::ObjectSet,
) -> Vec<([u8; 32], String, i128)> {
    let mut balances = std::collections::BTreeMap::<([u8; 32], String), i128>::new();

    for entry in &effects.changed_objects {
        let Ok(change) = bcs_reader::parse_changed_object(entry) else {
            continue;
        };
        let object_id = ObjectID::new(change.id);

        if let ObjectInSummary::Exist { version, .. } = change.input {
            if let Some(object) =
                object_set.get(&ObjectKey(object_id, SequenceNumber::from_u64(version)))
            {
                accumulate_coin_balance(object, -1, &mut balances);
            }
        }

        match change.output {
            ObjectOutSummary::ObjectWrite { .. } => {
                if let Some(object) = object_set.get(&ObjectKey(
                    object_id,
                    SequenceNumber::from_u64(effects.lamport_version),
                )) {
                    accumulate_coin_balance(object, 1, &mut balances);
                }
            }
            ObjectOutSummary::PackageWrite { version, .. } => {
                if let Some(object) =
                    object_set.get(&ObjectKey(object_id, SequenceNumber::from_u64(version)))
                {
                    accumulate_coin_balance(object, 1, &mut balances);
                }
            }
            ObjectOutSummary::AccumulatorWrite(write) => {
                accumulate_accumulator_balance(&write, &mut balances);
            }
            ObjectOutSummary::NotExist => {}
        }
    }

    balances
        .into_iter()
        .filter_map(|((address, coin_type), amount)| {
            if amount == 0 {
                None
            } else {
                Some((address, coin_type, amount))
            }
        })
        .collect()
}

fn accumulate_coin_balance(
    object: &Object,
    sign: i128,
    balances: &mut std::collections::BTreeMap<([u8; 32], String), i128>,
) {
    let owner = match object.owner() {
        Owner::AddressOwner(address)
        | Owner::ObjectOwner(address)
        | Owner::ConsensusAddressOwner { owner: address, .. } => address,
        Owner::Shared { .. } | Owner::Immutable => return,
    };
    let Some((coin_type, balance)) = Coin::extract_balance_if_coin(object).ok().flatten() else {
        return;
    };
    let mut address_bytes = [0u8; 32];
    address_bytes.copy_from_slice(owner.as_ref());
    *balances
        .entry((address_bytes, coin_type.to_canonical_string(true)))
        .or_default() += sign * balance as i128;
}

fn accumulate_accumulator_balance(
    write: &crate::bcs_reader::AccumulatorWrite<'_>,
    balances: &mut std::collections::BTreeMap<([u8; 32], String), i128>,
) {
    let amount = match (&write.operation, &write.value) {
        (AccumulatorOperation::Merge, AccumulatorValueSummary::Integer(value)) => *value as i128,
        (AccumulatorOperation::Split, AccumulatorValueSummary::Integer(value)) => -(*value as i128),
        _ => return,
    };
    let Some(coin_type) = balance_coin_type_from_bcs(write.type_tag_bcs) else {
        return;
    };
    *balances.entry((write.address, coin_type)).or_default() += amount;
}

fn balance_coin_type_from_bcs(bcs: &[u8]) -> Option<String> {
    let tag = bcs::from_bytes::<TypeTag>(bcs).ok()?;
    Balance::maybe_get_balance_type_param(&tag).map(|inner| inner.to_canonical_string(true))
}

fn format_non_move_command(command: &Command) -> (&'static str, String) {
    use serde_json::{Map, Value};

    fn arg_val(a: &Argument) -> Value {
        match a {
            Argument::GasCoin => Value::String("GasCoin".to_string()),
            Argument::Input(i) => Value::Object(Map::from_iter([(
                "Input".to_string(),
                Value::Number((*i).into()),
            )])),
            Argument::Result(i) => Value::Object(Map::from_iter([(
                "Result".to_string(),
                Value::Number((*i).into()),
            )])),
            Argument::NestedResult(a, b) => Value::Object(Map::from_iter([(
                "NestedResult".to_string(),
                Value::Array(vec![Value::Number((*a).into()), Value::Number((*b).into())]),
            )])),
        }
    }

    fn make_json(entries: Vec<(&str, Value)>) -> String {
        let mut map = Map::new();
        for (k, v) in entries {
            map.insert(k.to_string(), v);
        }
        Value::Object(map).to_string()
    }

    match command {
        Command::TransferObjects(objects, address) => (
            "TransferObjects",
            make_json(vec![
                (
                    "objects",
                    Value::Array(objects.iter().map(arg_val).collect()),
                ),
                ("address", arg_val(address)),
            ]),
        ),
        Command::SplitCoins(coin, amounts) => (
            "SplitCoins",
            make_json(vec![
                ("coin", arg_val(coin)),
                (
                    "amounts",
                    Value::Array(amounts.iter().map(arg_val).collect()),
                ),
            ]),
        ),
        Command::MergeCoins(coin, coins) => (
            "MergeCoins",
            make_json(vec![
                ("coin", arg_val(coin)),
                ("coins", Value::Array(coins.iter().map(arg_val).collect())),
            ]),
        ),
        Command::Publish(modules, deps) => (
            "Publish",
            make_json(vec![
                (
                    "modules",
                    Value::Array(
                        modules
                            .iter()
                            .map(|m| Value::String(format_address_hex(m)))
                            .collect(),
                    ),
                ),
                (
                    "dependencies",
                    Value::Array(
                        deps.iter()
                            .map(|id| Value::String(id.to_string()))
                            .collect(),
                    ),
                ),
            ]),
        ),
        Command::Upgrade(modules, deps, package, ticket) => (
            "Upgrade",
            make_json(vec![
                (
                    "modules",
                    Value::Array(
                        modules
                            .iter()
                            .map(|m| Value::String(format_address_hex(m)))
                            .collect(),
                    ),
                ),
                (
                    "dependencies",
                    Value::Array(
                        deps.iter()
                            .map(|id| Value::String(id.to_string()))
                            .collect(),
                    ),
                ),
                ("package", Value::String(package.to_string())),
                ("ticket", arg_val(ticket)),
            ]),
        ),
        Command::MakeMoveVec(element_type, elements) => (
            "MakeMoveVector",
            make_json(vec![
                (
                    "elementType",
                    element_type
                        .as_ref()
                        .map(|t| Value::String(t.to_canonical_string(true)))
                        .unwrap_or(Value::Null),
                ),
                (
                    "elements",
                    Value::Array(elements.iter().map(arg_val).collect()),
                ),
            ]),
        ),
        Command::MoveCall(_) => unreachable!(),
    }
}

fn format_system_transaction(kind: &TransactionKind) -> Option<(&'static str, String)> {
    use serde::Serialize;
    fn js<T: Serialize>(v: &T) -> String {
        serde_json::to_string(v).unwrap_or_else(|_| "{}".to_string())
    }
    match kind {
        TransactionKind::Genesis(d) => Some(("GENESIS", js(d))),
        TransactionKind::ChangeEpoch(d) => Some(("CHANGE_EPOCH", js(d))),
        TransactionKind::ConsensusCommitPrologue(d) => {
            Some(("CONSENSUS_COMMIT_PROLOGUE_V1", js(d)))
        }
        TransactionKind::ConsensusCommitPrologueV2(d) => {
            Some(("CONSENSUS_COMMIT_PROLOGUE_V2", js(d)))
        }
        TransactionKind::ConsensusCommitPrologueV3(d) => {
            Some(("CONSENSUS_COMMIT_PROLOGUE_V3", js(d)))
        }
        TransactionKind::ConsensusCommitPrologueV4(d) => {
            Some(("CONSENSUS_COMMIT_PROLOGUE_V4", js(d)))
        }
        TransactionKind::AuthenticatorStateUpdate(d) => Some(("AUTHENTICATOR_STATE_UPDATE", js(d))),
        TransactionKind::EndOfEpochTransaction(d) => Some(("END_OF_EPOCH", js(d))),
        TransactionKind::RandomnessStateUpdate(d) => Some(("RANDOMNESS_STATE_UPDATE", js(d))),
        TransactionKind::ProgrammableSystemTransaction(d) => {
            Some(("PROGRAMMABLE_SYSTEM_TRANSACTION", js(d)))
        }
        TransactionKind::ProgrammableTransaction(_) => None,
    }
}

fn derive_change_type_str(
    change: &sui_types::effects::ObjectChange,
    created: &HashSet<ObjectID>,
    mutated: &HashSet<ObjectID>,
    unwrapped: &HashSet<ObjectID>,
    deleted: &HashSet<ObjectID>,
    wrapped: &HashSet<ObjectID>,
    published: &HashSet<ObjectID>,
) -> &'static str {
    if published.contains(&change.id) {
        return "PACKAGE_WRITE";
    }
    match (
        change.input_version.is_some(),
        change.output_version.is_some(),
        change.id_operation,
    ) {
        (false, true, IDOperation::Created) => "CREATED",
        (false, true, _) if unwrapped.contains(&change.id) => "UNWRAPPED",
        (true, true, _) if mutated.contains(&change.id) => "MUTATED",
        (true, false, IDOperation::Deleted) if deleted.contains(&change.id) => "DELETED",
        (true, false, _) if wrapped.contains(&change.id) => "WRAPPED",
        (false, true, _) if created.contains(&change.id) => "CREATED",
        (false, true, _) => "UNWRAPPED",
        (true, true, _) => "MUTATED",
        (true, false, IDOperation::Deleted) => "DELETED",
        (true, false, _) => "WRAPPED",
        _ => "UNKNOWN",
    }
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

fn count_move_calls(kind: &TransactionKind) -> usize {
    match kind {
        TransactionKind::ProgrammableTransaction(pt)
        | TransactionKind::ProgrammableSystemTransaction(pt) => pt
            .commands
            .iter()
            .filter(|c| matches!(c, Command::MoveCall(_)))
            .count(),
        _ => 0,
    }
}

fn shared_mutability_name(m: &SharedObjectMutability) -> &'static str {
    match m {
        SharedObjectMutability::Immutable => "IMMUTABLE",
        SharedObjectMutability::Mutable => "MUTABLE",
        SharedObjectMutability::NonExclusiveWrite => "NON_EXCLUSIVE_WRITE",
    }
}

static HEX_LUT: &[u8; 16] = b"0123456789abcdef";

fn format_address_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("0x");
    for &b in bytes {
        s.push(HEX_LUT[(b >> 4) as usize] as char);
        s.push(HEX_LUT[(b & 0x0f) as usize] as char);
    }
    s
}
