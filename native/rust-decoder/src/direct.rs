//! Direct binary write — fused extraction + binary output, no intermediate String allocations.
//!
//! Same binary format as binary.rs, but writes fields directly to the output buffer
//! instead of building record structs with owned Strings.

use std::collections::{HashMap, HashSet};

use sui_types::base_types::ObjectID;
use sui_types::effects::{
    IDOperation, TransactionEffects,
    TransactionEffectsAPI, TransactionEvents, UnchangedConsensusKind,
};
use sui_types::event::Event;
use sui_types::execution_status::{ExecutionErrorKind, ExecutionStatus};
use sui_types::messages_checkpoint::CheckpointSummary;
use sui_types::object::{Object, Owner};
use sui_types::transaction::{
    Argument, CallArg, Command, ObjectArg, Reservation,
    SharedObjectMutability, TransactionData, TransactionDataAPI, TransactionKind, WithdrawFrom,
};
use sui_types::TypeTag;

use crate::binary::BinaryWriter;
use crate::extract::ExtractProfile;
use crate::proto;

/// Pre-parsed transaction data for a single executed transaction.
struct ParsedTx {
    effects: Option<TransactionEffects>,
    tx_data: Option<TransactionData>,
    events: Option<TransactionEvents>,
    /// Formatted transaction digest (base58 — can't avoid this allocation).
    digest: String,
    /// Formatted sender address (hex — can't avoid without writing twice).
    sender: String,
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
    let summary = summary_bcs
        .and_then(|b| bcs::from_bytes::<CheckpointSummary>(b).ok());

    // BCS decode all transactions
    let _t_bcs = Instant::now();
    let mut txs: Vec<ParsedTx> = Vec::with_capacity(parsed.transactions.len());

    for ptx in &parsed.transactions {
        let t1 = Instant::now();
        let effects = ptx
            .effects_bcs
            .and_then(|bytes| bcs::from_bytes::<TransactionEffects>(bytes).ok());
        profile.bcs_effects_ns += t1.elapsed().as_nanos() as u64;

        let t2 = Instant::now();
        let tx_data = ptx.transaction_bcs.and_then(crate::extract::decode_transaction_data_pub);
        profile.bcs_tx_data_ns += t2.elapsed().as_nanos() as u64;

        let t3 = Instant::now();
        let events = ptx
            .events_bcs
            .and_then(|bytes| bcs::from_bytes::<TransactionEvents>(bytes).ok());
        profile.bcs_events_ns += t3.elapsed().as_nanos() as u64;

        let digest = effects
            .as_ref()
            .map(|fx| fx.transaction_digest().to_string())
            .filter(|s| !s.is_empty())
            .or_else(|| {
                if ptx.digest.is_empty() { None } else { Some(ptx.digest.to_string()) }
            })
            .unwrap_or_default();

        let sender = tx_data
            .as_ref()
            .map(|tx| tx.sender().to_string())
            .unwrap_or_default();

        txs.push(ParsedTx { effects, tx_data, events, digest, sender });
    }

    profile.tx_count = txs.len();

    let t_extract = Instant::now();
    let mut w = BinaryWriter::new(output);

    // Reserve header: 10 × u32
    let header_pos = w.position();
    for _ in 0..10 { w.write_u32(0); }

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
        w.write_str("0"); w.write_str("0"); w.write_str("0"); w.write_str("0"); // gas
    }

    // ── Transactions ──
    let mut tx_count = 0u32;
    for tx in &txs {
        let Some(effects) = &tx.effects else { continue };
        let success = matches!(effects.status(), ExecutionStatus::Success);
        let gas = effects.gas_cost_summary();

        w.write_str(&tx.digest);
        w.write_str(&tx.sender);
        w.write_bool(success);
        w.write_u64_dec(gas.computation_cost);
        w.write_u64_dec(gas.storage_cost);
        w.write_u64_dec(gas.storage_rebate);
        w.write_u64_dec(gas.non_refundable_storage_fee);
        write_checkpoint_context(&mut w, &summary);
        let mc_count = tx.tx_data.as_ref().map(|td| count_move_calls(td.kind())).unwrap_or(0);
        w.write_usize_as_u32(mc_count);
        w.write_u64_dec(effects.executed_epoch());

        // Error fields
        match effects.status() {
            ExecutionStatus::Success => {
                w.write_null(); // error_kind
                w.write_null(); // error_description
                w.write_u8(0xFF); // error_command_index
                w.write_null(); // error_abort_code
                w.write_null(); // error_module
                w.write_null(); // error_function
            }
            ExecutionStatus::Failure(failure) => {
                w.write_str(&execution_error_kind_name(&failure.error));
                w.write_null(); // error_description
                w.write_u8(failure.command.map(|i| i as u8).unwrap_or(0xFF));
                match &failure.error {
                    ExecutionErrorKind::MoveAbort(location, code) => {
                        w.write_display(code);
                        w.write_display(&location.module);
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
        }

        // events_digest
        match effects.events_digest() {
            Some(d) => w.write_display(d),
            None => w.write_null(),
        }
        // lamport_version
        w.write_u64_dec(effects.lamport_version().value());
        // dependency_count
        w.write_usize_as_u32(effects.dependencies().len());

        tx_count += 1;
    }

    // ── Object changes ──
    let mut oc_count = 0u32;
    for tx in &txs {
        let Some(effects) = &tx.effects else { continue };
        write_object_changes(&mut w, &tx.digest, effects, &summary, &mut oc_count);
    }

    // ── Dependencies ──
    let mut dep_count = 0u32;
    for tx in &txs {
        let Some(effects) = &tx.effects else { continue };
        for dep in effects.dependencies() {
            w.write_str(&tx.digest);
            w.write_display(dep);
            write_checkpoint_context(&mut w, &summary);
            dep_count += 1;
        }
    }

    // ── Commands ──
    let mut cmd_count = 0u32;
    let mut sys_count = 0u32;
    let mut mc_count = 0u32;
    let mut input_count = 0u32;

    // Commands + Move calls must be written in separate sections, but we need to
    // process them together. Buffer move calls, then write after commands.
    struct McEntry { digest_idx: usize, call_index: usize, pkg: String, module: String, func: String }
    let mut move_call_buf: Vec<McEntry> = Vec::new();

    for (tx_idx, tx) in txs.iter().enumerate() {
        let Some(td) = &tx.tx_data else { continue };
        let programmable = match td.kind() {
            TransactionKind::ProgrammableTransaction(pt)
            | TransactionKind::ProgrammableSystemTransaction(pt) => pt,
            _ => continue,
        };

        let mut call_index = 0usize;
        for (command_index, command) in programmable.commands.iter().enumerate() {
            match command {
                Command::MoveCall(mc) => {
                    // Command record
                    w.write_str(&tx.digest);
                    w.write_usize_as_u32(command_index);
                    w.write_str("MoveCall");
                    w.write_display(&mc.package);
                    w.write_display(&mc.module);
                    w.write_str(mc.function.as_str());
                    // type_arguments as JSON array
                    if mc.type_arguments.is_empty() {
                        w.write_str("[]");
                    } else {
                        let ta: Vec<String> = mc.type_arguments.iter()
                            .map(|t| t.to_canonical_string(true))
                            .collect();
                        let json = serde_json::to_string(&ta).unwrap_or_else(|_| "[]".to_string());
                        w.write_str(&json);
                    }
                    w.write_null(); // args
                    write_checkpoint_context(&mut w, &summary);
                    cmd_count += 1;

                    // Buffer move call for later section
                    move_call_buf.push(McEntry {
                        digest_idx: tx_idx,
                        call_index,
                        pkg: mc.package.to_string(),
                        module: mc.module.to_string(),
                        func: mc.function.to_string(),
                    });
                    call_index += 1;
                }
                _ => {
                    let (kind, args_json) = format_non_move_command(command);
                    w.write_str(&tx.digest);
                    w.write_usize_as_u32(command_index);
                    w.write_str(kind);
                    w.write_null(); // package
                    w.write_null(); // module
                    w.write_null(); // function
                    w.write_null(); // type_arguments
                    w.write_str(&args_json);
                    write_checkpoint_context(&mut w, &summary);
                    cmd_count += 1;
                }
            }
        }
    }

    // ── System transactions ──
    for tx in &txs {
        let Some(td) = &tx.tx_data else { continue };
        if let Some((kind_name, data)) = format_system_transaction(td.kind()) {
            w.write_str(&tx.digest);
            w.write_str(kind_name);
            w.write_str(&data);
            write_checkpoint_context(&mut w, &summary);
            sys_count += 1;
        }
    }

    // ── Move calls ──
    for mc in &move_call_buf {
        let tx = &txs[mc.digest_idx];
        w.write_str(&tx.digest);
        w.write_usize_as_u32(mc.call_index);
        w.write_str(&mc.pkg);
        w.write_str(&mc.module);
        w.write_str(&mc.func);
        write_checkpoint_context(&mut w, &summary);
        mc_count += 1;
    }

    // ── Inputs ──
    for tx in &txs {
        let Some(td) = &tx.tx_data else { continue };
        let programmable = match td.kind() {
            TransactionKind::ProgrammableTransaction(pt)
            | TransactionKind::ProgrammableSystemTransaction(pt) => pt,
            _ => continue,
        };

        for (input_index, input) in programmable.inputs.iter().enumerate() {
            w.write_str(&tx.digest);
            w.write_usize_as_u32(input_index);
            write_input_fields(&mut w, input);
            write_checkpoint_context(&mut w, &summary);
            input_count += 1;
        }
    }

    // ── Unchanged consensus objects ──
    let mut uco_count = 0u32;
    for tx in &txs {
        let Some(effects) = &tx.effects else { continue };
        for (id, kind) in effects.unchanged_consensus_objects() {
            w.write_str(&tx.digest);
            w.write_display(&id);
            match kind {
                UnchangedConsensusKind::ReadOnlyRoot((version, digest)) => {
                    w.write_str("READ_ONLY_ROOT");
                    w.write_u64_dec(version.value());
                    w.write_display(&digest);
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
            w.write_null(); // object_type
            write_checkpoint_context(&mut w, &summary);
            uco_count += 1;
        }
    }

    // ── Events ──
    let mut event_count = 0u32;
    for tx in &txs {
        let Some(events) = &tx.events else { continue };
        for (event_seq, event) in events.data.iter().enumerate() {
            write_event(&mut w, &tx.digest, event_seq, event, &summary);
            event_count += 1;
        }
    }

    // ── Balance changes — derive_balance_changes_2 per transaction ──
    profile.extract_records_ns = t_extract.elapsed().as_nanos() as u64;

    let t_balance = Instant::now();

    // Build ObjectSet from proto objects
    let mut object_set = sui_types::full_checkpoint_content::ObjectSet::default();
    for obj in &parsed.objects {
        if let Ok(decoded) = bcs::from_bytes::<Object>(obj.bcs) {
            object_set.insert(decoded);
        }
    }

    let mut bal_count = 0u32;
    for tx in &txs {
        let Some(effects) = &tx.effects else { continue };
        for bc in sui_types::balance_change::derive_balance_changes_2(effects, &object_set) {
            w.write_str(&tx.digest); // tx_digest
            write_checkpoint_seq(&mut w, &summary);
            w.write_display(&bc.address);
            w.write_str(&bc.coin_type.to_canonical_string(true));
            w.write_i128_dec(bc.amount);
            write_timestamp(&mut w, &summary);
            bal_count += 1;
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
    let n = write_iso_to_buf(&mut buf, year, month, day, hours as u32, mins as u32, secs_of_min as u32, millis as u32);
    w.write_str(unsafe { std::str::from_utf8_unchecked(&buf[..n]) });
}

fn write_iso_to_buf(buf: &mut [u8; 28], y: i32, m: u32, d: u32, h: u32, min: u32, s: u32, ms: u32) -> usize {
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

fn write_object_changes(
    w: &mut BinaryWriter,
    digest: &str,
    effects: &TransactionEffects,
    summary: &Option<CheckpointSummary>,
    count: &mut u32,
) {
    let old_metadata: HashMap<ObjectID, Owner> = effects
        .old_object_metadata()
        .into_iter()
        .map(|((id, _, _), owner)| (id, owner))
        .collect();

    let mut new_metadata: HashMap<ObjectID, Owner> = HashMap::new();
    for ((id, _, _), owner) in effects.created() { new_metadata.insert(id, owner); }
    for ((id, _, _), owner) in effects.mutated() { new_metadata.insert(id, owner); }
    for ((id, _, _), owner) in effects.unwrapped() { new_metadata.insert(id, owner); }

    let created_ids: HashSet<ObjectID> = effects.created().into_iter().map(|(o, _)| o.0).collect();
    let mutated_ids: HashSet<ObjectID> = effects.mutated().into_iter().map(|(o, _)| o.0).collect();
    let unwrapped_ids: HashSet<ObjectID> = effects.unwrapped().into_iter().map(|(o, _)| o.0).collect();
    let deleted_ids: HashSet<ObjectID> = effects.deleted().into_iter().map(|o| o.0)
        .chain(effects.unwrapped_then_deleted().into_iter().map(|o| o.0)).collect();
    let wrapped_ids: HashSet<ObjectID> = effects.wrapped().into_iter().map(|o| o.0).collect();
    let published_ids: HashSet<ObjectID> = effects.published_packages().into_iter().collect();

    let gas_oid = effects.gas_object().0 .0;
    let gas_oid = if gas_oid == ObjectID::ZERO { None } else { Some(gas_oid) };

    for change in effects.object_changes() {
        w.write_str(digest);
        w.write_display(&change.id);

        // change_type
        let ct = derive_change_type_str(&change, &created_ids, &mutated_ids, &unwrapped_ids, &deleted_ids, &wrapped_ids, &published_ids);
        w.write_str(ct);

        w.write_null(); // object_type

        // input_version, input_digest
        w.write_opt_u64_dec(change.input_version.map(|v| v.value()));
        match change.input_digest {
            Some(d) => w.write_display(&d),
            None => w.write_null(),
        }

        // input_owner, input_owner_kind
        write_owner_fields(w, old_metadata.get(&change.id));
        // output_version, output_digest
        w.write_opt_u64_dec(change.output_version.map(|v| v.value()));
        match change.output_digest {
            Some(d) => w.write_display(&d),
            None => w.write_null(),
        }
        // output_owner, output_owner_kind
        write_owner_fields(w, new_metadata.get(&change.id));

        w.write_bool(gas_oid.as_ref().is_some_and(|id| *id == change.id));
        write_checkpoint_context(w, summary);
        *count += 1;
    }
}

fn write_owner_fields(w: &mut BinaryWriter, owner: Option<&Owner>) {
    match owner {
        Some(Owner::AddressOwner(addr)) => {
            w.write_display(addr);
            w.write_str("ADDRESS");
        }
        Some(Owner::ObjectOwner(addr)) => {
            w.write_display(addr);
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
            w.write_display(owner);
            w.write_str("CONSENSUS_ADDRESS");
        }
        None => {
            w.write_null();
            w.write_null();
        }
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
                    w.write_display(id);
                    w.write_u64_dec(version.value());
                    w.write_display(digest);
                    w.write_null(); // mutability
                    w.write_null(); // initial_shared_version
                }
                ObjectArg::SharedObject { id, initial_shared_version, mutability } => {
                    w.write_str("SHARED");
                    w.write_display(id);
                    w.write_null(); // version
                    w.write_null(); // digest
                    w.write_str(shared_mutability_name(mutability));
                    w.write_u64_dec(initial_shared_version.value());
                }
                ObjectArg::Receiving((id, version, digest)) => {
                    w.write_str("RECEIVING");
                    w.write_display(id);
                    w.write_u64_dec(version.value());
                    w.write_display(digest);
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
            let ct = format_type_tag_canonical(&withdrawal.type_arg.to_type_tag());
            w.write_str(&ct);
            match withdrawal.withdraw_from {
                WithdrawFrom::Sender => w.write_str("SENDER"),
                WithdrawFrom::Sponsor => w.write_str("SPONSOR"),
            }
        }
    }
}

fn write_event(
    w: &mut BinaryWriter,
    digest: &str,
    event_seq: usize,
    event: &Event,
    summary: &Option<CheckpointSummary>,
) {
    let event_type = format_type_tag_canonical(&TypeTag::Struct(Box::new(event.type_.clone())));
    // Binary format order: handler_name, checkpoint_seq, tx_digest, event_seq, sender, timestamp, data
    w.write_str(&event_type); // handler_name
    write_checkpoint_seq(w, summary); // checkpoint_seq only
    w.write_str(digest); // tx_digest
    w.write_usize_as_u32(event_seq);
    w.write_display(&event.sender);
    write_timestamp(w, summary); // timestamp only

    let data_json = format!(
        r#"{{"packageId":"{}","module":"{}","eventType":"{}","contents":"{}"}}"#,
        event.package_id,
        event.transaction_module,
        event_type,
        format_address_hex(&event.contents),
    );
    w.write_str(&data_json);
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

fn format_non_move_command(command: &Command) -> (&'static str, String) {
    use serde_json::{Map, Value};

    fn arg_val(a: &Argument) -> Value {
        match a {
            Argument::GasCoin => Value::String("GasCoin".to_string()),
            Argument::Input(i) => Value::Object(Map::from_iter([("Input".to_string(), Value::Number((*i).into()))])),
            Argument::Result(i) => Value::Object(Map::from_iter([("Result".to_string(), Value::Number((*i).into()))])),
            Argument::NestedResult(a, b) => Value::Object(Map::from_iter([
                ("NestedResult".to_string(), Value::Array(vec![Value::Number((*a).into()), Value::Number((*b).into())])),
            ])),
        }
    }

    fn make_json(entries: Vec<(&str, Value)>) -> String {
        let mut map = Map::new();
        for (k, v) in entries { map.insert(k.to_string(), v); }
        Value::Object(map).to_string()
    }

    match command {
        Command::TransferObjects(objects, address) => (
            "TransferObjects",
            make_json(vec![
                ("objects", Value::Array(objects.iter().map(arg_val).collect())),
                ("address", arg_val(address)),
            ]),
        ),
        Command::SplitCoins(coin, amounts) => (
            "SplitCoins",
            make_json(vec![
                ("coin", arg_val(coin)),
                ("amounts", Value::Array(amounts.iter().map(arg_val).collect())),
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
                ("modules", Value::Array(modules.iter().map(|m| Value::String(format_address_hex(m))).collect())),
                ("dependencies", Value::Array(deps.iter().map(|id| Value::String(id.to_string())).collect())),
            ]),
        ),
        Command::Upgrade(modules, deps, package, ticket) => (
            "Upgrade",
            make_json(vec![
                ("modules", Value::Array(modules.iter().map(|m| Value::String(format_address_hex(m))).collect())),
                ("dependencies", Value::Array(deps.iter().map(|id| Value::String(id.to_string())).collect())),
                ("package", Value::String(package.to_string())),
                ("ticket", arg_val(ticket)),
            ]),
        ),
        Command::MakeMoveVec(element_type, elements) => (
            "MakeMoveVector",
            make_json(vec![
                ("elementType", element_type.as_ref().map(|t| Value::String(t.to_canonical_string(true))).unwrap_or(Value::Null)),
                ("elements", Value::Array(elements.iter().map(arg_val).collect())),
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
        TransactionKind::ConsensusCommitPrologue(d) => Some(("CONSENSUS_COMMIT_PROLOGUE_V1", js(d))),
        TransactionKind::ConsensusCommitPrologueV2(d) => Some(("CONSENSUS_COMMIT_PROLOGUE_V2", js(d))),
        TransactionKind::ConsensusCommitPrologueV3(d) => Some(("CONSENSUS_COMMIT_PROLOGUE_V3", js(d))),
        TransactionKind::ConsensusCommitPrologueV4(d) => Some(("CONSENSUS_COMMIT_PROLOGUE_V4", js(d))),
        TransactionKind::AuthenticatorStateUpdate(d) => Some(("AUTHENTICATOR_STATE_UPDATE", js(d))),
        TransactionKind::EndOfEpochTransaction(d) => Some(("END_OF_EPOCH", js(d))),
        TransactionKind::RandomnessStateUpdate(d) => Some(("RANDOMNESS_STATE_UPDATE", js(d))),
        TransactionKind::ProgrammableSystemTransaction(d) => Some(("PROGRAMMABLE_SYSTEM_TRANSACTION", js(d))),
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
    if published.contains(&change.id) { return "PACKAGE_WRITE"; }
    match (change.input_version.is_some(), change.output_version.is_some(), change.id_operation) {
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
    debug.split(['(', '{']).next().unwrap_or(debug.as_str()).trim().to_string()
}

fn count_move_calls(kind: &TransactionKind) -> usize {
    match kind {
        TransactionKind::ProgrammableTransaction(pt)
        | TransactionKind::ProgrammableSystemTransaction(pt) => {
            pt.commands.iter().filter(|c| matches!(c, Command::MoveCall(_))).count()
        }
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

fn format_type_tag_canonical(tag: &TypeTag) -> String {
    tag.to_canonical_string(true)
}

