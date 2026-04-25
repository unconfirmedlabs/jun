//! jun-decoder — decode a checkpoint proto into jun-types records.
//!
//! Input: already-decompressed checkpoint proto bytes (from jun-archive).
//! Output: `ExtractedCheckpoint` with fields populated per the ExtractMask.
//!
//! v1 scope: CHECKPOINTS / TRANSACTIONS / MOVE_CALLS / BALANCE_CHANGES /
//! OBJECT_CHANGES / DEPENDENCIES. Other record types extend the same pattern
//! and will land in follow-up passes.

use anyhow::{anyhow, Result};
use std::collections::{HashMap, HashSet};

use jun_types::{
    BalanceChangeRecord, CheckpointRecord, DependencyRecord, ExtractMask,
    ExtractedCheckpoint, MoveCallRecord, ObjectChangeRecord, TransactionRecord,
};
use sui_types::balance_change::derive_balance_changes_2;
use sui_types::base_types::{ObjectID, SequenceNumber};
use sui_types::effects::{IDOperation, TransactionEffects, TransactionEffectsAPI};
use sui_types::execution_status::{ExecutionErrorKind, ExecutionStatus};
use sui_types::full_checkpoint_content::ObjectSet;
use sui_types::messages_checkpoint::CheckpointSummary;
use sui_types::object::{Object, Owner};
use sui_types::storage::ObjectKey;
use sui_types::transaction::{Command, SenderSignedData, TransactionData, TransactionDataAPI, TransactionKind};

mod proto;

/// Decode one checkpoint (decompressed proto bytes) into an ExtractedCheckpoint.
pub fn decode_checkpoint(proto_bytes: &[u8], mask: ExtractMask) -> Result<ExtractedCheckpoint> {
    let parsed = proto::parse_checkpoint(proto_bytes).map_err(|e| anyhow!("proto parse: {e}"))?;

    let summary = parsed.summary_bcs.and_then(decode_summary).unwrap_or_default();
    let checkpoint_seq = summary.sequence_number.clone();
    let checkpoint_ts = summary.timestamp_ms.clone();

    let mut out = ExtractedCheckpoint::default();
    out.checkpoint = CheckpointRecord {
        sequence_number: summary.sequence_number.clone(),
        epoch: summary.epoch.clone(),
        timestamp_ms: summary.timestamp_ms.clone(),
        digest: parsed.digest.unwrap_or_default(),
        previous_digest: summary.previous_digest,
        content_digest: summary.content_digest,
        total_network_transactions: summary.total_network_transactions,
        rolling_computation_cost: summary.gas.computation_cost,
        rolling_storage_cost: summary.gas.storage_cost,
        rolling_storage_rebate: summary.gas.storage_rebate,
        rolling_non_refundable_storage_fee: summary.gas.non_refundable_storage_fee,
    };

    let need_objects =
        mask.intersects(ExtractMask::BALANCE_CHANGES | ExtractMask::OBJECT_CHANGES);
    let object_set = if need_objects {
        let mut set = ObjectSet::default();
        for o in &parsed.objects {
            if let Ok(decoded) = bcs::from_bytes::<Object>(o.bcs) {
                set.insert(decoded);
            }
        }
        set
    } else {
        ObjectSet::default()
    };

    for ptx in &parsed.transactions {
        let effects: Option<TransactionEffects> =
            ptx.effects_bcs.and_then(|b| bcs::from_bytes(b).ok());
        let tx_data: Option<TransactionData> = ptx.transaction_bcs.and_then(decode_transaction_data);

        let digest = effects
            .as_ref()
            .map(|fx| fx.transaction_digest().to_string())
            .filter(|v| !v.is_empty())
            .or_else(|| (!ptx.digest.is_empty()).then(|| ptx.digest.to_string()))
            .unwrap_or_default();
        let sender = tx_data
            .as_ref()
            .map(|t| t.sender().to_string())
            .unwrap_or_default();

        if mask.contains(ExtractMask::TRANSACTIONS) {
            if let Some(rec) = build_transaction(
                &digest, &sender, &checkpoint_seq, &checkpoint_ts,
                tx_data.as_ref(), effects.as_ref(),
            ) {
                out.transactions.push(rec);
            }
        }

        if mask.contains(ExtractMask::MOVE_CALLS) {
            if let Some(td) = tx_data.as_ref() {
                extract_move_calls(
                    &digest, &checkpoint_seq, &checkpoint_ts,
                    td.kind(), &mut out.move_calls,
                );
            }
        }

        if let Some(fx) = effects.as_ref() {
            if mask.contains(ExtractMask::OBJECT_CHANGES) {
                extract_object_changes(
                    &digest, &checkpoint_seq, &checkpoint_ts,
                    fx, &object_set, &mut out.object_changes,
                );
            }
            if mask.contains(ExtractMask::DEPENDENCIES) {
                for dep in fx.dependencies() {
                    out.dependencies.push(DependencyRecord {
                        tx_digest: digest.clone(),
                        depends_on_digest: dep.to_string(),
                        checkpoint_seq: checkpoint_seq.clone(),
                        timestamp_ms: checkpoint_ts.clone(),
                    });
                }
            }
            if mask.contains(ExtractMask::BALANCE_CHANGES) {
                for bc in derive_balance_changes_2(fx, &object_set) {
                    out.balance_changes.push(BalanceChangeRecord {
                        tx_digest: digest.clone(),
                        checkpoint_seq: checkpoint_seq.clone(),
                        address: bc.address.to_string(),
                        coin_type: bc.coin_type.to_canonical_string(true),
                        amount: bc.amount.to_string(),
                        timestamp_ms: checkpoint_ts.clone(),
                    });
                }
            }
        }
    }

    Ok(out)
}

// --- helpers --------------------------------------------------------------

#[derive(Default)]
struct SummaryFields {
    sequence_number: String,
    epoch: String,
    timestamp_ms: String,
    previous_digest: Option<String>,
    content_digest: Option<String>,
    total_network_transactions: String,
    gas: SummaryGas,
}

#[derive(Default)]
struct SummaryGas {
    computation_cost: String,
    storage_cost: String,
    storage_rebate: String,
    non_refundable_storage_fee: String,
}

fn decode_summary(bytes: &[u8]) -> Option<SummaryFields> {
    let s: CheckpointSummary = bcs::from_bytes(bytes).ok()?;
    Some(SummaryFields {
        sequence_number: s.sequence_number.to_string(),
        epoch: s.epoch.to_string(),
        timestamp_ms: s.timestamp_ms.to_string(),
        previous_digest: s.previous_digest.map(|d| d.to_string()),
        content_digest: Some(s.content_digest.to_string()),
        total_network_transactions: s.network_total_transactions.to_string(),
        gas: SummaryGas {
            computation_cost: s.epoch_rolling_gas_cost_summary.computation_cost.to_string(),
            storage_cost: s.epoch_rolling_gas_cost_summary.storage_cost.to_string(),
            storage_rebate: s.epoch_rolling_gas_cost_summary.storage_rebate.to_string(),
            non_refundable_storage_fee: s.epoch_rolling_gas_cost_summary.non_refundable_storage_fee.to_string(),
        },
    })
}

fn decode_transaction_data(bytes: &[u8]) -> Option<TransactionData> {
    if let Ok(td) = bcs::from_bytes::<TransactionData>(bytes) {
        return Some(td);
    }
    if let Ok(ssd) = bcs::from_bytes::<SenderSignedData>(bytes) {
        return Some(ssd.into_inner().intent_message.value);
    }
    None
}

fn sequence_number_to_string(v: SequenceNumber) -> String {
    v.value().to_string()
}

fn count_move_calls(kind: &TransactionKind) -> u32 {
    match kind {
        TransactionKind::ProgrammableTransaction(pt)
        | TransactionKind::ProgrammableSystemTransaction(pt) => pt
            .commands
            .iter()
            .filter(|c| matches!(c, Command::MoveCall(_)))
            .count() as u32,
        _ => 0,
    }
}

fn execution_error_kind_name(err: &ExecutionErrorKind) -> String {
    let name = format!("{err:?}");
    name.split(|c: char| c == '(' || c == '{' || c.is_whitespace())
        .next()
        .unwrap_or("Unknown")
        .to_string()
}

fn flatten_owner(owner: Option<&Owner>) -> (Option<String>, Option<String>) {
    match owner {
        None => (None, None),
        Some(Owner::AddressOwner(a)) => (Some(a.to_string()), Some("AddressOwner".into())),
        Some(Owner::ObjectOwner(a)) => (Some(a.to_string()), Some("ObjectOwner".into())),
        Some(Owner::Shared { .. }) => (None, Some("Shared".into())),
        Some(Owner::Immutable) => (None, Some("Immutable".into())),
        Some(Owner::ConsensusAddressOwner { owner, .. }) => {
            (Some(owner.to_string()), Some("ConsensusAddressOwner".into()))
        }
    }
}

fn build_transaction(
    digest: &str,
    sender: &str,
    checkpoint_seq: &str,
    timestamp_ms: &str,
    tx_data: Option<&TransactionData>,
    effects: Option<&TransactionEffects>,
) -> Option<TransactionRecord> {
    let effects = effects?;
    let success = matches!(effects.status(), ExecutionStatus::Success);
    let gas = effects.gas_cost_summary();

    let (error_kind, error_command_index, error_abort_code, error_module, error_function) =
        match effects.status() {
            ExecutionStatus::Success => (None, None, None, None, None),
            ExecutionStatus::Failure(failure) => {
                let kind = execution_error_kind_name(&failure.error);
                let command = failure.command.map(|i| i as u32);
                let (abort_code, module, function) = match &failure.error {
                    ExecutionErrorKind::MoveAbort(loc, code) => (
                        Some(code.to_string()),
                        Some(loc.module.to_canonical_display(true).to_string()),
                        loc.function_name.clone(),
                    ),
                    _ => (None, None, None),
                };
                (Some(kind), command, abort_code, module, function)
            }
        };

    let (gas_owner, gas_price, gas_budget) = tx_data
        .map(|t| {
            (
                t.gas_owner().to_string(),
                t.gas_price().to_string(),
                t.gas_budget().to_string(),
            )
        })
        .unwrap_or_else(|| (sender.to_string(), "0".into(), "0".into()));

    let gas_obj = effects.gas_object();
    let gas_object_id = if gas_obj.0 .0 == ObjectID::ZERO {
        None
    } else {
        Some(gas_obj.0 .0.to_string())
    };

    let move_call_count = tx_data.map(|t| count_move_calls(t.kind())).unwrap_or(0);

    Some(TransactionRecord {
        digest: digest.to_string(),
        sender: sender.to_string(),
        success,
        computation_cost: gas.computation_cost.to_string(),
        storage_cost: gas.storage_cost.to_string(),
        storage_rebate: gas.storage_rebate.to_string(),
        non_refundable_storage_fee: gas.non_refundable_storage_fee.to_string(),
        checkpoint_seq: checkpoint_seq.to_string(),
        timestamp_ms: timestamp_ms.to_string(),
        move_call_count,
        epoch: effects.executed_epoch().to_string(),
        error_kind,
        error_description: None,
        error_command_index,
        error_abort_code,
        error_module,
        error_function,
        error_arg_idx: None,
        error_type_arg_idx: None,
        error_sub_kind: None,
        error_current_size: None,
        error_max_size: None,
        error_coin_type: None,
        error_denied_address: None,
        error_object_id: None,
        events_digest: effects.events_digest().map(|d| d.to_string()),
        lamport_version: Some(sequence_number_to_string(effects.lamport_version())),
        dependency_count: effects.dependencies().len() as u32,
        gas_owner,
        gas_price,
        gas_budget,
        gas_object_id,
        expiration_kind: "NONE".to_string(),
        expiration_epoch: None,
        expiration_min_epoch: None,
        expiration_max_epoch: None,
        expiration_min_ts_seconds: None,
        expiration_max_ts_seconds: None,
        expiration_chain: None,
        expiration_nonce: None,
    })
}

fn extract_move_calls(
    digest: &str,
    checkpoint_seq: &str,
    timestamp_ms: &str,
    kind: &TransactionKind,
    out: &mut Vec<MoveCallRecord>,
) {
    let pt = match kind {
        TransactionKind::ProgrammableTransaction(pt)
        | TransactionKind::ProgrammableSystemTransaction(pt) => pt,
        _ => return,
    };
    let mut call_index = 0u32;
    for cmd in &pt.commands {
        if let Command::MoveCall(c) = cmd {
            out.push(MoveCallRecord {
                tx_digest: digest.to_string(),
                call_index,
                package: c.package.to_string(),
                module: c.module.to_string(),
                function: c.function.to_string(),
                checkpoint_seq: checkpoint_seq.to_string(),
                timestamp_ms: timestamp_ms.to_string(),
            });
            call_index += 1;
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn derive_change_type(
    change_id: ObjectID,
    input_version: Option<SequenceNumber>,
    output_version: Option<SequenceNumber>,
    id_op: IDOperation,
    created: &HashSet<ObjectID>,
    mutated: &HashSet<ObjectID>,
    unwrapped: &HashSet<ObjectID>,
    deleted: &HashSet<ObjectID>,
    wrapped: &HashSet<ObjectID>,
    published: &HashSet<ObjectID>,
) -> String {
    if published.contains(&change_id) { return "PACKAGE_WRITE".into(); }
    if created.contains(&change_id)   { return "CREATED".into(); }
    if mutated.contains(&change_id)   { return "MUTATED".into(); }
    if unwrapped.contains(&change_id) { return "UNWRAPPED".into(); }
    if deleted.contains(&change_id)   { return "DELETED".into(); }
    if wrapped.contains(&change_id)   { return "WRAPPED".into(); }
    match id_op {
        IDOperation::Created => "CREATED".into(),
        IDOperation::Deleted => "DELETED".into(),
        IDOperation::None => {
            if input_version.is_some() && output_version.is_some() { "MUTATED".into() }
            else if output_version.is_some() { "CREATED".into() }
            else { "DELETED".into() }
        }
    }
}

fn extract_object_changes(
    digest: &str,
    checkpoint_seq: &str,
    timestamp_ms: &str,
    effects: &TransactionEffects,
    object_set: &ObjectSet,
    out: &mut Vec<ObjectChangeRecord>,
) {
    let v1_input_owners: HashMap<ObjectID, Owner> = match effects {
        TransactionEffects::V2(_) => effects
            .old_object_metadata()
            .into_iter()
            .map(|((id, _, _), o)| (id, o))
            .collect(),
        TransactionEffects::V1(_) => HashMap::new(),
    };

    let mut new_metadata: HashMap<ObjectID, Owner> = HashMap::new();
    for ((id, _, _), o) in effects.created().iter().chain(effects.mutated().iter()).chain(effects.unwrapped().iter()) {
        new_metadata.insert(*id, o.clone());
    }

    let created: HashSet<ObjectID> = effects.created().iter().map(|(r, _)| r.0).collect();
    let mutated: HashSet<ObjectID> = effects.mutated().iter().map(|(r, _)| r.0).collect();
    let unwrapped: HashSet<ObjectID> = effects.unwrapped().iter().map(|(r, _)| r.0).collect();
    let deleted: HashSet<ObjectID> = effects
        .deleted()
        .iter()
        .map(|r| r.0)
        .chain(effects.unwrapped_then_deleted().iter().map(|r| r.0))
        .collect();
    let wrapped: HashSet<ObjectID> = effects.wrapped().iter().map(|r| r.0).collect();
    let published: HashSet<ObjectID> = match effects {
        TransactionEffects::V2(_) => effects.published_packages().into_iter().collect(),
        TransactionEffects::V1(_) => effects
            .created()
            .iter()
            .filter_map(|((id, v, _), _)| {
                let obj = object_set.get(&ObjectKey(*id, *v))?;
                if obj.is_package() { Some(*id) } else { None }
            })
            .collect(),
    };

    let gas_obj_id = effects.gas_object().0 .0;
    let gas_obj_id = if gas_obj_id == ObjectID::ZERO { None } else { Some(gas_obj_id) };

    for change in effects.object_changes() {
        if change.input_version.is_none() && change.output_version.is_none() {
            continue;
        }
        let input_owner = v1_input_owners.get(&change.id);
        let output_owner = new_metadata.get(&change.id);
        let (iv, ik) = flatten_owner(input_owner);
        let (ov, ok) = flatten_owner(output_owner);

        let object_type = change
            .output_version
            .and_then(|v| object_set.get(&ObjectKey(change.id, v)))
            .or_else(|| change.input_version.and_then(|v| object_set.get(&ObjectKey(change.id, v))))
            .and_then(|obj| obj.type_().map(|t| t.to_canonical_string(true)));

        let change_type = derive_change_type(
            change.id,
            change.input_version,
            change.output_version,
            change.id_operation,
            &created, &mutated, &unwrapped, &deleted, &wrapped, &published,
        );

        out.push(ObjectChangeRecord {
            tx_digest: digest.to_string(),
            object_id: change.id.to_string(),
            change_type,
            object_type,
            input_version: change.input_version.map(sequence_number_to_string),
            input_digest: change.input_digest.map(|d| d.to_string()),
            input_owner: iv,
            input_owner_kind: ik,
            output_version: change.output_version.map(sequence_number_to_string),
            output_digest: change.output_digest.map(|d| d.to_string()),
            output_owner: ov,
            output_owner_kind: ok,
            is_gas_object: gas_obj_id.as_ref().is_some_and(|id| *id == change.id),
            checkpoint_seq: checkpoint_seq.to_string(),
            timestamp_ms: timestamp_ms.to_string(),
        });
    }
}
