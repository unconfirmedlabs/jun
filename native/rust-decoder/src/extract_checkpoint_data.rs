//! Checkpoint extraction — protobuf parsing + BCS decode + record extraction.

use std::collections::{HashMap, HashSet};

use serde::Serialize;
use serde_json::{Map, Value};
use sui_types::balance_change::derive_balance_changes_2;
use sui_types::base_types::{ObjectID, SequenceNumber, SuiAddress};
use sui_types::effects::{
    IDOperation, TransactionEffects, TransactionEffectsAPI, TransactionEvents,
    UnchangedConsensusKind,
};
use sui_types::event::Event;
use sui_types::execution_status::{ExecutionErrorKind, ExecutionStatus};
use sui_types::full_checkpoint_content::ObjectSet;
use sui_types::messages_checkpoint::CheckpointSummary;
use sui_types::object::{Object, Owner};
use sui_types::storage::ObjectKey;
use sui_types::transaction::{
    Argument, CallArg, Command, FundsWithdrawalArg, ObjectArg, Reservation, SenderSignedData,
    SharedObjectMutability, TransactionData, TransactionDataAPI, TransactionKind, WithdrawFrom,
};
use sui_types::TypeTag;

use crate::proto;

// ---------------------------------------------------------------------------
// Output types (already stringified to match SerializedProcessedCheckpoint)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct ExtractedCheckpoint {
    pub checkpoint: CheckpointRecord,
    pub events: Vec<EventRecord>,
    #[serde(rename = "balanceChanges")]
    pub balance_changes: Vec<BalanceChangeRecord>,
    pub transactions: Vec<TransactionRecord>,
    #[serde(rename = "moveCalls")]
    pub move_calls: Vec<MoveCallRecord>,
    #[serde(rename = "objectChanges")]
    pub object_changes: Vec<ObjectChangeRecord>,
    pub dependencies: Vec<DependencyRecord>,
    pub inputs: Vec<InputRecord>,
    pub commands: Vec<CommandRecord>,
    #[serde(rename = "systemTransactions")]
    pub system_transactions: Vec<SystemTransactionRecord>,
    #[serde(rename = "unchangedConsensusObjects")]
    pub unchanged_consensus_objects: Vec<UnchangedConsensusObjectRecord>,
}

#[derive(Serialize)]
pub struct CheckpointRecord {
    #[serde(rename = "sequenceNumber")]
    pub sequence_number: String,
    pub timestamp: String,
    pub source: String,
    pub epoch: String,
    pub digest: String,
    #[serde(rename = "previousDigest")]
    pub previous_digest: Option<String>,
    #[serde(rename = "contentDigest")]
    pub content_digest: Option<String>,
    #[serde(rename = "totalNetworkTransactions")]
    pub total_network_transactions: String,
    #[serde(rename = "epochRollingGasCostSummary")]
    pub epoch_rolling_gas_cost_summary: GasCostSummaryRecord,
}

#[derive(Serialize)]
pub struct GasCostSummaryRecord {
    #[serde(rename = "computationCost")]
    pub computation_cost: String,
    #[serde(rename = "storageCost")]
    pub storage_cost: String,
    #[serde(rename = "storageRebate")]
    pub storage_rebate: String,
    #[serde(rename = "nonRefundableStorageFee")]
    pub non_refundable_storage_fee: String,
}

#[derive(Serialize)]
pub struct EventRecord {
    #[serde(rename = "handlerName")]
    pub handler_name: String,
    #[serde(rename = "checkpointSeq")]
    pub checkpoint_seq: String,
    #[serde(rename = "txDigest")]
    pub tx_digest: String,
    #[serde(rename = "eventSeq")]
    pub event_seq: usize,
    pub sender: String,
    pub timestamp: String,
    pub data: Value,
}

#[derive(Serialize)]
pub struct BalanceChangeRecord {
    #[serde(rename = "txDigest")]
    pub tx_digest: String,
    #[serde(rename = "checkpointSeq")]
    pub checkpoint_seq: String,
    pub address: String,
    #[serde(rename = "coinType")]
    pub coin_type: String,
    pub amount: String,
    pub timestamp: String,
}

#[derive(Serialize)]
pub struct TransactionRecord {
    pub digest: String,
    pub sender: String,
    pub success: bool,
    #[serde(rename = "computationCost")]
    pub computation_cost: String,
    #[serde(rename = "storageCost")]
    pub storage_cost: String,
    #[serde(rename = "storageRebate")]
    pub storage_rebate: String,
    #[serde(rename = "nonRefundableStorageFee")]
    pub non_refundable_storage_fee: String,
    #[serde(rename = "checkpointSeq")]
    pub checkpoint_seq: String,
    pub timestamp: String,
    #[serde(rename = "moveCallCount")]
    pub move_call_count: usize,
    pub epoch: String,
    #[serde(rename = "errorKind")]
    pub error_kind: Option<String>,
    #[serde(rename = "errorDescription")]
    pub error_description: Option<String>,
    #[serde(rename = "errorCommandIndex")]
    pub error_command_index: Option<usize>,
    #[serde(rename = "errorAbortCode")]
    pub error_abort_code: Option<String>,
    #[serde(rename = "errorModule")]
    pub error_module: Option<String>,
    #[serde(rename = "errorFunction")]
    pub error_function: Option<String>,
    #[serde(rename = "eventsDigest")]
    pub events_digest: Option<String>,
    #[serde(rename = "lamportVersion")]
    pub lamport_version: Option<String>,
    #[serde(rename = "dependencyCount")]
    pub dependency_count: usize,
}

#[derive(Serialize)]
pub struct MoveCallRecord {
    #[serde(rename = "txDigest")]
    pub tx_digest: String,
    #[serde(rename = "callIndex")]
    pub call_index: usize,
    pub package: String,
    pub module: String,
    pub function: String,
    #[serde(rename = "checkpointSeq")]
    pub checkpoint_seq: String,
    pub timestamp: String,
}

#[derive(Serialize)]
pub struct ObjectChangeRecord {
    #[serde(rename = "txDigest")]
    pub tx_digest: String,
    #[serde(rename = "objectId")]
    pub object_id: String,
    #[serde(rename = "changeType")]
    pub change_type: String,
    #[serde(rename = "objectType")]
    pub object_type: Option<String>,
    #[serde(rename = "inputVersion")]
    pub input_version: Option<String>,
    #[serde(rename = "inputDigest")]
    pub input_digest: Option<String>,
    #[serde(rename = "inputOwner")]
    pub input_owner: Option<String>,
    #[serde(rename = "inputOwnerKind")]
    pub input_owner_kind: Option<String>,
    #[serde(rename = "outputVersion")]
    pub output_version: Option<String>,
    #[serde(rename = "outputDigest")]
    pub output_digest: Option<String>,
    #[serde(rename = "outputOwner")]
    pub output_owner: Option<String>,
    #[serde(rename = "outputOwnerKind")]
    pub output_owner_kind: Option<String>,
    #[serde(rename = "isGasObject")]
    pub is_gas_object: bool,
    #[serde(rename = "checkpointSeq")]
    pub checkpoint_seq: String,
    pub timestamp: String,
}

#[derive(Serialize)]
pub struct DependencyRecord {
    #[serde(rename = "txDigest")]
    pub tx_digest: String,
    #[serde(rename = "dependsOnDigest")]
    pub depends_on_digest: String,
    #[serde(rename = "checkpointSeq")]
    pub checkpoint_seq: String,
    pub timestamp: String,
}

#[derive(Serialize)]
pub struct InputRecord {
    #[serde(rename = "txDigest")]
    pub tx_digest: String,
    #[serde(rename = "inputIndex")]
    pub input_index: usize,
    pub kind: String,
    #[serde(rename = "objectId")]
    pub object_id: Option<String>,
    pub version: Option<String>,
    pub digest: Option<String>,
    pub mutability: Option<String>,
    #[serde(rename = "initialSharedVersion")]
    pub initial_shared_version: Option<String>,
    #[serde(rename = "pureBytes")]
    pub pure_bytes: Option<String>,
    pub amount: Option<String>,
    #[serde(rename = "coinType")]
    pub coin_type: Option<String>,
    pub source: Option<String>,
    #[serde(rename = "checkpointSeq")]
    pub checkpoint_seq: String,
    pub timestamp: String,
}

#[derive(Serialize)]
pub struct CommandRecord {
    #[serde(rename = "txDigest")]
    pub tx_digest: String,
    #[serde(rename = "commandIndex")]
    pub command_index: usize,
    pub kind: String,
    pub package: Option<String>,
    pub module: Option<String>,
    pub function: Option<String>,
    #[serde(rename = "typeArguments")]
    pub type_arguments: Option<String>,
    pub args: Option<String>,
    #[serde(rename = "checkpointSeq")]
    pub checkpoint_seq: String,
    pub timestamp: String,
}

#[derive(Serialize)]
pub struct SystemTransactionRecord {
    #[serde(rename = "txDigest")]
    pub tx_digest: String,
    pub kind: String,
    pub data: String,
    #[serde(rename = "checkpointSeq")]
    pub checkpoint_seq: String,
    pub timestamp: String,
}

#[derive(Serialize)]
pub struct UnchangedConsensusObjectRecord {
    #[serde(rename = "txDigest")]
    pub tx_digest: String,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub kind: String,
    pub version: Option<String>,
    pub digest: Option<String>,
    #[serde(rename = "objectType")]
    pub object_type: Option<String>,
    #[serde(rename = "checkpointSeq")]
    pub checkpoint_seq: String,
    pub timestamp: String,
}

struct DecodedSummary {
    sequence_number: String,
    timestamp: String,
    epoch: String,
    previous_digest: Option<String>,
    content_digest: Option<String>,
    total_network_transactions: String,
    gas_summary: GasCostSummaryRecord,
}

// ---------------------------------------------------------------------------
// Extraction
// ---------------------------------------------------------------------------

/// Profiling counters for a single checkpoint extraction.
#[derive(Default)]
pub struct ExtractProfile {
    pub proto_parse_ns: u64,
    pub bcs_effects_ns: u64,
    pub bcs_tx_data_ns: u64,
    pub bcs_events_ns: u64,
    pub extract_records_ns: u64,
    pub balance_changes_ns: u64,
    pub tx_count: usize,
}

/// Extract checkpoint into the struct (shared by JSON and binary output paths).
pub fn extract_checkpoint_data(
    decompressed: &[u8],
) -> Result<(ExtractedCheckpoint, ExtractProfile), Box<dyn std::error::Error>> {
    use std::time::Instant;
    let mut profile = ExtractProfile::default();

    let t0 = Instant::now();
    let parsed = proto::parse_checkpoint(decompressed).map_err(|e| format!("proto parse: {e}"))?;
    profile.proto_parse_ns = t0.elapsed().as_nanos() as u64;

    let summary = parsed
        .summary_bcs
        .and_then(decode_summary)
        .unwrap_or_else(default_summary);

    let checkpoint_seq = summary.sequence_number.clone();
    let checkpoint_timestamp = summary.timestamp.clone();
    let mut events = Vec::new();
    let mut transactions = Vec::new();
    let mut move_calls = Vec::new();
    let mut object_changes = Vec::new();
    let mut dependencies = Vec::new();
    let mut inputs = Vec::new();
    let mut commands = Vec::new();
    let mut system_transactions = Vec::new();
    let mut unchanged_consensus_objects = Vec::new();
    let mut balance_changes = Vec::new();

    // Build ObjectSet from proto objects (shared by balance changes + object type lookups)
    let mut object_set = ObjectSet::default();
    for obj in &parsed.objects {
        if let Ok(decoded) = bcs::from_bytes::<Object>(obj.bcs) {
            object_set.insert(decoded);
        }
    }

    profile.tx_count = parsed.transactions.len();

    for ptx in &parsed.transactions {
        let t1 = Instant::now();
        let effects = ptx
            .effects_bcs
            .and_then(|bytes| bcs::from_bytes::<TransactionEffects>(bytes).ok());
        profile.bcs_effects_ns += t1.elapsed().as_nanos() as u64;

        let t2 = Instant::now();
        let tx_data = ptx.transaction_bcs.and_then(decode_transaction_data);
        profile.bcs_tx_data_ns += t2.elapsed().as_nanos() as u64;

        let t3 = Instant::now();
        let tx_events = ptx
            .events_bcs
            .and_then(|bytes| bcs::from_bytes::<TransactionEvents>(bytes).ok());
        profile.bcs_events_ns += t3.elapsed().as_nanos() as u64;

        let t4 = Instant::now();

        let digest = effects
            .as_ref()
            .map(|fx| fx.transaction_digest().to_string())
            .filter(|value| !value.is_empty())
            .or_else(|| {
                if ptx.digest.is_empty() {
                    None
                } else {
                    Some(ptx.digest.to_string())
                }
            })
            .unwrap_or_default();

        let sender = tx_data
            .as_ref()
            .map(|tx| tx.sender().to_string())
            .unwrap_or_default();

        let tx_context = TxContext {
            digest: &digest,
            checkpoint_seq: &checkpoint_seq,
            checkpoint_timestamp: &checkpoint_timestamp,
        };

        if let Some(record) =
            build_transaction_record(&tx_context, tx_data.as_ref(), effects.as_ref(), &sender)
        {
            transactions.push(record);
        }

        if let Some(tx) = tx_data.as_ref() {
            let command_bundle = extract_commands_and_move_calls(&tx_context, tx.kind());
            move_calls.extend(command_bundle.move_calls);
            commands.extend(command_bundle.commands);
            inputs.extend(extract_inputs(&tx_context, tx));
            if let Some(system_record) = extract_system_transaction(&tx_context, tx.kind()) {
                system_transactions.push(system_record);
            }
        }

        if let Some(fx) = effects.as_ref() {
            object_changes.extend(extract_object_changes(&tx_context, fx, &object_set));
            dependencies.extend(extract_dependencies(&tx_context, fx));
            unchanged_consensus_objects
                .extend(extract_unchanged_consensus_objects(&tx_context, fx));

            // Derive balance changes using Mysten's official implementation
            for bc in derive_balance_changes_2(fx, &object_set) {
                balance_changes.push(BalanceChangeRecord {
                    tx_digest: tx_context.digest.to_string(),
                    checkpoint_seq: tx_context.checkpoint_seq.to_string(),
                    address: bc.address.to_string(),
                    coin_type: bc.coin_type.to_canonical_string(true),
                    amount: bc.amount.to_string(),
                    timestamp: tx_context.checkpoint_timestamp.to_string(),
                });
            }
        }

        if let Some(tx_events) = tx_events.as_ref() {
            events.extend(extract_events(&tx_context, tx_events));
        }

        profile.extract_records_ns += t4.elapsed().as_nanos() as u64;
    }

    profile.balance_changes_ns = 0; // Balance changes now computed inline per-transaction

    let result = ExtractedCheckpoint {
        checkpoint: CheckpointRecord {
            sequence_number: summary.sequence_number,
            timestamp: summary.timestamp,
            source: "backfill".to_string(),
            epoch: summary.epoch,
            digest: parsed.digest.unwrap_or_default(),
            previous_digest: summary.previous_digest,
            content_digest: summary.content_digest,
            total_network_transactions: summary.total_network_transactions,
            epoch_rolling_gas_cost_summary: summary.gas_summary,
        },
        events,
        balance_changes,
        transactions,
        move_calls,
        object_changes,
        dependencies,
        inputs,
        commands,
        system_transactions,
        unchanged_consensus_objects,
    };

    Ok((result, profile))
}

/// Extract checkpoint and serialize to JSON string.
pub fn extract_checkpoint(decompressed: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    let (result, _profile) = extract_checkpoint_data(decompressed)?;
    Ok(serde_json::to_string(&result)?)
}

/// Extract checkpoint and serialize to flat binary format.
/// Returns the number of bytes written and profiling data.
pub fn extract_checkpoint_binary(
    decompressed: &[u8],
    output: &mut [u8],
) -> Result<(usize, ExtractProfile), Box<dyn std::error::Error>> {
    let (result, profile) = extract_checkpoint_data(decompressed)?;
    let written = crate::binary::write_binary(&result, output);
    Ok((written, profile))
}

struct TxContext<'a> {
    digest: &'a str,
    checkpoint_seq: &'a str,
    checkpoint_timestamp: &'a str,
}

struct CommandBundle {
    commands: Vec<CommandRecord>,
    move_calls: Vec<MoveCallRecord>,
}

fn build_transaction_record(
    ctx: &TxContext<'_>,
    tx_data: Option<&TransactionData>,
    effects: Option<&TransactionEffects>,
    sender: &str,
) -> Option<TransactionRecord> {
    let effects = effects?;
    let success = matches!(effects.status(), ExecutionStatus::Success);
    let gas = effects.gas_cost_summary();

    let (error_kind, error_command_index, error_abort_code, error_module, error_function) =
        match effects.status() {
            ExecutionStatus::Success => (None, None, None, None, None),
            ExecutionStatus::Failure(failure) => {
                let kind = execution_error_kind_name(&failure.error);
                let command = failure.command.map(|index| index as usize);
                let abort = match &failure.error {
                    ExecutionErrorKind::MoveAbort(location, code) => (
                        Some(code.to_string()),
                        Some(location.module.to_canonical_display(true).to_string()),
                        location.function_name.clone(),
                    ),
                    _ => (None, None, None),
                };
                (Some(kind), command, abort.0, abort.1, abort.2)
            }
        };

    let move_call_count = tx_data
        .map(|tx| count_move_calls(tx.kind()))
        .unwrap_or_default();

    Some(TransactionRecord {
        digest: ctx.digest.to_string(),
        sender: sender.to_string(),
        success,
        computation_cost: gas.computation_cost.to_string(),
        storage_cost: gas.storage_cost.to_string(),
        storage_rebate: gas.storage_rebate.to_string(),
        non_refundable_storage_fee: gas.non_refundable_storage_fee.to_string(),
        checkpoint_seq: ctx.checkpoint_seq.to_string(),
        timestamp: ctx.checkpoint_timestamp.to_string(),
        move_call_count,
        epoch: effects.executed_epoch().to_string(),
        error_kind,
        error_description: None,
        error_command_index,
        error_abort_code,
        error_module,
        error_function,
        events_digest: effects.events_digest().map(|digest| digest.to_string()),
        lamport_version: Some(sequence_number_to_string(effects.lamport_version())),
        dependency_count: effects.dependencies().len(),
    })
}

fn extract_dependencies(
    ctx: &TxContext<'_>,
    effects: &TransactionEffects,
) -> Vec<DependencyRecord> {
    effects
        .dependencies()
        .iter()
        .map(|dep| DependencyRecord {
            tx_digest: ctx.digest.to_string(),
            depends_on_digest: dep.to_string(),
            checkpoint_seq: ctx.checkpoint_seq.to_string(),
            timestamp: ctx.checkpoint_timestamp.to_string(),
        })
        .collect()
}

fn extract_object_changes(
    ctx: &TxContext<'_>,
    effects: &TransactionEffects,
    object_set: &ObjectSet,
) -> Vec<ObjectChangeRecord> {
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

    let created_ids: HashSet<ObjectID> = effects
        .created()
        .into_iter()
        .map(|(oref, _)| oref.0)
        .collect();
    let mutated_ids: HashSet<ObjectID> = effects
        .mutated()
        .into_iter()
        .map(|(oref, _)| oref.0)
        .collect();
    let unwrapped_ids: HashSet<ObjectID> = effects
        .unwrapped()
        .into_iter()
        .map(|(oref, _)| oref.0)
        .collect();
    let deleted_ids: HashSet<ObjectID> = effects
        .deleted()
        .into_iter()
        .map(|oref| oref.0)
        .chain(
            effects
                .unwrapped_then_deleted()
                .into_iter()
                .map(|oref| oref.0),
        )
        .collect();
    let wrapped_ids: HashSet<ObjectID> = effects.wrapped().into_iter().map(|oref| oref.0).collect();
    let published_ids: HashSet<ObjectID> = effects.published_packages().into_iter().collect();

    let gas_object_id = effects.gas_object().0 .0;
    let gas_object_id = if gas_object_id == ObjectID::ZERO {
        None
    } else {
        Some(gas_object_id)
    };

    effects
        .object_changes()
        .into_iter()
        // Skip entries with no input or output version (e.g. accumulator state changes
        // that slip through the filter — see docs/checkpoint-data-model.md)
        .filter(|change| change.input_version.is_some() || change.output_version.is_some())
        .map(|change| {
            let input_owner = old_metadata.get(&change.id);
            let output_owner = new_metadata.get(&change.id);
            let (input_owner_value, input_owner_kind) = flatten_owner(input_owner);
            let (output_owner_value, output_owner_kind) = flatten_owner(output_owner);

            let object_type = change
                .output_version
                .and_then(|v| object_set.get(&ObjectKey(change.id, v)))
                .or_else(|| {
                    change
                        .input_version
                        .and_then(|v| object_set.get(&ObjectKey(change.id, v)))
                })
                .and_then(|obj| obj.type_().map(|t| t.to_canonical_string(true)));

            ObjectChangeRecord {
                tx_digest: ctx.digest.to_string(),
                object_id: change.id.to_string(),
                change_type: derive_change_type(
                    &change,
                    &created_ids,
                    &mutated_ids,
                    &unwrapped_ids,
                    &deleted_ids,
                    &wrapped_ids,
                    &published_ids,
                ),
                object_type,
                input_version: change.input_version.map(sequence_number_to_string),
                input_digest: change.input_digest.map(|digest| digest.to_string()),
                input_owner: input_owner_value,
                input_owner_kind,
                output_version: change.output_version.map(sequence_number_to_string),
                output_digest: change.output_digest.map(|digest| digest.to_string()),
                output_owner: output_owner_value,
                output_owner_kind,
                is_gas_object: gas_object_id.as_ref().is_some_and(|id| *id == change.id),
                checkpoint_seq: ctx.checkpoint_seq.to_string(),
                timestamp: ctx.checkpoint_timestamp.to_string(),
            }
        })
        .collect()
}

fn extract_inputs(ctx: &TxContext<'_>, tx_data: &TransactionData) -> Vec<InputRecord> {
    let programmable = match tx_data.kind() {
        TransactionKind::ProgrammableTransaction(pt)
        | TransactionKind::ProgrammableSystemTransaction(pt) => pt,
        _ => return Vec::new(),
    };

    programmable
        .inputs
        .iter()
        .enumerate()
        .map(|(input_index, input)| match input {
            CallArg::Pure(bytes) => InputRecord {
                tx_digest: ctx.digest.to_string(),
                input_index,
                kind: "PURE".to_string(),
                object_id: None,
                version: None,
                digest: None,
                mutability: None,
                initial_shared_version: None,
                pure_bytes: Some(hex_string(bytes)),
                amount: None,
                coin_type: None,
                source: None,
                checkpoint_seq: ctx.checkpoint_seq.to_string(),
                timestamp: ctx.checkpoint_timestamp.to_string(),
            },
            CallArg::Object(object) => {
                let (kind, object_id, version, digest, mutability, initial_shared_version) =
                    match object {
                        ObjectArg::ImmOrOwnedObject((id, version, digest)) => (
                            "IMMUTABLE_OR_OWNED".to_string(),
                            Some(id.to_string()),
                            Some(sequence_number_to_string(*version)),
                            Some(digest.to_string()),
                            None,
                            None,
                        ),
                        ObjectArg::SharedObject {
                            id,
                            initial_shared_version,
                            mutability,
                        } => (
                            "SHARED".to_string(),
                            Some(id.to_string()),
                            None,
                            None,
                            Some(shared_mutability_name(mutability).to_string()),
                            Some(sequence_number_to_string(*initial_shared_version)),
                        ),
                        ObjectArg::Receiving((id, version, digest)) => (
                            "RECEIVING".to_string(),
                            Some(id.to_string()),
                            Some(sequence_number_to_string(*version)),
                            Some(digest.to_string()),
                            None,
                            None,
                        ),
                    };

                InputRecord {
                    tx_digest: ctx.digest.to_string(),
                    input_index,
                    kind,
                    object_id,
                    version,
                    digest,
                    mutability,
                    initial_shared_version,
                    pure_bytes: None,
                    amount: None,
                    coin_type: None,
                    source: None,
                    checkpoint_seq: ctx.checkpoint_seq.to_string(),
                    timestamp: ctx.checkpoint_timestamp.to_string(),
                }
            }
            CallArg::FundsWithdrawal(withdrawal) => InputRecord {
                tx_digest: ctx.digest.to_string(),
                input_index,
                kind: "FUNDS_WITHDRAWAL".to_string(),
                object_id: None,
                version: None,
                digest: None,
                mutability: None,
                initial_shared_version: None,
                pure_bytes: None,
                amount: funds_withdrawal_amount(withdrawal),
                coin_type: Some(format_type_tag_canonical(
                    &withdrawal.type_arg.to_type_tag(),
                )),
                source: Some(match withdrawal.withdraw_from {
                    WithdrawFrom::Sender => "SENDER".to_string(),
                    WithdrawFrom::Sponsor => "SPONSOR".to_string(),
                }),
                checkpoint_seq: ctx.checkpoint_seq.to_string(),
                timestamp: ctx.checkpoint_timestamp.to_string(),
            },
        })
        .collect()
}

fn extract_commands_and_move_calls(ctx: &TxContext<'_>, kind: &TransactionKind) -> CommandBundle {
    let programmable = match kind {
        TransactionKind::ProgrammableTransaction(pt)
        | TransactionKind::ProgrammableSystemTransaction(pt) => pt,
        _ => {
            return CommandBundle {
                commands: Vec::new(),
                move_calls: Vec::new(),
            }
        }
    };

    let mut commands = Vec::new();
    let mut move_calls = Vec::new();
    let mut call_index = 0usize;

    for (command_index, command) in programmable.commands.iter().enumerate() {
        match command {
            Command::MoveCall(move_call) => {
                let type_arguments = move_call
                    .type_arguments
                    .iter()
                    .map(|tag| tag.to_canonical_string(true))
                    .collect::<Vec<_>>();

                move_calls.push(MoveCallRecord {
                    tx_digest: ctx.digest.to_string(),
                    call_index,
                    package: move_call.package.to_string(),
                    module: move_call.module.to_string(),
                    function: move_call.function.to_string(),
                    checkpoint_seq: ctx.checkpoint_seq.to_string(),
                    timestamp: ctx.checkpoint_timestamp.to_string(),
                });
                call_index += 1;

                commands.push(CommandRecord {
                    tx_digest: ctx.digest.to_string(),
                    command_index,
                    kind: "MoveCall".to_string(),
                    package: Some(move_call.package.to_string()),
                    module: Some(move_call.module.to_string()),
                    function: Some(move_call.function.to_string()),
                    type_arguments: Some(
                        serde_json::to_string(&type_arguments).unwrap_or_else(|_| "[]".to_string()),
                    ),
                    args: None,
                    checkpoint_seq: ctx.checkpoint_seq.to_string(),
                    timestamp: ctx.checkpoint_timestamp.to_string(),
                });
            }
            Command::TransferObjects(objects, address) => {
                commands.push(command_record_with_args(
                    ctx,
                    command_index,
                    "TransferObjects",
                    command_args_value(vec![
                        (
                            "objects",
                            Value::Array(objects.iter().map(argument_to_value).collect()),
                        ),
                        ("address", argument_to_value(address)),
                    ]),
                ));
            }
            Command::SplitCoins(coin, amounts) => {
                commands.push(command_record_with_args(
                    ctx,
                    command_index,
                    "SplitCoins",
                    command_args_value(vec![
                        ("coin", argument_to_value(coin)),
                        (
                            "amounts",
                            Value::Array(amounts.iter().map(argument_to_value).collect()),
                        ),
                    ]),
                ));
            }
            Command::MergeCoins(coin, coins) => {
                commands.push(command_record_with_args(
                    ctx,
                    command_index,
                    "MergeCoins",
                    command_args_value(vec![
                        ("coin", argument_to_value(coin)),
                        (
                            "coins",
                            Value::Array(coins.iter().map(argument_to_value).collect()),
                        ),
                    ]),
                ));
            }
            Command::Publish(modules, dependencies) => {
                commands.push(command_record_with_args(
                    ctx,
                    command_index,
                    "Publish",
                    command_args_value(vec![
                        (
                            "modules",
                            Value::Array(
                                modules
                                    .iter()
                                    .map(|module| Value::String(hex_string(module)))
                                    .collect(),
                            ),
                        ),
                        (
                            "dependencies",
                            Value::Array(
                                dependencies
                                    .iter()
                                    .map(|id| Value::String(id.to_string()))
                                    .collect(),
                            ),
                        ),
                    ]),
                ));
            }
            Command::Upgrade(modules, dependencies, package, ticket) => {
                commands.push(command_record_with_args(
                    ctx,
                    command_index,
                    "Upgrade",
                    command_args_value(vec![
                        (
                            "modules",
                            Value::Array(
                                modules
                                    .iter()
                                    .map(|module| Value::String(hex_string(module)))
                                    .collect(),
                            ),
                        ),
                        (
                            "dependencies",
                            Value::Array(
                                dependencies
                                    .iter()
                                    .map(|id| Value::String(id.to_string()))
                                    .collect(),
                            ),
                        ),
                        ("package", Value::String(package.to_string())),
                        ("ticket", argument_to_value(ticket)),
                    ]),
                ));
            }
            Command::MakeMoveVec(element_type, elements) => {
                commands.push(command_record_with_args(
                    ctx,
                    command_index,
                    "MakeMoveVector",
                    command_args_value(vec![
                        (
                            "elementType",
                            element_type
                                .as_ref()
                                .map(|tag| Value::String(tag.to_canonical_string(true)))
                                .unwrap_or(Value::Null),
                        ),
                        (
                            "elements",
                            Value::Array(elements.iter().map(argument_to_value).collect()),
                        ),
                    ]),
                ));
            }
        }
    }

    CommandBundle {
        commands,
        move_calls,
    }
}

fn extract_system_transaction(
    ctx: &TxContext<'_>,
    kind: &TransactionKind,
) -> Option<SystemTransactionRecord> {
    let (kind_name, data) = match kind {
        TransactionKind::Genesis(data) => ("GENESIS", to_json_string(data)),
        TransactionKind::ChangeEpoch(data) => ("CHANGE_EPOCH", to_json_string(data)),
        TransactionKind::ConsensusCommitPrologue(data) => {
            ("CONSENSUS_COMMIT_PROLOGUE_V1", to_json_string(data))
        }
        TransactionKind::ConsensusCommitPrologueV2(data) => {
            ("CONSENSUS_COMMIT_PROLOGUE_V2", to_json_string(data))
        }
        TransactionKind::ConsensusCommitPrologueV3(data) => {
            ("CONSENSUS_COMMIT_PROLOGUE_V3", to_json_string(data))
        }
        TransactionKind::ConsensusCommitPrologueV4(data) => {
            ("CONSENSUS_COMMIT_PROLOGUE_V4", to_json_string(data))
        }
        TransactionKind::AuthenticatorStateUpdate(data) => {
            ("AUTHENTICATOR_STATE_UPDATE", to_json_string(data))
        }
        TransactionKind::EndOfEpochTransaction(data) => ("END_OF_EPOCH", to_json_string(data)),
        TransactionKind::RandomnessStateUpdate(data) => {
            ("RANDOMNESS_STATE_UPDATE", to_json_string(data))
        }
        TransactionKind::ProgrammableSystemTransaction(data) => {
            ("PROGRAMMABLE_SYSTEM_TRANSACTION", to_json_string(data))
        }
        TransactionKind::ProgrammableTransaction(_) => return None,
    };

    Some(SystemTransactionRecord {
        tx_digest: ctx.digest.to_string(),
        kind: kind_name.to_string(),
        data,
        checkpoint_seq: ctx.checkpoint_seq.to_string(),
        timestamp: ctx.checkpoint_timestamp.to_string(),
    })
}

fn extract_unchanged_consensus_objects(
    ctx: &TxContext<'_>,
    effects: &TransactionEffects,
) -> Vec<UnchangedConsensusObjectRecord> {
    effects
        .unchanged_consensus_objects()
        .into_iter()
        .map(|(id, kind)| {
            let (kind_name, version, digest) = match kind {
                UnchangedConsensusKind::ReadOnlyRoot((version, digest)) => (
                    "READ_ONLY_ROOT".to_string(),
                    Some(sequence_number_to_string(version)),
                    Some(digest.to_string()),
                ),
                UnchangedConsensusKind::MutateConsensusStreamEnded(version) => (
                    "MUTATE_CONSENSUS_STREAM_ENDED".to_string(),
                    Some(sequence_number_to_string(version)),
                    None,
                ),
                UnchangedConsensusKind::ReadConsensusStreamEnded(version) => (
                    "READ_CONSENSUS_STREAM_ENDED".to_string(),
                    Some(sequence_number_to_string(version)),
                    None,
                ),
                UnchangedConsensusKind::Cancelled(version) => (
                    "CANCELED".to_string(),
                    Some(sequence_number_to_string(version)),
                    None,
                ),
                UnchangedConsensusKind::PerEpochConfig => {
                    ("PER_EPOCH_CONFIG".to_string(), None, None)
                }
            };

            UnchangedConsensusObjectRecord {
                tx_digest: ctx.digest.to_string(),
                object_id: id.to_string(),
                kind: kind_name,
                version,
                digest,
                object_type: None,
                checkpoint_seq: ctx.checkpoint_seq.to_string(),
                timestamp: ctx.checkpoint_timestamp.to_string(),
            }
        })
        .collect()
}

fn extract_events(ctx: &TxContext<'_>, tx_events: &TransactionEvents) -> Vec<EventRecord> {
    tx_events
        .data
        .iter()
        .enumerate()
        .map(|(event_seq, event)| event_record(ctx, event_seq, event))
        .collect()
}

fn event_record(ctx: &TxContext<'_>, event_seq: usize, event: &Event) -> EventRecord {
    let event_type = format_type_tag_canonical(&TypeTag::Struct(Box::new(event.type_.clone())));
    let mut data = Map::new();
    data.insert(
        "packageId".to_string(),
        Value::String(event.package_id.to_string()),
    );
    data.insert(
        "module".to_string(),
        Value::String(event.transaction_module.to_string()),
    );
    data.insert("eventType".to_string(), Value::String(event_type.clone()));
    data.insert(
        "contents".to_string(),
        Value::String(hex_string(&event.contents)),
    );

    EventRecord {
        handler_name: event_type,
        checkpoint_seq: ctx.checkpoint_seq.to_string(),
        tx_digest: ctx.digest.to_string(),
        event_seq,
        sender: event.sender.to_string(),
        timestamp: ctx.checkpoint_timestamp.to_string(),
        data: Value::Object(data),
    }
}

// ---------------------------------------------------------------------------
// Decoding helpers
// ---------------------------------------------------------------------------

fn decode_summary(summary_bcs: &[u8]) -> Option<DecodedSummary> {
    let summary = bcs::from_bytes::<CheckpointSummary>(summary_bcs).ok()?;
    Some(DecodedSummary {
        sequence_number: summary.sequence_number.to_string(),
        timestamp: unix_millis_to_iso(summary.timestamp_ms),
        epoch: summary.epoch.to_string(),
        previous_digest: summary
            .previous_digest
            .as_ref()
            .map(|digest| hex_string(digest.inner())),
        content_digest: Some(hex_string(summary.content_digest.inner())),
        total_network_transactions: summary.network_total_transactions.to_string(),
        gas_summary: GasCostSummaryRecord {
            computation_cost: summary
                .epoch_rolling_gas_cost_summary
                .computation_cost
                .to_string(),
            storage_cost: summary
                .epoch_rolling_gas_cost_summary
                .storage_cost
                .to_string(),
            storage_rebate: summary
                .epoch_rolling_gas_cost_summary
                .storage_rebate
                .to_string(),
            non_refundable_storage_fee: summary
                .epoch_rolling_gas_cost_summary
                .non_refundable_storage_fee
                .to_string(),
        },
    })
}

fn default_summary() -> DecodedSummary {
    DecodedSummary {
        sequence_number: "0".to_string(),
        timestamp: unix_millis_to_iso(0),
        epoch: "0".to_string(),
        previous_digest: None,
        content_digest: None,
        total_network_transactions: "0".to_string(),
        gas_summary: GasCostSummaryRecord {
            computation_cost: "0".to_string(),
            storage_cost: "0".to_string(),
            storage_rebate: "0".to_string(),
            non_refundable_storage_fee: "0".to_string(),
        },
    }
}

fn decode_transaction_data(tx_bcs: &[u8]) -> Option<TransactionData> {
    let raw = bcs::from_bytes::<TransactionData>(tx_bcs).ok();
    let signed = bcs::from_bytes::<SenderSignedData>(tx_bcs)
        .ok()
        .map(|signed| signed.transaction_data().clone());

    match (raw, signed) {
        (Some(raw), Some(signed)) => {
            let raw_score = transaction_quality(&raw);
            let signed_score = transaction_quality(&signed);
            if raw_score >= signed_score {
                Some(raw)
            } else {
                Some(signed)
            }
        }
        (Some(raw), None) => Some(raw),
        (None, Some(signed)) => Some(signed),
        (None, None) => None,
    }
}

fn transaction_quality(tx: &TransactionData) -> usize {
    let base = match tx.kind() {
        TransactionKind::ProgrammableTransaction(pt)
        | TransactionKind::ProgrammableSystemTransaction(pt) => {
            if pt.commands.is_empty() && pt.inputs.is_empty() {
                0
            } else {
                (pt.commands.len() * 10) + pt.inputs.len()
            }
        }
        _ => 1,
    };

    base + usize::from(tx.sender() != SuiAddress::ZERO) + tx.gas().len()
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

fn derive_change_type(
    change: &sui_types::effects::ObjectChange,
    created_ids: &HashSet<ObjectID>,
    mutated_ids: &HashSet<ObjectID>,
    unwrapped_ids: &HashSet<ObjectID>,
    deleted_ids: &HashSet<ObjectID>,
    wrapped_ids: &HashSet<ObjectID>,
    published_ids: &HashSet<ObjectID>,
) -> String {
    if published_ids.contains(&change.id) {
        return "PACKAGE_WRITE".to_string();
    }

    match (
        change.input_version.is_some(),
        change.output_version.is_some(),
        change.id_operation,
    ) {
        (false, true, IDOperation::Created) => "CREATED".to_string(),
        (false, true, _) if unwrapped_ids.contains(&change.id) => "UNWRAPPED".to_string(),
        (true, true, _) if mutated_ids.contains(&change.id) => "MUTATED".to_string(),
        (true, false, IDOperation::Deleted) if deleted_ids.contains(&change.id) => {
            "DELETED".to_string()
        }
        (true, false, _) if wrapped_ids.contains(&change.id) => "WRAPPED".to_string(),
        (false, true, _) if created_ids.contains(&change.id) => "CREATED".to_string(),
        (false, true, _) => "UNWRAPPED".to_string(),
        (true, true, _) => "MUTATED".to_string(),
        (true, false, IDOperation::Deleted) => "DELETED".to_string(),
        (true, false, _) => "WRAPPED".to_string(),
        _ => "UNKNOWN".to_string(),
    }
}

fn flatten_owner(owner: Option<&Owner>) -> (Option<String>, Option<String>) {
    match owner {
        Some(Owner::AddressOwner(address)) => {
            (Some(address.to_string()), Some("ADDRESS".to_string()))
        }
        Some(Owner::ObjectOwner(address)) => {
            (Some(address.to_string()), Some("OBJECT".to_string()))
        }
        Some(Owner::Shared { .. }) => (None, Some("SHARED".to_string())),
        Some(Owner::Immutable) => (None, Some("IMMUTABLE".to_string())),
        Some(Owner::ConsensusAddressOwner { owner, .. }) => (
            Some(owner.to_string()),
            Some("CONSENSUS_ADDRESS".to_string()),
        ),
        None => (None, None),
    }
}

fn shared_mutability_name(mutability: &SharedObjectMutability) -> &'static str {
    match mutability {
        SharedObjectMutability::Immutable => "IMMUTABLE",
        SharedObjectMutability::Mutable => "MUTABLE",
        SharedObjectMutability::NonExclusiveWrite => "NON_EXCLUSIVE_WRITE",
    }
}

fn funds_withdrawal_amount(withdrawal: &FundsWithdrawalArg) -> Option<String> {
    match withdrawal.reservation {
        Reservation::MaxAmountU64(value) => Some(value.to_string()),
    }
}

fn count_move_calls(kind: &TransactionKind) -> usize {
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

fn command_record_with_args(
    ctx: &TxContext<'_>,
    command_index: usize,
    kind: &str,
    args: Value,
) -> CommandRecord {
    CommandRecord {
        tx_digest: ctx.digest.to_string(),
        command_index,
        kind: kind.to_string(),
        package: None,
        module: None,
        function: None,
        type_arguments: None,
        args: Some(args.to_string()),
        checkpoint_seq: ctx.checkpoint_seq.to_string(),
        timestamp: ctx.checkpoint_timestamp.to_string(),
    }
}

fn command_args_value(entries: Vec<(&str, Value)>) -> Value {
    let mut map = Map::new();
    for (key, value) in entries {
        map.insert(key.to_string(), value);
    }
    Value::Object(map)
}

fn argument_to_value(argument: &Argument) -> Value {
    match argument {
        Argument::GasCoin => Value::String("GasCoin".to_string()),
        Argument::Input(index) => Value::Object(Map::from_iter([(
            "Input".to_string(),
            Value::Number((*index).into()),
        )])),
        Argument::Result(index) => Value::Object(Map::from_iter([(
            "Result".to_string(),
            Value::Number((*index).into()),
        )])),
        Argument::NestedResult(first, second) => Value::Object(Map::from_iter([(
            "NestedResult".to_string(),
            Value::Array(vec![
                Value::Number((*first).into()),
                Value::Number((*second).into()),
            ]),
        )])),
    }
}

fn to_json_string<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string())
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

fn format_type_tag_canonical(tag: &TypeTag) -> String {
    tag.to_canonical_string(true)
}

const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

fn hex_string(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("0x");
    for &b in bytes {
        s.push(HEX_CHARS[(b >> 4) as usize] as char);
        s.push(HEX_CHARS[(b & 0x0f) as usize] as char);
    }
    s
}

fn sequence_number_to_string(value: SequenceNumber) -> String {
    value.value().to_string()
}

fn unix_millis_to_iso(ms: u64) -> String {
    let ms = ms as i64;
    let secs = ms.div_euclid(1_000);
    let millis = ms.rem_euclid(1_000) as u32;
    let days = secs.div_euclid(86_400);
    let seconds_of_day = secs.rem_euclid(86_400);

    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if month <= 2 { 1 } else { 0 };

    (year as i32, month as u32, day as u32)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn test_extract_empty_proto() {
        let result = extract_checkpoint(&[]).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["transactions"].as_array().unwrap().len(), 0);
        assert_eq!(parsed["checkpoint"]["sequenceNumber"], "0");
    }

    #[test]
    fn test_unix_millis_to_iso() {
        assert_eq!(
            unix_millis_to_iso(1_755_463_195_493),
            "2025-08-17T20:39:55.493Z"
        );
    }

    #[test]
    fn test_extract_sample_checkpoint() {
        let fixture = concat!(env!("CARGO_MANIFEST_DIR"), "/test-data/180000005.binpb.zst");
        let compressed = std::fs::read(fixture).unwrap();

        let mut decoder = zstd::Decoder::new(compressed.as_slice()).unwrap();
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();

        let (extracted, _profile) = extract_checkpoint_data(&decompressed).unwrap();
        let result = serde_json::to_string(&extracted).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();

        assert_eq!(parsed["transactions"].as_array().unwrap().len(), 6);
        assert_eq!(parsed["objectChanges"].as_array().unwrap().len(), 16);
        assert_eq!(parsed["dependencies"].as_array().unwrap().len(), 18);
        assert_eq!(parsed["balanceChanges"].as_array().unwrap().len(), 6);
        assert!(parsed["commands"].as_array().unwrap().len() > 0);
        assert!(parsed["systemTransactions"].as_array().unwrap().len() > 0);
        assert_eq!(parsed["checkpoint"]["epoch"], "858");
        assert!(parsed["transactions"][0]["sender"]
            .as_str()
            .is_some_and(|sender| sender.starts_with("0x")));

        let mut actual_balance_changes = extracted
            .balance_changes
            .iter()
            .map(|change| {
                (
                    change.address.as_str(),
                    change.coin_type.as_str(),
                    change.amount.as_str(),
                )
            })
            .collect::<Vec<_>>();
        actual_balance_changes.sort_unstable();
        let mut expected_balance_changes = vec![
            (
                "0x91b0f1e07c627edba3426ff311fd7b19a618cb3b7395a34dc687dac61ce63bc5",
                "0xaafb102dd0902f5055cadecd687fb5b71ca82ef0e0285d90afde828ec58ca96b::btc::BTC",
                "1903",
            ),
            (
                "0x91b0f1e07c627edba3426ff311fd7b19a618cb3b7395a34dc687dac61ce63bc5",
                "0xe1b45a0e641b9955a20aa0ad1c1f4ad86aad8afb07296d4085e349a50e90bdca::blue::BLUE",
                "226",
            ),
            (
                "0xe98aaadcdf0dfdbaf182b91269566adc413401c423229bfc4b690e884e25e3ee",
                "0x0000000000000000000000000000000000000000000000000000000000000002::sui::SUI",
                "-509880",
            ),
            (
                "0x91b0f1e07c627edba3426ff311fd7b19a618cb3b7395a34dc687dac61ce63bc5",
                "0x876a4b7bce8aeaef60464c11f4026903e9afacab79b9b142686158aa86560b50::xbtc::XBTC",
                "1880",
            ),
            (
                "0x91b0f1e07c627edba3426ff311fd7b19a618cb3b7395a34dc687dac61ce63bc5",
                "0xd1b72982e40348d069bb1ff701e634c117bb5f741f44dff91e472d3b01461e55::stsui::STSUI",
                "4",
            ),
            (
                "0x91b0f1e07c627edba3426ff311fd7b19a618cb3b7395a34dc687dac61ce63bc5",
                "0x0000000000000000000000000000000000000000000000000000000000000002::sui::SUI",
                "-1221768",
            ),
        ];
        expected_balance_changes.sort_unstable();
        assert_eq!(actual_balance_changes, expected_balance_changes);
        assert!(extracted
            .balance_changes
            .iter()
            .all(|change| !change.tx_digest.is_empty()));
        assert!(extracted
            .balance_changes
            .iter()
            .all(|change| change.checkpoint_seq == "180000005"));
        assert!(extracted
            .balance_changes
            .iter()
            .all(|change| change.timestamp == "2025-08-17T20:39:55.493Z"));
    }
}
