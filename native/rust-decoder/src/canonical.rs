//! Canonical checkpoint decode output used by the JS application layer.
//!
//! The output shape intentionally matches Jun's checkpoint source contract:
//! a decoded checkpoint response with canonical chain semantics, but without
//! any Jun-specific processed row materialization.

use std::collections::HashMap;

use serde::Serialize;
use serde_json::{Map, Number, Value};
use sui_types::base_types::{ObjectID, SequenceNumber, SuiAddress};
use sui_types::effects::{
    AccumulatorOperation, AccumulatorValue, TransactionEffects, TransactionEffectsAPI,
    TransactionEvents, UnchangedConsensusKind,
};
use sui_types::event::Event;
use sui_types::execution_status::{ExecutionErrorKind, ExecutionStatus};
use sui_types::messages_checkpoint::CheckpointSummary;
use sui_types::object::Owner;
use sui_types::transaction::{
    Argument, CallArg, Command, FundsWithdrawalArg, ObjectArg, Reservation, SenderSignedData,
    SharedObjectMutability, TransactionData, TransactionDataAPI, TransactionKind, WithdrawFrom,
};
use sui_types::TypeTag;

use crate::proto::{self, ProtoBalanceChange};

#[derive(Serialize)]
pub struct CanonicalCheckpointResponse {
    pub cursor: String,
    pub checkpoint: CanonicalCheckpoint,
}

#[derive(Serialize)]
pub struct CanonicalCheckpoint {
    #[serde(rename = "sequenceNumber")]
    pub sequence_number: String,
    pub summary: Option<CanonicalCheckpointSummary>,
    pub transactions: Vec<CanonicalTransaction>,
}

#[derive(Serialize)]
pub struct CanonicalCheckpointSummary {
    pub timestamp: CanonicalTimestamp,
    pub epoch: String,
    pub digest: Option<String>,
    #[serde(rename = "previousDigest")]
    pub previous_digest: Option<String>,
    #[serde(rename = "contentDigest")]
    pub content_digest: Option<String>,
    #[serde(rename = "totalNetworkTransactions")]
    pub total_network_transactions: String,
    #[serde(rename = "epochRollingGasCostSummary")]
    pub epoch_rolling_gas_cost_summary: CanonicalGasCostSummary,
}

#[derive(Serialize)]
pub struct CanonicalTimestamp {
    pub seconds: String,
    pub nanos: u32,
}

#[derive(Serialize)]
pub struct CanonicalGasCostSummary {
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
pub struct CanonicalTransaction {
    pub digest: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction: Option<CanonicalTransactionData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events: Option<CanonicalTransactionEvents>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effects: Option<CanonicalEffects>,
    #[serde(rename = "balanceChanges", skip_serializing_if = "Option::is_none")]
    pub balance_changes: Option<Vec<CanonicalBalanceChange>>,
}

#[derive(Serialize)]
pub struct CanonicalTransactionData {
    pub sender: String,
    #[serde(rename = "systemKind", skip_serializing_if = "Option::is_none")]
    pub system_kind: Option<String>,
    #[serde(rename = "systemData", skip_serializing_if = "Option::is_none")]
    pub system_data: Option<String>,
    #[serde(
        rename = "programmableTransaction",
        skip_serializing_if = "Option::is_none"
    )]
    pub programmable_transaction: Option<CanonicalProgrammableTransaction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commands: Option<Vec<CanonicalCommand>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inputs: Option<Vec<CanonicalInput>>,
    #[serde(rename = "gasPayment", skip_serializing_if = "Option::is_none")]
    pub gas_payment: Option<CanonicalGasPayment>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration: Option<CanonicalExpiration>,
}

#[derive(Serialize)]
pub struct CanonicalProgrammableTransaction {
    pub commands: Vec<CanonicalCommand>,
}

#[derive(Clone, Serialize)]
pub struct CanonicalCommand {
    #[serde(rename = "moveCall", skip_serializing_if = "Option::is_none")]
    pub move_call: Option<CanonicalMoveCall>,
    #[serde(rename = "transferObjects", skip_serializing_if = "Option::is_none")]
    pub transfer_objects: Option<Value>,
    #[serde(rename = "splitCoins", skip_serializing_if = "Option::is_none")]
    pub split_coins: Option<Value>,
    #[serde(rename = "mergeCoins", skip_serializing_if = "Option::is_none")]
    pub merge_coins: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upgrade: Option<Value>,
    #[serde(rename = "makeMoveVector", skip_serializing_if = "Option::is_none")]
    pub make_move_vector: Option<Value>,
}

#[derive(Clone, Serialize)]
pub struct CanonicalMoveCall {
    pub package: String,
    pub module: String,
    pub function: String,
    #[serde(rename = "typeArguments", skip_serializing_if = "Option::is_none")]
    pub type_arguments: Option<Vec<String>>,
}

#[derive(Serialize)]
pub struct CanonicalInput {
    pub kind: String,
    #[serde(rename = "pure", skip_serializing_if = "Option::is_none")]
    pub pure: Option<Vec<u8>>,
    #[serde(rename = "objectId", skip_serializing_if = "Option::is_none")]
    pub object_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutability: Option<String>,
    #[serde(
        rename = "initialSharedVersion",
        skip_serializing_if = "Option::is_none"
    )]
    pub initial_shared_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount: Option<String>,
    #[serde(rename = "coinType", skip_serializing_if = "Option::is_none")]
    pub coin_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

#[derive(Serialize)]
pub struct CanonicalGasPayment {
    pub objects: Vec<CanonicalObjectReference>,
    pub owner: String,
    pub price: String,
    pub budget: String,
}

#[derive(Serialize)]
pub struct CanonicalObjectReference {
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub version: String,
    pub digest: String,
}

#[derive(Serialize)]
pub struct CanonicalExpiration {
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<String>,
}

#[derive(Serialize)]
pub struct CanonicalTransactionEvents {
    pub events: Vec<CanonicalEvent>,
}

#[derive(Serialize)]
pub struct CanonicalEvent {
    #[serde(rename = "packageId")]
    pub package_id: String,
    pub module: String,
    pub sender: String,
    #[serde(rename = "eventType")]
    pub event_type: String,
    #[serde(rename = "typeParams", skip_serializing_if = "Vec::is_empty")]
    pub type_params: Vec<String>,
    pub contents: CanonicalEventContents,
}

#[derive(Serialize)]
pub struct CanonicalEventContents {
    pub name: String,
    pub value: Vec<u8>,
}

#[derive(Serialize)]
pub struct CanonicalEffects {
    pub status: CanonicalExecutionStatus,
    #[serde(rename = "gasUsed")]
    pub gas_used: CanonicalGasCostSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub dependencies: Vec<String>,
    #[serde(rename = "changedObjects", skip_serializing_if = "Vec::is_empty")]
    pub changed_objects: Vec<CanonicalChangedObject>,
    #[serde(
        rename = "unchangedConsensusObjects",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub unchanged_consensus_objects: Vec<CanonicalUnchangedConsensusObject>,
    #[serde(rename = "eventsDigest", skip_serializing_if = "Option::is_none")]
    pub events_digest: Option<String>,
    #[serde(rename = "lamportVersion", skip_serializing_if = "Option::is_none")]
    pub lamport_version: Option<String>,
}

#[derive(Serialize)]
pub struct CanonicalExecutionStatus {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<CanonicalExecutionError>,
}

#[derive(Serialize)]
pub struct CanonicalExecutionError {
    pub kind: String,
    #[serde(rename = "description", skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "commandIndex", skip_serializing_if = "Option::is_none")]
    pub command_index: Option<usize>,
    #[serde(rename = "abortCode", skip_serializing_if = "Option::is_none")]
    pub abort_code: Option<String>,
    #[serde(rename = "moveLocation", skip_serializing_if = "Option::is_none")]
    pub move_location: Option<CanonicalMoveLocation>,
}

#[derive(Serialize)]
pub struct CanonicalMoveLocation {
    pub package: String,
    pub module: String,
    pub function: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instruction: Option<u16>,
}

#[derive(Serialize)]
pub struct CanonicalChangedObject {
    #[serde(rename = "objectId")]
    pub object_id: String,
    #[serde(rename = "inputState")]
    pub input_state: String,
    #[serde(rename = "inputVersion", skip_serializing_if = "Option::is_none")]
    pub input_version: Option<String>,
    #[serde(rename = "inputDigest", skip_serializing_if = "Option::is_none")]
    pub input_digest: Option<String>,
    #[serde(rename = "inputOwner", skip_serializing_if = "Option::is_none")]
    pub input_owner: Option<CanonicalOwner>,
    #[serde(rename = "outputState")]
    pub output_state: String,
    #[serde(rename = "outputVersion", skip_serializing_if = "Option::is_none")]
    pub output_version: Option<String>,
    #[serde(rename = "outputDigest", skip_serializing_if = "Option::is_none")]
    pub output_digest: Option<String>,
    #[serde(rename = "outputOwner", skip_serializing_if = "Option::is_none")]
    pub output_owner: Option<CanonicalOwner>,
    #[serde(rename = "idOperation")]
    pub id_operation: String,
    #[serde(rename = "objectType", skip_serializing_if = "Option::is_none")]
    pub object_type: Option<String>,
    #[serde(rename = "accumulatorWrite", skip_serializing_if = "Option::is_none")]
    pub accumulator_write: Option<CanonicalAccumulatorWrite>,
}

#[derive(Serialize)]
pub struct CanonicalOwner {
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Serialize)]
pub struct CanonicalAccumulatorWrite {
    pub address: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub operation: String,
    pub value: String,
}

#[derive(Serialize)]
pub struct CanonicalUnchangedConsensusObject {
    pub kind: String,
    #[serde(rename = "objectId")]
    pub object_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    #[serde(rename = "objectType", skip_serializing_if = "Option::is_none")]
    pub object_type: Option<String>,
}

#[derive(Serialize)]
pub struct CanonicalBalanceChange {
    pub address: String,
    #[serde(rename = "coinType")]
    pub coin_type: String,
    pub amount: String,
}

pub fn decode_archive_checkpoint(compressed: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    let mut decoder = zstd::Decoder::new(compressed)?;
    let mut decompressed = Vec::new();
    use std::io::Read;
    decoder.read_to_end(&mut decompressed)?;
    decode_checkpoint_proto(&decompressed)
}

pub fn decode_checkpoint_proto(
    checkpoint_bytes: &[u8],
) -> Result<String, Box<dyn std::error::Error>> {
    let parsed =
        proto::parse_checkpoint(checkpoint_bytes).map_err(|e| format!("proto parse: {e}"))?;
    let summary = parsed.summary_bcs.and_then(decode_summary);
    let sequence_number = summary
        .as_ref()
        .map(|summary| summary.sequence_number.clone())
        .or_else(|| parsed.sequence_number.clone())
        .unwrap_or_else(|| "0".to_string());
    let cursor = sequence_number.clone();

    let checkpoint = build_checkpoint(&parsed, summary, sequence_number);
    let response = CanonicalCheckpointResponse { cursor, checkpoint };
    Ok(serde_json::to_string(&response)?)
}

pub fn decode_subscribe_checkpoints_response(
    response_bytes: &[u8],
) -> Result<String, Box<dyn std::error::Error>> {
    let extracted = proto::parse_subscribe_checkpoints_response(response_bytes)
        .map_err(|e| format!("subscribe response parse: {e}"))?;
    let parsed = proto::parse_checkpoint(extracted.checkpoint_bytes)
        .map_err(|e| format!("checkpoint parse: {e}"))?;
    let summary = parsed.summary_bcs.and_then(decode_summary);
    let sequence_number = summary
        .as_ref()
        .map(|summary| summary.sequence_number.clone())
        .or_else(|| parsed.sequence_number.clone())
        .unwrap_or_else(|| extracted.cursor.clone());

    let checkpoint = build_checkpoint(&parsed, summary, sequence_number);
    let response = CanonicalCheckpointResponse {
        cursor: extracted.cursor,
        checkpoint,
    };
    Ok(serde_json::to_string(&response)?)
}

pub fn decode_get_checkpoint_response(
    response_bytes: &[u8],
) -> Result<String, Box<dyn std::error::Error>> {
    let checkpoint_bytes = proto::parse_get_checkpoint_response(response_bytes)
        .map_err(|e| format!("get checkpoint response parse: {e}"))?;
    let parsed =
        proto::parse_checkpoint(checkpoint_bytes).map_err(|e| format!("checkpoint parse: {e}"))?;
    let summary = parsed.summary_bcs.and_then(decode_summary);
    let sequence_number = summary
        .as_ref()
        .map(|summary| summary.sequence_number.clone())
        .or_else(|| parsed.sequence_number.clone())
        .unwrap_or_else(|| "0".to_string());
    let cursor = sequence_number.clone();
    let checkpoint = build_checkpoint(&parsed, summary, sequence_number);
    let response = CanonicalCheckpointResponse { cursor, checkpoint };
    Ok(serde_json::to_string(&response)?)
}

struct DecodedSummary {
    sequence_number: String,
    timestamp_ms: u64,
    epoch: String,
    previous_digest: Option<String>,
    content_digest: Option<String>,
    total_network_transactions: String,
    gas_summary: CanonicalGasCostSummary,
}

fn build_checkpoint(
    parsed: &proto::ProtoCheckpoint<'_>,
    summary: Option<DecodedSummary>,
    sequence_number: String,
) -> CanonicalCheckpoint {
    let transactions = parsed
        .transactions
        .iter()
        .map(build_transaction)
        .collect::<Vec<_>>();

    CanonicalCheckpoint {
        sequence_number,
        summary: summary.map(|summary| CanonicalCheckpointSummary {
            timestamp: timestamp_from_millis(summary.timestamp_ms),
            epoch: summary.epoch,
            digest: parsed
                .digest
                .map(|digest| digest.to_string())
                .filter(|digest| !digest.is_empty()),
            previous_digest: summary.previous_digest,
            content_digest: summary.content_digest,
            total_network_transactions: summary.total_network_transactions,
            epoch_rolling_gas_cost_summary: summary.gas_summary,
        }),
        transactions,
    }
}

fn build_transaction(ptx: &proto::ProtoTransaction<'_>) -> CanonicalTransaction {
    let effects = ptx
        .effects_bcs
        .and_then(|bytes| bcs::from_bytes::<TransactionEffects>(bytes).ok());
    let tx_data = ptx.transaction_bcs.and_then(decode_transaction_data);
    let tx_events = ptx
        .events_bcs
        .and_then(|bytes| bcs::from_bytes::<TransactionEvents>(bytes).ok());

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

    CanonicalTransaction {
        digest,
        transaction: tx_data.as_ref().map(canonical_transaction_data),
        events: tx_events.as_ref().map(canonical_events),
        effects: effects.as_ref().map(canonical_effects),
        balance_changes: if ptx.balance_changes.is_empty() {
            None
        } else {
            Some(
                ptx.balance_changes
                    .iter()
                    .map(proto_balance_change)
                    .collect(),
            )
        },
    }
}

fn canonical_transaction_data(tx: &TransactionData) -> CanonicalTransactionData {
    let system_kind = system_kind_name(tx.kind()).map(|value| value.to_string());
    let commands = canonical_commands(tx.kind());
    let inputs = canonical_inputs(tx.kind());

    CanonicalTransactionData {
        sender: tx.sender().to_string(),
        system_kind: system_kind.clone(),
        system_data: system_kind.map(|_| canonical_system_data(tx.kind())),
        programmable_transaction: commands.as_ref().map(|commands| {
            CanonicalProgrammableTransaction {
                commands: commands.clone(),
            }
        }),
        commands,
        inputs,
        gas_payment: canonical_gas_payment(tx),
        expiration: canonical_expiration(tx),
    }
}

fn canonical_commands(kind: &TransactionKind) -> Option<Vec<CanonicalCommand>> {
    let programmable = match kind {
        TransactionKind::ProgrammableTransaction(pt)
        | TransactionKind::ProgrammableSystemTransaction(pt) => pt,
        _ => return None,
    };

    Some(
        programmable
            .commands
            .iter()
            .map(canonical_command)
            .collect::<Vec<_>>(),
    )
}

fn canonical_command(command: &Command) -> CanonicalCommand {
    match command {
        Command::MoveCall(move_call) => CanonicalCommand {
            move_call: Some(CanonicalMoveCall {
                package: move_call.package.to_string(),
                module: move_call.module.to_string(),
                function: move_call.function.to_string(),
                type_arguments: if move_call.type_arguments.is_empty() {
                    None
                } else {
                    Some(
                        move_call
                            .type_arguments
                            .iter()
                            .map(|tag| tag.to_canonical_string(true))
                            .collect(),
                    )
                },
            }),
            transfer_objects: None,
            split_coins: None,
            merge_coins: None,
            publish: None,
            upgrade: None,
            make_move_vector: None,
        },
        Command::TransferObjects(objects, address) => CanonicalCommand {
            move_call: None,
            transfer_objects: Some(command_object(vec![
                (
                    "objects",
                    Value::Array(objects.iter().map(argument_to_value).collect()),
                ),
                ("address", argument_to_value(address)),
            ])),
            split_coins: None,
            merge_coins: None,
            publish: None,
            upgrade: None,
            make_move_vector: None,
        },
        Command::SplitCoins(coin, amounts) => CanonicalCommand {
            move_call: None,
            transfer_objects: None,
            split_coins: Some(command_object(vec![
                ("coin", argument_to_value(coin)),
                (
                    "amounts",
                    Value::Array(amounts.iter().map(argument_to_value).collect()),
                ),
            ])),
            merge_coins: None,
            publish: None,
            upgrade: None,
            make_move_vector: None,
        },
        Command::MergeCoins(coin, coins) => CanonicalCommand {
            move_call: None,
            transfer_objects: None,
            split_coins: None,
            merge_coins: Some(command_object(vec![
                ("coin", argument_to_value(coin)),
                (
                    "coins",
                    Value::Array(coins.iter().map(argument_to_value).collect()),
                ),
            ])),
            publish: None,
            upgrade: None,
            make_move_vector: None,
        },
        Command::Publish(modules, dependencies) => CanonicalCommand {
            move_call: None,
            transfer_objects: None,
            split_coins: None,
            merge_coins: None,
            publish: Some(command_object(vec![
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
                            .map(|dependency| Value::String(dependency.to_string()))
                            .collect(),
                    ),
                ),
            ])),
            upgrade: None,
            make_move_vector: None,
        },
        Command::Upgrade(modules, dependencies, package, ticket) => CanonicalCommand {
            move_call: None,
            transfer_objects: None,
            split_coins: None,
            merge_coins: None,
            publish: None,
            upgrade: Some(command_object(vec![
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
                            .map(|dependency| Value::String(dependency.to_string()))
                            .collect(),
                    ),
                ),
                ("package", Value::String(package.to_string())),
                ("ticket", argument_to_value(ticket)),
            ])),
            make_move_vector: None,
        },
        Command::MakeMoveVec(element_type, elements) => CanonicalCommand {
            move_call: None,
            transfer_objects: None,
            split_coins: None,
            merge_coins: None,
            publish: None,
            upgrade: None,
            make_move_vector: Some(command_object(vec![
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
            ])),
        },
    }
}

fn canonical_inputs(kind: &TransactionKind) -> Option<Vec<CanonicalInput>> {
    let programmable = match kind {
        TransactionKind::ProgrammableTransaction(pt)
        | TransactionKind::ProgrammableSystemTransaction(pt) => pt,
        _ => return None,
    };

    Some(
        programmable
            .inputs
            .iter()
            .map(canonical_input)
            .collect::<Vec<_>>(),
    )
}

fn canonical_input(input: &CallArg) -> CanonicalInput {
    match input {
        CallArg::Pure(bytes) => CanonicalInput {
            kind: "PURE".to_string(),
            pure: Some(bytes.clone()),
            object_id: None,
            version: None,
            digest: None,
            mutability: None,
            initial_shared_version: None,
            amount: None,
            coin_type: None,
            source: None,
        },
        CallArg::Object(object) => match object {
            ObjectArg::ImmOrOwnedObject((id, version, digest)) => CanonicalInput {
                kind: "IMMUTABLE_OR_OWNED".to_string(),
                pure: None,
                object_id: Some(id.to_string()),
                version: Some(sequence_number_to_string(*version)),
                digest: Some(digest.to_string()),
                mutability: None,
                initial_shared_version: None,
                amount: None,
                coin_type: None,
                source: None,
            },
            ObjectArg::SharedObject {
                id,
                initial_shared_version,
                mutability,
            } => CanonicalInput {
                kind: "SHARED".to_string(),
                pure: None,
                object_id: Some(id.to_string()),
                version: None,
                digest: None,
                mutability: Some(shared_mutability_name(mutability).to_string()),
                initial_shared_version: Some(sequence_number_to_string(*initial_shared_version)),
                amount: None,
                coin_type: None,
                source: None,
            },
            ObjectArg::Receiving((id, version, digest)) => CanonicalInput {
                kind: "RECEIVING".to_string(),
                pure: None,
                object_id: Some(id.to_string()),
                version: Some(sequence_number_to_string(*version)),
                digest: Some(digest.to_string()),
                mutability: None,
                initial_shared_version: None,
                amount: None,
                coin_type: None,
                source: None,
            },
        },
        CallArg::FundsWithdrawal(withdrawal) => CanonicalInput {
            kind: "FUNDS_WITHDRAWAL".to_string(),
            pure: None,
            object_id: None,
            version: None,
            digest: None,
            mutability: None,
            initial_shared_version: None,
            amount: funds_withdrawal_amount(withdrawal),
            coin_type: Some(withdrawal.type_arg.to_type_tag().to_canonical_string(true)),
            source: Some(match withdrawal.withdraw_from {
                WithdrawFrom::Sender => "SENDER".to_string(),
                WithdrawFrom::Sponsor => "SPONSOR".to_string(),
            }),
        },
    }
}

fn canonical_gas_payment(tx: &TransactionData) -> Option<CanonicalGasPayment> {
    let gas_objects = tx.gas();
    if gas_objects.is_empty() {
        return None;
    }

    Some(CanonicalGasPayment {
        objects: gas_objects
            .iter()
            .map(|(id, version, digest)| CanonicalObjectReference {
                object_id: id.to_string(),
                version: sequence_number_to_string(*version),
                digest: digest.to_string(),
            })
            .collect(),
        owner: tx.gas_owner().to_string(),
        price: tx.gas_price().to_string(),
        budget: tx.gas_budget().to_string(),
    })
}

fn canonical_expiration(tx: &TransactionData) -> Option<CanonicalExpiration> {
    match tx.expiration() {
        sui_types::transaction::TransactionExpiration::None => Some(CanonicalExpiration {
            kind: "NONE".to_string(),
            epoch: None,
        }),
        sui_types::transaction::TransactionExpiration::Epoch(epoch) => Some(CanonicalExpiration {
            kind: "EPOCH".to_string(),
            epoch: Some(epoch.to_string()),
        }),
        sui_types::transaction::TransactionExpiration::ValidDuring { .. } => {
            Some(CanonicalExpiration {
                kind: "VALID_DURING".to_string(),
                epoch: None,
            })
        }
    }
}

fn canonical_events(events: &TransactionEvents) -> CanonicalTransactionEvents {
    CanonicalTransactionEvents {
        events: events.data.iter().map(canonical_event).collect(),
    }
}

fn canonical_event(event: &Event) -> CanonicalEvent {
    let type_params = event
        .type_
        .type_params
        .iter()
        .map(|t| t.to_canonical_string(true))
        .collect::<Vec<_>>();
    let event_type = TypeTag::Struct(Box::new(event.type_.clone())).to_canonical_string(true);

    CanonicalEvent {
        package_id: event.package_id.to_string(),
        module: event.transaction_module.to_string(),
        sender: event.sender.to_string(),
        event_type: event_type.clone(),
        type_params,
        contents: CanonicalEventContents {
            name: event_type,
            value: event.contents.clone(),
        },
    }
}

fn canonical_effects(effects: &TransactionEffects) -> CanonicalEffects {
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

    let (success, error) = match effects.status() {
        ExecutionStatus::Success => (true, None),
        ExecutionStatus::Failure(failure) => {
            let (abort_code, move_location) = match &failure.error {
                ExecutionErrorKind::MoveAbort(location, code) => (
                    Some(code.to_string()),
                    Some(CanonicalMoveLocation {
                        package: format_address_bytes(location.module.address().as_ref()),
                        module: location.module.name().to_string(),
                        function: location.function_name.clone().unwrap_or_default(),
                        instruction: Some(location.instruction),
                    }),
                ),
                _ => (None, None),
            };

            (
                false,
                Some(CanonicalExecutionError {
                    kind: execution_error_kind_name(&failure.error),
                    description: None,
                    command_index: failure.command.map(|value| value as usize),
                    abort_code,
                    move_location,
                }),
            )
        }
    };

    let changed_objects = effects
        .object_changes()
        .into_iter()
        .map(|change| {
            let input_owner = old_metadata.get(&change.id).and_then(flatten_owner);
            let output_owner = new_metadata.get(&change.id).and_then(flatten_owner);
            let input_exists = input_owner.is_some() || change.input_version.is_some();
            let (output_state, output_version) = if effects
                .published_packages()
                .into_iter()
                .any(|id| id == change.id)
            {
                (
                    "PACKAGE_WRITE".to_string(),
                    change.output_version.map(sequence_number_to_string),
                )
            } else if change.output_digest.is_some() || output_owner.is_some() {
                ("OBJECT_WRITE".to_string(), None)
            } else {
                ("DOES_NOT_EXIST".to_string(), None)
            };

            CanonicalChangedObject {
                object_id: change.id.to_string(),
                input_state: if input_exists {
                    "EXISTS".to_string()
                } else {
                    "DOES_NOT_EXIST".to_string()
                },
                input_version: change.input_version.map(sequence_number_to_string),
                input_digest: change.input_digest.map(|digest| digest.to_string()),
                input_owner,
                output_state,
                output_version,
                output_digest: change.output_digest.map(|digest| digest.to_string()),
                output_owner,
                id_operation: match change.id_operation {
                    sui_types::effects::IDOperation::None => "NONE".to_string(),
                    sui_types::effects::IDOperation::Created => "CREATED".to_string(),
                    sui_types::effects::IDOperation::Deleted => "DELETED".to_string(),
                },
                object_type: None,
                accumulator_write: None,
            }
        })
        .collect::<Vec<_>>();

    let accumulator_writes = effects.accumulator_updates();
    let mut changed_objects = changed_objects;
    for (object_id, write) in accumulator_writes {
        changed_objects.push(CanonicalChangedObject {
            object_id: object_id.to_string(),
            input_state: "DOES_NOT_EXIST".to_string(),
            input_version: None,
            input_digest: None,
            input_owner: None,
            output_state: "ACCUMULATOR_WRITE".to_string(),
            output_version: None,
            output_digest: None,
            output_owner: None,
            id_operation: "NONE".to_string(),
            object_type: None,
            accumulator_write: Some(CanonicalAccumulatorWrite {
                address: write.address.address.to_string(),
                type_: write.address.ty.to_canonical_string(true),
                operation: match write.operation {
                    AccumulatorOperation::Merge => "MERGE".to_string(),
                    AccumulatorOperation::Split => "SPLIT".to_string(),
                },
                value: match write.value {
                    AccumulatorValue::Integer(value) => value.to_string(),
                    AccumulatorValue::IntegerTuple(first, second) => {
                        format!("{first},{second}")
                    }
                    AccumulatorValue::EventDigest(digests) => digests.len().to_string(),
                },
            }),
        });
    }

    CanonicalEffects {
        status: CanonicalExecutionStatus { success, error },
        gas_used: CanonicalGasCostSummary {
            computation_cost: effects.gas_cost_summary().computation_cost.to_string(),
            storage_cost: effects.gas_cost_summary().storage_cost.to_string(),
            storage_rebate: effects.gas_cost_summary().storage_rebate.to_string(),
            non_refundable_storage_fee: effects
                .gas_cost_summary()
                .non_refundable_storage_fee
                .to_string(),
        },
        epoch: Some(effects.executed_epoch().to_string()),
        dependencies: effects
            .dependencies()
            .iter()
            .map(|digest| digest.to_string())
            .collect(),
        changed_objects,
        unchanged_consensus_objects: effects
            .unchanged_consensus_objects()
            .into_iter()
            .map(|(object_id, kind)| canonical_unchanged_consensus_object(object_id, kind))
            .collect(),
        events_digest: effects.events_digest().map(|digest| digest.to_string()),
        lamport_version: Some(sequence_number_to_string(effects.lamport_version())),
    }
}

fn canonical_unchanged_consensus_object(
    object_id: ObjectID,
    kind: UnchangedConsensusKind,
) -> CanonicalUnchangedConsensusObject {
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
        UnchangedConsensusKind::PerEpochConfig => ("PER_EPOCH_CONFIG".to_string(), None, None),
    };

    CanonicalUnchangedConsensusObject {
        kind: kind_name,
        object_id: object_id.to_string(),
        version,
        digest,
        object_type: None,
    }
}

fn proto_balance_change(change: &ProtoBalanceChange<'_>) -> CanonicalBalanceChange {
    CanonicalBalanceChange {
        address: change.address.to_string(),
        coin_type: change.coin_type.to_string(),
        amount: change.amount.to_string(),
    }
}

fn decode_summary(summary_bcs: &[u8]) -> Option<DecodedSummary> {
    let summary = bcs::from_bytes::<CheckpointSummary>(summary_bcs).ok()?;
    Some(DecodedSummary {
        sequence_number: summary.sequence_number.to_string(),
        timestamp_ms: summary.timestamp_ms,
        epoch: summary.epoch.to_string(),
        previous_digest: summary
            .previous_digest
            .as_ref()
            .map(|digest| hex_string(digest.inner())),
        content_digest: Some(hex_string(summary.content_digest.inner())),
        total_network_transactions: summary.network_total_transactions.to_string(),
        gas_summary: CanonicalGasCostSummary {
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

fn system_kind_name(kind: &TransactionKind) -> Option<&'static str> {
    match kind {
        TransactionKind::ProgrammableTransaction(_) => None,
        TransactionKind::Genesis(_) => Some("GENESIS"),
        TransactionKind::ChangeEpoch(_) => Some("CHANGE_EPOCH"),
        TransactionKind::ConsensusCommitPrologue(_) => Some("CONSENSUS_COMMIT_PROLOGUE_V1"),
        TransactionKind::ConsensusCommitPrologueV2(_) => Some("CONSENSUS_COMMIT_PROLOGUE_V2"),
        TransactionKind::ConsensusCommitPrologueV3(_) => Some("CONSENSUS_COMMIT_PROLOGUE_V3"),
        TransactionKind::ConsensusCommitPrologueV4(_) => Some("CONSENSUS_COMMIT_PROLOGUE_V4"),
        TransactionKind::AuthenticatorStateUpdate(_) => Some("AUTHENTICATOR_STATE_UPDATE"),
        TransactionKind::EndOfEpochTransaction(_) => Some("END_OF_EPOCH"),
        TransactionKind::RandomnessStateUpdate(_) => Some("RANDOMNESS_STATE_UPDATE"),
        TransactionKind::ProgrammableSystemTransaction(_) => {
            Some("PROGRAMMABLE_SYSTEM_TRANSACTION")
        }
    }
}

fn canonical_system_data(kind: &TransactionKind) -> String {
    let value = match kind {
        TransactionKind::Genesis(data) => serde_json::to_value(data),
        TransactionKind::ChangeEpoch(data) => serde_json::to_value(data),
        TransactionKind::ConsensusCommitPrologue(data) => serde_json::to_value(data),
        TransactionKind::ConsensusCommitPrologueV2(data) => serde_json::to_value(data),
        TransactionKind::ConsensusCommitPrologueV3(data) => serde_json::to_value(data),
        TransactionKind::ConsensusCommitPrologueV4(data) => serde_json::to_value(data),
        TransactionKind::AuthenticatorStateUpdate(data) => serde_json::to_value(data),
        TransactionKind::EndOfEpochTransaction(data) => serde_json::to_value(data),
        TransactionKind::RandomnessStateUpdate(data) => serde_json::to_value(data),
        TransactionKind::ProgrammableSystemTransaction(data) => serde_json::to_value(data),
        TransactionKind::ProgrammableTransaction(_) => {
            serde_json::to_value(Map::<String, Value>::new())
        }
    }
    .unwrap_or(Value::Object(Map::new()));

    value.to_string()
}

fn flatten_owner(owner: &Owner) -> Option<CanonicalOwner> {
    match owner {
        Owner::AddressOwner(address) => Some(CanonicalOwner {
            kind: "ADDRESS".to_string(),
            address: Some(address.to_string()),
            version: None,
        }),
        Owner::ObjectOwner(address) => Some(CanonicalOwner {
            kind: "OBJECT".to_string(),
            address: Some(address.to_string()),
            version: None,
        }),
        Owner::Shared {
            initial_shared_version,
        } => Some(CanonicalOwner {
            kind: "SHARED".to_string(),
            address: None,
            version: Some(sequence_number_to_string(*initial_shared_version)),
        }),
        Owner::Immutable => Some(CanonicalOwner {
            kind: "IMMUTABLE".to_string(),
            address: None,
            version: None,
        }),
        Owner::ConsensusAddressOwner {
            owner,
            start_version,
        } => Some(CanonicalOwner {
            kind: "CONSENSUS_ADDRESS".to_string(),
            address: Some(owner.to_string()),
            version: Some(sequence_number_to_string(*start_version)),
        }),
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

fn execution_error_kind_name(error: &ExecutionErrorKind) -> String {
    let debug = format!("{error:?}");
    debug
        .split(['(', '{'])
        .next()
        .unwrap_or(debug.as_str())
        .trim()
        .to_string()
}

fn command_object(entries: Vec<(&str, Value)>) -> Value {
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
            Value::Number(Number::from(*index)),
        )])),
        Argument::Result(index) => Value::Object(Map::from_iter([(
            "Result".to_string(),
            Value::Number(Number::from(*index)),
        )])),
        Argument::NestedResult(first, second) => Value::Object(Map::from_iter([(
            "NestedResult".to_string(),
            Value::Array(vec![
                Value::Number(Number::from(*first)),
                Value::Number(Number::from(*second)),
            ]),
        )])),
    }
}

fn timestamp_from_millis(ms: u64) -> CanonicalTimestamp {
    CanonicalTimestamp {
        seconds: (ms / 1_000).to_string(),
        nanos: ((ms % 1_000) * 1_000_000) as u32,
    }
}


fn hex_string(bytes: &[u8]) -> String {
    format!(
        "0x{}",
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

fn format_address_bytes(bytes: &[u8]) -> String {
    format!(
        "0x{}",
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

fn sequence_number_to_string(value: SequenceNumber) -> String {
    value.value().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn load_sample_checkpoint_bytes() -> Vec<u8> {
        let fixture = concat!(env!("CARGO_MANIFEST_DIR"), "/test-data/180000005.binpb.zst");
        let compressed = std::fs::read(fixture).unwrap();
        let mut decoder = zstd::Decoder::new(compressed.as_slice()).unwrap();
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();
        decompressed
    }

    fn encode_varint(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if value == 0 {
                break;
            }
        }
        out
    }

    fn encode_length_delimited(field_number: u32, payload: &[u8]) -> Vec<u8> {
        let mut out = encode_varint(((field_number << 3) | 2) as u64);
        out.extend(encode_varint(payload.len() as u64));
        out.extend(payload);
        out
    }

    #[test]
    fn test_decode_checkpoint_proto() {
        let bytes = load_sample_checkpoint_bytes();
        let decoded = decode_checkpoint_proto(&bytes).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&decoded).unwrap();
        assert_eq!(
            parsed["checkpoint"]["transactions"]
                .as_array()
                .unwrap()
                .len(),
            6
        );
        assert_eq!(parsed["checkpoint"]["summary"]["epoch"], "858");
    }

    #[test]
    fn test_decode_subscribe_checkpoints_response() {
        let checkpoint = load_sample_checkpoint_bytes();
        let mut response = encode_varint(8);
        response.extend(encode_varint(180000005));
        response.extend(encode_length_delimited(2, &checkpoint));

        let decoded = decode_subscribe_checkpoints_response(&response).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&decoded).unwrap();
        assert_eq!(parsed["cursor"], "180000005");
        assert_eq!(
            parsed["checkpoint"]["transactions"]
                .as_array()
                .unwrap()
                .len(),
            6
        );
    }

    #[test]
    fn test_decode_get_checkpoint_response() {
        let checkpoint = load_sample_checkpoint_bytes();
        let response = encode_length_delimited(1, &checkpoint);

        let decoded = decode_get_checkpoint_response(&response).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&decoded).unwrap();
        assert_eq!(parsed["cursor"], "180000005");
        assert_eq!(
            parsed["checkpoint"]["transactions"]
                .as_array()
                .unwrap()
                .len(),
            6
        );
    }
}
