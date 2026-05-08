//! jun-checkpoint-decoder — protobuf checkpoint bytes → flat ExtractedCheckpoint records.
//!
//! Pipeline:
//!   1. prost decodes the v2 RPC protobuf envelope (via `sui-rpc` crate).
//!   2. The envelope's nested messages carry their canonical structures as
//!      raw BCS bytes inside a `Bcs { value: bytes }` wrapper. We BCS-decode
//!      each into `sui-types` canonical structs.
//!   3. Walk the canonical structs, producing flat `jun-types` records.
//!
//! Why this two-layer dance: the user's archive populates only the bcs.value
//! byte fields — the proto's parallel decoded fields (e.g. `effects.status`,
//! `transaction.sender`) are left empty. Live gRPC streams may set both, but
//! BCS-decoding is the universal path that works for any producer.
//!
//! v1 record set: CHECKPOINTS / TRANSACTIONS / MOVE_CALLS / BALANCE_CHANGES /
//! OBJECT_CHANGES / DEPENDENCIES.

use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, Result};
use prost::Message;

use jun_types::{
    BalanceChangeRecord, CheckpointRecord, CoinBalanceRecord, CommandRecord, DependencyRecord,
    EpochCommitteeRecord, EventRecord, ExtractMask, ExtractedCheckpoint, MoveCallRecord,
    ObjectChangeRecord, PackageModuleRecord, PackageRecord, TransactionRecord,
    TransactionSignatureRecord,
};

use sui_rpc::proto::sui::rpc::v2::Checkpoint as ProtoCheckpoint;

use sui_types::balance_change::derive_balance_changes_2;
use sui_types::base_types::{ObjectID, SequenceNumber};
use sui_types::effects::{IDOperation, TransactionEffects, TransactionEffectsAPI, TransactionEvents};
use sui_types::execution_status::{ExecutionErrorKind, ExecutionStatus};
use sui_types::full_checkpoint_content::ObjectSet;
use sui_types::message_envelope::Message as _;
use sui_types::messages_checkpoint::CheckpointSummary;
use sui_types::object::{Data, Object, Owner};
use sui_types::storage::ObjectKey;
use sui_types::transaction::{
    Command, SenderSignedData, TransactionData, TransactionDataAPI, TransactionKind,
};

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Decoder backend. Single variant — prost. We benchmarked a hand-rolled
/// skip-scan parser and a rayon-parallel inner-BCS variant; both lost to
/// prost on the production workload (the pipeline already runs N decoders
/// concurrently, so further parallelism inside one decode call just shifts
/// work between thread pools). Kept as an enum so the type stays in the
/// public API and we can experiment again if hardware changes.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
pub enum Decoder {
    #[default]
    Prost,
}

impl std::str::FromStr for Decoder {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "prost" => Ok(Decoder::Prost),
            other => Err(anyhow!("unknown decoder {other} (expected prost)")),
        }
    }
}

/// Decode protobuf bytes (decompressed `Checkpoint` message) and flatten into
/// per-record-type lists. Default backend is prost.
pub fn decode_checkpoint(proto_bytes: &[u8], mask: ExtractMask) -> Result<ExtractedCheckpoint> {
    decode_with(proto_bytes, mask, Decoder::Prost)
}

/// Decode with an explicit backend choice. Convenience for benchmarking and
/// for the binary's `--decoder` flag.
pub fn decode_with(
    proto_bytes: &[u8],
    mask: ExtractMask,
    _decoder: Decoder,
) -> Result<ExtractedCheckpoint> {
    let cp = ProtoCheckpoint::decode(proto_bytes).map_err(|e| anyhow!("proto decode: {e}"))?;
    Ok(flatten_checkpoint(&cp, mask))
}

/// Walk an already-decoded proto `Checkpoint` into flat records. Use this when
/// you already have a parsed proto (e.g. from a tonic gRPC subscribe stream).
pub fn flatten_checkpoint(cp: &ProtoCheckpoint, mask: ExtractMask) -> ExtractedCheckpoint {
    // --- Top-level summary --------------------------------------------------
    let summary = cp
        .summary
        .as_ref()
        .and_then(|s| s.bcs.as_ref())
        .and_then(|b| b.value.as_ref())
        .and_then(|v| bcs::from_bytes::<CheckpointSummary>(v).ok());

    let checkpoint_seq = summary
        .as_ref()
        .map(|s| s.sequence_number.to_string())
        .unwrap_or_default();
    let checkpoint_ts = summary
        .as_ref()
        .map(|s| s.timestamp_ms.to_string())
        .unwrap_or_default();

    let mut out = ExtractedCheckpoint::default();

    // Always populate the checkpoint record. Downstream encoders (juna-clickhouse)
    // denormalize `epoch`, `sequence_number`, and `timestamp_ms` onto every
    // leaf-table row from this struct — even tables that aren't in the mask.
    // Skipping it when CHECKPOINTS is masked off causes silent data corruption:
    // every row gets epoch=0, partition_key=0, etc. The CHECKPOINTS bit only
    // gates whether a CheckpointRow gets *inserted* into the `checkpoints`
    // table, not whether the metadata is populated for downstream reads.
    if let Some(s) = summary.as_ref() {
        // Mysten's per-checkpoint archive omits proto field 2 (digest) — only
        // the BCS-wrapped summary is shipped. Recompute it from the summary
        // so both producers (live gRPC + archive) yield identical records.
        // Sui digest = BLAKE2b-256(b"CheckpointSummary::" || BCS(summary)),
        // base58-encoded (handled by CheckpointDigest's Display impl).
        let digest_str = cp
            .digest
            .as_deref()
            .filter(|d| !d.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| s.digest().to_string());
        out.checkpoint = build_checkpoint_record(s, digest_str);
    }

    // --- Build the checkpoint-level ObjectSet for balance / object change derivation
    let need_objects = mask.intersects(
        ExtractMask::BALANCE_CHANGES
            | ExtractMask::OBJECT_CHANGES
            | ExtractMask::COIN_BALANCES,
    );
    let object_set = if need_objects {
        build_object_set(cp)
    } else {
        ObjectSet::default()
    };

    // --- Per-transaction walk ----------------------------------------------
    for tx in &cp.transactions {
        let effects: Option<TransactionEffects> = tx
            .effects
            .as_ref()
            .and_then(|e| e.bcs.as_ref())
            .and_then(|b| b.value.as_ref())
            .and_then(|v| bcs::from_bytes(v).ok());

        let tx_data_and_sigs = tx
            .transaction
            .as_ref()
            .and_then(|t| t.bcs.as_ref())
            .and_then(|b| b.value.as_ref())
            .and_then(|v| decode_tx_with_signatures(v));
        let tx_data: Option<TransactionData> =
            tx_data_and_sigs.as_ref().map(|(td, _)| td.clone());
        let ssd_signatures: &[sui_types::signature::GenericSignature] = tx_data_and_sigs
            .as_ref()
            .map(|(_, s)| s.as_slice())
            .unwrap_or(&[]);

        // Prefer the canonical digest from the BCS-decoded effects; fall back
        // to the proto envelope's string field.
        let digest = effects
            .as_ref()
            .map(|fx| fx.transaction_digest().to_string())
            .filter(|s| !s.is_empty())
            .or_else(|| tx.digest.clone().filter(|s| !s.is_empty()))
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

        if mask.contains(ExtractMask::COMMANDS) {
            if let Some(td) = tx_data.as_ref() {
                extract_commands(
                    &digest, &checkpoint_seq, &checkpoint_ts,
                    td.kind(), &mut out.commands,
                );
            }
        }

        if mask.contains(ExtractMask::TRANSACTION_SIGNATURES) {
            extract_transaction_signatures(
                &digest, &checkpoint_seq, &checkpoint_ts,
                tx, ssd_signatures, &mut out.transaction_signatures,
            );
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
            if mask.contains(ExtractMask::COIN_BALANCES) {
                extract_coin_balances(
                    &digest, &checkpoint_seq, &checkpoint_ts,
                    fx, &object_set, &mut out.coin_balances,
                );
            }
        }

        if mask.contains(ExtractMask::EVENTS) {
            extract_events(
                &digest, &checkpoint_seq, &checkpoint_ts,
                tx, &mut out.events,
            );
        }
    }

    if mask.contains(ExtractMask::PACKAGES) {
        extract_packages(
            cp, &checkpoint_seq, &checkpoint_ts,
            &mut out.packages, &mut out.package_modules,
        );
    }

    if mask.contains(ExtractMask::END_OF_EPOCH) {
        if let Some(s) = summary.as_ref() {
            extract_epoch_committees(
                s, &checkpoint_seq, &checkpoint_ts,
                &mut out.epoch_committees,
            );
        }
    }

    out
}


// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build the checkpoint-level ObjectSet by BCS-decoding each `Object` from the
/// proto's `cp.objects.objects[].bcs.value`. The archive populates this
/// top-level field (per-tx `tx.objects` is left empty in archive playback).
fn build_object_set(cp: &ProtoCheckpoint) -> ObjectSet {
    let mut set = ObjectSet::default();
    if let Some(os) = cp.objects.as_ref() {
        for o in &os.objects {
            let Some(bcs_) = o.bcs.as_ref() else { continue };
            let Some(v) = bcs_.value.as_ref() else { continue };
            if let Ok(decoded) = bcs::from_bytes::<Object>(v) {
                set.insert(decoded);
            }
        }
    }
    // Also fold in any per-tx ObjectSet (gRPC live streams may put objects there).
    for tx in &cp.transactions {
        if let Some(os) = tx.objects.as_ref() {
            for o in &os.objects {
                let Some(bcs_) = o.bcs.as_ref() else { continue };
                let Some(v) = bcs_.value.as_ref() else { continue };
                if let Ok(decoded) = bcs::from_bytes::<Object>(v) {
                    set.insert(decoded);
                }
            }
        }
    }
    set
}

fn build_checkpoint_record(s: &CheckpointSummary, digest: String) -> CheckpointRecord {
    CheckpointRecord {
        sequence_number: s.sequence_number.to_string(),
        epoch: s.epoch.to_string(),
        timestamp_ms: s.timestamp_ms.to_string(),
        digest,
        previous_digest: s.previous_digest.map(|d| d.to_string()),
        content_digest: Some(s.content_digest.to_string()),
        total_network_transactions: s.network_total_transactions.to_string(),
        rolling_computation_cost: s.epoch_rolling_gas_cost_summary.computation_cost.to_string(),
        rolling_storage_cost: s.epoch_rolling_gas_cost_summary.storage_cost.to_string(),
        rolling_storage_rebate: s.epoch_rolling_gas_cost_summary.storage_rebate.to_string(),
        rolling_non_refundable_storage_fee: s
            .epoch_rolling_gas_cost_summary
            .non_refundable_storage_fee
            .to_string(),
    }
}

/// Decode a `tx.transaction.bcs.value` blob and surface BOTH the
/// `TransactionData` and any signatures attached to it. Try
/// `SenderSignedData` FIRST — when the producer wrapped the tx in the
/// signed envelope, that path also gives us the `tx_signatures`. Fall
/// back to bare `TransactionData` (no signatures) if SSD decode fails.
/// Some archive producers serialize bare TransactionData, in which case
/// signatures aren't recoverable from this field alone.
fn decode_tx_with_signatures(
    bytes: &[u8],
) -> Option<(TransactionData, Vec<sui_types::signature::GenericSignature>)> {
    // Try SSD first — when the producer wrapped the tx in the signed
    // envelope, this path also gives us the `tx_signatures`. Our
    // unconfirmed.cloud archive format drops the SSD wrapper at producer
    // time, so this branch fails for archive playback and we fall back
    // to bare TransactionData (no sigs). Live gRPC streams via Mysten's
    // FullCheckpointContents preserve SSD and yield real signatures.
    if let Ok(ssd) = bcs::from_bytes::<SenderSignedData>(bytes) {
        let inner = ssd.into_inner();
        let sigs = inner.tx_signatures.clone();
        return Some((inner.intent_message.value, sigs));
    }
    if let Ok(td) = bcs::from_bytes::<TransactionData>(bytes) {
        return Some((td, Vec::new()));
    }
    None
}

fn sequence_number_to_string(v: SequenceNumber) -> String {
    v.value().to_string()
}

fn transaction_kind_name(kind: &TransactionKind) -> &'static str {
    match kind {
        TransactionKind::ProgrammableTransaction(_) => "ProgrammableTransaction",
        TransactionKind::ProgrammableSystemTransaction(_) => "ProgrammableSystemTransaction",
        TransactionKind::ChangeEpoch(_) => "ChangeEpoch",
        TransactionKind::Genesis(_) => "Genesis",
        TransactionKind::ConsensusCommitPrologue(_) => "ConsensusCommitPrologue",
        TransactionKind::ConsensusCommitPrologueV2(_) => "ConsensusCommitPrologueV2",
        TransactionKind::ConsensusCommitPrologueV3(_) => "ConsensusCommitPrologueV3",
        TransactionKind::ConsensusCommitPrologueV4(_) => "ConsensusCommitPrologueV4",
        TransactionKind::AuthenticatorStateUpdate(_) => "AuthenticatorStateUpdate",
        TransactionKind::EndOfEpochTransaction(_) => "EndOfEpochTransaction",
        TransactionKind::RandomnessStateUpdate(_) => "RandomnessStateUpdate",
    }
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
        .map(|t| (
            t.gas_owner().to_string(),
            t.gas_price().to_string(),
            t.gas_budget().to_string(),
        ))
        .unwrap_or_else(|| (sender.to_string(), "0".into(), "0".into()));

    let gas_obj = effects.gas_object();
    let gas_object_id = if gas_obj.0 .0 == ObjectID::ZERO {
        None
    } else {
        Some(gas_obj.0 .0.to_string())
    };

    let move_call_count = tx_data.map(|t| count_move_calls(t.kind())).unwrap_or(0);
    let tx_kind = tx_data
        .map(|t| transaction_kind_name(t.kind()).to_string())
        .unwrap_or_default();

    Some(TransactionRecord {
        digest: digest.to_string(),
        sender: sender.to_string(),
        success,
        tx_kind,
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

/// Returns `Some((owner_kind, owner_id, coin_type, balance))` when `obj` is a
/// Coin<T> owned by either `AddressOwner` or `ConsensusAddressOwner` — the only
/// kinds that count toward `address.balance` per Sui's GraphQL/RPC semantics.
/// Returns `None` for non-coins, immutable, shared, or object-owned coins.
fn coin_info_for_address_balance(
    obj: &Object,
) -> Option<(&'static str, sui_types::base_types::SuiAddress, sui_types::TypeTag, u64)> {
    let (kind, owner) = match obj.owner() {
        Owner::AddressOwner(a) => ("AddressOwner", *a),
        Owner::ConsensusAddressOwner { owner, .. } => ("ConsensusAddressOwner", *owner),
        Owner::ObjectOwner(_) | Owner::Shared { .. } | Owner::Immutable => return None,
    };
    let (coin_type, balance) =
        sui_types::coin::Coin::extract_balance_if_coin(obj).ok().flatten()?;
    Some((kind, owner, coin_type, balance))
}

/// Per-coin-object balance state extraction. Mirrors `sui-indexer-alt`'s
/// `coin_balance_buckets` semantics: emit an `is_deleted=false` row when a
/// Coin<T> owned by Fastpath/Consensus is created, has its balance changed,
/// or has its owner changed (in the owner-change case we ALSO emit a delete
/// row for the prior owner). Emit an `is_deleted=true` row when a coin
/// transitions out of Fastpath/Consensus ownership (became Object/Shared/
/// Immutable), was deleted, or was wrapped — capturing the *prior* owner so
/// the running aggregate can subtract correctly.
fn extract_coin_balances(
    digest: &str,
    checkpoint_seq: &str,
    timestamp_ms: &str,
    effects: &TransactionEffects,
    object_set: &ObjectSet,
    out: &mut Vec<CoinBalanceRecord>,
) {
    // Inputs (state before tx) and outputs (state after tx) keyed by object_id.
    let mut inputs: HashMap<ObjectID, &Object> = HashMap::new();
    for (id, ver) in effects.modified_at_versions() {
        if let Some(obj) = object_set.get(&ObjectKey(id, ver)) {
            inputs.insert(id, obj);
        }
    }
    let mut outputs: HashMap<ObjectID, &Object> = HashMap::new();
    for (oref, _owner, _kind) in effects.all_changed_objects() {
        if let Some(obj) = object_set.get(&ObjectKey(oref.0, oref.1)) {
            outputs.insert(oref.0, obj);
        }
    }

    // Walk the union of touched object ids; only coins in Fastpath/Consensus
    // ownership produce records.
    let mut ids: HashSet<ObjectID> = HashSet::new();
    ids.extend(inputs.keys().copied());
    ids.extend(outputs.keys().copied());

    let push_row = |out: &mut Vec<CoinBalanceRecord>,
                    object_id: ObjectID,
                    kind: &str,
                    owner: sui_types::base_types::SuiAddress,
                    coin_type: &sui_types::TypeTag,
                    balance: u64,
                    is_deleted: bool| {
        out.push(CoinBalanceRecord {
            tx_digest: digest.to_string(),
            object_id: object_id.to_string(),
            owner_kind: kind.to_string(),
            owner_id: owner.to_string(),
            coin_type: coin_type.to_canonical_string(true),
            balance: balance.to_string(),
            is_deleted,
            checkpoint_seq: checkpoint_seq.to_string(),
            timestamp_ms: timestamp_ms.to_string(),
        });
    };

    for id in ids {
        let inp = inputs.get(&id).and_then(|o| coin_info_for_address_balance(o));
        let outp = outputs.get(&id).and_then(|o| coin_info_for_address_balance(o));

        match (inp, outp) {
            // Coin not Fastpath/Consensus on either side — irrelevant for top-holders.
            (None, None) => {}

            // Coin appeared (created/unwrapped/transitioned IN to Fastpath/Consensus).
            (None, Some((kind, owner, ct, bal))) => {
                push_row(out, id, kind, owner, &ct, bal, false);
            }

            // Coin transitioned OUT (deleted, wrapped, transferred to Object/Shared/Immutable).
            // Capture prior owner so running aggregate can subtract.
            (Some((kind, owner, ct, bal)), None) => {
                push_row(out, id, kind, owner, &ct, bal, true);
            }

            // Both sides Fastpath/Consensus — only emit on actual change.
            (Some((ik, io, ict, ib)), Some((ok, oo, oct, ob))) => {
                let owner_changed = ik != ok || io != oo;
                let balance_changed = ib != ob;
                let type_changed = ict != oct; // shouldn't happen, but be defensive

                if !owner_changed && !balance_changed && !type_changed {
                    continue;
                }
                if owner_changed {
                    // Subtract prior owner's balance, then add to new owner.
                    push_row(out, id, ik, io, &ict, ib, true);
                }
                push_row(out, id, ok, oo, &oct, ob, false);
            }
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

        // Lookup the post-change Object once so we can pull both `object_type`
        // and `previous_transaction` from it (the `previous_transaction` field
        // on the post-change Object is set to the digest of the tx that mutated
        // it — i.e. the current tx for created/mutated, or the previous owner
        // for unwrapped). Fall back to the input_version Object on
        // delete/wrap, where no output object is present.
        let post_obj = change
            .output_version
            .and_then(|v| object_set.get(&ObjectKey(change.id, v)))
            .or_else(|| change.input_version.and_then(|v| object_set.get(&ObjectKey(change.id, v))));

        let object_type = post_obj
            .and_then(|obj| obj.type_().map(|t| t.to_canonical_string(true)));

        let previous_transaction = post_obj
            .map(|obj| obj.previous_transaction.to_string())
            .unwrap_or_default();

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
            previous_transaction,
            checkpoint_seq: checkpoint_seq.to_string(),
            timestamp_ms: timestamp_ms.to_string(),
        });
    }
}

// ---------------------------------------------------------------------------
// Events / Packages
// ---------------------------------------------------------------------------

/// Walk a transaction's events. The archive populates `tx.events.bcs.value`
/// with the canonical BCS encoding of `TransactionEvents`. Live gRPC streams
/// may also populate the proto-level `events: Vec<Event>` field as a
/// pre-decoded fallback. We try BCS first (universal path), then fall back to
/// the proto fields if BCS decode fails or is absent.
fn extract_events(
    tx_digest: &str,
    checkpoint_seq: &str,
    timestamp_ms: &str,
    tx: &sui_rpc::proto::sui::rpc::v2::ExecutedTransaction,
    out: &mut Vec<EventRecord>,
) {
    // BCS path
    let bcs_events: Option<TransactionEvents> = tx
        .events
        .as_ref()
        .and_then(|e| e.bcs.as_ref())
        .and_then(|b| b.value.as_ref())
        .and_then(|v| bcs::from_bytes(v).ok());

    if let Some(evs) = bcs_events {
        for (i, ev) in evs.data.iter().enumerate() {
            out.push(EventRecord {
                tx_digest: tx_digest.to_string(),
                event_seq: i as u32,
                package_id: ev.package_id.to_string(),
                module: ev.transaction_module.to_string(),
                event_type: ev.type_.to_canonical_string(true),
                sender: ev.sender.to_string(),
                contents_hex: hex::encode(&ev.contents),
                checkpoint_seq: checkpoint_seq.to_string(),
                timestamp_ms: timestamp_ms.to_string(),
            });
        }
        return;
    }

    // Proto fallback (live gRPC streams sometimes populate this without BCS).
    if let Some(events_msg) = tx.events.as_ref() {
        for (i, ev) in events_msg.events.iter().enumerate() {
            let contents_hex = ev
                .contents
                .as_ref()
                .and_then(|b| b.value.as_ref())
                .map(|v| hex::encode(v))
                .unwrap_or_default();
            out.push(EventRecord {
                tx_digest: tx_digest.to_string(),
                event_seq: i as u32,
                package_id: ev.package_id.clone().unwrap_or_default(),
                module: ev.module.clone().unwrap_or_default(),
                event_type: ev.event_type.clone().unwrap_or_default(),
                sender: ev.sender.clone().unwrap_or_default(),
                contents_hex,
                checkpoint_seq: checkpoint_seq.to_string(),
                timestamp_ms: timestamp_ms.to_string(),
            });
        }
    }
}

/// Walk every Object in the checkpoint's `objects` list, BCS-decode each, and
/// emit `PackageRecord` + `PackageModuleRecord`s for those whose data is a
/// `MovePackage`. PACKAGE_WRITE in object_changes is the change-side view of
/// this — same packages, different framing.
fn extract_packages(
    cp: &ProtoCheckpoint,
    checkpoint_seq: &str,
    timestamp_ms: &str,
    out_packages: &mut Vec<PackageRecord>,
    out_modules: &mut Vec<PackageModuleRecord>,
) {
    // Iterate top-level cp.objects + per-tx tx.objects (live streams may use
    // either; archives populate top-level only).
    let mut visited: HashSet<(ObjectID, SequenceNumber)> = HashSet::new();
    let mut walk_one = |bcs_value: &[u8]| {
        let Ok(obj) = bcs::from_bytes::<Object>(bcs_value) else { return };
        let Data::Package(pkg) = &obj.data else { return };
        let id = pkg.id();
        let version = pkg.version();
        if !visited.insert((id, version)) {
            return;
        }
        let modules = pkg.serialized_module_map();
        out_packages.push(PackageRecord {
            package_id: id.to_string(),
            version: version.value().to_string(),
            original_id: pkg.original_package_id().to_string(),
            module_count: modules.len() as u32,
            checkpoint_seq: checkpoint_seq.to_string(),
            timestamp_ms: timestamp_ms.to_string(),
        });
        for (name, bytes) in modules {
            out_modules.push(PackageModuleRecord {
                package_id: id.to_string(),
                version: version.value().to_string(),
                module_name: name.clone(),
                bytecode_hex: hex::encode(bytes),
                checkpoint_seq: checkpoint_seq.to_string(),
                timestamp_ms: timestamp_ms.to_string(),
            });
        }
    };

    if let Some(os) = cp.objects.as_ref() {
        for o in &os.objects {
            let Some(bcs_) = o.bcs.as_ref() else { continue };
            let Some(v) = bcs_.value.as_ref() else { continue };
            walk_one(v);
        }
    }
    for tx in &cp.transactions {
        if let Some(os) = tx.objects.as_ref() {
            for o in &os.objects {
                let Some(bcs_) = o.bcs.as_ref() else { continue };
                let Some(v) = bcs_.value.as_ref() else { continue };
                walk_one(v);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Commands / Signatures / Epoch committees
// ---------------------------------------------------------------------------

/// Walk every command in a programmable transaction and emit one row per
/// command of any kind (MoveCall, TransferObjects, SplitCoins, MergeCoins,
/// Publish, MakeMoveVec, Upgrade). The MoveCall sub-fields (package/module/
/// function) are duplicated here so the table is queryable in isolation
/// without joining `move_calls`.
fn extract_commands(
    digest: &str,
    checkpoint_seq: &str,
    timestamp_ms: &str,
    kind: &TransactionKind,
    out: &mut Vec<CommandRecord>,
) {
    let pt = match kind {
        TransactionKind::ProgrammableTransaction(pt)
        | TransactionKind::ProgrammableSystemTransaction(pt) => pt,
        _ => return,
    };
    for (i, cmd) in pt.commands.iter().enumerate() {
        let command_index = i as u32;
        let (kind_str, package, module, function, type_arguments, args) = match cmd {
            Command::MoveCall(c) => {
                let type_args_json = if c.type_arguments.is_empty() {
                    None
                } else {
                    let strs: Vec<String> = c
                        .type_arguments
                        .iter()
                        .map(|t| t.to_canonical_string(true))
                        .collect();
                    Some(format!("[{}]", strs.iter().map(|s| format!("\"{s}\"")).collect::<Vec<_>>().join(",")))
                };
                (
                    "MoveCall",
                    Some(c.package.to_string()),
                    Some(c.module.to_string()),
                    Some(c.function.to_string()),
                    type_args_json,
                    Some(format!("arg_count={}", c.arguments.len())),
                )
            }
            Command::TransferObjects(objs, _recipient) => (
                "TransferObjects",
                None, None, None, None,
                Some(format!("object_count={}", objs.len())),
            ),
            Command::SplitCoins(_, amounts) => (
                "SplitCoins",
                None, None, None, None,
                Some(format!("split_count={}", amounts.len())),
            ),
            Command::MergeCoins(_, sources) => (
                "MergeCoins",
                None, None, None, None,
                Some(format!("merge_count={}", sources.len())),
            ),
            Command::Publish(modules, deps) => (
                "Publish",
                None, None, None, None,
                Some(format!("module_count={},dep_count={}", modules.len(), deps.len())),
            ),
            Command::MakeMoveVec(ty, items) => {
                // TypeInput is private to sui_types::transaction; the public
                // interface only exposes Debug. That's fine for our purposes —
                // analytics consumers can string-match the type name.
                let ty_str = ty.as_ref().map(|t| format!("[\"{t:?}\"]"));
                (
                    "MakeMoveVec",
                    None, None, None,
                    ty_str,
                    Some(format!("item_count={}", items.len())),
                )
            }
            Command::Upgrade(modules, deps, package_id, _ticket) => (
                "Upgrade",
                Some(package_id.to_string()),
                None, None, None,
                Some(format!("module_count={},dep_count={}", modules.len(), deps.len())),
            ),
        };
        out.push(CommandRecord {
            tx_digest: digest.to_string(),
            command_index,
            kind: kind_str.to_string(),
            package,
            module,
            function,
            type_arguments,
            args,
            checkpoint_seq: checkpoint_seq.to_string(),
            timestamp_ms: timestamp_ms.to_string(),
        });
    }
}

/// Emit one row per `UserSignature` on the transaction. The first byte of the
/// canonical (non-length-prefixed) signature bytes is the scheme flag:
///   0 ed25519, 1 secp256k1, 2 secp256r1, 3 multisig, 5 zklogin, 6 passkey.
/// We read the scheme from the proto's `scheme` field when populated, falling
/// back to the first byte of `bcs.value`.
fn extract_transaction_signatures(
    tx_digest: &str,
    checkpoint_seq: &str,
    timestamp_ms: &str,
    tx: &sui_rpc::proto::sui::rpc::v2::ExecutedTransaction,
    ssd_signatures: &[sui_types::signature::GenericSignature],
    out: &mut Vec<TransactionSignatureRecord>,
) {
    // Archive playback: signatures live inside the BCS-decoded
    // SenderSignedData (passed via `ssd_signatures`). Live gRPC streams
    // may instead populate the proto-level `tx.signatures` field with
    // typed UserSignature messages. Prefer SSD when present.
    if !ssd_signatures.is_empty() {
        for (i, sig) in ssd_signatures.iter().enumerate() {
            let bytes: &[u8] = sig.as_ref();
            let scheme = bytes.first().copied().unwrap_or(u8::MAX);
            out.push(TransactionSignatureRecord {
                tx_digest: tx_digest.to_string(),
                index: i as u32,
                scheme,
                signature_hex: hex::encode(bytes),
                checkpoint_seq: checkpoint_seq.to_string(),
                timestamp_ms: timestamp_ms.to_string(),
            });
        }
        return;
    }

    // Fallback: proto-level signatures (live streams).
    for (i, sig) in tx.signatures.iter().enumerate() {
        let bcs_bytes = sig
            .bcs
            .as_ref()
            .and_then(|b| b.value.as_ref());
        let signature_hex = bcs_bytes.map(hex::encode).unwrap_or_default();
        let scheme = sig
            .scheme
            .map(|s| s as u8)
            .or_else(|| bcs_bytes.and_then(|v| v.first().copied()))
            .unwrap_or(u8::MAX);
        out.push(TransactionSignatureRecord {
            tx_digest: tx_digest.to_string(),
            index: i as u32,
            scheme,
            signature_hex,
            checkpoint_seq: checkpoint_seq.to_string(),
            timestamp_ms: timestamp_ms.to_string(),
        });
    }
}

/// Emit one row per validator in the next-epoch committee. This list is only
/// populated on the last checkpoint of each epoch; for all other checkpoints
/// `next_epoch_committee` is `None` and we emit nothing.
fn extract_epoch_committees(
    summary: &CheckpointSummary,
    checkpoint_seq: &str,
    timestamp_ms: &str,
    out: &mut Vec<EpochCommitteeRecord>,
) {
    let Some(eod) = summary.end_of_epoch_data.as_ref() else { return };
    // The committee in `next_epoch_committee` governs the next epoch.
    let target_epoch = (summary.epoch + 1).to_string();
    for (pubkey, stake) in &eod.next_epoch_committee {
        out.push(EpochCommitteeRecord {
            target_epoch: target_epoch.clone(),
            validator_pubkey_hex: hex::encode(pubkey.as_ref()),
            stake: stake.to_string(),
            checkpoint_seq: checkpoint_seq.to_string(),
            timestamp_ms: timestamp_ms.to_string(),
        });
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    use sui_types::base_types::{SequenceNumber, SuiAddress};
    use sui_types::execution_status::ExecutionErrorKind;
    use sui_types::object::Owner;
    use sui_types::transaction::TransactionKind;

    // ---- flatten_owner --------------------------------------------------

    #[test]
    fn flatten_owner_none() {
        assert_eq!(flatten_owner(None), (None, None));
    }

    #[test]
    fn flatten_owner_address_owner() {
        let addr = SuiAddress::ZERO;
        let owner = Owner::AddressOwner(addr);
        let (a, k) = flatten_owner(Some(&owner));
        assert_eq!(a, Some(addr.to_string()));
        assert_eq!(k.as_deref(), Some("AddressOwner"));
    }

    #[test]
    fn flatten_owner_object_owner() {
        let addr = SuiAddress::ZERO;
        let owner = Owner::ObjectOwner(addr);
        let (a, k) = flatten_owner(Some(&owner));
        assert_eq!(a, Some(addr.to_string()));
        assert_eq!(k.as_deref(), Some("ObjectOwner"));
    }

    #[test]
    fn flatten_owner_shared() {
        let owner = Owner::Shared {
            initial_shared_version: SequenceNumber::from_u64(42),
        };
        let (a, k) = flatten_owner(Some(&owner));
        assert_eq!(a, None);
        assert_eq!(k.as_deref(), Some("Shared"));
    }

    #[test]
    fn flatten_owner_immutable() {
        let owner = Owner::Immutable;
        let (a, k) = flatten_owner(Some(&owner));
        assert_eq!(a, None);
        assert_eq!(k.as_deref(), Some("Immutable"));
    }

    #[test]
    fn flatten_owner_consensus_address_owner() {
        let addr = SuiAddress::ZERO;
        let owner = Owner::ConsensusAddressOwner {
            start_version: SequenceNumber::from_u64(7),
            owner: addr,
        };
        let (a, k) = flatten_owner(Some(&owner));
        assert_eq!(a, Some(addr.to_string()));
        assert_eq!(k.as_deref(), Some("ConsensusAddressOwner"));
    }

    // ---- Decoder::from_str ----------------------------------------------

    #[test]
    fn decoder_from_str_prost_ok() {
        assert_eq!(Decoder::from_str("prost").unwrap(), Decoder::Prost);
    }

    #[test]
    fn decoder_from_str_skip_errors() {
        let err = Decoder::from_str("skip").unwrap_err().to_string();
        assert!(
            err.contains("expected prost"),
            "expected error to mention 'expected prost', got: {err}"
        );
        assert!(err.contains("skip"));
    }

    #[test]
    fn decoder_from_str_parallel_errors() {
        let err = Decoder::from_str("parallel").unwrap_err().to_string();
        assert!(err.contains("expected prost"));
    }

    #[test]
    fn decoder_from_str_arbitrary_errors() {
        let err = Decoder::from_str("nonsense-backend").unwrap_err().to_string();
        assert!(err.contains("expected prost"));
    }

    // ---- execution_error_kind_name --------------------------------------

    #[test]
    fn error_kind_name_unit_variant() {
        // No payload → Debug repr is just the variant name.
        let n = execution_error_kind_name(&ExecutionErrorKind::InsufficientGas);
        assert_eq!(n, "InsufficientGas");
        assert!(!n.contains('('));
        assert!(!n.contains('{'));
        assert!(!n.chars().any(char::is_whitespace));
    }

    #[test]
    fn error_kind_name_struct_variant() {
        // Struct-style payload → "EffectsTooLarge { ... }". Splitting on '{'/whitespace
        // must yield just the variant name.
        let n = execution_error_kind_name(&ExecutionErrorKind::EffectsTooLarge {
            current_size: 100,
            max_size: 50,
        });
        assert_eq!(n, "EffectsTooLarge");
        assert!(!n.contains('{'));
        assert!(!n.chars().any(char::is_whitespace));
    }

    #[test]
    fn error_kind_name_another_unit_variant() {
        let n = execution_error_kind_name(&ExecutionErrorKind::CoinBalanceOverflow);
        assert_eq!(n, "CoinBalanceOverflow");
    }

    #[test]
    fn error_kind_name_invariant_violation() {
        let n = execution_error_kind_name(&ExecutionErrorKind::InvariantViolation);
        assert_eq!(n, "InvariantViolation");
    }

    // ---- decode_transaction_data ----------------------------------------

    #[test]
    fn decode_transaction_data_invalid_returns_none() {
        // Garbage bytes — neither a valid TransactionData nor SenderSignedData.
        assert!(decode_tx_with_signatures(&[0u8; 4]).is_none());
    }

    #[test]
    fn decode_transaction_data_empty_returns_none() {
        assert!(decode_tx_with_signatures(&[]).is_none());
    }

    #[test]
    fn decode_transaction_data_random_garbage_returns_none() {
        // Longer non-BCS payload — must still gracefully return None,
        // not panic.
        let junk: Vec<u8> = (0..256).map(|i| (i as u8).wrapping_mul(31)).collect();
        assert!(decode_tx_with_signatures(&junk).is_none());
    }

    // ---- count_move_calls ------------------------------------------------

    #[test]
    fn count_move_calls_non_programmable_is_zero() {
        // EndOfEpochTransaction with empty Vec is the simplest non-PT
        // variant to construct — no nested types required.
        let kind = TransactionKind::EndOfEpochTransaction(Vec::new());
        assert_eq!(count_move_calls(&kind), 0);
    }

    // NOTE: Positive-case count_move_calls coverage (a real
    // ProgrammableTransaction with N MoveCall commands) is intentionally
    // skipped. Constructing a valid ProgrammableTransaction requires
    // building Argument/Command/CallArg trees with multiple internal types
    // that don't have public test constructors — the realistic path is to
    // exercise it via `decode_checkpoint` against a fixture, which is out
    // of scope for pure unit tests.
    //
    // NOTE: Positive-case decode_transaction_data coverage is also skipped
    // (would require a real BCS-encoded TransactionData fixture).

    // ---- digest fallback (archive parity regression) -------------------
    //
    // Mysten's per-checkpoint archive omits the proto's `digest` field —
    // only the BCS-wrapped summary is present. The decoder must recompute
    // the digest from the summary so archive and live-gRPC paths produce
    // identical CheckpointRecord rows. Regression for the byte-equivalence
    // gap found via tests/grpc_vs_archive.rs.

    use sui_rpc::proto::sui::rpc::v2::{
        Bcs as ProtoBcs, CheckpointSummary as ProtoCheckpointSummary,
    };
    use sui_types::digests::CheckpointContentsDigest;
    use sui_types::gas::GasCostSummary;
    use sui_types::messages_checkpoint::CheckpointSummary as TypedSummary;

    fn synthetic_summary() -> TypedSummary {
        TypedSummary {
            epoch: 42,
            sequence_number: 12345,
            network_total_transactions: 99,
            content_digest: CheckpointContentsDigest::new([0u8; 32]),
            previous_digest: None,
            epoch_rolling_gas_cost_summary: GasCostSummary::new(1, 2, 3, 4),
            timestamp_ms: 1_700_000_000_000,
            checkpoint_commitments: Vec::new(),
            end_of_epoch_data: None,
            version_specific_data: Vec::new(),
        }
    }

    fn proto_with_summary_only(summary: &TypedSummary) -> ProtoCheckpoint {
        let summary_bcs = bcs::to_bytes(summary).expect("encode summary");
        let mut bcs_wrapper = ProtoBcs::default();
        bcs_wrapper.name = Some("CheckpointSummary".to_string());
        bcs_wrapper.value = Some(summary_bcs.into());

        let mut summary_msg = ProtoCheckpointSummary::default();
        summary_msg.bcs = Some(bcs_wrapper);

        let mut proto = ProtoCheckpoint::default();
        proto.sequence_number = Some(summary.sequence_number);
        proto.digest = None; // archive parity: field 2 empty.
        proto.summary = Some(summary_msg);
        proto
    }

    #[test]
    fn checkpoint_digest_recomputed_when_proto_field_empty() {
        let summary = synthetic_summary();
        let expected = summary.digest().to_string();

        let proto = proto_with_summary_only(&summary);
        let out = flatten_checkpoint(&proto, ExtractMask::CHECKPOINTS);

        assert!(
            !out.checkpoint.digest.is_empty(),
            "digest must not be empty when proto.digest is None — archive parity regression"
        );
        assert_eq!(out.checkpoint.digest, expected);
        // Sui digests are 32-byte BLAKE2b base58-encoded → non-trivial length.
        assert!(out.checkpoint.digest.len() >= 32);
    }

    #[test]
    fn checkpoint_digest_prefers_proto_field_when_present() {
        let summary = synthetic_summary();
        let mut proto = proto_with_summary_only(&summary);
        let supplied = "SuPpLiEdByGrPcStReAm".to_string();
        proto.digest = Some(supplied.clone());

        let out = flatten_checkpoint(&proto, ExtractMask::CHECKPOINTS);
        assert_eq!(out.checkpoint.digest, supplied);
    }

    #[test]
    fn checkpoint_digest_falls_back_when_proto_field_empty_string() {
        // gRPC producers may set `digest = Some("")`. Treat that as missing.
        let summary = synthetic_summary();
        let mut proto = proto_with_summary_only(&summary);
        proto.digest = Some(String::new());

        let out = flatten_checkpoint(&proto, ExtractMask::CHECKPOINTS);
        assert_eq!(out.checkpoint.digest, summary.digest().to_string());
    }
}
