//! Checkpoint extraction — protobuf parsing + BCS decode + record extraction.

use sui_types::effects::{TransactionEffects, TransactionEffectsAPI, IDOperation};
use sui_types::transaction::{SenderSignedData, TransactionDataAPI, TransactionKind, Command};
use serde::Serialize;

use crate::proto;

// ---------------------------------------------------------------------------
// Output types (JSON-serializable, matching Jun's record shapes)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct ExtractedCheckpoint {
    pub checkpoint: CheckpointMeta,
    pub transactions: Vec<TransactionRecord>,
    #[serde(rename = "objectChanges")]
    pub object_changes: Vec<ObjectChangeRecord>,
    pub dependencies: Vec<DependencyRecord>,
    pub commands: Vec<CommandRecord>,
    #[serde(rename = "systemTransactions")]
    pub system_transactions: Vec<SystemTransactionRecord>,
}

#[derive(Serialize)]
pub struct CheckpointMeta {
    #[serde(rename = "sequenceNumber")]
    pub sequence_number: String,
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
    pub epoch: String,
    #[serde(rename = "dependencyCount")]
    pub dependency_count: usize,
    #[serde(rename = "moveCallCount")]
    pub move_call_count: usize,
    #[serde(rename = "errorKind", skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<String>,
}

#[derive(Serialize)]
pub struct ObjectChangeRecord {
    #[serde(rename = "txDigest")]
    pub tx_digest: String,
    #[serde(rename = "objectId")]
    pub object_id: String,
    #[serde(rename = "changeType")]
    pub change_type: String,
    #[serde(rename = "idOperation")]
    pub id_operation: String,
}

#[derive(Serialize)]
pub struct DependencyRecord {
    #[serde(rename = "txDigest")]
    pub tx_digest: String,
    #[serde(rename = "dependsOnDigest")]
    pub depends_on_digest: String,
}

#[derive(Serialize)]
pub struct CommandRecord {
    #[serde(rename = "txDigest")]
    pub tx_digest: String,
    #[serde(rename = "commandIndex")]
    pub command_index: usize,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub package: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub module: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
}

#[derive(Serialize)]
pub struct SystemTransactionRecord {
    #[serde(rename = "txDigest")]
    pub tx_digest: String,
    pub kind: String,
}

// ---------------------------------------------------------------------------
// Extraction
// ---------------------------------------------------------------------------

pub fn extract_checkpoint(decompressed: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    let parsed = proto::parse_checkpoint(decompressed)
        .map_err(|e| format!("proto parse: {}", e))?;

    let mut transactions = Vec::new();
    let mut object_changes = Vec::new();
    let mut dependencies = Vec::new();
    let mut commands = Vec::new();
    let mut system_transactions = Vec::new();

    for ptx in &parsed.transactions {
        let digest = ptx.digest.to_string();

        // --- Effects ---
        if let Some(effects_bcs) = ptx.effects_bcs {
            if let Ok(effects) = bcs::from_bytes::<TransactionEffects>(effects_bcs) {
                let (tx_rec, obj_changes, deps) = extract_effects(&digest, &effects);
                transactions.push(tx_rec);
                object_changes.extend(obj_changes);
                dependencies.extend(deps);
            }
        }

        // --- TransactionData ---
        if let Some(tx_bcs) = ptx.transaction_bcs {
            if let Ok(signed_data) = bcs::from_bytes::<SenderSignedData>(tx_bcs) {
                {
                    let tx_data = signed_data.transaction_data();
                    let kind = tx_data.kind();
                    // Extract commands
                    let cmds = extract_commands(&digest, kind);
                    commands.extend(cmds);
                    // Detect system transactions
                    if let Some(sys) = extract_system_tx(&digest, kind) {
                        system_transactions.push(sys);
                    }
                }
            }
        }
    }

    let result = ExtractedCheckpoint {
        checkpoint: CheckpointMeta {
            sequence_number: "0".to_string(), // TODO: from summary BCS
        },
        transactions,
        object_changes,
        dependencies,
        commands,
        system_transactions,
    };

    Ok(serde_json::to_string(&result)?)
}

fn extract_effects(
    digest: &str,
    effects: &TransactionEffects,
) -> (TransactionRecord, Vec<ObjectChangeRecord>, Vec<DependencyRecord>) {
    let mut obj_changes = Vec::new();
    let mut deps = Vec::new();

    // Use TransactionEffectsAPI trait methods (work on all versions)
    {
            let success = effects.status().is_ok();
            let error_kind = if !success {
                Some(format!("{:?}", effects.status()))
            } else {
                None
            };

            let gas = effects.gas_cost_summary();
            let tx_rec = TransactionRecord {
                digest: digest.to_string(),
                sender: String::new(), // sender is on TransactionData, not effects
                success,
                computation_cost: gas.computation_cost.to_string(),
                storage_cost: gas.storage_cost.to_string(),
                storage_rebate: gas.storage_rebate.to_string(),
                epoch: effects.executed_epoch().to_string(),
                dependency_count: effects.dependencies().len(),
                move_call_count: 0,
                error_kind,
            };

            // Changed objects — trait API returns Vec<ObjectChange>
            for change in effects.object_changes() {
                let change_type = format!("{:?}", change);
                obj_changes.push(ObjectChangeRecord {
                    tx_digest: digest.to_string(),
                    object_id: format!("{:?}", change),
                    change_type: "MUTATED".to_string(), // TODO: proper derivation from ObjectChange
                    id_operation: "None".to_string(),
                });
            }

            // Dependencies
            for dep in effects.dependencies() {
                deps.push(DependencyRecord {
                    tx_digest: digest.to_string(),
                    depends_on_digest: format!("{}", dep),
                });
            }

            (tx_rec, obj_changes, deps)
    }
}

fn extract_commands(digest: &str, kind: &TransactionKind) -> Vec<CommandRecord> {
    let mut records = Vec::new();
    if let TransactionKind::ProgrammableTransaction(ptb) = kind {
        for (i, cmd) in ptb.commands.iter().enumerate() {
            let (cmd_kind, package, module, function) = match cmd {
                Command::MoveCall(mc) => (
                    "MoveCall",
                    Some(mc.package.to_string()),
                    Some(mc.module.to_string()),
                    Some(mc.function.to_string()),
                ),
                Command::TransferObjects(_, _) => ("TransferObjects", None, None, None),
                Command::SplitCoins(_, _) => ("SplitCoins", None, None, None),
                Command::MergeCoins(_, _) => ("MergeCoins", None, None, None),
                Command::Publish(_, _) => ("Publish", None, None, None),
                Command::Upgrade(_, _, _, _) => ("Upgrade", None, None, None),
                Command::MakeMoveVec(_, _) => ("MakeMoveVector", None, None, None),
                _ => ("Unknown", None, None, None),
            };
            records.push(CommandRecord {
                tx_digest: digest.to_string(),
                command_index: i,
                kind: cmd_kind.to_string(),
                package,
                module,
                function,
            });
        }
    }
    records
}

fn extract_system_tx(digest: &str, kind: &TransactionKind) -> Option<SystemTransactionRecord> {
    let sys_kind = match kind {
        TransactionKind::Genesis(_) => "GENESIS",
        TransactionKind::ChangeEpoch(_) => "CHANGE_EPOCH",
        TransactionKind::ConsensusCommitPrologue(_) => "CONSENSUS_COMMIT_PROLOGUE_V1",
        TransactionKind::ConsensusCommitPrologueV2(_) => "CONSENSUS_COMMIT_PROLOGUE_V2",
        TransactionKind::ConsensusCommitPrologueV3(_) => "CONSENSUS_COMMIT_PROLOGUE_V3",
        TransactionKind::AuthenticatorStateUpdate(_) => "AUTHENTICATOR_STATE_UPDATE",
        TransactionKind::EndOfEpochTransaction(_) => "END_OF_EPOCH",
        TransactionKind::RandomnessStateUpdate(_) => "RANDOMNESS_STATE_UPDATE",
        TransactionKind::ProgrammableTransaction(_) => return None,
        _ => "UNKNOWN",
    };
    Some(SystemTransactionRecord {
        tx_digest: digest.to_string(),
        kind: sys_kind.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_empty_proto() {
        // Empty proto should produce no transactions
        let result = extract_checkpoint(&[]).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["transactions"].as_array().unwrap().len(), 0);
    }
}
