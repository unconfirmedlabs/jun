//! jun-types — shared record structs and the extraction mask.
//!
//! Every record type emitted by the decoder lives here so downstream crates
//! (jun-clickhouse, jun-sqlite, jun-pipeline) can operate on the same shapes.
//! Field names are snake_case; writers map to their own naming conventions.

use serde::Serialize;

// ---------------------------------------------------------------------------
// Extraction mask
//
// Bit positions must stay stable across jun-checkpoint-decoder + any external consumer.
// When adding a new record type, append a new bit at the next position and
// update `ALL`. Never reuse bits.
// ---------------------------------------------------------------------------

bitflags::bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct ExtractMask: u32 {
        const TRANSACTIONS           = 1 << 0;
        const MOVE_CALLS             = 1 << 1;
        const BALANCE_CHANGES        = 1 << 2;
        const OBJECT_CHANGES         = 1 << 3;
        const DEPENDENCIES           = 1 << 4;
        const INPUTS                 = 1 << 5;
        const COMMANDS               = 1 << 6;
        const SYSTEM_TRANSACTIONS    = 1 << 7;
        const UNCHANGED_CONSENSUS    = 1 << 8;
        const CHECKPOINTS            = 1 << 9;
        const EVENTS                 = 1 << 10;
        const CHECKPOINT_CONTENTS    = 1 << 11;
        const CHECKPOINT_SIGNATURE   = 1 << 12;
        const TRANSACTION_SIGNATURES = 1 << 13;
        const OBJECTS                = 1 << 14;
        const PACKAGES               = 1 << 15;
        const PACKAGE_LINKAGE        = 1 << 16;
        const TYPE_ORIGINS           = 1 << 17;
        const END_OF_EPOCH           = 1 << 18;
        const CHECKPOINT_COMMITMENTS = 1 << 19;
        const ACCUMULATOR_WRITES     = 1 << 20;
        const SYSTEM_TX_DECOMPOSED   = 1 << 21;
        const COIN_BALANCES          = 1 << 22;

        const ALL = 0x7F_FFFF;
    }
}

// ---------------------------------------------------------------------------
// Aggregate output of a single checkpoint extraction.
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Serialize)]
pub struct ExtractedCheckpoint {
    pub checkpoint: CheckpointRecord,
    pub transactions: Vec<TransactionRecord>,
    pub move_calls: Vec<MoveCallRecord>,
    pub balance_changes: Vec<BalanceChangeRecord>,
    pub coin_balances: Vec<CoinBalanceRecord>,
    pub object_changes: Vec<ObjectChangeRecord>,
    pub dependencies: Vec<DependencyRecord>,
    pub inputs: Vec<InputRecord>,
    pub commands: Vec<CommandRecord>,
    pub system_transactions: Vec<SystemTransactionRecord>,
    pub unchanged_consensus_objects: Vec<UnchangedConsensusObjectRecord>,
    pub events: Vec<EventRecord>,
    pub checkpoint_contents: Vec<CheckpointContentsRecord>,
    pub checkpoint_signature: Option<CheckpointSignatureRecord>,
    pub transaction_signatures: Vec<TransactionSignatureRecord>,
    pub objects: Vec<ObjectRecord>,
    pub packages: Vec<PackageRecord>,
    pub package_modules: Vec<PackageModuleRecord>,
    pub package_linkage: Vec<PackageLinkageRecord>,
    pub type_origins: Vec<TypeOriginRecord>,
    pub epoch_committees: Vec<EpochCommitteeRecord>,
    pub epoch_protocol_versions: Vec<EpochProtocolVersionRecord>,
    pub epoch_commitments: Vec<EpochCommitmentRecord>,
    pub checkpoint_commitments: Vec<CheckpointCommitmentRecord>,
    pub accumulator_writes: Vec<AccumulatorWriteRecord>,
    pub congested_objects: Vec<CongestedObjectRecord>,
    pub epoch_changes: Vec<EpochChangeRecord>,
    pub consensus_commits: Vec<ConsensusCommitRecord>,
    pub authenticator_updates: Vec<AuthenticatorUpdateRecord>,
    pub active_jwks: Vec<ActiveJwkRecord>,
    pub randomness_updates: Vec<RandomnessUpdateRecord>,
    pub end_of_epoch_operations: Vec<EndOfEpochOperationRecord>,
}

// ---------------------------------------------------------------------------
// Record structs
//
// Numeric fields that come from Sui as u64/u128 are stored as String so that
// writers can faithfully produce the full range. Integer writers (ClickHouse
// UInt64) parse back from String; JSON writers pass through as strings.
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Serialize)]
pub struct CheckpointRecord {
    pub sequence_number: String,
    pub epoch: String,
    pub timestamp_ms: String,
    pub digest: String,
    pub previous_digest: Option<String>,
    pub content_digest: Option<String>,
    pub total_network_transactions: String,
    pub rolling_computation_cost: String,
    pub rolling_storage_cost: String,
    pub rolling_storage_rebate: String,
    pub rolling_non_refundable_storage_fee: String,
}

#[derive(Debug, Serialize)]
pub struct TransactionRecord {
    pub digest: String,
    pub sender: String,
    pub success: bool,
    pub tx_kind: String,
    pub computation_cost: String,
    pub storage_cost: String,
    pub storage_rebate: String,
    pub non_refundable_storage_fee: String,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
    pub move_call_count: u32,
    pub epoch: String,
    pub error_kind: Option<String>,
    pub error_description: Option<String>,
    pub error_command_index: Option<u32>,
    pub error_abort_code: Option<String>,
    pub error_module: Option<String>,
    pub error_function: Option<String>,
    pub error_arg_idx: Option<u32>,
    pub error_type_arg_idx: Option<u32>,
    pub error_sub_kind: Option<String>,
    pub error_current_size: Option<String>,
    pub error_max_size: Option<String>,
    pub error_coin_type: Option<String>,
    pub error_denied_address: Option<String>,
    pub error_object_id: Option<String>,
    pub events_digest: Option<String>,
    pub lamport_version: Option<String>,
    pub dependency_count: u32,
    pub gas_owner: String,
    pub gas_price: String,
    pub gas_budget: String,
    pub gas_object_id: Option<String>,
    pub expiration_kind: String,
    pub expiration_epoch: Option<String>,
    pub expiration_min_epoch: Option<String>,
    pub expiration_max_epoch: Option<String>,
    pub expiration_min_ts_seconds: Option<String>,
    pub expiration_max_ts_seconds: Option<String>,
    pub expiration_chain: Option<String>,
    pub expiration_nonce: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct MoveCallRecord {
    pub tx_digest: String,
    pub call_index: u32,
    pub package: String,
    pub module: String,
    pub function: String,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct BalanceChangeRecord {
    pub tx_digest: String,
    pub checkpoint_seq: String,
    pub address: String,
    pub coin_type: String,
    pub amount: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct ObjectChangeRecord {
    pub tx_digest: String,
    pub object_id: String,
    pub change_type: String,
    pub object_type: Option<String>,
    pub input_version: Option<String>,
    pub input_digest: Option<String>,
    pub input_owner: Option<String>,
    pub input_owner_kind: Option<String>,
    pub output_version: Option<String>,
    pub output_digest: Option<String>,
    pub output_owner: Option<String>,
    pub output_owner_kind: Option<String>,
    pub is_gas_object: bool,
    pub previous_transaction: String,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

/// Per-coin-object balance state record. Mirrors Sui indexer-alt's
/// `coin_balance_buckets` semantics: emitted only when a Coin<T> object's
/// (owner, owner_kind, balance) tuple changes, OR when it transitions out of
/// Fastpath/Consensus ownership (in which case `is_deleted = true` and the
/// owner fields capture the *prior* state so the row can subtract from running
/// aggregates).
///
/// Owner kinds we record: `AddressOwner` (fast-path) and `ConsensusAddressOwner`.
/// Coins with `ObjectOwner`/`Shared`/`Immutable` never produce records here —
/// those don't count toward `address.balance` per the chain's semantics.
#[derive(Debug, Serialize)]
pub struct CoinBalanceRecord {
    pub tx_digest: String,
    pub object_id: String,
    /// `AddressOwner` | `ConsensusAddressOwner` (when `is_deleted = false`),
    /// or the prior owner_kind (when `is_deleted = true`).
    pub owner_kind: String,
    /// Hex address of the owner — wallet for AddressOwner, consensus owner
    /// SuiAddress for ConsensusAddressOwner.
    pub owner_id: String,
    /// Canonical Coin marker type, e.g. `0x2::sui::SUI`.
    pub coin_type: String,
    /// Raw balance after this change (or prior balance when `is_deleted = true`).
    pub balance: String,
    pub is_deleted: bool,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct DependencyRecord {
    pub tx_digest: String,
    pub depends_on_digest: String,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct InputRecord {
    pub tx_digest: String,
    pub input_index: u32,
    pub kind: String,
    pub object_id: Option<String>,
    pub version: Option<String>,
    pub digest: Option<String>,
    pub mutability: Option<String>,
    pub initial_shared_version: Option<String>,
    pub pure_bytes: Option<String>,
    pub amount: Option<String>,
    pub coin_type: Option<String>,
    pub source: Option<String>,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct CommandRecord {
    pub tx_digest: String,
    pub command_index: u32,
    pub kind: String,
    pub package: Option<String>,
    pub module: Option<String>,
    pub function: Option<String>,
    pub type_arguments: Option<String>,
    pub args: Option<String>,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct SystemTransactionRecord {
    pub tx_digest: String,
    pub kind: String,
    pub data_json: String,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct UnchangedConsensusObjectRecord {
    pub tx_digest: String,
    pub object_id: String,
    pub kind: String,
    pub version: Option<String>,
    pub digest: Option<String>,
    pub object_type: Option<String>,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct EventRecord {
    pub tx_digest: String,
    pub event_seq: u32,
    pub package_id: String,
    pub module: String,
    pub event_type: String,
    pub sender: String,
    pub contents_hex: String,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

// --- Checkpoint-scope extras (new in this rewrite) -------------------------

#[derive(Debug, Serialize)]
pub struct CheckpointContentsRecord {
    pub checkpoint_seq: String,
    pub position: u32,
    pub tx_digest: String,
    pub effects_digest: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct CheckpointSignatureRecord {
    pub checkpoint_seq: String,
    pub epoch: String,
    /// Hex-encoded 48-byte BLS12-381 aggregate signature.
    pub signature_hex: String,
    /// Hex-encoded committee bitmap of contributing members.
    pub bitmap_hex: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct TransactionSignatureRecord {
    pub tx_digest: String,
    pub index: u32,
    /// Scheme flag byte from the canonical bytes.
    /// 0 ed25519, 1 secp256k1, 2 secp256r1, 3 multisig, 5 zklogin, 6 passkey.
    pub scheme: u8,
    pub signature_hex: String,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct ObjectRecord {
    pub object_id: String,
    pub version: String,
    pub digest: String,
    pub owner: Option<String>,
    pub owner_kind: String,
    pub object_type: Option<String>,
    pub has_public_transfer: bool,
    pub is_package: bool,
    pub storage_rebate: String,
    pub previous_transaction: String,
    /// Raw Move struct BCS (hex). Empty for packages.
    pub contents_bcs_hex: String,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct PackageRecord {
    pub package_id: String,
    pub version: String,
    pub original_id: String,
    pub module_count: u32,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct PackageModuleRecord {
    pub package_id: String,
    pub version: String,
    pub module_name: String,
    pub bytecode_hex: String,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct PackageLinkageRecord {
    pub package_id: String,
    pub version: String,
    pub dep_original_id: String,
    pub upgraded_id: String,
    pub upgraded_version: String,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct TypeOriginRecord {
    pub package_id: String,
    pub version: String,
    pub module_name: String,
    pub datatype_name: String,
    pub defining_package_id: String,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

// --- End-of-epoch ----------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct EpochCommitteeRecord {
    /// The epoch this committee will govern (= summary.epoch + 1).
    pub target_epoch: String,
    pub validator_pubkey_hex: String,
    pub stake: String,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct EpochProtocolVersionRecord {
    pub target_epoch: String,
    pub protocol_version: String,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct EpochCommitmentRecord {
    pub target_epoch: String,
    pub kind: String,
    pub digest: String,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct CheckpointCommitmentRecord {
    pub checkpoint_seq: String,
    pub kind: String,
    pub digest: String,
    pub timestamp_ms: String,
}

// --- Accumulator (effects v2) ---------------------------------------------

#[derive(Debug, Serialize)]
pub struct AccumulatorWriteRecord {
    pub tx_digest: String,
    pub object_id: String,
    pub address: String,
    pub accumulator_type: String,
    /// MERGE | SPLIT
    pub operation: String,
    /// INTEGER | INTEGER_TUPLE | EVENT_DIGEST
    pub value_kind: String,
    pub value_json: String,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

/// Sibling rows for the `ExecutionCancelledDueToSharedObjectCongestion` error.
#[derive(Debug, Serialize)]
pub struct CongestedObjectRecord {
    pub tx_digest: String,
    pub object_id: String,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

// --- Decomposed system transactions ----------------------------------------

#[derive(Debug, Serialize)]
pub struct EpochChangeRecord {
    pub tx_digest: String,
    pub epoch: String,
    pub protocol_version: String,
    pub storage_charge: String,
    pub computation_charge: String,
    pub storage_rebate: String,
    pub non_refundable_storage_fee: String,
    pub epoch_start_ts_ms: String,
    pub system_packages_count: u32,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct ConsensusCommitRecord {
    pub tx_digest: String,
    /// 1 | 2 | 3 | 4
    pub version: u8,
    pub epoch: String,
    pub round: String,
    pub commit_ts_ms: String,
    pub consensus_commit_digest: Option<String>,
    pub sub_dag_index: Option<String>,
    pub additional_state_digest: Option<String>,
    pub version_assignments_json: Option<String>,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct AuthenticatorUpdateRecord {
    pub tx_digest: String,
    pub epoch: String,
    pub round: String,
    pub authenticator_obj_initial_shared_version: String,
    pub jwk_count: u32,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct ActiveJwkRecord {
    pub tx_digest: String,
    pub iss: String,
    pub kid: String,
    pub kty: String,
    pub e: String,
    pub n: String,
    pub alg: String,
    pub epoch: String,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct RandomnessUpdateRecord {
    pub tx_digest: String,
    pub epoch: String,
    pub randomness_round: String,
    pub random_bytes_hex: String,
    pub randomness_obj_initial_shared_version: String,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

#[derive(Debug, Serialize)]
pub struct EndOfEpochOperationRecord {
    pub tx_digest: String,
    pub idx: u32,
    pub kind: String,
    pub data_json: String,
    pub checkpoint_seq: String,
    pub timestamp_ms: String,
}

// ---------------------------------------------------------------------------
// Compile-time assertions. If these fail the mask/record list got out of sync.
// ---------------------------------------------------------------------------

const _: () = {
    // Every mask bit should correspond to at least one record vec. Not enforced
    // at compile time (the mask is a bitflag), but documented here as a
    // contract: adding a bit requires adding a corresponding field to
    // `ExtractedCheckpoint` (or justifying why not, e.g. OBJECTS enables
    // ObjectRecord but also PackageRecord via PACKAGES).
};
