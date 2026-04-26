# Checkpoint Data Model — Archive vs gRPC

Both the archive (checkpoints.mainnet.sui.io) and gRPC subscription serialize from the same Rust struct: `sui_types::full_checkpoint_content::Checkpoint`.

## Source of Truth

```rust
// crates/sui-types/src/full_checkpoint_content.rs
pub struct Checkpoint {
    pub summary: CertifiedCheckpointSummary,
    pub contents: CheckpointContents,
    pub transactions: Vec<ExecutedTransaction>,
    pub object_set: ObjectSet,  // BTreeMap<ObjectKey, Object>
}

pub struct ExecutedTransaction {
    pub transaction: TransactionData,
    pub signatures: Vec<GenericSignature>,
    pub effects: TransactionEffects,
    pub events: Option<TransactionEvents>,
    pub unchanged_loaded_runtime_objects: Vec<ObjectKey>,
}
```

Both paths convert this struct to `rpc::v2::Checkpoint` proto via the `Merge` trait (`crates/sui-types/src/rpc_proto_conversions.rs`), applying different field masks.

## Archive Format (`{seq}.binpb.zst`)

Written by `sui-checkpoint-blob-indexer` (`crates/sui-checkpoint-blob-indexer/src/handlers/checkpoint_blob.rs`).

Hardcoded field mask:
```
sequence_number
summary.bcs.value
signature
contents.bcs.value
transactions.transaction.bcs.value
transactions.effects.bcs.value
transactions.effects.unchanged_loaded_runtime_objects
transactions.events.bcs.value
objects.objects.bcs.value
```

Objects are ALWAYS included. Digest is NOT included (field 2 absent — must be computed from `BLAKE2b-256("CheckpointSummary::" + summary_bcs)`). Balance changes are NOT included.

## gRPC Subscription

Handler: `crates/sui-rpc-api/src/grpc/v2/subscription_service.rs`

Client provides `read_mask` in `SubscribeCheckpointsRequest`. Field mask controls what's returned. `balance_changes` are populated separately from an RPC index (not from the checkpoint struct).

## Proto Field Numbers (rpc::v2::Checkpoint)

```
1: sequence_number (uint64)
2: digest (string) — base58 of CheckpointSummary hash
3: summary (CheckpointSummary message, contains bcs.value)
4: signature (ValidatorAggregatedSignature)
5: contents (CheckpointContents, contains bcs.value)
6: transactions (repeated ExecutedTransaction)
7: objects (ObjectSet — repeated Object with bcs.value)
```

## Digest Computation

Archive protos don't include field 2 (digest). Computed as:
```
BLAKE2b-256("CheckpointSummary::" + summary_bcs_bytes)
```
Base58 encoded. The `"CheckpointSummary::"` prefix comes from the `BcsSignable` trait's `Signable::write` impl (`crates/sui-types/src/crypto.rs`), NOT from an intent prefix.

## Read Mask for Parity

To get identical data from gRPC subscription as from archive:
```
sequence_number
digest
summary.bcs.value
transactions.digest
transactions.transaction.bcs.value
transactions.effects.bcs.value
transactions.events.bcs.value
objects.objects.bcs.value
```

## Key Crate Locations

| Purpose | Path |
|---------|------|
| In-memory checkpoint struct | `crates/sui-types/src/full_checkpoint_content.rs` |
| Proto conversions (Merge trait) | `crates/sui-types/src/rpc_proto_conversions.rs` |
| Archive writer | `crates/sui-checkpoint-blob-indexer/src/handlers/checkpoint_blob.rs` |
| gRPC subscription handler | `crates/sui-rpc-api/src/grpc/v2/subscription_service.rs` |
| Subscription broadcaster | `crates/sui-rpc-api/src/subscription.rs` |
| Checkpoint executor | `crates/sui-core/src/checkpoints/checkpoint_executor/mod.rs` |
| Object loading for checkpoints | `crates/sui-core/src/checkpoints/checkpoint_executor/data_ingestion_handler.rs` |

## Data Flow

```
Validator executes checkpoint
    → CheckpointExecutor builds Checkpoint struct (with ObjectSet)
    → Sends to SubscriptionService channel
    
gRPC path:
    → SubscriptionService broadcasts to subscribers
    → Handler applies client read_mask via Merge trait
    → Serializes to proto, streams to client
    
Archive path:
    → sui-checkpoint-blob-indexer reads from consensus layer
    → Applies hardcoded field mask via Merge trait
    → Serializes to proto, zstd compresses
    → Uploads to object storage (S3/GCS)
```
