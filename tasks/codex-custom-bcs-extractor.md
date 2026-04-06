# Codex Task: Custom BCS Field Extractor for jun Rust Decoder

## Goal

Create a custom BCS reader in `native/rust-decoder/src/bcs_reader.rs` that extracts fields from `TransactionEffectsV2` and `TransactionDataV1` BCS bytes and writes them directly to a `BinaryWriter` buffer — no intermediate struct allocation, no String heap allocation.

This replaces `bcs::from_bytes::<TransactionEffects>()` + `.to_string()` on every field, which currently takes 257µs per checkpoint (54% of decode time). Target: <80µs.

## Context

- BCS (Binary Canonical Serialization) is positional — fields are serialized in struct declaration order
- Enum variants are ULEB128-tagged
- Vec is ULEB128 length prefix + N elements
- Option is 0x00 (None) or 0x01 + value
- Strings are ULEB128 length + UTF-8 bytes
- Fixed-size types: u8=1byte, u16=2LE, u32=4LE, u64=8LE, u128=16LE, bool=1byte
- ObjectID = [u8; 32], SuiAddress = [u8; 32], TransactionDigest = [u8; 32], ObjectDigest = [u8; 32]
- SequenceNumber = u64

## BCS Struct Layouts (field order is critical)

### TransactionEffects (enum)
```
ULEB128 variant:
  0 → TransactionEffectsV1 (skip — legacy, read with full deserialize fallback)
  1 → TransactionEffectsV2
```

### TransactionEffectsV2
```
1. status: ExecutionStatus (enum)
2. executed_epoch: u64
3. gas_used: GasCostSummary
4. transaction_digest: [u8; 32] (TransactionDigest)
5. gas_object_index: Option<u32>
6. events_digest: Option<[u8; 32]> (TransactionEventsDigest)
7. dependencies: Vec<[u8; 32]> (Vec<TransactionDigest>)
8. lamport_version: u64 (SequenceNumber)
9. changed_objects: Vec<(ObjectID, EffectsObjectChange)>
10. unchanged_consensus_objects: Vec<(ObjectID, UnchangedConsensusKind)>
11. aux_data_digest: Option<[u8; 32]>
```

### ExecutionStatus (enum)
```
ULEB128 variant:
  0 → Success (no fields)
  1 → Failure { error: ExecutionFailureStatus, command: Option<u64> }
```

### ExecutionFailureStatus (enum)
Large enum with many variants. For the extractor, we only need:
- Variant tag → map to kind name string
- If variant is MoveAbort (variant 0): read (MoveLocation, u64 abort_code)
- MoveLocation = { module: ModuleId, function: u16, instruction: u16, function_name: Option<String> }
- ModuleId = { address: [u8; 32], name: String }

For all other variants, just read the variant tag and map to a name. Skip the variant data.

### GasCostSummary
```
1. computation_cost: u64
2. storage_cost: u64
3. storage_rebate: u64
4. non_refundable_storage_fee: u64
```

### EffectsObjectChange
```
1. input_state: ObjectIn (enum)
2. output_state: ObjectOut (enum)
3. id_operation: IDOperation (enum)
```

### ObjectIn (enum)
```
0 → NotExist
1 → Exist((VersionDigest, Owner))
    VersionDigest = (u64 version, [u8; 32] digest)
    Owner = enum { AddressOwner([u8;32]), ObjectOwner([u8;32]), Shared{initial_shared_version: u64}, Immutable, ConsensusAddressOwner{epoch: u64, owner: [u8;32]} }
```

### ObjectOut (enum)
```
0 → NotExist
1 → ObjectWrite(([u8;32] digest, Owner))
2 → PackageWrite((u64 version, [u8;32] digest))
3 → AccumulatorWriteV1(AccumulatorWriteV1)
```

### IDOperation (enum)
```
0 → None
1 → Created
2 → Deleted
```

### UnchangedConsensusKind (enum)
```
0 → ReadOnlyRoot((u64 version, [u8;32] digest))
1 → MutateConsensusStreamEnded(u64 version)
2 → ReadConsensusStreamEnded(u64 version)
3 → Cancelled(u64 version)
4 → PerEpochConfig (no fields)
```

### TransactionData (enum)
```
0 → TransactionDataV1
```

### TransactionDataV1
```
1. kind: TransactionKind (enum)
2. sender: [u8; 32] (SuiAddress)
3. gas_data: GasData
4. expiration: TransactionExpiration (enum)
```

### TransactionKind (enum) — variants
```
0 → ProgrammableTransaction(ProgrammableTransaction)
1 → ChangeEpoch(...)
2 → Genesis(...)
3 → ConsensusCommitPrologue(...)
4 → AuthenticatorStateUpdate(...)
5 → EndOfEpochTransaction(...)
6 → RandomnessStateUpdate(...)
7 → ConsensusCommitPrologueV2(...)
8 → ConsensusCommitPrologueV3(...)
9 → ProgrammableSystemTransaction(ProgrammableTransaction)
10 → ConsensusCommitPrologueV4(...)
```

### ProgrammableTransaction
```
1. inputs: Vec<CallArg>
2. commands: Vec<Command>
```

### CallArg (enum)
```
0 → Pure(Vec<u8>)
1 → Object(ObjectArg)
2 → FundsWithdrawal(FundsWithdrawalArg)
```

### ObjectArg (enum)
```
0 → ImmOrOwnedObject((ObjectID, SequenceNumber, ObjectDigest))
1 → SharedObject { id: ObjectID, initial_shared_version: u64, mutability: SharedObjectMutability }
2 → Receiving((ObjectID, SequenceNumber, ObjectDigest))
```

### Command (enum)
```
0 → MoveCall(MoveCall)
1 → TransferObjects(Vec<Argument>, Argument)
2 → SplitCoins(Argument, Vec<Argument>)
3 → MergeCoins(Argument, Vec<Argument>)
4 → Publish(Vec<Vec<u8>>, Vec<ObjectID>)
5 → MakeMoveVec(Option<TypeTag>, Vec<Argument>)
6 → Upgrade(Vec<Vec<u8>>, Vec<ObjectID>, ObjectID, Argument)
```

### MoveCall
```
1. package: ObjectID
2. module: String (Identifier)
3. function: String (Identifier)
4. type_arguments: Vec<TypeTag>
5. arguments: Vec<Argument>
```

### Argument (enum)
```
0 → GasCoin
1 → Input(u16)
2 → Result(u16)
3 → NestedResult(u16, u16)
```

## What to Build

### File: `native/rust-decoder/src/bcs_reader.rs`

Create a `BcsReader` struct:

```rust
pub struct BcsReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> BcsReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self;
    pub fn position(&self) -> usize;
    pub fn remaining(&self) -> usize;
    
    // Primitives
    pub fn read_u8(&mut self) -> Result<u8>;
    pub fn read_u16(&mut self) -> Result<u16>;
    pub fn read_u32(&mut self) -> Result<u32>;
    pub fn read_u64(&mut self) -> Result<u64>;
    pub fn read_u128(&mut self) -> Result<u128>;
    pub fn read_bool(&mut self) -> Result<bool>;
    pub fn read_uleb128(&mut self) -> Result<u64>;
    pub fn read_bytes(&mut self, n: usize) -> Result<&'a [u8]>;
    pub fn read_fixed<const N: usize>(&mut self) -> Result<[u8; N]>;
    
    // BCS compound types
    pub fn read_vec_len(&mut self) -> Result<usize>;
    pub fn read_option_tag(&mut self) -> Result<bool>; // true = Some
    pub fn read_string(&mut self) -> Result<&'a str>;
    
    // Skip helpers (advance past a field without reading)
    pub fn skip_bytes(&mut self, n: usize) -> Result<()>;
    pub fn skip_vec_of_fixed(&mut self, elem_size: usize) -> Result<()>;
    pub fn skip_string(&mut self) -> Result<()>;
    pub fn skip_option_fixed(&mut self, size: usize) -> Result<()>;
}
```

Then create extraction functions that take a `BcsReader` and `BinaryWriter`:

```rust
/// Extract TransactionEffects fields and write directly to binary output.
/// Returns metadata needed by the caller (digest, dependencies count, etc.)
pub struct EffectsExtract {
    pub digest: [u8; 32],
    pub success: bool,
    pub epoch: u64,
    pub lamport_version: u64,
    pub dependency_count: usize,
    pub gas_object_index: Option<u32>,
}

pub fn extract_effects(bcs: &[u8], w: &mut BinaryWriter) -> Result<EffectsExtract>;

/// Extract TransactionData fields. Returns sender and move call / command info.
pub fn extract_tx_data(bcs: &[u8], w: &mut BinaryWriter) -> Result<TxDataExtract>;
```

### Integration point

In `direct.rs`, replace:
```rust
let effects = ptx.effects_bcs.and_then(|bytes| bcs::from_bytes::<TransactionEffects>(bytes).ok());
```
with:
```rust
let effects_extract = ptx.effects_bcs.and_then(|bytes| bcs_reader::extract_effects(bytes, &mut w).ok());
```

## Key Design Decisions

1. **V1 fallback**: If effects variant is 0 (V1), fall back to `bcs::from_bytes` + the existing struct-based path. V2 is used by all recent checkpoints.

2. **Write directly**: For fields like transaction_digest ([u8;32]), write as base58 directly to the BinaryWriter using `w.write_base58(bytes)`. You'll need to add a `write_base58` method to BinaryWriter that encodes without allocating a String.

3. **Skip what you don't need**: For object_changes extraction, you need to walk `changed_objects` to get input/output versions and owners. But for some fields deep in nested enums (like the full ExecutionFailureStatus variants beyond MoveAbort), just read the variant tag, map to a name, and skip the rest.

4. **Error handling**: Return `Result<T, &'static str>` for simplicity. On error, the caller falls back to the full `bcs::from_bytes` path.

5. **TypeTag parsing**: TypeTag is recursive (Vector<TypeTag>, Struct with type_params). Write a `read_type_tag_canonical` that reads BCS TypeTag and writes the canonical string directly to a buffer. This replaces `format_type_tag`.

## TypeTag BCS Layout
```
ULEB128 variant:
  0 → Bool
  1 → U8
  2 → U64
  3 → U128
  4 → Address
  5 → Signer
  6 → Vector(TypeTag) — recursive
  7 → Struct(StructTag)
  8 → U16
  9 → U32
  10 → U256

StructTag:
  1. address: [u8; 32]
  2. module: String (Identifier)
  3. name: String (Identifier)
  4. type_params: Vec<TypeTag>
```

## Additional Helpers to Build

### write_base58 on BinaryWriter (`binary.rs`)

TransactionDigest and ObjectDigest are [u8; 32] displayed as base58. Add `write_base58(&mut self, bytes: &[u8; 32])` that encodes directly into the buffer without allocating a String. Base58 alphabet (Bitcoin/Sui): `123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz`. Max output for 32 bytes is 44 chars. Implement inline (~30 lines, no crate). Verify output matches `sui_types::digests::TransactionDigest::new(bytes).to_string()`.

### ExecutionFailureStatus variant name map

Read the exact variant order from `~/.cargo/git/checkouts/sui-e0a047c8ed89192d/211e22a/crates/sui-types/src/execution_status.rs`. Create a static array mapping ULEB128 variant index → `&'static str` name. For MoveAbort (variant 0), also extract the MoveLocation fields. For all others, just return the name and skip the variant data.

## What NOT to do

- Don't replace the full `bcs::from_bytes` path — keep it as fallback for V1 effects and error cases
- Don't implement every ExecutionFailureStatus variant — just the tag→name mapping + MoveAbort field extraction
- Don't handle system transaction data extraction (Genesis, ChangeEpoch, etc.) — these are rare and can use `serde_json::to_string` on the full struct

## Files to Read for Reference

- `native/rust-decoder/src/binary.rs` — BinaryWriter with write_hex, write_u64_dec, write_str, write_display, etc.
- `native/rust-decoder/src/direct.rs` — The integration point. See how `ParsedTx` is built and how records are written.
- `native/rust-decoder/src/extract.rs` — The current struct-based extraction. Use for output format reference.
- The sui-types source at `~/.cargo/git/checkouts/sui-e0a047c8ed89192d/211e22a/crates/sui-types/src/` — authoritative struct layouts.

## Testing

After implementing, verify output parity:
1. Run with `JUN_DIRECT_BINARY=0` (old path) and generate SQLite
2. Run with the new BCS reader path and generate SQLite
3. Compare all table content hashes (see `.claude/commands/parity-test.md`)

Focus on getting TransactionEffectsV2 working first — it's the highest impact (every transaction has effects). TransactionData is second priority.
