/**
 * Extended Sui BCS types that the @mysten/sui SDK is missing.
 *
 * The SDK's TransactionKind enum only has 4 of 11 variants, causing parse
 * failures on any mainnet checkpoint with system transactions.
 *
 * These definitions are derived from:
 *   sui/crates/sui-types/src/transaction.rs
 *   sui/crates/sui-types/src/messages_consensus.rs
 *   sui/crates/sui-types/src/authenticator_state.rs
 *   sui/crates/sui-types/src/crypto.rs
 *   sui/crates/sui-types/src/digests.rs
 *
 * TODO: Remove once @mysten/sui/bcs adds these variants.
 * Tracking: https://github.com/unconfirmedlabs/jun/issues/1
 */
import { bcs } from "@mysten/sui/bcs";

// ─── Digest types (all wrap Vec<u8> in BCS) ──────────────────────────────────

const SuiDigest = bcs.vector(bcs.U8);
const ConsensusCommitDigest = SuiDigest; // struct ConsensusCommitDigest(Digest) where Digest = [u8; 32]
const AdditionalConsensusStateDigest = SuiDigest;
const ChainIdentifier = SuiDigest; // struct ChainIdentifier(CheckpointDigest)

// ─── Authenticator types ─────────────────────────────────────────────────────

const JwkId = bcs.struct("JwkId", {
  iss: bcs.string(),
  kid: bcs.string(),
});

const JWK = bcs.struct("JWK", {
  kty: bcs.string(),
  e: bcs.string(),
  n: bcs.string(),
  alg: bcs.string(),
});

const ActiveJwk = bcs.struct("ActiveJwk", {
  jwkId: JwkId,
  jwk: JWK,
  epoch: bcs.u64(),
});

const AuthenticatorStateExpire = bcs.struct("AuthenticatorStateExpire", {
  minEpoch: bcs.u64(),
  authenticatorObjInitialSharedVersion: bcs.u64(),
});

// ─── System transaction types ────────────────────────────────────────────────

const AuthenticatorStateUpdate = bcs.struct("AuthenticatorStateUpdate", {
  epoch: bcs.u64(),
  round: bcs.u64(),
  newActiveJwks: bcs.vector(ActiveJwk),
  authenticatorObjInitialSharedVersion: bcs.u64(),
});

const RandomnessStateUpdate = bcs.struct("RandomnessStateUpdate", {
  epoch: bcs.u64(),
  randomnessRound: bcs.u64(), // RandomnessRound(u64)
  randomBytes: bcs.vector(bcs.U8),
  randomnessObjInitialSharedVersion: bcs.u64(),
});

// ConsensusCommitPrologue variants
const ConsensusCommitPrologueV2 = bcs.struct("ConsensusCommitPrologueV2", {
  epoch: bcs.u64(),
  round: bcs.u64(),
  commitTimestampMs: bcs.u64(),
  consensusCommitDigest: ConsensusCommitDigest,
});

// ConsensusDeterminedVersionAssignments enum
// ObjectID = Address (32 bytes), SequenceNumber = u64
const CancelledTransactions = bcs.vector(
  bcs.tuple([
    bcs.Address, // TransactionDigest
    bcs.vector(bcs.tuple([bcs.Address, bcs.u64()])), // Vec<(ObjectID, SequenceNumber)>
  ]),
);

// ConsensusObjectSequenceKey = (ObjectID, SequenceNumber) = (Address, u64)
const CancelledTransactionsV2 = bcs.vector(
  bcs.tuple([
    bcs.Address, // TransactionDigest
    bcs.vector(bcs.tuple([bcs.tuple([bcs.Address, bcs.u64()]), bcs.u64()])), // Vec<(ConsensusObjectSequenceKey, SequenceNumber)>
  ]),
);

const ConsensusDeterminedVersionAssignments = bcs.enum("ConsensusDeterminedVersionAssignments", {
  CancelledTransactions: CancelledTransactions,
  CancelledTransactionsV2: CancelledTransactionsV2,
});

const ConsensusCommitPrologueV3 = bcs.struct("ConsensusCommitPrologueV3", {
  epoch: bcs.u64(),
  round: bcs.u64(),
  subDagIndex: bcs.option(bcs.u64()),
  commitTimestampMs: bcs.u64(),
  consensusCommitDigest: ConsensusCommitDigest,
  consensusDeterminedVersionAssignments: ConsensusDeterminedVersionAssignments,
});

const ConsensusCommitPrologueV4 = bcs.struct("ConsensusCommitPrologueV4", {
  epoch: bcs.u64(),
  round: bcs.u64(),
  subDagIndex: bcs.option(bcs.u64()),
  commitTimestampMs: bcs.u64(),
  consensusCommitDigest: ConsensusCommitDigest,
  consensusDeterminedVersionAssignments: ConsensusDeterminedVersionAssignments,
  additionalStateDigest: AdditionalConsensusStateDigest,
});

// ChangeEpoch struct
const ChangeEpoch = bcs.struct("ChangeEpoch", {
  epoch: bcs.u64(),
  protocolVersion: bcs.u64(),
  storageCharge: bcs.u64(),
  computationCharge: bcs.u64(),
  storageRebate: bcs.u64(),
  nonRefundableStorageFee: bcs.u64(),
  epochStartTimestampMs: bcs.u64(),
  systemPackages: bcs.vector(
    bcs.tuple([
      bcs.u64(), // SequenceNumber
      bcs.vector(bcs.vector(bcs.U8)), // Vec<Vec<u8>> (module bytecodes)
      bcs.vector(bcs.Address), // Vec<ObjectID>
    ]),
  ),
});

// EndOfEpochTransactionKind enum
// Duration in Rust BCS = struct { secs: u64, nanos: u32 }
const Duration = bcs.struct("Duration", {
  secs: bcs.u64(),
  nanos: bcs.u32(),
});

// ExecutionTimeObservationKey = u16
// AuthorityName = [u8; 32] (public key)
const StoredExecutionTimeObservations = bcs.enum("StoredExecutionTimeObservations", {
  V1: bcs.vector(
    bcs.tuple([
      bcs.u16(), // ExecutionTimeObservationKey
      bcs.vector(bcs.tuple([bcs.fixedArray(32, bcs.U8), Duration])), // Vec<(AuthorityName, Duration)>
    ]),
  ),
});

const EndOfEpochTransactionKind = bcs.enum("EndOfEpochTransactionKind", {
  ChangeEpoch: ChangeEpoch,
  AuthenticatorStateCreate: null,
  AuthenticatorStateExpire: AuthenticatorStateExpire,
  RandomnessStateCreate: null,
  DenyListStateCreate: null,
  BridgeStateCreate: ChainIdentifier,
  BridgeCommitteeInit: bcs.u64(), // SequenceNumber
  StoreExecutionTimeObservations: StoredExecutionTimeObservations,
  AccumulatorRootCreate: null,
  CoinRegistryCreate: null,
  DisplayRegistryCreate: null,
});

// GenesisObject enum — simplified, we just need to skip past it
const GenesisObject = bcs.enum("GenesisObject", {
  RawObject: bcs.struct("RawObject", {
    data: bcs.Object,
    owner: bcs.Owner,
  }),
});

const GenesisTransaction = bcs.struct("GenesisTransaction", {
  objects: bcs.vector(GenesisObject),
});

// The original ConsensusCommitPrologue (V1)
const ConsensusCommitPrologue = bcs.struct("ConsensusCommitPrologue", {
  epoch: bcs.u64(),
  round: bcs.u64(),
  commitTimestampMs: bcs.u64(),
});

// ─── Complete TransactionKind enum ───────────────────────────────────────────

export const TransactionKind = bcs.enum("TransactionKind", {
  ProgrammableTransaction: bcs.ProgrammableTransaction,
  ChangeEpoch: ChangeEpoch,
  Genesis: GenesisTransaction,
  ConsensusCommitPrologue: ConsensusCommitPrologue,
  AuthenticatorStateUpdate: AuthenticatorStateUpdate,
  EndOfEpochTransaction: bcs.vector(EndOfEpochTransactionKind),
  RandomnessStateUpdate: RandomnessStateUpdate,
  ConsensusCommitPrologueV2: ConsensusCommitPrologueV2,
  ConsensusCommitPrologueV3: ConsensusCommitPrologueV3,
  ConsensusCommitPrologueV4: ConsensusCommitPrologueV4,
  ProgrammableSystemTransaction: bcs.ProgrammableTransaction,
});

// ─── Complete TransactionData ────────────────────────────────────────────────

const TransactionDataV1 = bcs.struct("TransactionDataV1", {
  kind: TransactionKind,
  sender: bcs.Address,
  gasData: bcs.GasData,
  expiration: bcs.TransactionExpiration,
});

export const TransactionData = bcs.enum("TransactionData", {
  V1: TransactionDataV1,
});

/**
 * Parse TransactionData BCS and extract the sender address.
 * Works for ALL transaction kinds (user + system).
 */
export function parseSender(txDataBcs: Uint8Array): string | null {
  try {
    const txData = TransactionData.parse(txDataBcs);
    return txData.V1?.sender ?? null;
  } catch {
    return null;
  }
}
