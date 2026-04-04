/**
 * Compute balance changes from archive checkpoint data.
 *
 * The archive doesn't include pre-computed balance_changes (those are computed
 * by fullnodes). Instead, we derive them by diffing coin objects in the
 * checkpoint's ObjectSet:
 *
 * 1. Parse all objects, detect Coin<T> types, extract balance + owner
 * 2. Group by objectId — each coin appears with input (old) and output (new) versions
 * 3. Diff: owner changed = transfer, balance changed = gain/loss
 * 4. Single-version objects: check effects for created (positive) or deleted (negative)
 *
 * Algorithm matches Sui's official indexer (tx_balance_changes.rs).
 */
import { bcs as suiBcs } from "@mysten/sui/bcs";
import type { BalanceChange } from "./balance-processor.ts";
import { normalizeSuiAddress, normalizeCoinType } from "./normalize.ts";

/** Use native suiBcs parsers instead of custom fast parsers. Set JUN_LEGACY_PARSERS=1 to enable. */
const USE_LEGACY_PARSERS = process.env.JUN_LEGACY_PARSERS === "1";

// Pre-compute normalized SUI type to avoid repeated normalization
const NORMALIZED_SUI = normalizeCoinType("0x2::sui::SUI");

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface CoinSnapshot {
  version: bigint;
  owner: string | null;
  coinType: string;
  balance: bigint;
}

// ---------------------------------------------------------------------------
// Fast BCS coin parser
// ---------------------------------------------------------------------------
// Replaces suiBcs.Object.parse() with targeted reads of only the fields we
// need: coinType, balance, owner. Avoids allocating intermediate JS objects
// for every field (UID, previousTransaction, storageRebate, etc.).
//
// BCS layout (from @mysten/sui/bcs):
//   ObjectInner { data: Data, owner: Owner, previousTransaction, storageRebate }
//   Data enum: 0=Move(MoveObject), 1=Package(MovePackage)
//   MoveObject { type: MoveObjectType, hasPublicTransfer: bool, version: u64, contents: vec<u8> }
//   MoveObjectType enum: 0=Other(StructTag), 1=GasCoin, 2=StakedSui, 3=Coin(TypeTag), 4=AccumulatorBalanceWrapper
//   TypeTag enum: 0=bool..6=vector(T), 7=struct(StructTag), 8=u16..10=u256
//   Owner enum: 0=AddressOwner(32), 1=ObjectOwner(32), 2=Shared{u64}, 3=Immutable, 4=ConsensusAddressOwner{u64,32}

import { readUleb128, readBcsString } from "./uleb.ts";

/** Convert 32 raw bytes to a 0x-prefixed hex string. */
const HEX_CHARS = "0123456789abcdef";
function bytesToHex(buf: Uint8Array, offset: number): string {
  let hex = "0x";
  for (let i = 0; i < 32; i++) {
    const byte = buf[offset + i]!;
    hex += HEX_CHARS[byte >> 4]! + HEX_CHARS[byte & 0xf]!;
  }
  return hex;
}

/** Skip over a BCS-encoded TypeTag at the given offset. Returns new offset, or -1 on error. */
function skipTypeTag(buf: Uint8Array, offset: number): number {
  const tag = buf[offset]!;
  offset++;
  switch (tag) {
    case 0: case 1: case 2: case 3: case 4: case 5: case 8: case 9: case 10:
      return offset; // Primitive types: no additional data
    case 6: // vector<TypeTag>
      return skipTypeTag(buf, offset);
    case 7: // struct(StructTag)
      return skipStructTag(buf, offset);
    default:
      return -1;
  }
}

/** Skip over a BCS-encoded StructTag. Returns new offset, or -1 on error. */
function skipStructTag(buf: Uint8Array, offset: number): number {
  offset += 32; // address (32 bytes)
  const [modLen, modLenBytes] = readUleb128(buf, offset);
  offset += modLenBytes + modLen; // module string
  const [nameLen, nameLenBytes] = readUleb128(buf, offset);
  offset += nameLenBytes + nameLen; // name string
  const [paramCount, paramCountBytes] = readUleb128(buf, offset);
  offset += paramCountBytes;
  for (let i = 0; i < paramCount; i++) {
    offset = skipTypeTag(buf, offset);
    if (offset === -1) return -1;
  }
  return offset;
}

const TYPE_TAG_NAMES = ["bool","u8","u64","u128","address","signer","vector","struct","u16","u32","u256"];

/**
 * Format a BCS-encoded TypeTag to its string representation.
 * Returns [formatted string, new offset]. Unlike skipTypeTag, this reads and formats
 * the full type including nested generics (e.g., "AToken<0x...::usdc::USDC>").
 */
function formatTypeTagFromBcs(buf: Uint8Array, offset: number): [string, number] {
  const tag = buf[offset]!;
  offset++;

  if (tag <= 5 || (tag >= 8 && tag <= 10)) {
    return [TYPE_TAG_NAMES[tag]!, offset];
  }

  if (tag === 6) {
    let inner: string;
    [inner, offset] = formatTypeTagFromBcs(buf, offset);
    return [`vector<${inner}>`, offset];
  }

  if (tag === 7) {
    // struct(StructTag): address + module + name + typeParams
    const addr = bytesToHex(buf, offset);
    offset += 32;
    let mod: string;
    [mod, offset] = readBcsString(buf, offset);
    let name: string;
    [name, offset] = readBcsString(buf, offset);

    const [paramCount, pcBytes] = readUleb128(buf, offset);
    offset += pcBytes;

    if (paramCount === 0) {
      return [`${addr}::${mod}::${name}`, offset];
    }

    const params: string[] = [];
    for (let i = 0; i < paramCount; i++) {
      let p: string;
      [p, offset] = formatTypeTagFromBcs(buf, offset);
      params.push(p);
    }
    return [`${addr}::${mod}::${name}<${params.join(", ")}>`, offset];
  }

  return ["unknown", offset];
}

interface FastCoinResult {
  coinType: string;
  balance: bigint;
  owner: string | null;
}

/**
 * Fast BCS parser for coin objects. Reads only coinType, balance, and owner
 * without allocating intermediate JS objects for every field.
 *
 * Expects: byte[0]=0 (Move), byte[1] ∈ {1=GasCoin, 3=Coin<T>}.
 * Returns null for unparseable or non-coin objects.
 */
function fastParseCoinObject(raw: Uint8Array): FastCoinResult | null {
  const kind = raw[1]!;
  let offset = 2; // past Data enum (0) + MoveObjectType enum
  let coinType: string;

  if (kind === 1) {
    // GasCoin — no type data needed, always SUI
    coinType = NORMALIZED_SUI;
  } else if (kind === 3) {
    // Coin<T> — TypeTag encodes the inner coin type T
    const typeTagKind = raw[offset]!;
    offset++;

    if (typeTagKind !== 7) return null; // Must be struct TypeTag for a coin type

    // Read StructTag: address(32) + module(string) + name(string) + typeParams(vec<TypeTag>)
    const address = bytesToHex(raw, offset);
    offset += 32;

    let module: string;
    [module, offset] = readBcsString(raw, offset);

    let name: string;
    [name, offset] = readBcsString(raw, offset);

    // Format inner typeParams (e.g., AToken<USDC> needs the <USDC> part)
    const [paramCount, paramCountBytes] = readUleb128(raw, offset);
    offset += paramCountBytes;

    if (paramCount === 0) {
      coinType = normalizeCoinType(`${address}::${module}::${name}`);
    } else {
      const params: string[] = [];
      for (let i = 0; i < paramCount; i++) {
        let p: string;
        [p, offset] = formatTypeTagFromBcs(raw, offset);
        if (offset === -1) return null;
        params.push(p);
      }
      coinType = normalizeCoinType(`${address}::${module}::${name}<${params.join(", ")}>`);
    }
  } else {
    return null; // StakedSui (2), AccumulatorBalanceWrapper (4), Other (0)
  }

  // hasPublicTransfer (bool, 1 byte)
  offset += 1;

  // version (u64 LE, 8 bytes)
  offset += 8;

  // contents: ULEB128 length + raw bytes
  const [contentsLen, contentsLenBytes] = readUleb128(raw, offset);
  offset += contentsLenBytes;

  if (contentsLen < 40) return null; // Need at least 32 (UID) + 8 (balance)

  // Balance is at contents_start + 32 (skip UID)
  const balance = new DataView(raw.buffer, raw.byteOffset + offset + 32, 8).getBigUint64(0, true);

  // Skip past contents to reach Owner
  offset += contentsLen;

  // Owner enum: 0=AddressOwner(32), 1=ObjectOwner(32), ...
  const ownerKind = raw[offset]!;
  offset++;

  let owner: string | null = null;
  if (ownerKind === 0) {
    owner = bytesToHex(raw, offset);
  }

  return { coinType, balance, owner };
}

/** Parse a coin object using native suiBcs (slow but battle-tested). */
function nativeParseCoinObject(raw: Uint8Array): FastCoinResult | null {
  try {
    const parsed = suiBcs.Object.parse(raw);
    if (!parsed.data.Move) return null;
    const type = parsed.data.Move.type;
    let coinType: string;
    if (type.$kind === "GasCoin") coinType = NORMALIZED_SUI;
    else if (type.$kind === "Coin") coinType = normalizeCoinType(String(type.Coin));
    else return null;
    const contents = parsed.data.Move.contents;
    if (contents.length < 40) return null;
    const balance = new DataView(contents.buffer, contents.byteOffset + 32, 8).getBigUint64(0, true);
    const owner = parsed.owner?.AddressOwner ?? null;
    return { coinType, balance, owner };
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Fast TransactionEffects parser
// ---------------------------------------------------------------------------
// Extracts only changedObjects data needed for balance computation:
// - created objects (inputState=NotExist, outputState=ObjectWrite)
// - deleted objects (outputState=NotExist)
// - accumulator writes (outputState=AccumulatorWriteV1)
//
// For V2 Success transactions (99%+ of cases), skips directly to changedObjects
// without allocating intermediate objects for status, gas, digest, dependencies.
// Returns null for V1 or Failure → caller falls back to suiBcs.TransactionEffects.parse().
//
// BCS layout (from @mysten/sui/bcs/effects.mjs):
//   TransactionEffects enum: 0=V1, 1=V2
//   TransactionEffectsV2 struct {
//     status: ExecutionStatus,         -- enum: 0=Success, 1=Failure(complex)
//     executedEpoch: u64,              -- 8 bytes
//     gasUsed: GasCostSummary,         -- 4×u64 = 32 bytes
//     transactionDigest: ObjectDigest, -- vec<u8> (33 bytes for Sui digests)
//     gasObjectIndex: option<u32>,     -- 1+0 or 1+4 bytes
//     eventsDigest: option<ObjDigest>, -- 1+0 or 1+33 bytes
//     dependencies: vec<ObjectDigest>, -- ULEB128 count + count×33
//     lamportVersion: u64,             -- 8 bytes
//     changedObjects: vec<(Address, EffectsObjectChange)>,  ← TARGET
//     unchangedConsensusObjects: ...,
//     auxDataDigest: ...
//   }

interface AccumulatorWriteData {
  ownerAddress: string;
  coinType: string;
  operation: number; // 0=Merge, 1=Split
  amount: bigint;
}

interface FastEffectsResult {
  createdObjects: string[];
  deletedObjects: string[];
  accumulatorWrites: AccumulatorWriteData[];
}

/** Skip a BCS vec<u8> (ULEB128 length + raw bytes). */
function skipVecU8(buf: Uint8Array, offset: number): number {
  const [len, lenBytes] = readUleb128(buf, offset);
  return offset + lenBytes + len;
}

/**
 * Skip a BCS-encoded Owner enum.
 *   0=AddressOwner(32), 1=ObjectOwner(32), 2=Shared{u64}(8),
 *   3=Immutable(0), 4=ConsensusAddressOwner{u64,Address}(40)
 */
function skipOwner(buf: Uint8Array, offset: number): number {
  const tag = buf[offset]!;
  offset++;
  switch (tag) {
    case 0: case 1: return offset + 32;
    case 2: return offset + 8;
    case 3: return offset;
    case 4: return offset + 40;
    default: return -1;
  }
}

/**
 * Parse a BCS TypeTag that should be Balance<CoinType>.
 * Extracts the inner coin type string; returns [coinType, newOffset].
 * coinType is null if not a Balance<T> struct.
 */
function parseBalanceCoinType(buf: Uint8Array, offset: number): [string | null, number] {
  // TypeTag enum: must be 7 (struct)
  if (buf[offset]! !== 7) {
    offset = skipTypeTag(buf, offset);
    return [null, offset];
  }
  offset++; // past TypeTag enum tag

  // StructTag: address(32) + module(string) + name(string) + typeParams(vec<TypeTag>)
  // Skip address — we only need module/name to check if this is balance::Balance
  const addrOffset = offset;
  offset += 32;

  let module: string;
  [module, offset] = readBcsString(buf, offset);
  let name: string;
  [name, offset] = readBcsString(buf, offset);

  const [paramCount, paramCountBytes] = readUleb128(buf, offset);
  offset += paramCountBytes;

  if (module !== "balance" || name !== "Balance" || paramCount === 0) {
    for (let i = 0; i < paramCount; i++) {
      offset = skipTypeTag(buf, offset);
      if (offset === -1) return [null, -1];
    }
    return [null, offset];
  }

  // This IS Balance<T> — parse first typeParam for the coin type
  if (buf[offset]! !== 7) {
    // Inner type is not a struct — skip all params
    for (let i = 0; i < paramCount; i++) {
      offset = skipTypeTag(buf, offset);
      if (offset === -1) return [null, -1];
    }
    return [null, offset];
  }
  offset++; // past inner TypeTag enum tag (7=struct)

  // Read inner StructTag
  const innerAddr = bytesToHex(buf, offset);
  offset += 32;
  let innerModule: string;
  [innerModule, offset] = readBcsString(buf, offset);
  let innerName: string;
  [innerName, offset] = readBcsString(buf, offset);

  // Format inner type params (e.g., Balance<AToken<USDC>> needs the <USDC> on AToken)
  const [innerParamCount, innerParamCountBytes] = readUleb128(buf, offset);
  offset += innerParamCountBytes;

  let coinType: string;
  if (innerParamCount === 0) {
    coinType = `${innerAddr}::${innerModule}::${innerName}`;
  } else {
    const innerParams: string[] = [];
    for (let i = 0; i < innerParamCount; i++) {
      let p: string;
      [p, offset] = formatTypeTagFromBcs(buf, offset);
      if (offset === -1) return [null, -1];
      innerParams.push(p);
    }
    coinType = `${innerAddr}::${innerModule}::${innerName}<${innerParams.join(", ")}>`;
  }

  // Skip remaining outer params (Balance only has 1, but be safe)
  for (let i = 1; i < paramCount; i++) {
    offset = skipTypeTag(buf, offset);
    if (offset === -1) return [null, -1];
  }

  return [normalizeCoinType(coinType), offset];
}

/**
 * Parse changedObjects from V2 effects starting at the given offset.
 * Extracts created objects, deleted objects, and accumulator writes.
 */
function parseChangedObjects(buf: Uint8Array, offset: number): FastEffectsResult | null {
  const result: FastEffectsResult = {
    createdObjects: [],
    deletedObjects: [],
    accumulatorWrites: [],
  };

  const [count, countBytes] = readUleb128(buf, offset);
  offset += countBytes;

  for (let i = 0; i < count; i++) {
    // Address (32 bytes) = objectId
    const objectId = bytesToHex(buf, offset);
    offset += 32;

    // --- inputState: ObjectIn enum ---
    // 0=NotExist(null), 1=Exist(tuple(VersionDigest, Owner))
    const inputTag = buf[offset]!;
    offset++;
    if (inputTag === 1) {
      // VersionDigest = tuple(u64, ObjectDigest) = 8 + vec<u8>
      offset += 8;
      offset = skipVecU8(buf, offset);
      offset = skipOwner(buf, offset);
      if (offset === -1) return null;
    }

    // --- outputState: ObjectOut enum ---
    // 0=NotExist, 1=ObjectWrite(ObjDigest,Owner), 2=PackageWrite(VerDigest), 3=AccumulatorWriteV1
    const outputTag = buf[offset]!;
    offset++;

    if (outputTag === 0) {
      // NotExist — object deleted
      result.deletedObjects.push(objectId);
    } else if (outputTag === 1) {
      // ObjectWrite: tuple(ObjectDigest, Owner)
      offset = skipVecU8(buf, offset);
      offset = skipOwner(buf, offset);
      if (offset === -1) return null;
      if (inputTag === 0) {
        result.createdObjects.push(objectId);
      }
    } else if (outputTag === 2) {
      // PackageWrite: VersionDigest = tuple(u64, ObjectDigest)
      offset += 8;
      offset = skipVecU8(buf, offset);
    } else if (outputTag === 3) {
      // AccumulatorWriteV1 { address: AccumulatorAddress, operation, value }
      // AccumulatorAddress { address: Address(32), ty: TypeTag }
      const ownerAddress = bytesToHex(buf, offset);
      offset += 32;

      // Parse TypeTag to extract coin type from Balance<CoinType>
      let coinType: string | null;
      [coinType, offset] = parseBalanceCoinType(buf, offset);
      if (offset === -1) return null;

      // AccumulatorOperation enum: 0=Merge, 1=Split (1 byte, no payload)
      const operation = buf[offset]!;
      offset++;

      // AccumulatorValue enum: 0=Integer(u64), 1=IntegerTuple(u64,u64), 2=EventDigest(vec)
      const valueTag = buf[offset]!;
      offset++;
      let amount = 0n;
      if (valueTag === 0) {
        amount = new DataView(buf.buffer, buf.byteOffset + offset, 8).getBigUint64(0, true);
        offset += 8;
      } else if (valueTag === 1) {
        offset += 16; // two u64s
      } else if (valueTag === 2) {
        // vec<tuple(u64, ObjectDigest)>
        const [edCount, edCountBytes] = readUleb128(buf, offset);
        offset += edCountBytes;
        for (let j = 0; j < edCount; j++) {
          offset += 8;
          offset = skipVecU8(buf, offset);
        }
      }

      if (coinType) {
        result.accumulatorWrites.push({ ownerAddress, coinType, operation, amount });
      }
    } else {
      return null; // Unknown ObjectOut variant
    }

    // --- idOperation: IDOperation enum ---
    // 0=None, 1=Created, 2=Deleted — all null payloads, just skip the tag
    offset++;
  }

  return result;
}

/**
 * Fast-path parser for TransactionEffectsV2 with Success status.
 * Skips directly to changedObjects using known field offsets.
 *
 * Returns null for V1, Failure, or any parse error → caller falls back
 * to suiBcs.TransactionEffects.parse() for full correctness.
 */
function fastParseEffects(raw: Uint8Array): FastEffectsResult | null {
  // TransactionEffects enum: 0=V1, 1=V2
  if (raw[0] !== 1) return null;

  // ExecutionStatus: 0=Success, 1=Failure
  // Failure has a deeply nested ExecutionFailureStatus enum (40+ variants)
  // that's impractical to skip. Fall back to full parse for the rare failure case.
  if (raw[1] !== 0) return null;

  let offset = 2;

  // executedEpoch: u64
  offset += 8;

  // gasUsed: GasCostSummary { computationCost, storageCost, storageRebate, nonRefundableStorageFee }
  offset += 32; // 4 × u64

  // transactionDigest: ObjectDigest = vec<u8>
  offset = skipVecU8(raw, offset);

  // gasObjectIndex: option<u32>
  if (raw[offset]! === 1) {
    offset += 5; // tag(1) + u32(4)
  } else {
    offset += 1; // tag(1) only
  }

  // eventsDigest: option<ObjectDigest>
  if (raw[offset]! === 1) {
    offset += 1;
    offset = skipVecU8(raw, offset);
  } else {
    offset += 1;
  }

  // dependencies: vec<ObjectDigest>
  const [depCount, depCountBytes] = readUleb128(raw, offset);
  offset += depCountBytes;
  for (let i = 0; i < depCount; i++) {
    offset = skipVecU8(raw, offset);
  }

  // lamportVersion: u64
  offset += 8;

  // changedObjects — parse this
  return parseChangedObjects(raw, offset);
}

// ---------------------------------------------------------------------------
// Helpers (fallback path)
// ---------------------------------------------------------------------------

/**
 * Extract coin type from a Balance<CoinType> type tag string (from full BCS parse).
 * Used only in the fallback path for V1/Failure transactions.
 * Input: "0x2::balance::Balance<0x2::sui::SUI>" → normalized coin type
 */
function extractCoinTypeFromBalanceTag(typeTag: string): string | null {
  if (typeof typeTag === "string") {
    const match = typeTag.match(/Balance<(.+)>$/);
    if (match) {
      const inner = match[1]!;
      const parts = inner.split("::");
      if (parts.length >= 3) {
        parts[0] = normalizeSuiAddress(parts[0]!);
      }
      return parts.join("::");
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

/**
 * Compute balance changes from a decoded archive checkpoint protobuf.
 *
 * @param checkpointProto - The decoded protobuf Checkpoint object (from Checkpoint.toObject())
 * @param checkpointSeq - Checkpoint sequence number
 * @param timestamp - Checkpoint timestamp
 * @param coinTypeFilter - Set of coin types to filter (null = all)
 */
export function computeBalanceChangesFromArchive(
  checkpointProto: any,
  checkpointSeq: bigint,
  timestamp: Date,
  coinTypeFilter: Set<string> | null,
): BalanceChange[] {
  const objects = checkpointProto.objects?.objects ?? [];
  if (objects.length === 0) return [];

  // 1. Parse transaction effects: build created/deleted sets + extract accumulator writes
  const createdObjects = new Set<string>();
  const deletedObjects = new Set<string>();
  const aggregated = new Map<string, bigint>(); // "owner:coinType" → delta

  // Fast-path aggregation: inputs are already normalized by fast parsers
  // (bytesToHex produces canonical 0x+64 hex, normalizeCoinType called in fast parsers).
  // No re-normalization needed — just aggregate directly.
  function addDelta(owner: string | null, coinType: string, delta: bigint): void {
    if (!owner || delta === 0n) return;
    if (coinTypeFilter && !coinTypeFilter.has(coinType)) return;
    const key = `${owner}:${coinType}`;
    aggregated.set(key, (aggregated.get(key) ?? 0n) + delta);
  }

  for (const transaction of checkpointProto.transactions ?? []) {
    const effectsBcs = transaction.effects?.bcs?.value;
    if (!effectsBcs) continue;
    try {
      const raw = new Uint8Array(effectsBcs);

      // Fast path: V2 Success transactions (91%+ of cases)
      const fastResult = USE_LEGACY_PARSERS ? null : fastParseEffects(raw);
      if (fastResult) {
        for (const id of fastResult.createdObjects) createdObjects.add(id);
        for (const id of fastResult.deletedObjects) deletedObjects.add(id);
        for (const aw of fastResult.accumulatorWrites) {
          addDelta(aw.ownerAddress, aw.coinType, aw.operation === 0 ? aw.amount : -aw.amount);
        }
        continue;
      }

      // Fallback: V1 or Failure — use full BCS parse for correctness
      const effects = suiBcs.TransactionEffects.parse(raw);
      const v2 = effects.V2;
      if (!v2) continue;
      for (const [objectId, change] of v2.changedObjects ?? []) {
        const inputKind = change.inputState?.$kind;
        const outputKind = change.outputState?.$kind;

        if (inputKind === "NotExist" && outputKind === "ObjectWrite") {
          createdObjects.add(objectId);
        }
        if (outputKind === "NotExist") {
          deletedObjects.add(objectId);
        }

        if (outputKind === "AccumulatorWriteV1") {
          const write = change.outputState.AccumulatorWriteV1;
          const address = write.address?.address;
          const typeTag = write.address?.ty;
          if (!address || !typeTag) continue;

          const coinType = extractCoinTypeFromBalanceTag(typeTag);
          if (!coinType) continue;

          // Fallback path: normalize before addDelta (fast path pre-normalizes)
          const normalizedAddr = normalizeSuiAddress(address);
          const normalizedCoin = normalizeCoinType(coinType);
          const operation = write.operation?.$kind;
          const valueKind = write.value?.$kind;
          const amount = valueKind === "Integer" ? BigInt(write.value.Integer) : 0n;

          if (operation === "Merge") {
            addDelta(normalizedAddr, normalizedCoin, amount);
          } else if (operation === "Split") {
            addDelta(normalizedAddr, normalizedCoin, -amount);
          }
        }
      }
    } catch {
      // Skip unparseable effects
    }
  }

  // 2. Extract all coin objects from the ObjectSet using fast BCS parser
  const coinVersions = new Map<string, CoinSnapshot[]>();

  for (const object of objects) {
    if (!object.bcs?.value) continue;
    const rawBytes = new Uint8Array(object.bcs.value);

    // Fast filter: skip non-coin objects without any BCS parsing.
    // byte[0] = Data enum: 0=Move, 1=Package
    // byte[1] = MoveObjectType enum: 0=Other, 1=GasCoin, 2=StakedSui, 3=Coin<T>, 4=AccumulatorBalanceWrapper
    // Only keep GasCoin (1) and Coin<T> (3).
    if (rawBytes[0] !== 0 || (rawBytes[1] !== 1 && rawBytes[1] !== 3)) continue;

    try {
      const parsed = USE_LEGACY_PARSERS ? nativeParseCoinObject(rawBytes) : fastParseCoinObject(rawBytes);
      if (!parsed) continue;

      // Apply filter (both sides normalized)
      if (coinTypeFilter && !coinTypeFilter.has(parsed.coinType)) continue;

      const objectId = object.objectId;
      if (!coinVersions.has(objectId)) coinVersions.set(objectId, []);
      coinVersions.get(objectId)!.push({
        version: BigInt(object.version),
        owner: parsed.owner,
        coinType: parsed.coinType,
        balance: parsed.balance,
      });
    } catch {
      // Skip unparseable objects
    }
  }

  // 3. Compute balance changes by diffing input/output coin versions
  for (const [objectId, versions] of coinVersions) {
    versions.sort((a, b) => (a.version < b.version ? -1 : 1));

    if (versions.length >= 2) {
      // Input (old version) and output (new version)
      const input = versions[0]!;
      const output = versions[versions.length - 1]!;

      if (input.owner === output.owner) {
        // Same owner: balance changed (gas fee, split, merge)
        addDelta(input.owner, input.coinType, output.balance - input.balance);
      } else {
        // Owner changed: transfer
        addDelta(input.owner, input.coinType, -input.balance);
        addDelta(output.owner, output.coinType, output.balance);
      }
    } else if (versions.length === 1) {
      const snapshot = versions[0]!;
      if (createdObjects.has(objectId)) {
        // Newly created coin → positive for owner
        addDelta(snapshot.owner, snapshot.coinType, snapshot.balance);
      } else if (deletedObjects.has(objectId)) {
        // Deleted coin → negative for owner
        addDelta(snapshot.owner, snapshot.coinType, -snapshot.balance);
      }
    }
  }

  // 4. Convert to BalanceChange array
  const changes: BalanceChange[] = [];
  for (const [key, delta] of aggregated) {
    if (delta === 0n) continue;
    const colonIndex = key.indexOf(":");
    const address = key.slice(0, colonIndex);
    const coinType = key.slice(colonIndex + 1);

    changes.push({
      txDigest: "", // Archive-level: aggregated per checkpoint, not per-tx
      checkpointSeq,
      address,
      coinType,
      amount: delta.toString(),
      timestamp,
    });
  }

  return changes;
}
