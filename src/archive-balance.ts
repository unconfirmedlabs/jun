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
 * Algorithm matches Sui's official indexer (tx_balance_changes.rs). This is the
 * pure Mysten BCS baseline — every BCS value is decoded via @mysten/sui/bcs.
 */
import { suiBcs } from "./bcs-provider.ts";
import type { BalanceChange } from "./balance-processor.ts";
import { normalizeSuiAddress, normalizeCoinType } from "./normalize.ts";

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

interface ParsedCoin {
  coinType: string;
  balance: bigint;
  owner: string | null;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Parse a coin object using @mysten/sui/bcs. */
function parseCoinObject(raw: Uint8Array): ParsedCoin | null {
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

/**
 * Extract coin type from a Balance<CoinType> type tag string.
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

  // 2. Extract all coin objects from the ObjectSet
  const coinVersions = new Map<string, CoinSnapshot[]>();

  for (const object of objects) {
    if (!object.bcs?.value) continue;
    const rawBytes = new Uint8Array(object.bcs.value);

    // Fast filter: skip non-coin objects without any BCS parsing.
    // byte[0] = Data enum: 0=Move, 1=Package
    // byte[1] = MoveObjectType enum: 0=Other, 1=GasCoin, 2=StakedSui, 3=Coin<T>, 4=AccumulatorBalanceWrapper
    // Only keep GasCoin (1) and Coin<T> (3).
    if (rawBytes[0] !== 0 || (rawBytes[1] !== 1 && rawBytes[1] !== 3)) continue;

    const parsed = parseCoinObject(rawBytes);
    if (!parsed) continue;

    if (coinTypeFilter && !coinTypeFilter.has(parsed.coinType)) continue;

    const objectId = object.objectId;
    if (!coinVersions.has(objectId)) coinVersions.set(objectId, []);
    coinVersions.get(objectId)!.push({
      version: BigInt(object.version),
      owner: parsed.owner,
      coinType: parsed.coinType,
      balance: parsed.balance,
    });
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
