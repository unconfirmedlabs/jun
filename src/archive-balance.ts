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
// Helpers
// ---------------------------------------------------------------------------

/** Extract coin type string from a StructTag type parameter. */
function extractCoinTypeFromTypeParam(typeParam: any): string {
  if (typeParam?.$kind === "Struct") {
    const struct = typeParam.Struct;
    return `${normalizeSuiAddress(struct.address)}::${struct.module}::${struct.name}`;
  }
  return "unknown";
}

/**
 * Extract coin type from a Balance<CoinType> type tag string.
 * Input: "0x2::balance::Balance<0x2::sui::SUI>" → "0x2::sui::SUI"
 * Also handles the full address form from BCS.
 */
function extractCoinTypeFromBalanceTag(typeTag: string): string | null {
  // String form: "0x...::balance::Balance<0x...::module::Name>"
  if (typeof typeTag === "string") {
    const match = typeTag.match(/Balance<(.+)>$/);
    if (match) {
      const inner = match[1]!;
      // Normalize the package address
      const parts = inner.split("::");
      if (parts.length >= 3) {
        parts[0] = normalizeAddress(parts[0]!);
      }
      return parts.join("::");
    }
  }
  // BCS StructTag form
  if (typeTag && typeof typeTag === "object") {
    const struct = (typeTag as any).Struct ?? typeTag;
    if (struct?.module === "balance" && struct?.name === "Balance" && struct?.typeParams?.length > 0) {
      return extractCoinTypeFromTypeParam(struct.typeParams[0]);
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
    const normalizedCoin = normalizeCoinType(coinType);
    if (coinTypeFilter && !coinTypeFilter.has(normalizedCoin)) return;
    const normalizedOwner = normalizeSuiAddress(owner);
    const key = `${normalizedOwner}:${normalizedCoin}`;
    aggregated.set(key, (aggregated.get(key) ?? 0n) + delta);
  }

  for (const transaction of checkpointProto.transactions ?? []) {
    const effectsBcs = transaction.effects?.bcs?.value;
    if (!effectsBcs) continue;
    try {
      const effects = suiBcs.TransactionEffects.parse(new Uint8Array(effectsBcs));
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

        // Accumulator writes: direct balance changes without coin objects
        if (outputKind === "AccumulatorWriteV1") {
          const write = change.outputState.AccumulatorWriteV1;
          const address = write.address?.address;
          const typeTag = write.address?.ty;
          if (!address || !typeTag) continue;

          // Extract coin type from Balance<CoinType> type tag
          const coinType = extractCoinTypeFromBalanceTag(typeTag);
          if (!coinType) continue;

          const operation = write.operation?.$kind;
          const valueKind = write.value?.$kind;
          const amount = valueKind === "Integer" ? BigInt(write.value.Integer) : 0n;

          if (operation === "Merge") {
            addDelta(address, coinType, amount);
          } else if (operation === "Split") {
            addDelta(address, coinType, -amount);
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
    try {
      const parsed = suiBcs.Object.parse(new Uint8Array(object.bcs.value));
      if (!parsed.data.Move) continue;

      const type = parsed.data.Move.type;
      const kind = type.$kind;

      let coinType: string;
      if (kind === "GasCoin") {
        coinType = normalizeCoinType("0x2::sui::SUI");
      } else if (kind === "Other" && type.Other?.module === "coin" && type.Other?.name === "Coin") {
        coinType = normalizeCoinType(extractCoinTypeFromTypeParam(type.Other.typeParams?.[0]));
      } else {
        continue; // Not a coin
      }

      // Apply filter (both sides normalized)
      if (coinTypeFilter && !coinTypeFilter.has(coinType)) continue;

      // Extract balance: Coin BCS layout = UID (32 bytes) + Balance { value: u64 (8 bytes) }
      const contents = parsed.data.Move.contents;
      if (contents.length < 40) continue;
      const balance = new DataView(contents.buffer, contents.byteOffset + 32, 8).getBigUint64(0, true);

      const owner = parsed.owner?.AddressOwner ?? null;
      const objectId = object.objectId;

      if (!coinVersions.has(objectId)) coinVersions.set(objectId, []);
      coinVersions.get(objectId)!.push({
        version: BigInt(object.version),
        owner,
        coinType,
        balance,
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
