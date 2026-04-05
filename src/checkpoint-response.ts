import type { BalanceChange } from "./balance-processor.ts";
import type { GrpcCheckpointResponse } from "./grpc.ts";
import type { Checkpoint } from "./pipeline/types.ts";
import { parseTimestamp } from "./timestamp.ts";

export interface SerializedBalanceChange {
  txDigest: string;
  checkpointSeq: string;
  address: string;
  coinType: string;
  amount: string;
  timestamp: string;
}

export function deserializeBalanceChanges(
  changes: SerializedBalanceChange[] | undefined,
): BalanceChange[] | undefined {
  if (!changes?.length) return undefined;
  return changes.map(change => ({
    txDigest: change.txDigest,
    checkpointSeq: BigInt(change.checkpointSeq),
    address: change.address,
    coinType: change.coinType,
    amount: change.amount,
    timestamp: new Date(change.timestamp),
  }));
}

export function checkpointFromGrpcResponse(
  response: GrpcCheckpointResponse,
  source: "live" | "backfill",
  precomputedBalanceChanges?: SerializedBalanceChange[] | BalanceChange[],
): Checkpoint {
  const summary = response.checkpoint.summary;
  const rollingGas = summary?.epochRollingGasCostSummary;

  const normalizedBalances = Array.isArray(precomputedBalanceChanges) && precomputedBalanceChanges.length > 0
    ? typeof precomputedBalanceChanges[0]?.checkpointSeq === "string"
      ? deserializeBalanceChanges(precomputedBalanceChanges as SerializedBalanceChange[])
      : precomputedBalanceChanges as BalanceChange[]
    : undefined;

  return {
    sequenceNumber: BigInt(response.checkpoint.sequenceNumber || response.cursor),
    timestamp: parseTimestamp(summary?.timestamp),
    transactions: response.checkpoint.transactions,
    source,
    precomputedBalanceChanges: normalizedBalances,
    epoch: summary?.epoch ? BigInt(summary.epoch) : 0n,
    digest: summary?.digest ?? "",
    previousDigest: summary?.previousDigest ?? null,
    contentDigest: summary?.contentDigest ?? null,
    totalNetworkTransactions: summary?.totalNetworkTransactions
      ? BigInt(summary.totalNetworkTransactions)
      : 0n,
    epochRollingGasCostSummary: {
      computationCost: rollingGas?.computationCost ?? "0",
      storageCost: rollingGas?.storageCost ?? "0",
      storageRebate: rollingGas?.storageRebate ?? "0",
      nonRefundableStorageFee: rollingGas?.nonRefundableStorageFee ?? "0",
    },
  };
}
