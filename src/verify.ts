/**
 * Transaction and object verification using kei.
 *
 * Full trustless chain: gRPC lookup → archive fetch → kei verification
 * (checkpoint signature → contents → transaction → effects → events → objects)
 */
import {
  verifyCheckpoint,
  verifyTransactionEffects,
  verifyTransactionEvents,
  verifyObjectInEffects,
  parseBcsSummary,
  bcsCheckpointContents,
  checkpointContentsDigest,
  digestsEqual,
  type PreparedCommittee,
} from "@unconfirmed/kei";
import { fetchRawCheckpoint, getCommittee, type RawCheckpoint } from "./archive.ts";
import { getSuiClient } from "./rpc.ts";

// ─── Types ───────────────────────────────────────────────────────────────────

export interface VerifyStep {
  label: string;
  detail?: string;
}

export interface VerifyOptions {
  rpcUrl: string;
  archiveUrl: string;
  grpcUrl: string;
}

export interface VerifyTxResult {
  steps: VerifyStep[];
  checkpointSeq: string;
  epoch: string;
  signers: number;
  committeeSize: number;
  hasEvents: boolean;
}

export interface VerifyObjectResult extends VerifyTxResult {
  txDigest: string;
  objectDigest: string | null;
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function hexToBytes(hex: string): Uint8Array {
  const clean = (hex.startsWith("0x") ? hex.slice(2) : hex).padStart(64, "0");
  const bytes = new Uint8Array(clean.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

function bytesToHex(bytes: Uint8Array): string {
  return "0x" + Array.from(bytes).map(b => b.toString(16).padStart(2, "0")).join("");
}

interface ParsedExecDigests {
  transaction: Uint8Array;
  effects: Uint8Array;
}

function parseContentsDigests(contentsBcs: Uint8Array): ParsedExecDigests[] {
  const parsed = bcsCheckpointContents.parse(contentsBcs);
  if ("V1" in parsed && parsed.V1) {
    return parsed.V1.transactions.map((t: { transaction: number[]; effects: number[] }) => ({
      transaction: Uint8Array.from(t.transaction),
      effects: Uint8Array.from(t.effects),
    }));
  }
  const v2 = (parsed as { V2: { transactions: { digest: { transaction: number[]; effects: number[] } }[] } }).V2;
  return v2.transactions.map((t) => ({
    transaction: Uint8Array.from(t.digest.transaction),
    effects: Uint8Array.from(t.digest.effects),
  }));
}

// ─── Core verification ──────────────────────────────────────────────────────

async function verifyCheckpointChain(
  raw: RawCheckpoint,
  grpcUrl: string,
): Promise<{ steps: VerifyStep[]; epoch: string; committee: PreparedCommittee }> {
  const steps: VerifyStep[] = [];
  const summaryBcs = raw.summary.bcs.value;
  const contentsBcs = raw.contents.bcs.value;
  const epoch = raw.signature.epoch;

  // Verify checkpoint signature
  const committee = await getCommittee(grpcUrl, epoch);
  const authSignature = {
    epoch: BigInt(epoch),
    signature: raw.signature.signature,
    signersMap: raw.signature.bitmap,
  };
  verifyCheckpoint(summaryBcs, authSignature, committee);
  steps.push({ label: "Checkpoint signature verified", detail: `epoch ${epoch}, ${committee.members.length} validators` });

  // Verify contents digest
  const summary = parseBcsSummary(summaryBcs);
  const computed = checkpointContentsDigest(contentsBcs);
  if (!digestsEqual(computed, summary.contentDigest)) {
    throw new Error("Checkpoint contents digest mismatch");
  }
  steps.push({ label: "Checkpoint contents verified", detail: `${raw.transactions.length} transactions` });

  return { steps, epoch, committee };
}

export async function verifyTransaction(
  digest: string,
  opts: VerifyOptions,
): Promise<VerifyTxResult> {
  const steps: VerifyStep[] = [];

  // 1. Look up which checkpoint contains this transaction (via gRPC)
  const client = getSuiClient(opts.rpcUrl);
  const { response: txLookup } = await client.ledgerService.getTransaction({
    digest,
    readMask: { paths: ["checkpoint"] },
  });
  const checkpointSeq = txLookup.transaction?.checkpoint?.toString();
  if (!checkpointSeq) throw new Error(`Transaction ${digest} not found or has no checkpoint`);
  steps.push({ label: "Transaction located", detail: `checkpoint ${checkpointSeq}` });

  // 2. Fetch raw checkpoint from archive
  const raw = await fetchRawCheckpoint(BigInt(checkpointSeq), opts.archiveUrl);
  steps.push({ label: "Checkpoint fetched from archive", detail: `${raw.transactions.length} transactions` });

  // 3. Verify checkpoint signature + contents
  const { steps: ckSteps, epoch, committee } = await verifyCheckpointChain(raw, opts.grpcUrl);
  steps.push(...ckSteps);

  // 4. Find transaction in contents
  const execDigests = parseContentsDigests(raw.contents.bcs.value);
  const { fromBase58 } = await import("@mysten/bcs");
  const txDigestBytes = fromBase58(digest);

  let txIndex = -1;
  for (let i = 0; i < execDigests.length; i++) {
    if (digestsEqual(execDigests[i]!.transaction, txDigestBytes)) {
      txIndex = i;
      break;
    }
  }
  if (txIndex === -1) throw new Error("Transaction not found in checkpoint contents");
  steps.push({ label: "Transaction found in checkpoint", detail: `index ${txIndex}` });

  // 5. Verify effects
  const effectsBcs = raw.transactions[txIndex]!.effects.bcs.value;
  const eventsDigest = verifyTransactionEffects(effectsBcs, execDigests[txIndex]!.effects);
  steps.push({ label: "Transaction effects verified", detail: `${effectsBcs.length} bytes` });

  // 6. Verify events (if present)
  let hasEvents = false;
  if (eventsDigest && raw.transactions[txIndex]!.events) {
    const eventsBcs = raw.transactions[txIndex]!.events!.bcs.value;
    verifyTransactionEvents(eventsBcs, eventsDigest);
    hasEvents = true;
    steps.push({ label: "Transaction events verified", detail: `${eventsBcs.length} bytes` });
  }

  return {
    steps,
    checkpointSeq,
    epoch,
    signers: committee.members.length,
    committeeSize: committee.members.length,
    hasEvents,
  };
}

export async function verifyObject(
  objectId: string,
  opts: VerifyOptions,
): Promise<VerifyObjectResult> {
  // 1. Look up which transaction last modified this object (via gRPC)
  const client = getSuiClient(opts.rpcUrl);
  const objResult = await client.core.getObject({
    objectId,
    include: { previousTransaction: true },
  });
  const txDigest = objResult.object?.previousTransaction;
  if (!txDigest) throw new Error(`Object ${objectId} not found`);

  // 2. Verify the transaction (full chain)
  const txResult = await verifyTransaction(txDigest, opts);

  // 3. Verify object is in effects
  const raw = await fetchRawCheckpoint(BigInt(txResult.checkpointSeq), opts.archiveUrl);
  const execDigests = parseContentsDigests(raw.contents.bcs.value);
  const { fromBase58 } = await import("@mysten/bcs");
  const txDigestBytes = fromBase58(txDigest);

  let txIndex = -1;
  for (let i = 0; i < execDigests.length; i++) {
    if (digestsEqual(execDigests[i]!.transaction, txDigestBytes)) {
      txIndex = i;
      break;
    }
  }

  const effectsBcs = raw.transactions[txIndex]!.effects.bcs.value;
  const objIdBytes = hexToBytes(objectId);
  const objDigestBytes = verifyObjectInEffects(objIdBytes, effectsBcs);
  const objectDigest = objDigestBytes ? bytesToHex(objDigestBytes) : null;

  txResult.steps.push({
    label: objectDigest ? "Object found in effects" : "Object deleted/wrapped in effects",
    detail: objectDigest ? `digest ${objectDigest.slice(0, 18)}...` : undefined,
  });

  return {
    ...txResult,
    txDigest,
    objectDigest,
  };
}
