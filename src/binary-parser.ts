import type { GrpcTransaction } from "./grpc.ts";
import type {
  BalanceChange,
  Checkpoint,
  CommandRecord,
  DecodedEvent,
  MoveCallRecord,
  ObjectChangeRecord,
  ProcessedCheckpoint,
  RawEventRecord,
  SystemTransactionRecord,
  TransactionDependencyRecord,
  TransactionInputRecord,
  TransactionRecord,
  UnchangedConsensusObjectRecord,
} from "./pipeline/types.ts";

const NULL_STR = 0xFFFF;
const textDecoder = new TextDecoder();

export interface ParsedBinaryCheckpoint {
  checkpoint: Checkpoint;
  processed: ProcessedCheckpoint;
}

export function parseBinaryCheckpoint(buf: Uint8Array): ParsedBinaryCheckpoint {
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  let pos = 0;

  function readU8(): number {
    const value = buf[pos]!;
    pos += 1;
    return value;
  }

  function readU16(): number {
    const value = view.getUint16(pos, true);
    pos += 2;
    return value;
  }

  function readU32(): number {
    const value = view.getUint32(pos, true);
    pos += 4;
    return value;
  }

  function readStr(): string {
    const len = readU16();
    const value = textDecoder.decode(buf.subarray(pos, pos + len));
    pos += len;
    return value;
  }

  function readOptStr(): string | null {
    const len = readU16();
    if (len === NULL_STR) return null;
    const value = textDecoder.decode(buf.subarray(pos, pos + len));
    pos += len;
    return value;
  }

  function readBool(): boolean {
    return readU8() === 1;
  }

  function parseBigInt(value: string | null | undefined): bigint {
    if (!value) return 0n;
    return BigInt(value);
  }

  function parseDate(value: string): Date {
    return new Date(value);
  }

  // Header: 10 record counts
  const numTx = readU32();
  const numOc = readU32();
  const numDeps = readU32();
  const numCmds = readU32();
  const numSysTx = readU32();
  const numMc = readU32();
  const numInputs = readU32();
  const numUco = readU32();
  const numEvents = readU32();
  const numBal = readU32();

  // Checkpoint summary
  const sequenceNumber = readStr();
  const epoch = readStr();
  const timestamp = readStr();
  const digest = readStr();
  const previousDigest = readOptStr();
  const contentDigest = readOptStr();
  const totalNetworkTransactions = readStr();
  const rollingComputationCost = readStr();
  const rollingStorageCost = readStr();
  const rollingStorageRebate = readStr();
  const rollingNonRefundableStorageFee = readStr();

  const transactions: TransactionRecord[] = [];
  for (let i = 0; i < numTx; i++) {
    transactions.push({
      digest: readStr(),
      sender: readStr(),
      success: readBool(),
      computationCost: readStr(),
      storageCost: readStr(),
      storageRebate: readStr(),
      nonRefundableStorageFee: readStr(),
      checkpointSeq: parseBigInt(readStr()),
      timestamp: parseDate(readStr()),
      moveCallCount: readU32(),
      epoch: parseBigInt(readStr()),
      errorKind: readOptStr(),
      errorDescription: readOptStr(),
      errorCommandIndex: ((value: number) => value === 0xFF ? null : value)(readU8()),
      errorAbortCode: readOptStr(),
      errorModule: readOptStr(),
      errorFunction: readOptStr(),
      eventsDigest: readOptStr(),
      lamportVersion: readOptStr(),
      dependencyCount: readU32(),
    });
  }

  const objectChanges: ObjectChangeRecord[] = [];
  for (let i = 0; i < numOc; i++) {
    objectChanges.push({
      txDigest: readStr(),
      objectId: readStr(),
      changeType: readStr() as ObjectChangeRecord["changeType"],
      objectType: readOptStr(),
      inputVersion: readOptStr(),
      inputDigest: readOptStr(),
      inputOwner: readOptStr(),
      inputOwnerKind: readOptStr(),
      outputVersion: readOptStr(),
      outputDigest: readOptStr(),
      outputOwner: readOptStr(),
      outputOwnerKind: readOptStr(),
      isGasObject: readBool(),
      checkpointSeq: parseBigInt(readStr()),
      timestamp: parseDate(readStr()),
    });
  }

  const dependencies: TransactionDependencyRecord[] = [];
  for (let i = 0; i < numDeps; i++) {
    dependencies.push({
      txDigest: readStr(),
      dependsOnDigest: readStr(),
      checkpointSeq: parseBigInt(readStr()),
      timestamp: parseDate(readStr()),
    });
  }

  const commands: CommandRecord[] = [];
  for (let i = 0; i < numCmds; i++) {
    commands.push({
      txDigest: readStr(),
      commandIndex: readU32(),
      kind: readStr(),
      package: readOptStr(),
      module: readOptStr(),
      function: readOptStr(),
      typeArguments: readOptStr(),
      args: readOptStr(),
      checkpointSeq: parseBigInt(readStr()),
      timestamp: parseDate(readStr()),
    });
  }

  const systemTransactions: SystemTransactionRecord[] = [];
  for (let i = 0; i < numSysTx; i++) {
    systemTransactions.push({
      txDigest: readStr(),
      kind: readStr(),
      data: readStr(),
      checkpointSeq: parseBigInt(readStr()),
      timestamp: parseDate(readStr()),
    });
  }

  const moveCalls: MoveCallRecord[] = [];
  for (let i = 0; i < numMc; i++) {
    moveCalls.push({
      txDigest: readStr(),
      callIndex: readU32(),
      package: readStr(),
      module: readStr(),
      function: readStr(),
      checkpointSeq: parseBigInt(readStr()),
      timestamp: parseDate(readStr()),
    });
  }

  const inputs: TransactionInputRecord[] = [];
  for (let i = 0; i < numInputs; i++) {
    inputs.push({
      txDigest: readStr(),
      inputIndex: readU32(),
      kind: readStr(),
      objectId: readOptStr(),
      version: readOptStr(),
      digest: readOptStr(),
      mutability: readOptStr(),
      initialSharedVersion: readOptStr(),
      pureBytes: readOptStr(),
      amount: readOptStr(),
      coinType: readOptStr(),
      source: readOptStr(),
      checkpointSeq: parseBigInt(readStr()),
      timestamp: parseDate(readStr()),
    });
  }

  const unchangedConsensusObjects: UnchangedConsensusObjectRecord[] = [];
  for (let i = 0; i < numUco; i++) {
    unchangedConsensusObjects.push({
      txDigest: readStr(),
      objectId: readStr(),
      kind: readStr(),
      version: readOptStr(),
      digest: readOptStr(),
      objectType: readOptStr(),
      checkpointSeq: parseBigInt(readStr()),
      timestamp: parseDate(readStr()),
    });
  }

  // Raw event records — generic schema, no BCS decoding
  const rawEvents: RawEventRecord[] = [];
  for (let i = 0; i < numEvents; i++) {
    const eventType = readStr();
    const checkpointSeq = parseBigInt(readStr());
    const txDigest = readStr();
    const eventSeq = readU32();
    const sender = readStr();
    const timestamp = parseDate(readStr());
    const dataJson = readStr();

    // data_json: {"packageId":"0x...","module":"...","eventType":"...","contents":"0x..."}
    let packageId = "";
    let module = "";
    let contents = "";
    try {
      const data = JSON.parse(dataJson);
      packageId = data.packageId ?? "";
      module = data.module ?? "";
      contents = data.contents ?? "";
    } catch {}

    rawEvents.push({
      txDigest,
      eventSeq,
      packageId,
      module,
      eventType,
      sender,
      contents,
      checkpointSeq,
      timestamp,
    });
  }

  const balanceChanges: BalanceChange[] = [];
  for (let i = 0; i < numBal; i++) {
    balanceChanges.push({
      txDigest: readStr(),
      checkpointSeq: parseBigInt(readStr()),
      address: readStr(),
      coinType: readStr(),
      amount: readStr(),
      timestamp: parseDate(readStr()),
    });
  }

  const checkpointTimestamp = parseDate(timestamp);
  const checkpointTransactions: GrpcTransaction[] = transactions.map(tx => ({
    digest: tx.digest,
    transaction: { sender: tx.sender },
    events: null,
    effects: {
      status: { success: tx.success },
      gasUsed: {
        computationCost: tx.computationCost,
        storageCost: tx.storageCost,
        storageRebate: tx.storageRebate,
        nonRefundableStorageFee: tx.nonRefundableStorageFee ?? undefined,
      },
      epoch: tx.epoch.toString(),
      eventsDigest: tx.eventsDigest ?? undefined,
      lamportVersion: tx.lamportVersion ?? undefined,
    },
  }));

  const checkpoint: Checkpoint = {
    sequenceNumber: parseBigInt(sequenceNumber),
    timestamp: checkpointTimestamp,
    transactions: checkpointTransactions,
    source: "backfill",
    epoch: parseBigInt(epoch),
    digest,
    previousDigest,
    contentDigest,
    totalNetworkTransactions: parseBigInt(totalNetworkTransactions),
    epochRollingGasCostSummary: {
      computationCost: rollingComputationCost,
      storageCost: rollingStorageCost,
      storageRebate: rollingStorageRebate,
      nonRefundableStorageFee: rollingNonRefundableStorageFee,
    },
  };

  const processed: ProcessedCheckpoint = {
    checkpoint,
    events: [] as DecodedEvent[],
    balanceChanges,
    transactions,
    moveCalls,
    objectChanges,
    dependencies,
    inputs,
    commands,
    systemTransactions,
    unchangedConsensusObjects,
    rawEvents,
  };

  return { checkpoint, processed };
}
