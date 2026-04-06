import type { Database } from "bun:sqlite";

import type { ParsedBinaryCheckpoint } from "../../binary-parser.ts";

export interface EnabledSqlProcessors {
  balances: boolean;
  transactions: boolean;
  objectChanges: boolean;
  dependencies: boolean;
  inputs: boolean;
  commands: boolean;
  systemTransactions: boolean;
  unchangedConsensusObjects: boolean;
  events: boolean;
  checkpoints: boolean;
}

type SqliteQuery = ReturnType<Database["query"]>;

interface SnapshotShardTableDef {
  name: string;
  enabled: (enabled: EnabledSqlProcessors) => boolean;
  columns: Array<{ name: string; type: string }>;
}

interface SnapshotShardStatements {
  checkpoints: SqliteQuery | null;
  transactions: SqliteQuery | null;
  moveCalls: SqliteQuery | null;
  balanceChanges: SqliteQuery | null;
  objectChanges: SqliteQuery | null;
  dependencies: SqliteQuery | null;
  inputs: SqliteQuery | null;
  commands: SqliteQuery | null;
  systemTransactions: SqliteQuery | null;
  unchangedConsensusObjects: SqliteQuery | null;
}

function numOrNull(value: string | null | undefined): string | null {
  return value == null || value === "" ? null : value;
}

const SNAPSHOT_SHARD_TABLES: SnapshotShardTableDef[] = [
  {
    name: "checkpoints",
    enabled: enabled => enabled.checkpoints,
    columns: [
      { name: "sequence_number", type: "TEXT NOT NULL" },
      { name: "epoch", type: "TEXT NOT NULL" },
      { name: "digest", type: "TEXT NOT NULL" },
      { name: "previous_digest", type: "TEXT" },
      { name: "content_digest", type: "TEXT" },
      { name: "sui_timestamp", type: "TEXT NOT NULL" },
      { name: "total_network_transactions", type: "TEXT NOT NULL" },
      { name: "rolling_computation_cost", type: "TEXT NOT NULL" },
      { name: "rolling_storage_cost", type: "TEXT NOT NULL" },
      { name: "rolling_storage_rebate", type: "TEXT NOT NULL" },
      { name: "rolling_non_refundable_storage_fee", type: "TEXT NOT NULL" },
    ],
  },
  {
    name: "transactions",
    enabled: enabled => enabled.transactions,
    columns: [
      { name: "digest", type: "TEXT NOT NULL" },
      { name: "sender", type: "TEXT NOT NULL" },
      { name: "success", type: "INTEGER NOT NULL" },
      { name: "computation_cost", type: "TEXT NOT NULL" },
      { name: "storage_cost", type: "TEXT NOT NULL" },
      { name: "storage_rebate", type: "TEXT NOT NULL" },
      { name: "non_refundable_storage_fee", type: "TEXT" },
      { name: "move_call_count", type: "INTEGER NOT NULL DEFAULT 0" },
      { name: "checkpoint_seq", type: "TEXT NOT NULL" },
      { name: "sui_timestamp", type: "TEXT NOT NULL" },
      { name: "epoch", type: "TEXT NOT NULL DEFAULT '0'" },
      { name: "error_kind", type: "TEXT" },
      { name: "error_description", type: "TEXT" },
      { name: "error_command_index", type: "INTEGER" },
      { name: "error_abort_code", type: "TEXT" },
      { name: "error_module", type: "TEXT" },
      { name: "error_function", type: "TEXT" },
      { name: "events_digest", type: "TEXT" },
      { name: "lamport_version", type: "TEXT" },
      { name: "dependency_count", type: "INTEGER NOT NULL DEFAULT 0" },
    ],
  },
  {
    name: "move_calls",
    enabled: enabled => enabled.transactions,
    columns: [
      { name: "tx_digest", type: "TEXT NOT NULL" },
      { name: "call_index", type: "INTEGER NOT NULL" },
      { name: "package", type: "TEXT NOT NULL" },
      { name: "module", type: "TEXT NOT NULL" },
      { name: "function", type: "TEXT NOT NULL" },
      { name: "checkpoint_seq", type: "TEXT NOT NULL" },
      { name: "sui_timestamp", type: "TEXT NOT NULL" },
    ],
  },
  {
    name: "balance_changes",
    enabled: enabled => enabled.balances,
    columns: [
      { name: "tx_digest", type: "TEXT NOT NULL" },
      { name: "checkpoint_seq", type: "TEXT NOT NULL" },
      { name: "address", type: "TEXT NOT NULL" },
      { name: "coin_type", type: "TEXT NOT NULL" },
      { name: "amount", type: "TEXT NOT NULL" },
      { name: "sui_timestamp", type: "TEXT NOT NULL" },
    ],
  },
  {
    name: "object_changes",
    enabled: enabled => enabled.objectChanges,
    columns: [
      { name: "tx_digest", type: "TEXT NOT NULL" },
      { name: "object_id", type: "TEXT NOT NULL" },
      { name: "change_type", type: "TEXT NOT NULL" },
      { name: "object_type", type: "TEXT" },
      { name: "input_version", type: "TEXT" },
      { name: "input_digest", type: "TEXT" },
      { name: "input_owner", type: "TEXT" },
      { name: "input_owner_kind", type: "TEXT" },
      { name: "output_version", type: "TEXT" },
      { name: "output_digest", type: "TEXT" },
      { name: "output_owner", type: "TEXT" },
      { name: "output_owner_kind", type: "TEXT" },
      { name: "is_gas_object", type: "INTEGER NOT NULL" },
      { name: "checkpoint_seq", type: "TEXT NOT NULL" },
      { name: "sui_timestamp", type: "TEXT NOT NULL" },
    ],
  },
  {
    name: "transaction_dependencies",
    enabled: enabled => enabled.dependencies,
    columns: [
      { name: "tx_digest", type: "TEXT NOT NULL" },
      { name: "depends_on_digest", type: "TEXT NOT NULL" },
      { name: "checkpoint_seq", type: "TEXT NOT NULL" },
      { name: "sui_timestamp", type: "TEXT NOT NULL" },
    ],
  },
  {
    name: "transaction_inputs",
    enabled: enabled => enabled.inputs,
    columns: [
      { name: "tx_digest", type: "TEXT NOT NULL" },
      { name: "input_index", type: "INTEGER NOT NULL" },
      { name: "kind", type: "TEXT NOT NULL" },
      { name: "object_id", type: "TEXT" },
      { name: "version", type: "TEXT" },
      { name: "digest", type: "TEXT" },
      { name: "mutability", type: "TEXT" },
      { name: "initial_shared_version", type: "TEXT" },
      { name: "pure_bytes", type: "TEXT" },
      { name: "amount", type: "TEXT" },
      { name: "coin_type", type: "TEXT" },
      { name: "source", type: "TEXT" },
      { name: "checkpoint_seq", type: "TEXT NOT NULL" },
      { name: "sui_timestamp", type: "TEXT NOT NULL" },
    ],
  },
  {
    name: "commands",
    enabled: enabled => enabled.commands,
    columns: [
      { name: "tx_digest", type: "TEXT NOT NULL" },
      { name: "command_index", type: "INTEGER NOT NULL" },
      { name: "kind", type: "TEXT NOT NULL" },
      { name: "package", type: "TEXT" },
      { name: "module", type: "TEXT" },
      { name: "function", type: "TEXT" },
      { name: "type_arguments", type: "TEXT" },
      { name: "args", type: "TEXT" },
      { name: "checkpoint_seq", type: "TEXT NOT NULL" },
      { name: "sui_timestamp", type: "TEXT NOT NULL" },
    ],
  },
  {
    name: "system_transactions",
    enabled: enabled => enabled.systemTransactions,
    columns: [
      { name: "tx_digest", type: "TEXT NOT NULL" },
      { name: "kind", type: "TEXT NOT NULL" },
      { name: "data", type: "TEXT NOT NULL" },
      { name: "checkpoint_seq", type: "TEXT NOT NULL" },
      { name: "sui_timestamp", type: "TEXT NOT NULL" },
    ],
  },
  {
    name: "unchanged_consensus_objects",
    enabled: enabled => enabled.unchangedConsensusObjects,
    columns: [
      { name: "tx_digest", type: "TEXT NOT NULL" },
      { name: "object_id", type: "TEXT NOT NULL" },
      { name: "kind", type: "TEXT NOT NULL" },
      { name: "version", type: "TEXT" },
      { name: "digest", type: "TEXT" },
      { name: "object_type", type: "TEXT" },
      { name: "checkpoint_seq", type: "TEXT NOT NULL" },
      { name: "sui_timestamp", type: "TEXT NOT NULL" },
    ],
  },
];

function getEnabledTableDefs(enabled: EnabledSqlProcessors): SnapshotShardTableDef[] {
  return SNAPSHOT_SHARD_TABLES.filter(table => table.enabled(enabled));
}

function getInsertStatement(db: Database, table: SnapshotShardTableDef): SqliteQuery {
  const columns = table.columns.map(column => column.name);
  const placeholders = columns.map(() => "?").join(", ");
  return db.query(
    `INSERT INTO ${table.name} (${columns.join(", ")}) VALUES (${placeholders})`,
  );
}

export function configureSnapshotShardDatabase(db: Database): void {
  db.exec("PRAGMA synchronous = OFF");
  db.exec("PRAGMA journal_mode = OFF");
  db.exec("PRAGMA locking_mode = EXCLUSIVE");
  db.exec("PRAGMA temp_store = MEMORY");
  db.exec("PRAGMA page_size = 32768");
}

export function getSnapshotShardTableNames(enabled: EnabledSqlProcessors): string[] {
  return getEnabledTableDefs(enabled).map(table => table.name);
}

export function getSnapshotShardTableColumns(tableName: string): string[] {
  const table = SNAPSHOT_SHARD_TABLES.find(candidate => candidate.name === tableName);
  if (!table) {
    throw new Error(`Unknown snapshot shard table: ${tableName}`);
  }
  return table.columns.map(column => column.name);
}

export function createSnapshotShardTables(db: Database, enabled: EnabledSqlProcessors): void {
  for (const table of getEnabledTableDefs(enabled)) {
    const columns = table.columns
      .map(column => `${column.name} ${column.type}`)
      .join(",\n  ");
    db.exec(`CREATE TABLE IF NOT EXISTS ${table.name} (\n  ${columns}\n);`);
  }
}

export function prepareSnapshotShardStatements(
  db: Database,
  enabled: EnabledSqlProcessors,
): SnapshotShardStatements {
  const tableMap = new Map(getEnabledTableDefs(enabled).map(table => [table.name, table]));

  return {
    checkpoints: tableMap.has("checkpoints") ? getInsertStatement(db, tableMap.get("checkpoints")!) : null,
    transactions: tableMap.has("transactions") ? getInsertStatement(db, tableMap.get("transactions")!) : null,
    moveCalls: tableMap.has("move_calls") ? getInsertStatement(db, tableMap.get("move_calls")!) : null,
    balanceChanges: tableMap.has("balance_changes") ? getInsertStatement(db, tableMap.get("balance_changes")!) : null,
    objectChanges: tableMap.has("object_changes") ? getInsertStatement(db, tableMap.get("object_changes")!) : null,
    dependencies: tableMap.has("transaction_dependencies") ? getInsertStatement(db, tableMap.get("transaction_dependencies")!) : null,
    inputs: tableMap.has("transaction_inputs") ? getInsertStatement(db, tableMap.get("transaction_inputs")!) : null,
    commands: tableMap.has("commands") ? getInsertStatement(db, tableMap.get("commands")!) : null,
    systemTransactions: tableMap.has("system_transactions") ? getInsertStatement(db, tableMap.get("system_transactions")!) : null,
    unchangedConsensusObjects: tableMap.has("unchanged_consensus_objects")
      ? getInsertStatement(db, tableMap.get("unchanged_consensus_objects")!)
      : null,
  };
}

export function insertParsedCheckpointToSnapshotShard(
  statements: SnapshotShardStatements,
  parsed: ParsedBinaryCheckpoint,
): void {
  if (statements.checkpoints) {
    const checkpoint = parsed.checkpoint;
    statements.checkpoints.run(
      checkpoint.sequenceNumber.toString(),
      checkpoint.epoch.toString(),
      checkpoint.digest,
      checkpoint.previousDigest,
      checkpoint.contentDigest,
      checkpoint.timestamp.toISOString(),
      checkpoint.totalNetworkTransactions.toString(),
      checkpoint.epochRollingGasCostSummary.computationCost,
      checkpoint.epochRollingGasCostSummary.storageCost,
      checkpoint.epochRollingGasCostSummary.storageRebate,
      checkpoint.epochRollingGasCostSummary.nonRefundableStorageFee,
    );
  }

  if (statements.transactions) {
    for (const tx of parsed.processed.transactions) {
      statements.transactions.run(
        tx.digest,
        tx.sender,
        tx.success ? 1 : 0,
        tx.computationCost,
        tx.storageCost,
        tx.storageRebate,
        tx.nonRefundableStorageFee,
        tx.moveCallCount,
        tx.checkpointSeq.toString(),
        tx.timestamp.toISOString(),
        tx.epoch.toString(),
        tx.errorKind,
        tx.errorDescription,
        tx.errorCommandIndex,
        tx.errorAbortCode,
        tx.errorModule,
        tx.errorFunction,
        tx.eventsDigest,
        tx.lamportVersion,
        tx.dependencyCount,
      );
    }
  }

  if (statements.moveCalls) {
    for (const call of parsed.processed.moveCalls) {
      statements.moveCalls.run(
        call.txDigest,
        call.callIndex,
        call.package,
        call.module,
        call.function,
        call.checkpointSeq.toString(),
        call.timestamp.toISOString(),
      );
    }
  }

  if (statements.balanceChanges) {
    for (const change of parsed.processed.balanceChanges) {
      statements.balanceChanges.run(
        change.txDigest,
        change.checkpointSeq.toString(),
        change.address,
        change.coinType,
        change.amount,
        change.timestamp.toISOString(),
      );
    }
  }

  if (statements.objectChanges) {
    for (const change of parsed.processed.objectChanges) {
      statements.objectChanges.run(
        change.txDigest,
        change.objectId,
        change.changeType,
        change.objectType,
        numOrNull(change.inputVersion),
        change.inputDigest,
        change.inputOwner,
        change.inputOwnerKind,
        numOrNull(change.outputVersion),
        change.outputDigest,
        change.outputOwner,
        change.outputOwnerKind,
        change.isGasObject ? 1 : 0,
        change.checkpointSeq.toString(),
        change.timestamp.toISOString(),
      );
    }
  }

  if (statements.dependencies) {
    for (const dependency of parsed.processed.dependencies) {
      statements.dependencies.run(
        dependency.txDigest,
        dependency.dependsOnDigest,
        dependency.checkpointSeq.toString(),
        dependency.timestamp.toISOString(),
      );
    }
  }

  if (statements.inputs) {
    for (const input of parsed.processed.inputs) {
      statements.inputs.run(
        input.txDigest,
        input.inputIndex,
        input.kind,
        input.objectId,
        numOrNull(input.version),
        input.digest,
        input.mutability,
        numOrNull(input.initialSharedVersion),
        input.pureBytes,
        numOrNull(input.amount),
        input.coinType,
        input.source,
        input.checkpointSeq.toString(),
        input.timestamp.toISOString(),
      );
    }
  }

  if (statements.commands) {
    for (const command of parsed.processed.commands) {
      statements.commands.run(
        command.txDigest,
        command.commandIndex,
        command.kind,
        command.package,
        command.module,
        command.function,
        command.typeArguments,
        command.args,
        command.checkpointSeq.toString(),
        command.timestamp.toISOString(),
      );
    }
  }

  if (statements.systemTransactions) {
    for (const tx of parsed.processed.systemTransactions) {
      statements.systemTransactions.run(
        tx.txDigest,
        tx.kind,
        tx.data,
        tx.checkpointSeq.toString(),
        tx.timestamp.toISOString(),
      );
    }
  }

  if (statements.unchangedConsensusObjects) {
    for (const object of parsed.processed.unchangedConsensusObjects) {
      statements.unchangedConsensusObjects.run(
        object.txDigest,
        object.objectId,
        object.kind,
        numOrNull(object.version),
        object.digest,
        object.objectType,
        object.checkpointSeq.toString(),
        object.timestamp.toISOString(),
      );
    }
  }
}
