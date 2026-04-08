/**
 * Per-table SQLite storage for index-chain.
 *
 * Each table gets its own .db file with PKs and indexes created upfront.
 * No deferred indexes, no balance materialization, no conditional branches.
 * Prepared statements for maximum insert throughput.
 */
import { Database } from "bun:sqlite";
import { mkdirSync } from "fs";
import { join } from "path";
import type {
  Checkpoint,
  ProcessedCheckpoint,
  Storage,
  TransactionRecord,
  MoveCallRecord,
  BalanceChange,
  ObjectChangeRecord,
  TransactionDependencyRecord,
  TransactionInputRecord,
  CommandRecord,
  SystemTransactionRecord,
  UnchangedConsensusObjectRecord,
  RawEventRecord,
} from "../types.ts";

// ---------------------------------------------------------------------------
// Table definitions — static, no conditionals
// ---------------------------------------------------------------------------

export interface TableDef {
  name: string;
  ddl: string;
  columns: string[];
  mapRow: (record: any) => unknown[];
  getRecords: (cp: ProcessedCheckpoint) => any[];
}

export const TABLES: TableDef[] = [
  {
    name: "transactions",
    ddl: `
      CREATE TABLE IF NOT EXISTS transactions (
        digest TEXT NOT NULL PRIMARY KEY,
        sender TEXT NOT NULL,
        success INTEGER NOT NULL,
        computation_cost TEXT,
        storage_cost TEXT,
        storage_rebate TEXT,
        non_refundable_storage_fee TEXT,
        move_call_count INTEGER NOT NULL,
        checkpoint_seq TEXT NOT NULL,
        sui_timestamp TEXT NOT NULL,
        epoch TEXT NOT NULL,
        error_kind TEXT,
        error_description TEXT,
        error_command_index INTEGER,
        error_abort_code TEXT,
        error_module TEXT,
        error_function TEXT,
        events_digest TEXT,
        lamport_version TEXT,
        dependency_count INTEGER NOT NULL DEFAULT 0
      );
      CREATE INDEX IF NOT EXISTS idx_tx_sender ON transactions(sender);
      CREATE INDEX IF NOT EXISTS idx_tx_checkpoint ON transactions(checkpoint_seq);
      CREATE INDEX IF NOT EXISTS idx_tx_epoch ON transactions(epoch);
    `,
    columns: [
      "digest", "sender", "success",
      "computation_cost", "storage_cost", "storage_rebate", "non_refundable_storage_fee",
      "move_call_count", "checkpoint_seq", "sui_timestamp",
      "epoch", "error_kind", "error_description", "error_command_index",
      "error_abort_code", "error_module", "error_function",
      "events_digest", "lamport_version", "dependency_count",
    ],
    mapRow: (tx: TransactionRecord) => [
      tx.digest, tx.sender, tx.success ? 1 : 0,
      tx.computationCost ?? null, tx.storageCost ?? null,
      tx.storageRebate ?? null, tx.nonRefundableStorageFee ?? null,
      tx.moveCallCount, tx.checkpointSeq.toString(), tx.timestamp.toISOString(),
      (tx.epoch ?? 0n).toString(),
      tx.errorKind ?? null, tx.errorDescription ?? null,
      tx.errorCommandIndex ?? null, tx.errorAbortCode ?? null,
      tx.errorModule ?? null, tx.errorFunction ?? null,
      tx.eventsDigest ?? null, tx.lamportVersion ?? null,
      tx.dependencyCount ?? 0,
    ],
    getRecords: (cp) => cp.transactions,
  },
  {
    name: "move_calls",
    ddl: `
      CREATE TABLE IF NOT EXISTS move_calls (
        tx_digest TEXT NOT NULL,
        call_index INTEGER NOT NULL,
        package TEXT NOT NULL,
        module TEXT NOT NULL,
        function TEXT NOT NULL,
        checkpoint_seq TEXT NOT NULL,
        sui_timestamp TEXT NOT NULL,
        PRIMARY KEY (tx_digest, call_index)
      );
      CREATE INDEX IF NOT EXISTS idx_mc_package ON move_calls(package);
      CREATE INDEX IF NOT EXISTS idx_mc_module ON move_calls(package, module);
      CREATE INDEX IF NOT EXISTS idx_mc_function ON move_calls(package, module, function);
    `,
    columns: ["tx_digest", "call_index", "package", "module", "function", "checkpoint_seq", "sui_timestamp"],
    mapRow: (mc: MoveCallRecord) => [
      mc.txDigest, mc.callIndex, mc.package, mc.module, mc.function,
      mc.checkpointSeq.toString(), mc.timestamp.toISOString(),
    ],
    getRecords: (cp) => cp.moveCalls,
  },
  {
    name: "balance_changes",
    ddl: `
      CREATE TABLE IF NOT EXISTS balance_changes (
        tx_digest TEXT NOT NULL,
        checkpoint_seq TEXT NOT NULL,
        address TEXT NOT NULL,
        coin_type TEXT NOT NULL,
        amount TEXT NOT NULL,
        sui_timestamp TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_bc_address ON balance_changes(address);
      CREATE INDEX IF NOT EXISTS idx_bc_coin_type ON balance_changes(coin_type);
      CREATE INDEX IF NOT EXISTS idx_bc_checkpoint ON balance_changes(checkpoint_seq);
    `,
    columns: ["tx_digest", "checkpoint_seq", "address", "coin_type", "amount", "sui_timestamp"],
    mapRow: (bc: BalanceChange) => [
      bc.txDigest, bc.checkpointSeq.toString(), bc.address, bc.coinType,
      bc.amount, bc.timestamp.toISOString(),
    ],
    getRecords: (cp) => cp.balanceChanges,
  },
  {
    name: "object_changes",
    ddl: `
      CREATE TABLE IF NOT EXISTS object_changes (
        tx_digest TEXT NOT NULL,
        object_id TEXT NOT NULL,
        change_type TEXT NOT NULL,
        object_type TEXT,
        input_version TEXT,
        input_digest TEXT,
        input_owner TEXT,
        input_owner_kind TEXT,
        output_version TEXT,
        output_digest TEXT,
        output_owner TEXT,
        output_owner_kind TEXT,
        is_gas_object INTEGER NOT NULL DEFAULT 0,
        checkpoint_seq TEXT NOT NULL,
        sui_timestamp TEXT NOT NULL,
        PRIMARY KEY (tx_digest, object_id)
      );
      CREATE INDEX IF NOT EXISTS idx_oc_object_id ON object_changes(object_id);
      CREATE INDEX IF NOT EXISTS idx_oc_change_type ON object_changes(change_type);
      CREATE INDEX IF NOT EXISTS idx_oc_checkpoint ON object_changes(checkpoint_seq);
    `,
    columns: [
      "tx_digest", "object_id", "change_type", "object_type",
      "input_version", "input_digest", "input_owner", "input_owner_kind",
      "output_version", "output_digest", "output_owner", "output_owner_kind",
      "is_gas_object", "checkpoint_seq", "sui_timestamp",
    ],
    mapRow: (oc: ObjectChangeRecord) => [
      oc.txDigest, oc.objectId, oc.changeType, oc.objectType ?? null,
      oc.inputVersion ?? null, oc.inputDigest ?? null, oc.inputOwner ?? null, oc.inputOwnerKind ?? null,
      oc.outputVersion ?? null, oc.outputDigest ?? null, oc.outputOwner ?? null, oc.outputOwnerKind ?? null,
      oc.isGasObject ? 1 : 0,
      oc.checkpointSeq.toString(), oc.timestamp.toISOString(),
    ],
    getRecords: (cp) => cp.objectChanges,
  },
  {
    name: "transaction_dependencies",
    ddl: `
      CREATE TABLE IF NOT EXISTS transaction_dependencies (
        tx_digest TEXT NOT NULL,
        depends_on_digest TEXT NOT NULL,
        checkpoint_seq TEXT NOT NULL,
        sui_timestamp TEXT NOT NULL,
        PRIMARY KEY (tx_digest, depends_on_digest)
      );
      CREATE INDEX IF NOT EXISTS idx_dep_depends ON transaction_dependencies(depends_on_digest);
    `,
    columns: ["tx_digest", "depends_on_digest", "checkpoint_seq", "sui_timestamp"],
    mapRow: (dep: TransactionDependencyRecord) => [
      dep.txDigest, dep.dependsOnDigest, dep.checkpointSeq.toString(), dep.timestamp.toISOString(),
    ],
    getRecords: (cp) => cp.dependencies,
  },
  {
    name: "transaction_inputs",
    ddl: `
      CREATE TABLE IF NOT EXISTS transaction_inputs (
        tx_digest TEXT NOT NULL,
        input_index INTEGER NOT NULL,
        kind TEXT NOT NULL,
        object_id TEXT,
        version TEXT,
        digest TEXT,
        mutability TEXT,
        initial_shared_version TEXT,
        pure_bytes TEXT,
        amount TEXT,
        coin_type TEXT,
        source TEXT,
        checkpoint_seq TEXT NOT NULL,
        sui_timestamp TEXT NOT NULL,
        PRIMARY KEY (tx_digest, input_index)
      );
      CREATE INDEX IF NOT EXISTS idx_inp_kind ON transaction_inputs(kind);
    `,
    columns: [
      "tx_digest", "input_index", "kind", "object_id", "version", "digest",
      "mutability", "initial_shared_version", "pure_bytes", "amount", "coin_type", "source",
      "checkpoint_seq", "sui_timestamp",
    ],
    mapRow: (inp: TransactionInputRecord) => [
      inp.txDigest, inp.inputIndex, inp.kind,
      inp.objectId ?? null, inp.version ?? null, inp.digest ?? null,
      inp.mutability ?? null, inp.initialSharedVersion ?? null,
      inp.pureBytes ?? null, inp.amount ?? null, inp.coinType ?? null, inp.source ?? null,
      inp.checkpointSeq.toString(), inp.timestamp.toISOString(),
    ],
    getRecords: (cp) => cp.inputs,
  },
  {
    name: "commands",
    ddl: `
      CREATE TABLE IF NOT EXISTS commands (
        tx_digest TEXT NOT NULL,
        command_index INTEGER NOT NULL,
        kind TEXT NOT NULL,
        package TEXT,
        module TEXT,
        function TEXT,
        type_arguments TEXT,
        args TEXT,
        checkpoint_seq TEXT NOT NULL,
        sui_timestamp TEXT NOT NULL,
        PRIMARY KEY (tx_digest, command_index)
      );
      CREATE INDEX IF NOT EXISTS idx_cmd_kind ON commands(kind);
    `,
    columns: [
      "tx_digest", "command_index", "kind", "package", "module", "function",
      "type_arguments", "args", "checkpoint_seq", "sui_timestamp",
    ],
    mapRow: (cmd: CommandRecord) => [
      cmd.txDigest, cmd.commandIndex, cmd.kind,
      cmd.package ?? null, cmd.module ?? null, cmd.function ?? null,
      cmd.typeArguments ?? null, cmd.args ?? null,
      cmd.checkpointSeq.toString(), cmd.timestamp.toISOString(),
    ],
    getRecords: (cp) => cp.commands,
  },
  {
    name: "system_transactions",
    ddl: `
      CREATE TABLE IF NOT EXISTS system_transactions (
        tx_digest TEXT NOT NULL PRIMARY KEY,
        kind TEXT NOT NULL,
        data TEXT NOT NULL,
        checkpoint_seq TEXT NOT NULL,
        sui_timestamp TEXT NOT NULL
      );
    `,
    columns: ["tx_digest", "kind", "data", "checkpoint_seq", "sui_timestamp"],
    mapRow: (sys: SystemTransactionRecord) => [
      sys.txDigest, sys.kind, sys.data, sys.checkpointSeq.toString(), sys.timestamp.toISOString(),
    ],
    getRecords: (cp) => cp.systemTransactions,
  },
  {
    name: "unchanged_consensus_objects",
    ddl: `
      CREATE TABLE IF NOT EXISTS unchanged_consensus_objects (
        tx_digest TEXT NOT NULL,
        object_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        version TEXT,
        digest TEXT,
        object_type TEXT,
        checkpoint_seq TEXT NOT NULL,
        sui_timestamp TEXT NOT NULL,
        PRIMARY KEY (tx_digest, object_id)
      );
      CREATE INDEX IF NOT EXISTS idx_uco_object_id ON unchanged_consensus_objects(object_id);
    `,
    columns: ["tx_digest", "object_id", "kind", "version", "digest", "object_type", "checkpoint_seq", "sui_timestamp"],
    mapRow: (uco: UnchangedConsensusObjectRecord) => [
      uco.txDigest, uco.objectId, uco.kind,
      uco.version ?? null, uco.digest ?? null, uco.objectType ?? null,
      uco.checkpointSeq.toString(), uco.timestamp.toISOString(),
    ],
    getRecords: (cp) => cp.unchangedConsensusObjects,
  },
  {
    name: "checkpoints",
    ddl: `
      CREATE TABLE IF NOT EXISTS checkpoints (
        sequence_number TEXT NOT NULL PRIMARY KEY,
        epoch TEXT NOT NULL,
        digest TEXT NOT NULL,
        previous_digest TEXT,
        content_digest TEXT,
        sui_timestamp TEXT NOT NULL,
        total_network_transactions TEXT NOT NULL,
        rolling_computation_cost TEXT NOT NULL,
        rolling_storage_cost TEXT NOT NULL,
        rolling_storage_rebate TEXT NOT NULL,
        rolling_non_refundable_storage_fee TEXT NOT NULL
      );
    `,
    columns: [
      "sequence_number", "epoch", "digest", "previous_digest", "content_digest",
      "sui_timestamp", "total_network_transactions",
      "rolling_computation_cost", "rolling_storage_cost", "rolling_storage_rebate",
      "rolling_non_refundable_storage_fee",
    ],
    mapRow: (cp: Checkpoint) => [
      cp.sequenceNumber.toString(), (cp.epoch ?? 0n).toString(),
      cp.digest ?? "", cp.previousDigest ?? null, cp.contentDigest ?? null,
      cp.timestamp.toISOString(), (cp.totalNetworkTransactions ?? 0n).toString(),
      cp.epochRollingGasCostSummary?.computationCost ?? "0",
      cp.epochRollingGasCostSummary?.storageCost ?? "0",
      cp.epochRollingGasCostSummary?.storageRebate ?? "0",
      cp.epochRollingGasCostSummary?.nonRefundableStorageFee ?? "0",
    ],
    getRecords: (cp) => [cp.checkpoint],
  },
  {
    name: "raw_events",
    ddl: `
      CREATE TABLE IF NOT EXISTS raw_events (
        tx_digest TEXT NOT NULL,
        event_seq INTEGER NOT NULL,
        package_id TEXT NOT NULL,
        module TEXT NOT NULL,
        event_type TEXT NOT NULL,
        sender TEXT NOT NULL,
        contents TEXT NOT NULL,
        checkpoint_seq TEXT NOT NULL,
        sui_timestamp TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_re_type ON raw_events(event_type);
      CREATE INDEX IF NOT EXISTS idx_re_package ON raw_events(package_id);
      CREATE INDEX IF NOT EXISTS idx_re_checkpoint ON raw_events(checkpoint_seq);
    `,
    columns: [
      "tx_digest", "event_seq", "package_id", "module", "event_type",
      "sender", "contents", "checkpoint_seq", "sui_timestamp",
    ],
    mapRow: (ev: RawEventRecord) => [
      ev.txDigest, ev.eventSeq, ev.packageId, ev.module, ev.eventType,
      ev.sender, ev.contents, ev.checkpointSeq.toString(), ev.timestamp.toISOString(),
    ],
    getRecords: (cp) => cp.rawEvents,
  },
];

// Map table name → file name (raw_events goes into events.db)
const TABLE_FILE: Record<string, string> = {
  raw_events: "events",
};

// ---------------------------------------------------------------------------
// Storage implementation
// ---------------------------------------------------------------------------

// Map table name → ExtractMask bit for filtering
export const TABLE_MASK_BIT: Record<string, number> = {
  transactions: 1 << 0,
  move_calls: 1 << 1,
  balance_changes: 1 << 2,
  object_changes: 1 << 3,
  transaction_dependencies: 1 << 4,
  transaction_inputs: 1 << 5,
  commands: 1 << 6,
  system_transactions: 1 << 7,
  unchanged_consensus_objects: 1 << 8,
  checkpoints: 1 << 9,
  raw_events: 1 << 10,
};

export function createPerTableSqliteStorage(dir: string, enabledMask = 0x7FF): Storage {
  mkdirSync(dir, { recursive: true });
  const enabledTables = TABLES.filter(t => enabledMask & (TABLE_MASK_BIT[t.name] ?? 0));

  const connections = new Map<string, Database>();
  const stmts = new Map<string, ReturnType<Database["query"]>>();

  function getDb(tableDef: TableDef): Database {
    const fileKey = TABLE_FILE[tableDef.name] ?? tableDef.name;
    let db = connections.get(fileKey);
    if (!db) {
      db = new Database(join(dir, `${fileKey}.db`));
      db.exec("PRAGMA synchronous = OFF");
      db.exec("PRAGMA journal_mode = OFF");
      db.exec("PRAGMA locking_mode = EXCLUSIVE");
      db.exec("PRAGMA temp_store = MEMORY");
      db.exec("PRAGMA page_size = 32768");
      connections.set(fileKey, db);
    }
    return db;
  }

  return {
    name: "per-table-sqlite",

    async initialize(): Promise<void> {
      for (const tableDef of enabledTables) {
        const db = getDb(tableDef);
        // DDL can contain multiple statements (CREATE TABLE + CREATE INDEX)
        for (const stmt of tableDef.ddl.split(";").map(s => s.trim()).filter(Boolean)) {
          db.exec(stmt);
        }
        // Prepare INSERT statement
        const placeholders = tableDef.columns.map(() => "?").join(", ");
        const insertSql = `INSERT OR IGNORE INTO ${tableDef.name} (${tableDef.columns.join(", ")}) VALUES (${placeholders})`;
        stmts.set(tableDef.name, db.query(insertSql));
      }
    },

    async write(batch: ProcessedCheckpoint[]): Promise<void> {
      // BEGIN on all open connections
      for (const db of connections.values()) db.exec("BEGIN");

      try {
        for (const tableDef of enabledTables) {
          const stmt = stmts.get(tableDef.name);
          if (!stmt) continue;

          for (const cp of batch) {
            const records = tableDef.getRecords(cp);
            for (const record of records) {
              stmt.run(...tableDef.mapRow(record));
            }
          }
        }

        // COMMIT on all connections
        for (const db of connections.values()) db.exec("COMMIT");
      } catch (e) {
        for (const db of connections.values()) {
          try { db.exec("ROLLBACK"); } catch {}
        }
        throw e;
      }
    },

    async shutdown(): Promise<void> {
      for (const db of connections.values()) db.close();
      connections.clear();
      stmts.clear();
    },
  };
}

/**
 * SQLite storage for live streaming (stream-chain).
 *
 * WAL journal mode allows concurrent readers while writing.
 * NORMAL synchronous is safe — crash-safe but no fsync on every commit.
 * No EXCLUSIVE lock so other processes can read the DB concurrently.
 * INSERT OR IGNORE for idempotency (live stream may replay on reconnect).
 */
export function createLiveSqliteStorage(dir: string, enabledMask = 0x7FF): Storage {
  mkdirSync(dir, { recursive: true });
  const enabledTables = TABLES.filter(t => enabledMask & (TABLE_MASK_BIT[t.name] ?? 0));

  const connections = new Map<string, Database>();
  const stmts = new Map<string, ReturnType<Database["query"]>>();

  function getDb(tableDef: TableDef): Database {
    const fileKey = TABLE_FILE[tableDef.name] ?? tableDef.name;
    let db = connections.get(fileKey);
    if (!db) {
      db = new Database(join(dir, `${fileKey}.db`));
      db.exec("PRAGMA journal_mode = WAL");
      db.exec("PRAGMA synchronous = NORMAL");
      db.exec("PRAGMA temp_store = MEMORY");
      db.exec("PRAGMA cache_size = -32000");
      connections.set(fileKey, db);
    }
    return db;
  }

  return {
    name: "live-sqlite",

    async initialize(): Promise<void> {
      for (const tableDef of enabledTables) {
        const db = getDb(tableDef);
        for (const stmt of tableDef.ddl.split(";").map(s => s.trim()).filter(Boolean)) {
          db.exec(stmt);
        }
        const placeholders = tableDef.columns.map(() => "?").join(", ");
        const insertSql = `INSERT OR IGNORE INTO ${tableDef.name} (${tableDef.columns.join(", ")}) VALUES (${placeholders})`;
        stmts.set(tableDef.name, db.query(insertSql));
      }
    },

    async write(batch: ProcessedCheckpoint[]): Promise<void> {
      for (const db of connections.values()) db.exec("BEGIN");
      try {
        for (const tableDef of enabledTables) {
          const stmt = stmts.get(tableDef.name);
          if (!stmt) continue;
          for (const cp of batch) {
            for (const record of tableDef.getRecords(cp)) {
              stmt.run(...tableDef.mapRow(record));
            }
          }
        }
        for (const db of connections.values()) db.exec("COMMIT");
      } catch (e) {
        for (const db of connections.values()) {
          try { db.exec("ROLLBACK"); } catch {}
        }
        throw e;
      }
    },

    async shutdown(): Promise<void> {
      for (const db of connections.values()) db.close();
      connections.clear();
      stmts.clear();
    },
  };
}
