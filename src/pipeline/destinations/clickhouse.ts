/**
 * ClickHouse storage destination.
 *
 * MergeTree tables optimized for analytics:
 * - PARTITION BY toYYYYMM(sui_timestamp) — monthly, per ClickHouse best practices
 * - ORDER BY with low-cardinality columns first for compression + index efficiency
 * - bloom_filter skipping indexes on high-cardinality filter columns
 * - No Nullable except where null is semantically distinct (version fields)
 * - SummingMergeTree materialized view for running balances
 *
 * Requires: bun add @clickhouse/client
 *
 * Usage:
 *   pipeline.storage(createClickHouseStorage("http://localhost:8123", "jun"))
 *
 * Query running balances (always use SUM + GROUP BY — merges are async):
 *   SELECT address, coin_type, sum(balance) AS balance
 *   FROM balances
 *   WHERE address = '0x...'
 *   GROUP BY address, coin_type
 */
import { createClient } from "@clickhouse/client";
import type { ClickHouseClient } from "@clickhouse/client";
import { parseBinaryCheckpoint } from "../../binary-parser.ts";
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
// Helpers
// ---------------------------------------------------------------------------

/** Format a Date for ClickHouse DateTime64(3, 'UTC'): '2024-01-15 10:30:00.123' */
function fmtDate(d: Date): string {
  // toISOString() → '2024-01-15T10:30:00.123Z' → remove T and Z
  return d.toISOString().replace("T", " ").slice(0, -1);
}

// ---------------------------------------------------------------------------
// Table definitions
// ---------------------------------------------------------------------------

interface ChTableDef {
  name: string;
  ddl: string;
  getRecords(cp: ProcessedCheckpoint): unknown[];
  mapRow(record: unknown, checkpoint: Checkpoint): Record<string, unknown>;
}

const TABLES: ChTableDef[] = [
  // ── transactions ──────────────────────────────────────────────────────────
  {
    name: "transactions",
    ddl: `
      CREATE TABLE IF NOT EXISTS transactions (
        digest                      String,
        sender                      String,
        success                     UInt8,
        computation_cost            UInt64,
        storage_cost                UInt64,
        storage_rebate              UInt64,
        non_refundable_storage_fee  UInt64 DEFAULT 0,
        move_call_count             UInt32,
        checkpoint_seq              UInt64,
        sui_timestamp               DateTime64(3, 'UTC'),
        epoch                       UInt64,
        error_kind                  String DEFAULT '',
        error_description           String DEFAULT '',
        error_command_index         UInt32 DEFAULT 4294967295,
        error_abort_code            String DEFAULT '',
        error_module                String DEFAULT '',
        error_function              String DEFAULT '',
        events_digest               String DEFAULT '',
        lamport_version             UInt64 DEFAULT 0,
        dependency_count            UInt32 DEFAULT 0,
        INDEX idx_sender sender TYPE bloom_filter GRANULARITY 4,
        INDEX idx_digest digest TYPE bloom_filter GRANULARITY 1
      ) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(sui_timestamp)
      ORDER BY (success, epoch, checkpoint_seq, digest)
    `,
    getRecords: (cp) => cp.transactions,
    mapRow: (r) => {
      const tx = r as TransactionRecord;
      return {
        digest:                     tx.digest,
        sender:                     tx.sender,
        success:                    tx.success ? 1 : 0,
        computation_cost:           tx.computationCost,
        storage_cost:               tx.storageCost,
        storage_rebate:             tx.storageRebate,
        non_refundable_storage_fee: tx.nonRefundableStorageFee ?? "0",
        move_call_count:            tx.moveCallCount,
        checkpoint_seq:             tx.checkpointSeq.toString(),
        sui_timestamp:              fmtDate(tx.timestamp),
        epoch:                      tx.epoch.toString(),
        error_kind:                 tx.errorKind ?? "",
        error_description:          tx.errorDescription ?? "",
        error_command_index:        tx.errorCommandIndex ?? 4294967295,
        error_abort_code:           tx.errorAbortCode ?? "",
        error_module:               tx.errorModule ?? "",
        error_function:             tx.errorFunction ?? "",
        events_digest:              tx.eventsDigest ?? "",
        lamport_version:            tx.lamportVersion ?? "0",
        dependency_count:           tx.dependencyCount ?? 0,
      };
    },
  },

  // ── move_calls ────────────────────────────────────────────────────────────
  {
    name: "move_calls",
    ddl: `
      CREATE TABLE IF NOT EXISTS move_calls (
        tx_digest      String,
        call_index     UInt32,
        package        String,
        module         String,
        function       String,
        checkpoint_seq UInt64,
        epoch          UInt64,
        sui_timestamp  DateTime64(3, 'UTC'),
      ) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(sui_timestamp)
      ORDER BY (package, module, function, checkpoint_seq)
    `,
    getRecords: (cp) => cp.moveCalls,
    mapRow: (r, checkpoint) => {
      const mc = r as MoveCallRecord;
      return {
        tx_digest:      mc.txDigest,
        call_index:     mc.callIndex,
        package:        mc.package,
        module:         mc.module,
        function:       mc.function,
        checkpoint_seq: mc.checkpointSeq.toString(),
        epoch:          checkpoint.epoch.toString(),
        sui_timestamp:  fmtDate(mc.timestamp),
      };
    },
  },

  // ── balance_changes ───────────────────────────────────────────────────────
  {
    name: "balance_changes",
    ddl: `
      CREATE TABLE IF NOT EXISTS balance_changes (
        tx_digest      String,
        checkpoint_seq UInt64,
        epoch          UInt64,
        address        String,
        coin_type      String,
        amount         Int128,
        sui_timestamp  DateTime64(3, 'UTC'),
        -- address is the leading ORDER BY key; primary index handles WHERE address = X.
        -- coin_type skip index helps cross-address queries: WHERE coin_type = '0x2::sui::SUI'.
        INDEX idx_coin_type coin_type TYPE bloom_filter GRANULARITY 4
      ) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(sui_timestamp)
      ORDER BY (address, coin_type, checkpoint_seq)
    `,
    getRecords: (cp) => cp.balanceChanges,
    mapRow: (r, checkpoint) => {
      const bc = r as BalanceChange;
      return {
        tx_digest:      bc.txDigest,
        checkpoint_seq: bc.checkpointSeq.toString(),
        epoch:          checkpoint.epoch.toString(),
        address:        bc.address,
        coin_type:      bc.coinType,
        amount:         bc.amount,
        sui_timestamp:  fmtDate(bc.timestamp),
      };
    },
  },

  // ── object_changes ────────────────────────────────────────────────────────
  // input/output version use Nullable(UInt64): null is semantically distinct
  // (CREATED = no prior version, DELETED = no output version).
  {
    name: "object_changes",
    ddl: `
      CREATE TABLE IF NOT EXISTS object_changes (
        tx_digest         String,
        object_id         String,
        change_type       LowCardinality(String),
        object_type       String DEFAULT '',
        input_version     Nullable(UInt64),
        input_digest      String DEFAULT '',
        input_owner       String DEFAULT '',
        input_owner_kind  LowCardinality(String) DEFAULT '',
        output_version    Nullable(UInt64),
        output_digest     String DEFAULT '',
        output_owner      String DEFAULT '',
        output_owner_kind LowCardinality(String) DEFAULT '',
        is_gas_object     UInt8,
        checkpoint_seq    UInt64,
        epoch             UInt64,
        sui_timestamp     DateTime64(3, 'UTC'),
        INDEX idx_object_id object_id TYPE bloom_filter GRANULARITY 4
      ) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(sui_timestamp)
      ORDER BY (change_type, checkpoint_seq, object_id)
    `,
    getRecords: (cp) => cp.objectChanges,
    mapRow: (r, checkpoint) => {
      const oc = r as ObjectChangeRecord;
      return {
        tx_digest:         oc.txDigest,
        object_id:         oc.objectId,
        change_type:       oc.changeType,
        object_type:       oc.objectType ?? "",
        input_version:     oc.inputVersion ?? null,
        input_digest:      oc.inputDigest ?? "",
        input_owner:       oc.inputOwner ?? "",
        input_owner_kind:  oc.inputOwnerKind ?? "",
        output_version:    oc.outputVersion ?? null,
        output_digest:     oc.outputDigest ?? "",
        output_owner:      oc.outputOwner ?? "",
        output_owner_kind: oc.outputOwnerKind ?? "",
        is_gas_object:     oc.isGasObject ? 1 : 0,
        checkpoint_seq:    oc.checkpointSeq.toString(),
        epoch:             checkpoint.epoch.toString(),
        sui_timestamp:     fmtDate(oc.timestamp),
      };
    },
  },

  // ── transaction_dependencies ──────────────────────────────────────────────
  {
    name: "transaction_dependencies",
    ddl: `
      CREATE TABLE IF NOT EXISTS transaction_dependencies (
        tx_digest         String,
        depends_on_digest String,
        checkpoint_seq    UInt64,
        epoch             UInt64,
        sui_timestamp     DateTime64(3, 'UTC')
      ) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(sui_timestamp)
      ORDER BY (checkpoint_seq, tx_digest, depends_on_digest)
    `,
    getRecords: (cp) => cp.dependencies,
    mapRow: (r, checkpoint) => {
      const dep = r as TransactionDependencyRecord;
      return {
        tx_digest:         dep.txDigest,
        depends_on_digest: dep.dependsOnDigest,
        checkpoint_seq:    dep.checkpointSeq.toString(),
        epoch:             checkpoint.epoch.toString(),
        sui_timestamp:     fmtDate(dep.timestamp),
      };
    },
  },

  // ── transaction_inputs ────────────────────────────────────────────────────
  // version uses Nullable(UInt64): PURE inputs have no object version.
  {
    name: "transaction_inputs",
    ddl: `
      CREATE TABLE IF NOT EXISTS transaction_inputs (
        tx_digest              String,
        input_index            UInt32,
        kind                   LowCardinality(String),
        object_id              String DEFAULT '',
        version                Nullable(UInt64),
        digest                 String DEFAULT '',
        mutability             LowCardinality(String) DEFAULT '',
        initial_shared_version Nullable(UInt64),
        pure_bytes             String DEFAULT '',
        amount                 Nullable(UInt64),
        coin_type              String DEFAULT '',
        source                 String DEFAULT '',
        checkpoint_seq         UInt64,
        epoch                  UInt64,
        sui_timestamp          DateTime64(3, 'UTC'),
        INDEX idx_object_id object_id TYPE bloom_filter GRANULARITY 4
      ) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(sui_timestamp)
      ORDER BY (kind, checkpoint_seq, tx_digest, input_index)
    `,
    getRecords: (cp) => cp.inputs,
    mapRow: (r, checkpoint) => {
      const inp = r as TransactionInputRecord;
      return {
        tx_digest:              inp.txDigest,
        input_index:            inp.inputIndex,
        kind:                   inp.kind,
        object_id:              inp.objectId ?? "",
        version:                inp.version ?? null,
        digest:                 inp.digest ?? "",
        mutability:             inp.mutability ?? "",
        initial_shared_version: inp.initialSharedVersion ?? null,
        pure_bytes:             inp.pureBytes ?? "",
        amount:                 inp.amount !== null ? BigInt(inp.amount).toString() : null,
        coin_type:              inp.coinType ?? "",
        source:                 inp.source ?? "",
        checkpoint_seq:         inp.checkpointSeq.toString(),
        epoch:                  checkpoint.epoch.toString(),
        sui_timestamp:          fmtDate(inp.timestamp),
      };
    },
  },

  // ── commands ──────────────────────────────────────────────────────────────
  {
    name: "commands",
    ddl: `
      CREATE TABLE IF NOT EXISTS commands (
        tx_digest      String,
        command_index  UInt32,
        kind           LowCardinality(String),
        package        String DEFAULT '',
        module         String DEFAULT '',
        function       String DEFAULT '',
        type_arguments String DEFAULT '',
        args           String DEFAULT '',
        checkpoint_seq UInt64,
        epoch          UInt64,
        sui_timestamp  DateTime64(3, 'UTC'),
        INDEX idx_package package TYPE bloom_filter GRANULARITY 4
      ) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(sui_timestamp)
      ORDER BY (kind, package, module, function, checkpoint_seq)
    `,
    getRecords: (cp) => cp.commands,
    mapRow: (r, checkpoint) => {
      const cmd = r as CommandRecord;
      return {
        tx_digest:      cmd.txDigest,
        command_index:  cmd.commandIndex,
        kind:           cmd.kind,
        package:        cmd.package ?? "",
        module:         cmd.module ?? "",
        function:       cmd.function ?? "",
        type_arguments: cmd.typeArguments ?? "",
        args:           cmd.args ?? "",
        checkpoint_seq: cmd.checkpointSeq.toString(),
        epoch:          checkpoint.epoch.toString(),
        sui_timestamp:  fmtDate(cmd.timestamp),
      };
    },
  },

  // ── system_transactions ───────────────────────────────────────────────────
  {
    name: "system_transactions",
    ddl: `
      CREATE TABLE IF NOT EXISTS system_transactions (
        tx_digest      String,
        kind           LowCardinality(String),
        data           String,
        checkpoint_seq UInt64,
        epoch          UInt64,
        sui_timestamp  DateTime64(3, 'UTC')
      ) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(sui_timestamp)
      ORDER BY (kind, checkpoint_seq)
    `,
    getRecords: (cp) => cp.systemTransactions,
    mapRow: (r, checkpoint) => {
      const sys = r as SystemTransactionRecord;
      return {
        tx_digest:      sys.txDigest,
        kind:           sys.kind,
        data:           sys.data,
        checkpoint_seq: sys.checkpointSeq.toString(),
        epoch:          checkpoint.epoch.toString(),
        sui_timestamp:  fmtDate(sys.timestamp),
      };
    },
  },

  // ── unchanged_consensus_objects ───────────────────────────────────────────
  // version uses Nullable(UInt64): some reference kinds have no version.
  {
    name: "unchanged_consensus_objects",
    ddl: `
      CREATE TABLE IF NOT EXISTS unchanged_consensus_objects (
        tx_digest      String,
        object_id      String,
        kind           LowCardinality(String),
        version        Nullable(UInt64),
        digest         String DEFAULT '',
        object_type    String DEFAULT '',
        checkpoint_seq UInt64,
        epoch          UInt64,
        sui_timestamp  DateTime64(3, 'UTC'),
        INDEX idx_object_id object_id TYPE bloom_filter GRANULARITY 4
      ) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(sui_timestamp)
      ORDER BY (kind, checkpoint_seq, object_id)
    `,
    getRecords: (cp) => cp.unchangedConsensusObjects,
    mapRow: (r, checkpoint) => {
      const uco = r as UnchangedConsensusObjectRecord;
      return {
        tx_digest:      uco.txDigest,
        object_id:      uco.objectId,
        kind:           uco.kind,
        version:        uco.version ?? null,
        digest:         uco.digest ?? "",
        object_type:    uco.objectType ?? "",
        checkpoint_seq: uco.checkpointSeq.toString(),
        epoch:          checkpoint.epoch.toString(),
        sui_timestamp:  fmtDate(uco.timestamp),
      };
    },
  },

  // ── checkpoints ───────────────────────────────────────────────────────────
  // No PARTITION BY — low volume (~2800/day), no TTL needed.
  {
    name: "checkpoints",
    ddl: `
      CREATE TABLE IF NOT EXISTS checkpoints (
        sequence_number                    UInt64,
        epoch                              UInt64,
        digest                             String,
        previous_digest                    String DEFAULT '',
        content_digest                     String DEFAULT '',
        sui_timestamp                      DateTime64(3, 'UTC'),
        total_network_transactions         UInt64,
        rolling_computation_cost           UInt64,
        rolling_storage_cost               UInt64,
        rolling_storage_rebate             UInt64,
        rolling_non_refundable_storage_fee UInt64
      ) ENGINE = MergeTree()
      ORDER BY (epoch, sequence_number)
    `,
    getRecords: (cp) => [cp.checkpoint],
    mapRow: (r) => {
      const c = r as Checkpoint;
      const gas = c.epochRollingGasCostSummary;
      return {
        sequence_number:                    c.sequenceNumber.toString(),
        epoch:                              c.epoch.toString(),
        digest:                             c.digest,
        previous_digest:                    c.previousDigest ?? "",
        content_digest:                     c.contentDigest ?? "",
        sui_timestamp:                      fmtDate(c.timestamp),
        total_network_transactions:         c.totalNetworkTransactions.toString(),
        rolling_computation_cost:           gas.computationCost,
        rolling_storage_cost:               gas.storageCost,
        rolling_storage_rebate:             gas.storageRebate,
        rolling_non_refundable_storage_fee: gas.nonRefundableStorageFee,
      };
    },
  },

  // ── raw_events ────────────────────────────────────────────────────────────
  {
    name: "raw_events",
    ddl: `
      CREATE TABLE IF NOT EXISTS raw_events (
        tx_digest      String,
        event_seq      UInt32,
        package_id     String,
        module         String,
        event_type     String,
        sender         String,
        contents       String,
        checkpoint_seq UInt64,
        epoch          UInt64,
        sui_timestamp  DateTime64(3, 'UTC'),
        -- package_id is the leading ORDER BY key; skip index would be redundant.
        -- event_type skip index helps exact-type queries without package filter.
        INDEX idx_event_type event_type TYPE bloom_filter GRANULARITY 4
      ) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(sui_timestamp)
      ORDER BY (package_id, module, event_type, checkpoint_seq)
    `,
    getRecords: (cp) => cp.rawEvents,
    mapRow: (r, checkpoint) => {
      const ev = r as RawEventRecord;
      return {
        tx_digest:      ev.txDigest,
        event_seq:      ev.eventSeq,
        package_id:     ev.packageId,
        module:         ev.module,
        event_type:     ev.eventType,
        sender:         ev.sender,
        contents:       ev.contents,
        checkpoint_seq: ev.checkpointSeq.toString(),
        epoch:          checkpoint.epoch.toString(),
        sui_timestamp:  fmtDate(ev.timestamp),
      };
    },
  },
];

// ---------------------------------------------------------------------------
// Running balances
//
// An incremental SummingMergeTree MV was considered but rejected: the pipeline
// inserts tables sequentially and retries on failure. If balance_changes succeeds
// but a later table fails, the retry re-inserts the same balance_change rows,
// firing the MV again and doubling those deltas with no way to deduplicate.
//
// Accurate current balances must be computed directly from balance_changes:
//
//   SELECT address, coin_type, sum(toInt128(amount)) AS balance
//   FROM balance_changes
//   WHERE address = '0x...'
//   GROUP BY address, coin_type
//
// For a fast pre-aggregated view, use a REFRESHABLE materialized view (requires
// ClickHouse 23.8+) which recalculates from scratch on a schedule, making it
// resilient to duplicate rows:
//
//   CREATE MATERIALIZED VIEW balances_refresh
//   REFRESH EVERY 1 MINUTE
//   ENGINE = MergeTree() ORDER BY (address, coin_type)
//   AS SELECT address, coin_type, sum(toInt128(amount)) AS balance
//      FROM balance_changes GROUP BY address, coin_type
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Storage factory
// ---------------------------------------------------------------------------

export interface ClickHouseStorageOptions {
  /** ClickHouse URL (default: http://localhost:8123) */
  url?: string;
  /** Database name (default: jun) */
  database?: string;
  /** Username (default: default) */
  username?: string;
  /** Password (default: empty) */
  password?: string;
  /** Compress INSERT payloads with LZ4 (default: false). Recommended for remote ClickHouse; skip for localhost. */
  compress?: boolean;
}

export function createClickHouseStorage(options: ClickHouseStorageOptions = {}): Storage {
  const {
    url = "http://localhost:8123",
    database = "jun",
    username = "default",
    password = "",
  } = options;

  let client: ClickHouseClient;

  return {
    name: "clickhouse",

    async initialize(): Promise<void> {
      client = createClient({
        url,
        database,
        username,
        password,
        compression: { request: options.compress ?? false },
      });

      for (const table of TABLES) {
        await client.exec({ query: table.ddl });
      }
    },

    async write(batch: ProcessedCheckpoint[]): Promise<void> {
      // Resolve any deferred binary checkpoints (archive source optimization)
      const resolved: ProcessedCheckpoint[] = batch.map((item) => {
        const raw = (item as unknown as { _rawBinary?: Uint8Array })._rawBinary;
        if (raw) {
          const result = parseBinaryCheckpoint(raw);
          return { ...result.processed, checkpoint: result.checkpoint };
        }
        return item;
      });

      await Promise.all(
        TABLES.map((table) => {
          const rows: Record<string, unknown>[] = [];
          for (const cp of resolved) {
            for (const record of table.getRecords(cp)) {
              rows.push(table.mapRow(record, cp.checkpoint));
            }
          }
          if (rows.length === 0) return Promise.resolve();

          return client.insert({
            table: table.name,
            values: rows,
            format: "JSONEachRow",
          });
        }),
      );
    },

    async shutdown(): Promise<void> {
      await client.close();
    },
  };
}
