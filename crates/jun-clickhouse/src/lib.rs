//! jun-clickhouse — ClickHouse Native protocol writer (TCP port 9000).
//!
//! Uses `klickhouse` for columnar Block encoding + LZ4 wire compression.
//! Faster than HTTP/RowBinary: amortizes framing, reuses the TCP connection,
//! compresses on the wire.
//!
//! v1 scope: 6 tables (checkpoints, transactions, move_calls, balance_changes,
//! object_changes, transaction_dependencies). Each table has a DDL + a `Row`
//! struct + a converter from `jun-types` records.

use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use jun_types::{
    BalanceChangeRecord, CheckpointRecord, DependencyRecord, ExtractedCheckpoint,
    MoveCallRecord, ObjectChangeRecord, TransactionRecord,
};
use klickhouse::{Client, ClientOptions, DateTime64, Row};
use tracing::debug;

// --- Row structs: one per table, column order MUST match DDL --------------

#[derive(Row, Clone, Debug)]
pub struct CheckpointRow {
    pub sequence_number: u64,
    pub epoch: u64,
    pub sui_timestamp: DateTime64<3>,
    pub digest: String,
    pub previous_digest: String,
    pub content_digest: String,
    pub total_network_transactions: u64,
    pub rolling_computation_cost: u64,
    pub rolling_storage_cost: u64,
    pub rolling_storage_rebate: u64,
    pub rolling_non_refundable_storage_fee: u64,
}

#[derive(Row, Clone, Debug)]
pub struct TransactionRow {
    pub digest: String,
    pub sender: String,
    pub success: u8,
    pub computation_cost: u64,
    pub storage_cost: u64,
    pub storage_rebate: u64,
    pub non_refundable_storage_fee: u64,
    pub move_call_count: u32,
    pub checkpoint_seq: u64,
    pub sui_timestamp: DateTime64<3>,
    pub epoch: u64,
    pub error_kind: String,
    pub error_description: String,
    pub error_command_index: u32,
    pub error_abort_code: String,
    pub error_module: String,
    pub error_function: String,
    pub events_digest: String,
    pub lamport_version: u64,
    pub dependency_count: u32,
    pub gas_owner: String,
    pub gas_price: u64,
    pub gas_budget: u64,
    pub gas_object_id: String,
    pub expiration_kind: String,
    pub expiration_epoch: u64,
}

#[derive(Row, Clone, Debug)]
pub struct MoveCallRow {
    pub tx_digest: String,
    pub call_index: u32,
    pub package: String,
    pub module: String,
    pub function: String,
    pub checkpoint_seq: u64,
    pub sui_timestamp: DateTime64<3>,
}

#[derive(Row, Clone, Debug)]
pub struct BalanceChangeRow {
    pub tx_digest: String,
    pub checkpoint_seq: u64,
    pub address: String,
    pub coin_type: String,
    pub amount: i128,
    pub sui_timestamp: DateTime64<3>,
}

#[derive(Row, Clone, Debug)]
pub struct ObjectChangeRow {
    pub tx_digest: String,
    pub object_id: String,
    pub change_type: String,
    pub object_type: String,
    pub input_version: u64,
    pub input_digest: String,
    pub input_owner: String,
    pub input_owner_kind: String,
    pub output_version: u64,
    pub output_digest: String,
    pub output_owner: String,
    pub output_owner_kind: String,
    pub is_gas_object: u8,
    pub checkpoint_seq: u64,
    pub sui_timestamp: DateTime64<3>,
}

#[derive(Row, Clone, Debug)]
pub struct DependencyRow {
    pub tx_digest: String,
    pub depends_on_digest: String,
    pub checkpoint_seq: u64,
    pub sui_timestamp: DateTime64<3>,
}

// --- DDL ------------------------------------------------------------------

pub const DDL_CHECKPOINTS: &str = r#"
CREATE TABLE IF NOT EXISTS {db}.checkpoints (
    sequence_number UInt64,
    epoch UInt64,
    sui_timestamp DateTime64(3, 'UTC'),
    digest String,
    previous_digest String,
    content_digest String,
    total_network_transactions UInt64,
    rolling_computation_cost UInt64,
    rolling_storage_cost UInt64,
    rolling_storage_rebate UInt64,
    rolling_non_refundable_storage_fee UInt64
) ENGINE = MergeTree() PARTITION BY toYYYYMM(sui_timestamp) ORDER BY (sequence_number)
"#;

pub const DDL_TRANSACTIONS: &str = r#"
CREATE TABLE IF NOT EXISTS {db}.transactions (
    digest String,
    sender String,
    success UInt8,
    computation_cost UInt64,
    storage_cost UInt64,
    storage_rebate UInt64,
    non_refundable_storage_fee UInt64,
    move_call_count UInt32,
    checkpoint_seq UInt64,
    sui_timestamp DateTime64(3, 'UTC'),
    epoch UInt64,
    error_kind String,
    error_description String,
    error_command_index UInt32,
    error_abort_code String,
    error_module String,
    error_function String,
    events_digest String,
    lamport_version UInt64,
    dependency_count UInt32,
    gas_owner String,
    gas_price UInt64,
    gas_budget UInt64,
    gas_object_id String,
    expiration_kind LowCardinality(String),
    expiration_epoch UInt64,
    INDEX idx_sender sender TYPE bloom_filter GRANULARITY 4
) ENGINE = MergeTree() PARTITION BY toYYYYMM(sui_timestamp) ORDER BY (success, epoch, checkpoint_seq, digest)
"#;

pub const DDL_MOVE_CALLS: &str = r#"
CREATE TABLE IF NOT EXISTS {db}.move_calls (
    tx_digest String, call_index UInt32, package String, module String, function String,
    checkpoint_seq UInt64, sui_timestamp DateTime64(3, 'UTC')
) ENGINE = MergeTree() PARTITION BY toYYYYMM(sui_timestamp) ORDER BY (package, module, function, checkpoint_seq)
"#;

pub const DDL_BALANCE_CHANGES: &str = r#"
CREATE TABLE IF NOT EXISTS {db}.balance_changes (
    tx_digest String, checkpoint_seq UInt64, address String, coin_type String, amount Int128,
    sui_timestamp DateTime64(3, 'UTC')
) ENGINE = MergeTree() PARTITION BY toYYYYMM(sui_timestamp) ORDER BY (address, coin_type, checkpoint_seq)
"#;

pub const DDL_OBJECT_CHANGES: &str = r#"
CREATE TABLE IF NOT EXISTS {db}.object_changes (
    tx_digest String, object_id String, change_type LowCardinality(String), object_type String,
    input_version UInt64, input_digest String, input_owner String, input_owner_kind LowCardinality(String),
    output_version UInt64, output_digest String, output_owner String, output_owner_kind LowCardinality(String),
    is_gas_object UInt8, checkpoint_seq UInt64, sui_timestamp DateTime64(3, 'UTC')
) ENGINE = MergeTree() PARTITION BY toYYYYMM(sui_timestamp) ORDER BY (object_id, checkpoint_seq)
"#;

pub const DDL_DEPENDENCIES: &str = r#"
CREATE TABLE IF NOT EXISTS {db}.transaction_dependencies (
    tx_digest String, depends_on_digest String, checkpoint_seq UInt64, sui_timestamp DateTime64(3, 'UTC')
) ENGINE = MergeTree() PARTITION BY toYYYYMM(sui_timestamp) ORDER BY (tx_digest, depends_on_digest)
"#;

pub const ALL_DDL: &[&str] = &[
    DDL_CHECKPOINTS, DDL_TRANSACTIONS, DDL_MOVE_CALLS,
    DDL_BALANCE_CHANGES, DDL_OBJECT_CHANGES, DDL_DEPENDENCIES,
];

// --- Conversions: jun-types records -> CH Row ----------------------------

fn p_u64(s: &str) -> u64 { s.parse().unwrap_or(0) }
fn p_i128(s: &str) -> i128 { s.parse().unwrap_or(0) }
fn ts(s: &str) -> DateTime64<3> { DateTime64(klickhouse::Tz::UTC, p_u64(s)) }

impl From<&CheckpointRecord> for CheckpointRow {
    fn from(r: &CheckpointRecord) -> Self {
        Self {
            sequence_number: p_u64(&r.sequence_number),
            epoch: p_u64(&r.epoch),
            sui_timestamp: ts(&r.timestamp_ms),
            digest: r.digest.clone(),
            previous_digest: r.previous_digest.clone().unwrap_or_default(),
            content_digest: r.content_digest.clone().unwrap_or_default(),
            total_network_transactions: p_u64(&r.total_network_transactions),
            rolling_computation_cost: p_u64(&r.rolling_computation_cost),
            rolling_storage_cost: p_u64(&r.rolling_storage_cost),
            rolling_storage_rebate: p_u64(&r.rolling_storage_rebate),
            rolling_non_refundable_storage_fee: p_u64(&r.rolling_non_refundable_storage_fee),
        }
    }
}

impl From<&TransactionRecord> for TransactionRow {
    fn from(r: &TransactionRecord) -> Self {
        Self {
            digest: r.digest.clone(),
            sender: r.sender.clone(),
            success: if r.success { 1 } else { 0 },
            computation_cost: p_u64(&r.computation_cost),
            storage_cost: p_u64(&r.storage_cost),
            storage_rebate: p_u64(&r.storage_rebate),
            non_refundable_storage_fee: p_u64(&r.non_refundable_storage_fee),
            move_call_count: r.move_call_count,
            checkpoint_seq: p_u64(&r.checkpoint_seq),
            sui_timestamp: ts(&r.timestamp_ms),
            epoch: p_u64(&r.epoch),
            error_kind: r.error_kind.clone().unwrap_or_default(),
            error_description: r.error_description.clone().unwrap_or_default(),
            error_command_index: r.error_command_index.unwrap_or(u32::MAX),
            error_abort_code: r.error_abort_code.clone().unwrap_or_default(),
            error_module: r.error_module.clone().unwrap_or_default(),
            error_function: r.error_function.clone().unwrap_or_default(),
            events_digest: r.events_digest.clone().unwrap_or_default(),
            lamport_version: r.lamport_version.as_deref().map(p_u64).unwrap_or(0),
            dependency_count: r.dependency_count,
            gas_owner: r.gas_owner.clone(),
            gas_price: p_u64(&r.gas_price),
            gas_budget: p_u64(&r.gas_budget),
            gas_object_id: r.gas_object_id.clone().unwrap_or_default(),
            expiration_kind: r.expiration_kind.clone(),
            expiration_epoch: r.expiration_epoch.as_deref().map(p_u64).unwrap_or(0),
        }
    }
}

impl From<&MoveCallRecord> for MoveCallRow {
    fn from(r: &MoveCallRecord) -> Self {
        Self {
            tx_digest: r.tx_digest.clone(),
            call_index: r.call_index,
            package: r.package.clone(),
            module: r.module.clone(),
            function: r.function.clone(),
            checkpoint_seq: p_u64(&r.checkpoint_seq),
            sui_timestamp: ts(&r.timestamp_ms),
        }
    }
}

impl From<&BalanceChangeRecord> for BalanceChangeRow {
    fn from(r: &BalanceChangeRecord) -> Self {
        Self {
            tx_digest: r.tx_digest.clone(),
            checkpoint_seq: p_u64(&r.checkpoint_seq),
            address: r.address.clone(),
            coin_type: r.coin_type.clone(),
            amount: p_i128(&r.amount),
            sui_timestamp: ts(&r.timestamp_ms),
        }
    }
}

impl From<&ObjectChangeRecord> for ObjectChangeRow {
    fn from(r: &ObjectChangeRecord) -> Self {
        Self {
            tx_digest: r.tx_digest.clone(),
            object_id: r.object_id.clone(),
            change_type: r.change_type.clone(),
            object_type: r.object_type.clone().unwrap_or_default(),
            input_version: r.input_version.as_deref().map(p_u64).unwrap_or(0),
            input_digest: r.input_digest.clone().unwrap_or_default(),
            input_owner: r.input_owner.clone().unwrap_or_default(),
            input_owner_kind: r.input_owner_kind.clone().unwrap_or_default(),
            output_version: r.output_version.as_deref().map(p_u64).unwrap_or(0),
            output_digest: r.output_digest.clone().unwrap_or_default(),
            output_owner: r.output_owner.clone().unwrap_or_default(),
            output_owner_kind: r.output_owner_kind.clone().unwrap_or_default(),
            is_gas_object: if r.is_gas_object { 1 } else { 0 },
            checkpoint_seq: p_u64(&r.checkpoint_seq),
            sui_timestamp: ts(&r.timestamp_ms),
        }
    }
}

impl From<&DependencyRecord> for DependencyRow {
    fn from(r: &DependencyRecord) -> Self {
        Self {
            tx_digest: r.tx_digest.clone(),
            depends_on_digest: r.depends_on_digest.clone(),
            checkpoint_seq: p_u64(&r.checkpoint_seq),
            sui_timestamp: ts(&r.timestamp_ms),
        }
    }
}

// --- Batch + client -------------------------------------------------------

pub struct TableBatch {
    pub checkpoints: Vec<CheckpointRow>,
    pub transactions: Vec<TransactionRow>,
    pub move_calls: Vec<MoveCallRow>,
    pub balance_changes: Vec<BalanceChangeRow>,
    pub object_changes: Vec<ObjectChangeRow>,
    pub dependencies: Vec<DependencyRow>,
    pub checkpoint_count: u64,
    pub tx_count: u64,
}

pub fn encode_batch(cps: &[ExtractedCheckpoint]) -> TableBatch {
    let mut cp = Vec::with_capacity(cps.len());
    let mut tx = Vec::with_capacity(cps.len() * 16);
    let mut mc = Vec::with_capacity(cps.len() * 50);
    let mut bc = Vec::with_capacity(cps.len() * 30);
    let mut oc = Vec::with_capacity(cps.len() * 100);
    let mut dp = Vec::with_capacity(cps.len() * 100);
    let mut n_cp = 0u64;
    let mut n_tx = 0u64;
    for c in cps {
        cp.push((&c.checkpoint).into());
        n_cp += 1;
        for r in &c.transactions { tx.push(r.into()); n_tx += 1; }
        for r in &c.move_calls      { mc.push(r.into()); }
        for r in &c.balance_changes { bc.push(r.into()); }
        for r in &c.object_changes  { oc.push(r.into()); }
        for r in &c.dependencies    { dp.push(r.into()); }
    }
    TableBatch {
        checkpoints: cp, transactions: tx, move_calls: mc,
        balance_changes: bc, object_changes: oc, dependencies: dp,
        checkpoint_count: n_cp, tx_count: n_tx,
    }
}

/// Indices into the per-table client array. Order is fixed so the array can be
/// laid out at compile time and write_batch picks clients without a HashMap.
const IDX_CHECKPOINTS: usize = 0;
const IDX_TRANSACTIONS: usize = 1;
const IDX_MOVE_CALLS: usize = 2;
const IDX_BALANCE_CHANGES: usize = 3;
const IDX_OBJECT_CHANGES: usize = 4;
const IDX_DEPENDENCIES: usize = 5;
const N_TABLES: usize = 6;

#[derive(Clone)]
pub struct ClickHouseClient {
    /// One TCP connection per table. klickhouse's `Client` serializes every
    /// query through a single channel, so concurrent inserts to one client
    /// queue at the socket. Giving each table its own client lets the 6
    /// per-batch inserts truly multiplex over distinct sockets.
    clients: [Arc<Client>; N_TABLES],
    database: String,
}

impl ClickHouseClient {
    /// `addr` is host:port (native TCP, typically 9000). Example: "localhost:9000".
    pub async fn connect(
        addr: impl AsRef<str>,
        database: impl Into<String>,
        username: Option<String>,
        password: Option<String>,
    ) -> Result<Self> {
        let addr = addr.as_ref();
        let database = database.into();
        let user = username.unwrap_or_else(|| "default".into());
        let pass = password.unwrap_or_default();

        // Open all 6 connections in parallel — saves ~5×handshake latency at startup.
        let mut futs = Vec::with_capacity(N_TABLES);
        for _ in 0..N_TABLES {
            let opts = ClientOptions {
                username: user.clone(),
                password: pass.clone(),
                default_database: "default".into(),
                tcp_nodelay: true,
            };
            futs.push(Client::connect(addr, opts));
        }
        let results = futures::future::try_join_all(futs)
            .await
            .with_context(|| format!("connect to CH {addr}"))?;
        let clients: [Arc<Client>; N_TABLES] = results
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>()
            .try_into()
            .unwrap_or_else(|_| unreachable!("vec len == N_TABLES by construction"));

        Ok(Self { clients, database })
    }

    pub async fn init_schema(&self) -> Result<()> {
        let c = &self.clients[0];
        c.execute(format!("CREATE DATABASE IF NOT EXISTS {}", self.database))
            .await
            .map_err(|e| anyhow!("CREATE DATABASE: {e}"))?;
        for ddl in ALL_DDL {
            let stmt = ddl.replace("{db}", &self.database);
            c.execute(stmt).await.map_err(|e| anyhow!("DDL: {e}"))?;
        }
        Ok(())
    }

    pub async fn write_batch(&self, batch: TableBatch) -> Result<()> {
        let db = &self.database;

        let f1 = insert_rows(&self.clients[IDX_CHECKPOINTS],     db, "checkpoints",              batch.checkpoints);
        let f2 = insert_rows(&self.clients[IDX_TRANSACTIONS],    db, "transactions",             batch.transactions);
        let f3 = insert_rows(&self.clients[IDX_MOVE_CALLS],      db, "move_calls",               batch.move_calls);
        let f4 = insert_rows(&self.clients[IDX_BALANCE_CHANGES], db, "balance_changes",          batch.balance_changes);
        let f5 = insert_rows(&self.clients[IDX_OBJECT_CHANGES],  db, "object_changes",           batch.object_changes);
        let f6 = insert_rows(&self.clients[IDX_DEPENDENCIES],    db, "transaction_dependencies", batch.dependencies);

        let (r1, r2, r3, r4, r5, r6) = tokio::join!(f1, f2, f3, f4, f5, f6);
        r1?; r2?; r3?; r4?; r5?; r6?;
        debug!(cps = batch.checkpoint_count, txs = batch.tx_count, "batch written");
        Ok(())
    }
}

async fn insert_rows<R: Row + Send + Sync + 'static>(
    client: &Client,
    db: &str,
    table: &str,
    rows: Vec<R>,
) -> Result<()> {
    if rows.is_empty() { return Ok(()); }
    let query = format!("INSERT INTO {}.{} FORMAT Native", db, table);
    client
        .insert_native_block(&query, rows)
        .await
        .map_err(|e| anyhow!("insert {table}: {e}"))
}
