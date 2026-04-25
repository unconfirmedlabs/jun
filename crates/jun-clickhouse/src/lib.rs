//! jun-clickhouse — ClickHouse HTTP writer using the official `clickhouse` crate.
//!
//! HTTP/1.1 over the CH HTTP port (8123) with LZ4 wire compression. The
//! `clickhouse::Client` wraps a `hyper` connection pool — clones share the
//! pool, so we don't manage sockets ourselves the way we did with klickhouse.
//! Concurrent inserts to different tables (or different in-flight batches)
//! pick up separate connections from the hyper pool automatically.
//!
//! v1 scope: 6 tables (checkpoints, transactions, move_calls, balance_changes,
//! object_changes, transaction_dependencies).

use anyhow::{anyhow, Context, Result};
use clickhouse::{Client, Compression, Row};
use jun_types::{
    BalanceChangeRecord, CheckpointRecord, DependencyRecord, ExtractedCheckpoint,
    MoveCallRecord, ObjectChangeRecord, TransactionRecord,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tracing::debug;

// --- Row structs: one per table, column order MUST match DDL --------------

#[derive(Row, Serialize, Deserialize, Clone, Debug)]
pub struct CheckpointRow {
    pub sequence_number: u64,
    pub epoch: u64,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub sui_timestamp: OffsetDateTime,
    pub digest: String,
    pub previous_digest: String,
    pub content_digest: String,
    pub total_network_transactions: u64,
    pub rolling_computation_cost: u64,
    pub rolling_storage_cost: u64,
    pub rolling_storage_rebate: u64,
    pub rolling_non_refundable_storage_fee: u64,
}

#[derive(Row, Serialize, Deserialize, Clone, Debug)]
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
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub sui_timestamp: OffsetDateTime,
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

#[derive(Row, Serialize, Deserialize, Clone, Debug)]
pub struct MoveCallRow {
    pub tx_digest: String,
    pub call_index: u32,
    pub package: String,
    pub module: String,
    pub function: String,
    pub checkpoint_seq: u64,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub sui_timestamp: OffsetDateTime,
}

#[derive(Row, Serialize, Deserialize, Clone, Debug)]
pub struct BalanceChangeRow {
    pub tx_digest: String,
    pub checkpoint_seq: u64,
    pub address: String,
    pub coin_type: String,
    pub amount: i128,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub sui_timestamp: OffsetDateTime,
}

#[derive(Row, Serialize, Deserialize, Clone, Debug)]
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
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub sui_timestamp: OffsetDateTime,
}

#[derive(Row, Serialize, Deserialize, Clone, Debug)]
pub struct DependencyRow {
    pub tx_digest: String,
    pub depends_on_digest: String,
    pub checkpoint_seq: u64,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub sui_timestamp: OffsetDateTime,
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
/// Parse a millisecond-since-epoch string into OffsetDateTime. Sui timestamps
/// fit comfortably in i64 (year 2262+), so saturating conversion is safe.
fn ts(s: &str) -> OffsetDateTime {
    let ms = p_u64(s) as i128 * 1_000_000;
    OffsetDateTime::from_unix_timestamp_nanos(ms).unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

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

#[derive(Clone)]
pub struct ClickHouseClient {
    /// `clickhouse::Client` is cheap to clone; clones share the underlying
    /// hyper connection pool. Concurrent inserts to different tables open
    /// separate HTTP requests served by separate pooled TCP connections.
    inner: Client,
    database: String,
}

impl ClickHouseClient {
    /// `url` is the HTTP endpoint, e.g. "http://localhost:8123".
    pub async fn connect(
        url: impl Into<String>,
        database: impl Into<String>,
        username: Option<String>,
        password: Option<String>,
    ) -> Result<Self> {
        let database = database.into();
        let mut client = Client::default()
            .with_url(url)
            .with_compression(Compression::Lz4);
        if let Some(u) = username { client = client.with_user(u); }
        if let Some(p) = password { client = client.with_password(p); }

        // Verify reachability — surfaces wrong URL / auth before the first batch.
        client
            .query("SELECT 1")
            .execute()
            .await
            .with_context(|| "ping CH")?;

        Ok(Self { inner: client, database })
    }

    pub async fn init_schema(&self) -> Result<()> {
        self.inner
            .query(&format!("CREATE DATABASE IF NOT EXISTS {}", self.database))
            .execute()
            .await
            .map_err(|e| anyhow!("CREATE DATABASE: {e}"))?;
        for ddl in ALL_DDL {
            let stmt = ddl.replace("{db}", &self.database);
            self.inner.query(&stmt).execute().await.map_err(|e| anyhow!("DDL: {e}"))?;
        }
        Ok(())
    }

    pub async fn write_batch(&self, batch: TableBatch) -> Result<()> {
        // Each call expands at a concrete row type so the compiler can
        // resolve `<R as Row>::Value<'_>` (which generic helpers can't, since
        // there's no way to prove `R == R::Value<'_>` in a where-clause).
        let f1 = insert_table(self.inner.clone(), &self.database, "checkpoints",              batch.checkpoints);
        let f2 = insert_table(self.inner.clone(), &self.database, "transactions",             batch.transactions);
        let f3 = insert_table(self.inner.clone(), &self.database, "move_calls",               batch.move_calls);
        let f4 = insert_table(self.inner.clone(), &self.database, "balance_changes",          batch.balance_changes);
        let f5 = insert_table(self.inner.clone(), &self.database, "object_changes",           batch.object_changes);
        let f6 = insert_table(self.inner.clone(), &self.database, "transaction_dependencies", batch.dependencies);

        let (r1, r2, r3, r4, r5, r6) = tokio::join!(f1, f2, f3, f4, f5, f6);
        r1?; r2?; r3?; r4?; r5?; r6?;
        debug!(cps = batch.checkpoint_count, txs = batch.tx_count, "batch written");
        Ok(())
    }
}

/// Macro because each table needs the row type at compile time so the
/// `clickhouse::Row::Value<'_>` GAT resolves to the concrete struct.
macro_rules! impl_insert {
    ($name:ident, $row:ty) => {
        async fn $name(client: Client, db: &str, table: &str, rows: Vec<$row>) -> Result<()> {
            if rows.is_empty() { return Ok(()); }
            let client = client.with_database(db);
            let mut insert = client.insert::<$row>(table).await
                .map_err(|e| anyhow!("insert {table}: prepare: {e}"))?;
            for row in &rows {
                insert.write(row).await
                    .map_err(|e| anyhow!("insert {table}: write: {e}"))?;
            }
            insert.end().await.map_err(|e| anyhow!("insert {table}: end: {e}"))?;
            Ok(())
        }
    };
}

impl_insert!(insert_checkpoints,  CheckpointRow);
impl_insert!(insert_transactions, TransactionRow);
impl_insert!(insert_move_calls,   MoveCallRow);
impl_insert!(insert_balances,     BalanceChangeRow);
impl_insert!(insert_objects,      ObjectChangeRow);
impl_insert!(insert_deps,         DependencyRow);

async fn insert_table<R>(client: Client, db: &str, table: &str, rows: Vec<R>) -> Result<()>
where R: TableInsert {
    R::insert(client, db, table, rows).await
}

trait TableInsert: Sized + Send + 'static {
    fn insert(client: Client, db: &str, table: &str, rows: Vec<Self>)
        -> futures::future::BoxFuture<'static, Result<()>>;
}

macro_rules! impl_table_insert {
    ($row:ty, $fn:ident) => {
        impl TableInsert for $row {
            fn insert(client: Client, db: &str, table: &str, rows: Vec<Self>)
                -> futures::future::BoxFuture<'static, Result<()>>
            {
                let db = db.to_string();
                let table = table.to_string();
                Box::pin(async move { $fn(client, &db, &table, rows).await })
            }
        }
    };
}

impl_table_insert!(CheckpointRow,    insert_checkpoints);
impl_table_insert!(TransactionRow,   insert_transactions);
impl_table_insert!(MoveCallRow,      insert_move_calls);
impl_table_insert!(BalanceChangeRow, insert_balances);
impl_table_insert!(ObjectChangeRow,  insert_objects);
impl_table_insert!(DependencyRow,    insert_deps);
