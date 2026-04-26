//! Output sinks for `jun stream`.
//!
//! Each sink consumes flattened records from a single decoded checkpoint and
//! ships them to its destination. The trait is intentionally minimal —
//! consumers that want richer batching wrap a base sink.

use std::io::Write as _;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use jun_types::ExtractedCheckpoint;
use serde::Serialize;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tracing::trace;

/// Subset of record tables to emit. Used both for routing (NATS subjects,
/// per-table file modes) and as a coarse filter — empty means "all tables".
#[derive(Clone, Debug, Default)]
pub struct TableFilter {
    pub tables: Vec<String>,
}

impl TableFilter {
    pub fn allows(&self, table: &str) -> bool {
        self.tables.is_empty() || self.tables.iter().any(|t| t == table)
    }

    pub fn single(&self) -> Option<&str> {
        if self.tables.len() == 1 {
            Some(self.tables[0].as_str())
        } else {
            None
        }
    }
}

/// Sink trait for live-stream record output.
///
/// `write_checkpoint` is called once per decoded checkpoint with the full
/// `ExtractedCheckpoint`. Sinks decide what to fan out (per-record JSONL,
/// per-table NATS subjects, etc.) and how to filter via [`TableFilter`].
///
/// Methods are async to allow non-blocking IO. Sinks must be `Send + Sync`
/// because the stream task may be on a different worker than the sink.
#[async_trait::async_trait]
pub trait RecordSink: Send + Sync {
    async fn write_checkpoint(&self, ck: &ExtractedCheckpoint) -> Result<()>;

    /// Optional flush hook. Default: noop.
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

/// Discards everything. Useful for `--mode no-write` parity with replay.
pub struct NullSink;

#[async_trait::async_trait]
impl RecordSink for NullSink {
    async fn write_checkpoint(&self, _ck: &ExtractedCheckpoint) -> Result<()> {
        Ok(())
    }
}

/// Writes one JSONL line per record to stdout. Tagged form: each line is
/// `{"table": "...", "row": {...}}`. If `filter.single()` is set, the lines
/// are *untagged* — the row is emitted directly so consumers can stream
/// straight into a single-table sink (e.g. ClickHouse `INSERT FORMAT JSONEachRow`).
pub struct StdoutSink {
    filter: TableFilter,
    // Stdout writes are serialized to keep JSONL lines from interleaving.
    out: Arc<Mutex<std::io::Stdout>>,
}

impl StdoutSink {
    pub fn new(filter: TableFilter) -> Self {
        Self {
            filter,
            out: Arc::new(Mutex::new(std::io::stdout())),
        }
    }
}

#[async_trait::async_trait]
impl RecordSink for StdoutSink {
    async fn write_checkpoint(&self, ck: &ExtractedCheckpoint) -> Result<()> {
        let untagged_table = self.filter.single().map(str::to_string);
        let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
        emit_records(&self.filter, ck, untagged_table.as_deref(), &mut buf)?;
        if buf.is_empty() {
            return Ok(());
        }
        let stdout = self.out.clone();
        tokio::task::spawn_blocking(move || {
            let mut g = stdout.blocking_lock();
            g.write_all(&buf).context("stdout write")
        })
        .await
        .context("stdout writer task")??;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let stdout = self.out.clone();
        tokio::task::spawn_blocking(move || {
            let mut g = stdout.blocking_lock();
            g.flush().context("stdout flush")
        })
        .await
        .context("stdout flush task")?
    }
}

/// Writes one JSONL line per record to an append-mode file. Same tagging
/// rules as [`StdoutSink`].
pub struct FileSink {
    filter: TableFilter,
    file: Arc<Mutex<tokio::fs::File>>,
}

impl FileSink {
    pub async fn open(path: &Path, filter: TableFilter) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .with_context(|| format!("open {}", path.display()))?;
        Ok(Self {
            filter,
            file: Arc::new(Mutex::new(file)),
        })
    }
}

#[async_trait::async_trait]
impl RecordSink for FileSink {
    async fn write_checkpoint(&self, ck: &ExtractedCheckpoint) -> Result<()> {
        let untagged_table = self.filter.single().map(str::to_string);
        let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
        emit_records(&self.filter, ck, untagged_table.as_deref(), &mut buf)?;
        if buf.is_empty() {
            return Ok(());
        }
        let mut g = self.file.lock().await;
        g.write_all(&buf).await.context("file write")?;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let mut g = self.file.lock().await;
        g.flush().await.context("file flush")?;
        Ok(())
    }
}

/// NATS sink. One subject per record table: `jun.sui.<table>`.
///
/// Available behind the `nats` cargo feature so the OSS build doesn't take
/// async-nats as a hard dep.
#[cfg(feature = "nats")]
pub struct NatsSink {
    filter: TableFilter,
    client: async_nats::Client,
    subject_prefix: String,
}

#[cfg(feature = "nats")]
impl NatsSink {
    /// Connect and prepare the publisher. `subject_prefix` is prepended to
    /// each table name (default convention: `jun.sui`).
    pub async fn connect(url: &str, subject_prefix: &str, filter: TableFilter) -> Result<Self> {
        let client = async_nats::connect(url)
            .await
            .with_context(|| format!("connect to NATS {url}"))?;
        Ok(Self {
            filter,
            client,
            subject_prefix: subject_prefix.trim_end_matches('.').to_string(),
        })
    }

    fn subject_for(&self, table: &str) -> String {
        format!("{}.{}", self.subject_prefix, table)
    }
}

#[cfg(feature = "nats")]
#[async_trait::async_trait]
impl RecordSink for NatsSink {
    async fn write_checkpoint(&self, ck: &ExtractedCheckpoint) -> Result<()> {
        // For NATS we publish one record per message, with the table encoded
        // in the subject — no need for the wrapping "{table, row}" JSON.
        let publish_one = |table: &str, payload_json: Vec<u8>| {
            let subj = self.subject_for(table);
            let client = self.client.clone();
            async move {
                client
                    .publish(subj.clone(), payload_json.into())
                    .await
                    .with_context(|| format!("nats publish to {subj}"))
            }
        };
        for_each_record_json(&self.filter, ck, |table, json| {
            // Sequential publish keeps memory low; async-nats batches under
            // the hood so this still pipelines fine.
            futures::executor::block_on(publish_one(table, json))
        })?;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        self.client.flush().await.context("nats flush")?;
        Ok(())
    }
}

// ---- shared serialization helpers ----------------------------------------

/// Walk every record in the ExtractedCheckpoint and call `emit` per row.
/// `emit` receives the canonical table name and the JSON payload bytes.
fn for_each_record_json<F>(
    filter: &TableFilter,
    ck: &ExtractedCheckpoint,
    mut emit: F,
) -> Result<()>
where
    F: FnMut(&str, Vec<u8>) -> Result<()>,
{
    macro_rules! emit_table {
        ($name:literal, $field:expr) => {
            if filter.allows($name) {
                for row in $field.iter() {
                    let json = sonic_rs::to_vec(row)
                        .with_context(|| concat!("serialize ", $name))?;
                    emit($name, json)?;
                }
            }
        };
        (single $name:literal, $field:expr) => {
            if filter.allows($name) {
                let json = sonic_rs::to_vec(&$field)
                    .with_context(|| concat!("serialize ", $name))?;
                emit($name, json)?;
            }
        };
    }

    emit_table!(single "checkpoints", ck.checkpoint);
    emit_table!("transactions", ck.transactions);
    emit_table!("move_calls", ck.move_calls);
    emit_table!("balance_changes", ck.balance_changes);
    emit_table!("object_changes", ck.object_changes);
    emit_table!("dependencies", ck.dependencies);
    emit_table!("inputs", ck.inputs);
    emit_table!("commands", ck.commands);
    emit_table!("system_transactions", ck.system_transactions);
    emit_table!("unchanged_consensus_objects", ck.unchanged_consensus_objects);
    emit_table!("events", ck.events);
    emit_table!("checkpoint_contents", ck.checkpoint_contents);
    if filter.allows("checkpoint_signature") {
        if let Some(sig) = &ck.checkpoint_signature {
            let json = sonic_rs::to_vec(sig)
                .context("serialize checkpoint_signature")?;
            emit("checkpoint_signature", json)?;
        }
    }
    emit_table!("transaction_signatures", ck.transaction_signatures);
    emit_table!("objects", ck.objects);
    emit_table!("packages", ck.packages);
    emit_table!("package_modules", ck.package_modules);
    emit_table!("package_linkage", ck.package_linkage);
    emit_table!("type_origins", ck.type_origins);
    emit_table!("epoch_committees", ck.epoch_committees);
    emit_table!("epoch_protocol_versions", ck.epoch_protocol_versions);
    emit_table!("epoch_commitments", ck.epoch_commitments);
    emit_table!("checkpoint_commitments", ck.checkpoint_commitments);
    emit_table!("accumulator_writes", ck.accumulator_writes);
    emit_table!("congested_objects", ck.congested_objects);
    emit_table!("epoch_changes", ck.epoch_changes);
    emit_table!("consensus_commits", ck.consensus_commits);
    emit_table!("authenticator_updates", ck.authenticator_updates);
    emit_table!("active_jwks", ck.active_jwks);
    emit_table!("randomness_updates", ck.randomness_updates);
    emit_table!("end_of_epoch_operations", ck.end_of_epoch_operations);

    Ok(())
}

/// Serialize all records into `buf` as JSONL.
///
/// When `untagged_table` is `Some`, lines are emitted as the bare row JSON
/// (single-table mode). Otherwise each line is `{"table","row"}`.
fn emit_records(
    filter: &TableFilter,
    ck: &ExtractedCheckpoint,
    untagged_table: Option<&str>,
    buf: &mut Vec<u8>,
) -> Result<()> {
    for_each_record_json(filter, ck, |table, json| {
        if untagged_table.is_some_and(|t| t == table) {
            buf.extend_from_slice(&json);
            buf.push(b'\n');
        } else if untagged_table.is_some() {
            // Untagged mode but non-matching table — skip.
        } else {
            #[derive(Serialize)]
            struct Tagged<'a> {
                table: &'a str,
                row: serde_json::Value,
            }
            let row: serde_json::Value = serde_json::from_slice(&json)?;
            let tagged = Tagged { table, row };
            buf.extend_from_slice(&sonic_rs::to_vec(&tagged)?);
            buf.push(b'\n');
        }
        trace!(table, "emitted record");
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use jun_types::{CheckpointRecord, ExtractedCheckpoint, TransactionRecord};

    fn fake_extracted() -> ExtractedCheckpoint {
        let mut ck = ExtractedCheckpoint::default();
        ck.checkpoint = CheckpointRecord {
            sequence_number: "100".into(),
            epoch: "1".into(),
            timestamp_ms: "1700000000000".into(),
            digest: "ABC".into(),
            ..Default::default()
        };
        ck.transactions.push(TransactionRecord {
            digest: "tx1".into(),
            sender: "0x1".into(),
            success: true,
            computation_cost: "0".into(),
            storage_cost: "0".into(),
            storage_rebate: "0".into(),
            non_refundable_storage_fee: "0".into(),
            checkpoint_seq: "100".into(),
            timestamp_ms: "1700000000000".into(),
            move_call_count: 0,
            epoch: "1".into(),
            error_kind: None,
            error_description: None,
            error_command_index: None,
            error_abort_code: None,
            error_module: None,
            error_function: None,
            error_arg_idx: None,
            error_type_arg_idx: None,
            error_sub_kind: None,
            error_current_size: None,
            error_max_size: None,
            error_coin_type: None,
            error_denied_address: None,
            error_object_id: None,
            events_digest: None,
            lamport_version: None,
            dependency_count: 0,
            gas_owner: "0x1".into(),
            gas_price: "1000".into(),
            gas_budget: "1000000".into(),
            gas_object_id: None,
            expiration_kind: "NONE".into(),
            expiration_epoch: None,
            expiration_min_epoch: None,
            expiration_max_epoch: None,
            expiration_min_ts_seconds: None,
            expiration_max_ts_seconds: None,
            expiration_chain: None,
            expiration_nonce: None,
        });
        ck
    }

    #[test]
    fn table_filter_allows_all_when_empty() {
        let f = TableFilter::default();
        assert!(f.allows("transactions"));
        assert!(f.allows("anything"));
    }

    #[test]
    fn table_filter_restricts_when_set() {
        let f = TableFilter {
            tables: vec!["transactions".into()],
        };
        assert!(f.allows("transactions"));
        assert!(!f.allows("events"));
        assert_eq!(f.single(), Some("transactions"));
    }

    #[test]
    fn emit_records_tagged_format() {
        let f = TableFilter::default();
        let ck = fake_extracted();
        let mut buf = Vec::new();
        emit_records(&f, &ck, None, &mut buf).unwrap();
        let text = String::from_utf8(buf).unwrap();
        // Two lines: 1 checkpoint, 1 transaction.
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"table\":\"checkpoints\""));
        assert!(lines[1].contains("\"table\":\"transactions\""));
    }

    #[test]
    fn emit_records_untagged_single_table() {
        let f = TableFilter {
            tables: vec!["transactions".into()],
        };
        let ck = fake_extracted();
        let mut buf = Vec::new();
        emit_records(&f, &ck, Some("transactions"), &mut buf).unwrap();
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 1);
        assert!(lines[0].starts_with("{\"digest\":\"tx1\""));
        // No "table" wrapper key in untagged mode.
        assert!(!lines[0].contains("\"table\":"));
    }
}
