//! `PipeSink` — write flat records as JSON Lines to stdout.
//!
//! Each line is one record, carrying a `_table` field discriminator so a
//! single stream can be demultiplexed by the consumer:
//!
//! ```text
//! {"_table":"transactions","digest":"...","sender":"...",...}
//! {"_table":"balance_changes","tx_digest":"...","amount":"...",...}
//! ```
//!
//! With `--table=t`, the `_table` discriminator is omitted and the consumer
//! gets a homogeneous stream — drops directly into:
//!
//! ```text
//! jun replay --pipe --table=transactions ... \
//!   | clickhouse-client --query "INSERT INTO transactions FORMAT JSONEachRow"
//! ```
//!
//! Stdout is JSON; progress logs go to stderr via tracing.

use std::collections::HashSet;
use std::io::{self, BufWriter, Write};
use std::sync::Mutex;

use anyhow::Result;
use async_trait::async_trait;
use jun_pipeline::Sink;
use jun_types::{ExtractMask, ExtractedCheckpoint};
use serde::Serialize;
use tracing::warn;

/// Sink that emits JSONL to stdout. One record per line.
pub struct PipeSink {
    /// Optional table whitelist. `None` = all record types in mask are emitted.
    /// `Some(set)` = only those tables are emitted, and `_table` is omitted
    /// when the set has exactly one entry (homogeneous stream).
    tables: Option<HashSet<String>>,
    mask: ExtractMask,
    /// Buffered stdout. Held under a Mutex because stdout is shared and
    /// `Sink::write_batch` is called from async tasks; we don't want
    /// interleaved bytes.
    out: Mutex<BufWriter<io::Stdout>>,
}

impl PipeSink {
    /// Create a stdout JSONL sink. `tables` filters which record types get
    /// emitted; pass `None` to emit everything in `mask`.
    pub fn new(tables: Option<Vec<String>>, mask: ExtractMask) -> Self {
        let set = tables.map(|v| v.into_iter().collect::<HashSet<_>>());
        Self {
            tables: set,
            mask,
            out: Mutex::new(BufWriter::with_capacity(64 * 1024, io::stdout())),
        }
    }

    fn want(&self, table: &str) -> bool {
        match &self.tables {
            None => true,
            Some(s) => s.contains(table),
        }
    }

    /// True when the user requested exactly one table — emit untagged rows so
    /// the stream is consumer-ready (e.g. for `clickhouse-client INSERT
    /// FORMAT JSONEachRow`).
    fn untagged(&self) -> bool {
        matches!(self.tables.as_ref(), Some(s) if s.len() == 1)
    }
}

#[derive(Serialize)]
struct Tagged<'a, T: Serialize> {
    #[serde(rename = "_table")]
    table: &'a str,
    #[serde(flatten)]
    inner: &'a T,
}

fn write_record<W: Write, T: Serialize>(
    out: &mut W,
    table: &str,
    untagged: bool,
    record: &T,
) -> io::Result<()> {
    // sonic-rs (SIMD JSON) for the hot pipe path — ~2-3× faster than
    // serde_json on this AVX2/AVX-512 box. We go via `to_vec` + `write_all`
    // since sonic-rs's `to_writer` requires its own `WriteExt` trait (not
    // std::io::Write). The intermediate Vec is per-record, ~hundreds of
    // bytes, allocator hot-path noise.
    let bytes = if untagged {
        sonic_rs::to_vec(record).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
    } else {
        let wrapped = Tagged { table, inner: record };
        sonic_rs::to_vec(&wrapped).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
    };
    out.write_all(&bytes)?;
    out.write_all(b"\n")
}

#[async_trait]
impl Sink for PipeSink {
    async fn write_batch(&self, batch: Vec<ExtractedCheckpoint>) -> Result<u64> {
        let mut total = 0u64;
        let mut out = self
            .out
            .lock()
            .map_err(|e| anyhow::anyhow!("pipe stdout poisoned: {e}"))?;
        let untagged = self.untagged();

        for cp in &batch {
            if self.mask.contains(ExtractMask::CHECKPOINTS) && self.want("checkpoints") {
                if let Err(e) = write_record(&mut *out, "checkpoints", untagged, &cp.checkpoint) {
                    warn!(error = %e, "pipe write failed");
                    return Err(e.into());
                }
            }
            if self.mask.contains(ExtractMask::TRANSACTIONS) && self.want("transactions") {
                for r in &cp.transactions {
                    write_record(&mut *out, "transactions", untagged, r)?;
                }
            }
            if self.mask.contains(ExtractMask::MOVE_CALLS) && self.want("move_calls") {
                for r in &cp.move_calls {
                    write_record(&mut *out, "move_calls", untagged, r)?;
                }
            }
            if self.mask.contains(ExtractMask::BALANCE_CHANGES) && self.want("balance_changes") {
                for r in &cp.balance_changes {
                    write_record(&mut *out, "balance_changes", untagged, r)?;
                }
            }
            if self.mask.contains(ExtractMask::OBJECT_CHANGES) && self.want("object_changes") {
                for r in &cp.object_changes {
                    write_record(&mut *out, "object_changes", untagged, r)?;
                }
            }
            if self.mask.contains(ExtractMask::DEPENDENCIES) && self.want("dependencies") {
                for r in &cp.dependencies {
                    write_record(&mut *out, "dependencies", untagged, r)?;
                }
            }
            total += 1;
        }
        // Flush so consumers can see progress in real time on long replays.
        out.flush()?;
        Ok(total)
    }
}
