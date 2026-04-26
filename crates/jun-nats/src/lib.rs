//! jun-nats — publish flat records on per-record-type NATS subjects.
//!
//! Subject layout: `jun.<table>` (e.g. `jun.transactions`, `jun.balance_changes`).
//! Payload: protobuf, see proto/jun.proto.
//!
//! This crate is one of jun's OSS sinks. The contract subscribers consume is
//! the `.proto` schema in `crates/jun-nats/proto/jun.proto`.

use anyhow::{Context, Result};
use async_nats::Client;
use async_trait::async_trait;
use bytes::BytesMut;
use jun_pipeline::Sink;
use jun_types::{ExtractedCheckpoint, ExtractMask};
use prost::Message;
use tracing::{debug, warn};

#[allow(clippy::all, missing_docs)]
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/jun.v1.rs"));
}

mod convert;
mod pipe;
pub use pipe::PipeSink;

/// NATS sink. Connects on construction; publishes per-record-type subjects on
/// each batch.
pub struct NatsSink {
    client: Client,
    prefix: String,
    mask: ExtractMask,
}

impl NatsSink {
    /// Connect to a NATS server and produce a sink.
    ///
    /// `prefix` is prepended to every subject (default `jun`). `mask` controls
    /// which record types get published — pass `ExtractMask::ALL` for everything.
    pub async fn connect(url: &str, prefix: impl Into<String>, mask: ExtractMask) -> Result<Self> {
        let client = async_nats::connect(url)
            .await
            .with_context(|| format!("connect NATS at {url}"))?;
        Ok(Self {
            client,
            prefix: prefix.into(),
            mask,
        })
    }

    fn subject(&self, table: &str) -> String {
        format!("{}.{}", self.prefix, table)
    }

    async fn publish_msg<M: Message>(&self, table: &str, msg: &M, scratch: &mut BytesMut) {
        scratch.clear();
        if let Err(e) = msg.encode(scratch) {
            warn!(table, error = %e, "proto encode failed; dropping record");
            return;
        }
        let bytes = scratch.split().freeze();
        if let Err(e) = self.client.publish(self.subject(table), bytes).await {
            warn!(table, error = %e, "NATS publish failed; dropping record");
        }
    }
}

#[async_trait]
impl Sink for NatsSink {
    async fn write_batch(&self, batch: Vec<ExtractedCheckpoint>) -> Result<u64> {
        let mut scratch = BytesMut::with_capacity(4096);
        let mut total: u64 = 0;

        // Counts for visibility into what flows on each subject.
        let mut n_cp = 0u64;
        let mut n_tx = 0u64;
        let mut n_mc = 0u64;
        let mut n_bc = 0u64;
        let mut n_oc = 0u64;
        let mut n_dep = 0u64;

        for cp in &batch {
            if self.mask.contains(ExtractMask::CHECKPOINTS) {
                let m = convert::checkpoint(&cp.checkpoint);
                self.publish_msg("checkpoints", &m, &mut scratch).await;
                n_cp += 1;
            }
            if self.mask.contains(ExtractMask::TRANSACTIONS) {
                for r in &cp.transactions {
                    let m = convert::transaction(r);
                    self.publish_msg("transactions", &m, &mut scratch).await;
                    n_tx += 1;
                }
            }
            if self.mask.contains(ExtractMask::MOVE_CALLS) {
                for r in &cp.move_calls {
                    let m = convert::move_call(r);
                    self.publish_msg("move_calls", &m, &mut scratch).await;
                    n_mc += 1;
                }
            }
            if self.mask.contains(ExtractMask::BALANCE_CHANGES) {
                for r in &cp.balance_changes {
                    let m = convert::balance_change(r);
                    self.publish_msg("balance_changes", &m, &mut scratch).await;
                    n_bc += 1;
                }
            }
            if self.mask.contains(ExtractMask::OBJECT_CHANGES) {
                for r in &cp.object_changes {
                    let m = convert::object_change(r);
                    self.publish_msg("object_changes", &m, &mut scratch).await;
                    n_oc += 1;
                }
            }
            if self.mask.contains(ExtractMask::DEPENDENCIES) {
                for r in &cp.dependencies {
                    let m = convert::dependency(r);
                    self.publish_msg("dependencies", &m, &mut scratch).await;
                    n_dep += 1;
                }
            }
            total += 1;
        }
        debug!(
            cps = n_cp, tx = n_tx, mc = n_mc, bc = n_bc, oc = n_oc, dep = n_dep,
            "batch published"
        );

        // Flush any in-flight pending publishes so the caller doesn't advance
        // the cursor before NATS has acked them. For core NATS this is a
        // server-side flush of the publisher's outbound buffer.
        if let Err(e) = self.client.flush().await {
            debug!(error = %e, "NATS flush returned error (non-fatal)");
        }

        Ok(total)
    }
}
