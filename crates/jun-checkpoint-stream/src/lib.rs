//! jun-checkpoint-stream — live Sui fullnode subscription + decode.
//!
//! `jun stream` connects to a fullnode's `sui.rpc.v2.SubscriptionService` and
//! pumps decoded records into a [`RecordSink`] (stdout, file pipe, NATS).
//!
//! Sui's `SubscribeCheckpoints` endpoint always starts from the latest checkpoint
//! the server has executed. To resume from a specific sequence number we backfill
//! the gap with unary `LedgerService.GetCheckpoint` calls before tailing the
//! live stream. Same shape applies after a transient reconnect: we resume from
//! `last_seen + 1` via gap-fill, then re-attach to the live tail.
//!
//! Decode reuses [`jun_checkpoint_decoder::decode_checkpoint`]. The sui-rpc-generated
//! `Checkpoint` proto and the archive's stored `Checkpoint` proto are the same
//! wire message, so we re-encode with prost and feed the bytes through the
//! existing parser — no duplicated decode logic.

pub mod client;
pub mod sink;
pub mod stream;

pub use client::GrpcClient;
pub use sink::{FileSink, NullSink, RecordSink, StdoutSink, TableFilter};
pub use stream::{stream_records, StreamArgs};
