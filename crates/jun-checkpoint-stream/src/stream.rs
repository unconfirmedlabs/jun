//! `jun stream` orchestrator: gRPC subscribe + decode + sink dispatch.

use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use jun_checkpoint_decoder::decode_checkpoint;
use jun_types::ExtractMask;
use prost::Message;
use tracing::{debug, info, warn};

use crate::client::GrpcClient;
use crate::sink::RecordSink;

/// Args for [`stream_records`]. Lean — sink construction lives in the
/// caller (the CLI knows whether to wire stdout/file/NATS).
pub struct StreamArgs {
    pub url: String,
    pub start_seq: Option<u64>,
    pub mask: ExtractMask,
    /// Per-frame TLS override. Most callers pass `None`.
    pub tls: Option<bool>,
}

/// Run the live stream: subscribe → decode → fan out to `sink`. Returns when
/// the underlying stream errors or the process is interrupted.
///
/// SIGINT handling is the caller's job — wrap this future in `tokio::select!`
/// against `tokio::signal::ctrl_c()`.
pub async fn stream_records(args: StreamArgs, sink: Arc<dyn RecordSink>) -> Result<()> {
    let client = GrpcClient::connect(&args.url, args.tls).await?;
    info!(endpoint = %client.endpoint(), start_seq = ?args.start_seq, "jun stream starting");

    let mut stream = Box::pin(client.subscribe_checkpoints(args.start_seq));

    let mut count: u64 = 0;
    while let Some(item) = stream.next().await {
        match item {
            Ok((seq, cp)) => {
                let mask = args.mask;
                let sink = sink.clone();
                // Decode is CPU-bound; off-load to the blocking pool to
                // avoid starving the tokio reactor running the gRPC stream.
                let bytes = cp.encode_to_vec();
                let decoded = tokio::task::spawn_blocking(move || decode_checkpoint(&bytes, mask))
                    .await
                    .map_err(|e| anyhow!("decode task join: {e}"))??;
                if let Err(e) = sink.write_checkpoint(&decoded).await {
                    warn!(error = %e, seq, "sink write_checkpoint failed");
                    return Err(e.context(format!("sink failed at seq={seq}")));
                }
                count += 1;
                if count % 100 == 0 {
                    debug!(count, last_seq = seq, "stream progress");
                }
            }
            Err(e) => {
                return Err(e).with_context(|| "subscription stream errored");
            }
        }
    }
    sink.flush().await.context("final sink flush")?;
    info!(count, "jun stream ended");
    Ok(())
}
