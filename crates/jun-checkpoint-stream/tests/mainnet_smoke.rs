//! End-to-end smoke against a real mainnet fullnode.
//!
//! Marked `#[ignore]` so it stays out of CI by default. Run with:
//!   cargo test -p jun-checkpoint-stream -- --ignored mainnet_smoke
//!
//! Pulls 10 checkpoints from the live tail and asserts each decodes via
//! `jun_checkpoint_decoder::decode_checkpoint`.

use std::time::Duration;

use futures::StreamExt;
use jun_checkpoint_decoder::decode_checkpoint;
use jun_checkpoint_stream::GrpcClient;
use jun_types::ExtractMask;
use prost::Message as _;

const MAINNET_URL: &str = "https://fullnode.mainnet.sui.io:443";
const HOW_MANY: usize = 10;
const HARD_TIMEOUT: Duration = Duration::from_secs(60);

#[tokio::test]
#[ignore = "hits the public mainnet fullnode; run on demand"]
async fn pulls_and_decodes_ten_checkpoints() {
    let client = GrpcClient::connect(MAINNET_URL, None)
        .await
        .expect("connect to mainnet");

    let stream = client.subscribe_checkpoints(None);
    let collected = tokio::time::timeout(
        HARD_TIMEOUT,
        stream.take(HOW_MANY).collect::<Vec<_>>(),
    )
    .await
    .expect("timed out waiting for checkpoints");

    assert_eq!(collected.len(), HOW_MANY, "expected {HOW_MANY} checkpoints");

    let mut last_seq: Option<u64> = None;
    for item in collected {
        let (seq, cp) = item.expect("checkpoint frame");
        // Re-encode + decode to mirror the production path.
        let bytes = cp.encode_to_vec();
        let decoded = decode_checkpoint(&bytes, ExtractMask::ALL).expect("decode_checkpoint");
        assert!(
            !decoded.checkpoint.sequence_number.is_empty(),
            "decoded checkpoint should have a seq"
        );
        // Monotonic + dense.
        if let Some(prev) = last_seq {
            assert_eq!(seq, prev + 1, "expected dense sequence numbers");
        }
        last_seq = Some(seq);
    }
}
