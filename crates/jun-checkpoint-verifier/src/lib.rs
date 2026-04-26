//! Sui checkpoint verifier.
//!
//! Thin wrapper over the official MystenLabs crates (sui-sdk-types +
//! fastcrypto) plus a minimal protobuf parser and roaring-bitmap decoder.
//!
//! Native-only. Used by the ingester to BLS-verify each checkpoint against
//! its epoch's committee at archive write time. The proxy (Cloudflare
//! Workers, TypeScript) reads from a pre-verified R2 archive and does no
//! crypto — the ingest-side check is the integrity claim.
//!
//! Committee packed layout:
//!   u32 n_validators                 (LE)
//!   for each:
//!     96-byte BLS12-381 G2 pubkey
//!     u64 stake                      (LE)

use anyhow::{Context, Result};

pub const ENTRY_SIZE: usize = 96 + 8;
pub const INTENT_PREFIX: [u8; 3] = [2, 0, 0]; // CheckpointSummary, V0, Sui

/// Decompress a zstd stream (possibly multi-frame) into a single Vec.
/// Uses libzstd via the `zstd` crate — SIMD-accelerated on x86 and ARM,
/// ~2× faster than the pure-Rust `ruzstd` we used while the verifier
/// also targeted wasm.
fn zstd_decode(zst: &[u8]) -> Result<Vec<u8>> {
    zstd::stream::decode_all(zst).map_err(|e| anyhow::anyhow!("zstd decode: {e}"))
}

// ── Protobuf parser (only the fields we need) ────────────────────────────────

pub struct ParsedProto<'a> {
    pub summary_bcs: &'a [u8],
    pub sig_epoch: u64,
    pub sig: [u8; 48],
    pub bitmap: &'a [u8],
}

fn read_varint(data: &[u8], pos: &mut usize) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        let b = *data.get(*pos).context("proto: unexpected end")?;
        *pos += 1;
        result |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Ok(result);
        }
        shift = shift.checked_add(7).context("proto: varint overflow")?;
        if shift >= 64 {
            anyhow::bail!("proto: varint overflow");
        }
    }
}

fn skip_field(data: &[u8], pos: &mut usize, wire_type: u8) -> Result<()> {
    match wire_type {
        0 => {
            read_varint(data, pos)?;
        }
        1 => {
            if *pos + 8 > data.len() {
                anyhow::bail!("proto: unexpected end");
            }
            *pos += 8;
        }
        2 => {
            let len = read_varint(data, pos)? as usize;
            if *pos + len > data.len() {
                anyhow::bail!("proto: unexpected end");
            }
            *pos += len;
        }
        5 => {
            if *pos + 4 > data.len() {
                anyhow::bail!("proto: unexpected end");
            }
            *pos += 4;
        }
        _ => anyhow::bail!("proto: unsupported wire type {wire_type}"),
    }
    Ok(())
}

fn extract_bcs_value(data: &[u8]) -> Result<Option<&[u8]>> {
    let mut pos = 0usize;
    while pos < data.len() {
        let tag = read_varint(data, &mut pos)?;
        let field_num = (tag >> 3) as u32;
        let wire_type = (tag & 7) as u8;
        if wire_type != 2 {
            skip_field(data, &mut pos, wire_type)?;
            continue;
        }
        let len = read_varint(data, &mut pos)? as usize;
        if pos + len > data.len() {
            anyhow::bail!("proto: unexpected end");
        }
        let sub = &data[pos..pos + len];
        pos += len;
        if field_num != 1 {
            continue;
        }

        let mut p2 = 0usize;
        while p2 < sub.len() {
            let t2 = read_varint(sub, &mut p2)?;
            let fn2 = (t2 >> 3) as u32;
            let wt2 = (t2 & 7) as u8;
            if wt2 != 2 {
                skip_field(sub, &mut p2, wt2)?;
                continue;
            }
            let l2 = read_varint(sub, &mut p2)? as usize;
            if p2 + l2 > sub.len() {
                anyhow::bail!("proto: unexpected end");
            }
            let val = &sub[p2..p2 + l2];
            p2 += l2;
            if fn2 == 2 {
                return Ok(Some(val));
            }
        }
    }
    Ok(None)
}

pub fn parse_proto(data: &[u8]) -> Result<ParsedProto<'_>> {
    let mut summary_bcs: Option<&[u8]> = None;
    let mut sig_epoch: u64 = 0;
    let mut sig: Option<[u8; 48]> = None;
    let mut bitmap: Option<&[u8]> = None;

    let mut pos = 0usize;
    while pos < data.len() {
        let tag = read_varint(data, &mut pos)?;
        let field_num = (tag >> 3) as u32;
        let wire_type = (tag & 7) as u8;
        if wire_type != 2 {
            skip_field(data, &mut pos, wire_type)?;
            continue;
        }
        let len = read_varint(data, &mut pos)? as usize;
        if pos + len > data.len() {
            anyhow::bail!("proto: unexpected end");
        }
        let payload = &data[pos..pos + len];
        pos += len;

        match field_num {
            3 => summary_bcs = extract_bcs_value(payload)?,
            4 => {
                let mut p2 = 0usize;
                while p2 < payload.len() {
                    let t2 = read_varint(payload, &mut p2)?;
                    let fn2 = (t2 >> 3) as u32;
                    let wt2 = (t2 & 7) as u8;
                    if wt2 == 0 {
                        let v = read_varint(payload, &mut p2)?;
                        if fn2 == 1 {
                            sig_epoch = v;
                        }
                    } else if wt2 == 2 {
                        let l2 = read_varint(payload, &mut p2)? as usize;
                        if p2 + l2 > payload.len() {
                            anyhow::bail!("proto: unexpected end");
                        }
                        let sub = &payload[p2..p2 + l2];
                        p2 += l2;
                        if fn2 == 2 {
                            if sub.len() != 48 {
                                anyhow::bail!("sig length {} != 48", sub.len());
                            }
                            let mut arr = [0u8; 48];
                            arr.copy_from_slice(sub);
                            sig = Some(arr);
                        } else if fn2 == 3 {
                            bitmap = Some(sub);
                        }
                    } else {
                        skip_field(payload, &mut p2, wt2)?;
                    }
                }
            }
            _ => {}
        }
    }

    Ok(ParsedProto {
        summary_bcs: summary_bcs.context("proto: missing summary_bcs")?,
        sig_epoch,
        sig: sig.context("proto: missing sig")?,
        bitmap: bitmap.context("proto: missing bitmap")?,
    })
}

// ── Committee pack / unpack ──────────────────────────────────────────────────

pub fn pack_committee(validators: &[(Vec<u8>, u64)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + validators.len() * ENTRY_SIZE);
    out.extend_from_slice(&(validators.len() as u32).to_le_bytes());
    for (pk, stake) in validators {
        assert_eq!(pk.len(), 96, "pubkey must be 96 bytes");
        out.extend_from_slice(pk);
        out.extend_from_slice(&stake.to_le_bytes());
    }
    out
}

fn unpack_committee(packed: &[u8]) -> Result<Vec<(&[u8], u64)>> {
    if packed.len() < 4 {
        anyhow::bail!("committee: too small");
    }
    let n = u32::from_le_bytes(packed[0..4].try_into().unwrap()) as usize;
    let body = &packed[4..];
    if body.len() != n * ENTRY_SIZE {
        anyhow::bail!(
            "committee: expected {} body bytes, got {}",
            n * ENTRY_SIZE,
            body.len()
        );
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let off = i * ENTRY_SIZE;
        let pk = &body[off..off + 96];
        let stake = u64::from_le_bytes(body[off + 96..off + 104].try_into().unwrap());
        out.push((pk, stake));
    }
    Ok(out)
}

// ── RoaringBitmap signer decoder (strict: key=0, stored offsets verified) ────

fn decode_roaring_signers(data: &[u8]) -> Result<Vec<usize>> {
    const SERIAL_COOKIE_NO_RUN: u32 = 12346;
    if data.len() < 8 {
        anyhow::bail!("roaring: truncated");
    }
    let cookie = u32::from_le_bytes(data[0..4].try_into().unwrap());
    if cookie != SERIAL_COOKIE_NO_RUN {
        anyhow::bail!("roaring: unsupported cookie {cookie}");
    }
    let num_containers = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
    let header_end = 8 + num_containers * 8;
    if data.len() < header_end {
        anyhow::bail!("roaring: truncated header");
    }

    let mut out = Vec::new();
    let mut offset = header_end;

    for i in 0..num_containers {
        let hdr = 8 + i * 4;
        let key = u16::from_le_bytes(data[hdr..hdr + 2].try_into().unwrap());
        if key != 0 {
            anyhow::bail!("roaring: non-zero key {key} not supported");
        }
        let stored_offset_pos = 8 + num_containers * 4 + i * 4;
        let stored_offset = u32::from_le_bytes(
            data[stored_offset_pos..stored_offset_pos + 4].try_into().unwrap(),
        ) as usize;
        if stored_offset != offset {
            anyhow::bail!(
                "roaring: offset mismatch (stored {stored_offset}, expected {offset})"
            );
        }
        let cardinality = u16::from_le_bytes(data[hdr + 2..hdr + 4].try_into().unwrap()) as usize + 1;

        if cardinality <= 4096 {
            let needed = cardinality * 2;
            if offset + needed > data.len() {
                anyhow::bail!("roaring: truncated array container");
            }
            for j in 0..cardinality {
                let v = u16::from_le_bytes(
                    data[offset + j * 2..offset + j * 2 + 2].try_into().unwrap(),
                );
                out.push(v as usize);
            }
            offset += needed;
        } else {
            if offset + 1024 > data.len() {
                anyhow::bail!("roaring: truncated bitset container");
            }
            for bit in 0..8192 {
                if data[offset + bit / 8] & (1u8 << (bit % 8)) != 0 {
                    out.push(bit);
                }
            }
            offset += 1024;
        }
    }
    Ok(out)
}

// ── Public native API ────────────────────────────────────────────────────────

/// Decompress + parse a checkpoint `.binpb.zst`, extract the
/// `next_epoch_committee` from `end_of_epoch_data`, pack into the canonical
/// wire layout.
pub fn extract_next_committee_from_zst(zst: &[u8]) -> Result<Vec<u8>> {
    let proto = zstd_decode(zst).context("zstd decompress")?;
    extract_next_committee(&proto)
}

pub fn extract_next_committee(proto: &[u8]) -> Result<Vec<u8>> {
    let parsed = parse_proto(proto)?;
    let summary: sui_sdk_types::CheckpointSummary =
        bcs::from_bytes(parsed.summary_bcs).context("bcs decode CheckpointSummary")?;
    let eoe = summary
        .end_of_epoch_data
        .as_ref()
        .context("not an end-of-epoch checkpoint")?;
    let validators: Vec<(Vec<u8>, u64)> = eoe
        .next_epoch_committee
        .iter()
        .map(|m| (m.public_key.inner().to_vec(), m.stake))
        .collect();
    Ok(pack_committee(&validators))
}

/// Domain separation tag for Sui's BLS12-381 min-sig signatures. Sig lives
/// in G1, message is hashed to G1; DST names the hash-to-curve variant.
pub const SUI_DST: &[u8] = b"BLS_SIG_BLS12381G1_XMD:SHA-256_SSWU_RO_NUL_";

/// Verify a decompressed checkpoint proto against a packed committee.
/// Canonical Sui message: `[2,0,0] ++ bcs(summary) ++ u64_le(epoch)`.
pub fn verify_checkpoint(
    proto: &[u8],
    committee_packed: &[u8],
    committee_epoch: u64,
) -> Result<()> {
    use blst::min_sig::{AggregatePublicKey, PublicKey, Signature};
    use blst::BLST_ERROR;

    let parsed = parse_proto(proto)?;
    let validators = unpack_committee(committee_packed)?;

    if parsed.sig_epoch != committee_epoch {
        anyhow::bail!(
            "wrong epoch: sig={} committee={}",
            parsed.sig_epoch,
            committee_epoch
        );
    }

    let signers = decode_roaring_signers(parsed.bitmap)?;
    let total_stake: u64 = validators.iter().map(|(_, s)| *s).sum();
    let signed_stake: u64 = signers
        .iter()
        .map(|&i| {
            validators
                .get(i)
                .map(|(_, s)| *s)
                .ok_or_else(|| anyhow::anyhow!("signer index {i} out of range"))
        })
        .sum::<Result<_>>()?;
    if signed_stake * 3 < total_stake * 2 + 1 {
        anyhow::bail!("insufficient stake: {signed_stake}/{total_stake}");
    }

    // Aggregate the signing pubkeys (with subgroup + infinity checks). Sui
    // enforces proof-of-possession when committees rotate, so we trust the
    // pubkeys are individually valid G2 points; but we still deserialise
    // strictly — a malformed committee row should never verify.
    let pks: Vec<PublicKey> = signers
        .iter()
        .map(|&i| {
            PublicKey::from_bytes(validators[i].0)
                .map_err(|e| anyhow::anyhow!("bad pubkey: {:?}", e))
        })
        .collect::<Result<_>>()?;
    if pks.is_empty() {
        anyhow::bail!("no signers");
    }
    let mut agg = AggregatePublicKey::from_public_key(&pks[0]);
    let pk_refs: Vec<&PublicKey> = pks.iter().skip(1).collect();
    for pk in pk_refs {
        agg.add_public_key(pk, true)
            .map_err(|e| anyhow::anyhow!("aggregate pk: {:?}", e))?;
    }
    let agg_pk = agg.to_public_key();

    let sig = Signature::from_bytes(&parsed.sig)
        .map_err(|e| anyhow::anyhow!("bad sig: {:?}", e))?;

    let mut msg = Vec::with_capacity(INTENT_PREFIX.len() + parsed.summary_bcs.len() + 8);
    msg.extend_from_slice(&INTENT_PREFIX);
    msg.extend_from_slice(parsed.summary_bcs);
    msg.extend_from_slice(&committee_epoch.to_le_bytes());

    let result = sig.verify(true, &msg, SUI_DST, &[], &agg_pk, true);
    if result != BLST_ERROR::BLST_SUCCESS {
        anyhow::bail!("aggregate verify failed: {:?}", result);
    }
    Ok(())
}

/// Convenience: decompress a `.binpb.zst` and verify in one call (used by
/// the WASM entrypoint since Workers can't natively decompress zstd).
pub fn verify_checkpoint_zst(
    zst: &[u8],
    committee_packed: &[u8],
    committee_epoch: u64,
) -> Result<()> {
    let proto = zstd_decode(zst).context("zstd decompress")?;
    verify_checkpoint(&proto, committee_packed, committee_epoch)
}

// ── Committee cache (deserialized once per epoch) ────────────────────────────
//
// `PublicKey::from_bytes` runs subgroup + infinity checks on a 96-byte
// G2 pubkey — single-digit milliseconds per call when fully validating.
// In the ingest hot path we hit the SAME committee 350K+ times per epoch
// (each cp's signers are a subset of one fixed committee). Deserializing
// the validator pubkeys once at job start, then indexing into the cached
// `Committee`, drops the per-cp pubkey-deserialize cost from
// ~6-7ms → ~0 (just an Arc bump + a few G2 additions for aggregation).

#[derive(Clone, Debug)]
pub struct CommitteeValidator {
    pub stake: u64,
    pub pubkey: blst::min_sig::PublicKey,
}

#[derive(Clone, Debug)]
pub struct Committee {
    pub validators: Vec<CommitteeValidator>,
    pub total_stake: u64,
}

impl Committee {
    /// Deserialize the canonical packed format into per-validator
    /// `(stake, PublicKey)` pairs. PublicKeys are validated here once;
    /// downstream aggregation skips re-validation for speed.
    pub fn from_packed(packed: &[u8]) -> Result<Self> {
        let raw = unpack_committee(packed)?;
        let mut total_stake: u64 = 0;
        let mut validators = Vec::with_capacity(raw.len());
        for (pk_bytes, stake) in raw {
            total_stake = total_stake
                .checked_add(stake)
                .ok_or_else(|| anyhow::anyhow!("committee: total_stake overflow"))?;
            let pubkey = blst::min_sig::PublicKey::from_bytes(pk_bytes)
                .map_err(|e| anyhow::anyhow!("bad pubkey: {:?}", e))?;
            validators.push(CommitteeValidator { stake, pubkey });
        }
        Ok(Self {
            validators,
            total_stake,
        })
    }

    pub fn len(&self) -> usize {
        self.validators.len()
    }

    pub fn is_empty(&self) -> bool {
        self.validators.is_empty()
    }
}

// ── Batched verification (multi-Miller-loop, single final_exp) ───────────────
//
// Each `add()` does parsing + pubkey aggregation + a Miller loop into a shared
// `Pairing` accumulator. The expensive `final_exp` (~70% of pairing cost)
// runs once per batch in `finalize()`. With batch size 64-128 this drops
// per-checkpoint BLS cost from ~6ms (full pairing) to ~1.5-2ms (just the
// Miller loop) — a 3-4× speedup.
//
// Failure mode: if `finalize` returns Err, you don't know which cp is bad.
// Caller should fall back to per-cp `verify_checkpoint` on the batch's
// inputs to localize. For trusted upstream archives (e.g. Mysten's), this
// path is effectively never taken.

/// Stateful accumulator for batched BLS verification of N checkpoints
/// against the *same* committee + epoch.
pub struct CheckpointBatch {
    pairing: blst::Pairing,
    count: usize,
}

impl Default for CheckpointBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl CheckpointBatch {
    pub fn new() -> Self {
        Self {
            pairing: blst::Pairing::new(true, SUI_DST),
            count: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Add one decompressed checkpoint proto to the batch using a cached
    /// `Committee` (pubkeys pre-deserialized once per epoch).
    pub fn add(
        &mut self,
        proto: &[u8],
        committee: &Committee,
        committee_epoch: u64,
    ) -> Result<()> {
        use blst::min_sig::{AggregatePublicKey, PublicKey, Signature};
        use blst::{blst_p1_affine, blst_p2_affine, BLST_ERROR};

        fn pk_raw(pk: &PublicKey) -> &blst_p2_affine {
            unsafe { std::mem::transmute(pk) }
        }
        fn sig_raw(sig: &Signature) -> &blst_p1_affine {
            unsafe { std::mem::transmute(sig) }
        }

        let parsed = parse_proto(proto)?;

        if parsed.sig_epoch != committee_epoch {
            anyhow::bail!(
                "wrong epoch: sig={} committee={}",
                parsed.sig_epoch,
                committee_epoch
            );
        }

        let signers = decode_roaring_signers(parsed.bitmap)?;
        let mut signed_stake: u64 = 0;
        for &i in &signers {
            let v = committee
                .validators
                .get(i)
                .ok_or_else(|| anyhow::anyhow!("signer index {i} out of range"))?;
            signed_stake = signed_stake
                .checked_add(v.stake)
                .ok_or_else(|| anyhow::anyhow!("signed_stake overflow"))?;
        }
        if signed_stake * 3 < committee.total_stake * 2 + 1 {
            anyhow::bail!(
                "insufficient stake: {signed_stake}/{}",
                committee.total_stake
            );
        }

        if signers.is_empty() {
            anyhow::bail!("no signers");
        }
        // Aggregate signing pubkeys via cached, already-validated PublicKeys.
        // `add_public_key(pk, false)` skips re-validation since `Committee`
        // validated all members at construction time.
        let mut agg =
            AggregatePublicKey::from_public_key(&committee.validators[signers[0]].pubkey);
        for &i in signers.iter().skip(1) {
            agg.add_public_key(&committee.validators[i].pubkey, false)
                .map_err(|e| anyhow::anyhow!("aggregate pk: {:?}", e))?;
        }
        let agg_pk = agg.to_public_key();

        let sig = Signature::from_bytes(&parsed.sig)
            .map_err(|e| anyhow::anyhow!("bad sig: {:?}", e))?;

        let mut msg = Vec::with_capacity(INTENT_PREFIX.len() + parsed.summary_bcs.len() + 8);
        msg.extend_from_slice(&INTENT_PREFIX);
        msg.extend_from_slice(parsed.summary_bcs);
        msg.extend_from_slice(&committee_epoch.to_le_bytes());

        // pk_validate=false: agg of pre-validated members is in-subgroup by
        // construction. sig_groupcheck=true: still subgroup-check the signature
        // (sig comes from the wire, not pre-validated).
        let result = self
            .pairing
            .aggregate(pk_raw(&agg_pk), false, sig_raw(&sig), true, &msg, &[]);
        if result != BLST_ERROR::BLST_SUCCESS {
            anyhow::bail!("pairing aggregate: {:?}", result);
        }
        self.count += 1;
        Ok(())
    }

    /// `add` over a zstd-compressed checkpoint.
    pub fn add_zst(
        &mut self,
        zst: &[u8],
        committee: &Committee,
        committee_epoch: u64,
    ) -> Result<()> {
        let proto = zstd_decode(zst).context("zstd decompress")?;
        self.add(&proto, committee, committee_epoch)
    }

    /// Run the deferred final_exp + verify the entire batch atomically.
    /// Consumes the accumulator. Returns the count of cps that were
    /// verified together. On failure, all cps in the batch must be
    /// re-verified individually to localize the bad one.
    pub fn finalize(mut self) -> Result<usize> {
        if self.count == 0 {
            return Ok(0);
        }
        self.pairing.commit();
        if self.pairing.finalverify(None) {
            Ok(self.count)
        } else {
            anyhow::bail!("batch verify failed (cps in batch: {})", self.count)
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    const PROXY: &str = "https://checkpoints.mainnet.sui.unconfirmed.cloud";

    // Epoch 499 last checkpoint = committee source for epoch 500.
    // Epoch 500 first checkpoint = what we verify against that committee.
    const EOE_SEQ: u64   = 50840098;
    const CHECK_SEQ: u64 = 50840099;
    const EPOCH: u64     = 500;

    fn fetch(url: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        ureq::get(url)
            .call()
            .expect("HTTP request failed")
            .into_reader()
            .read_to_end(&mut buf)
            .expect("read body");
        buf
    }

    fn get_committee() -> Vec<u8> {
        let zst = fetch(&format!("{PROXY}/{EOE_SEQ}.binpb.zst"));
        let proto = zstd_decode(&zst).expect("decompress eoe");
        extract_next_committee(&proto).expect("extract committee")
    }

    fn get_checkpoint_proto() -> Vec<u8> {
        let zst = fetch(&format!("{PROXY}/{CHECK_SEQ}.binpb.zst"));
        zstd_decode(&zst).expect("decompress checkpoint")
    }

    #[test]
    fn valid_checkpoint_passes() {
        let committee = get_committee();
        let proto     = get_checkpoint_proto();
        verify_checkpoint(&proto, &committee, EPOCH).expect("valid checkpoint should verify");
    }

    #[test]
    fn tampered_summary_fails() {
        let committee = get_committee();
        let mut proto = get_checkpoint_proto();

        // Locate summary_bcs via pointer arithmetic, flip bytes in the middle.
        let offset = {
            let parsed = parse_proto(&proto).expect("parse");
            let mid = parsed.summary_bcs.as_ptr() as usize
                - proto.as_ptr() as usize
                + parsed.summary_bcs.len() / 2;
            mid
        };
        proto[offset]     ^= 0xFF;
        proto[offset + 1] ^= 0xFF;

        let err = verify_checkpoint(&proto, &committee, EPOCH)
            .expect_err("tampered summary must fail");
        eprintln!("tampered summary error: {err}");
    }

    #[test]
    fn batch_with_single_cp_matches_per_cp_verify() {
        let packed = get_committee();
        let proto = get_checkpoint_proto();
        let committee = Committee::from_packed(&packed).expect("committee");

        // Per-cp path (unchanged: still takes packed bytes for back-compat).
        verify_checkpoint(&proto, &packed, EPOCH).expect("per-cp verify");

        // Batch path with a single member, using cached committee.
        let mut batch = CheckpointBatch::new();
        batch.add(&proto, &committee, EPOCH).expect("batch add");
        let n = batch.finalize().expect("batch finalize");
        assert_eq!(n, 1);
    }

    #[test]
    fn batch_tampered_summary_fails() {
        let packed = get_committee();
        let committee = Committee::from_packed(&packed).expect("committee");
        let mut proto = get_checkpoint_proto();
        let offset = {
            let parsed = parse_proto(&proto).expect("parse");
            parsed.summary_bcs.as_ptr() as usize - proto.as_ptr() as usize
                + parsed.summary_bcs.len() / 2
        };
        proto[offset] ^= 0xFF;
        proto[offset + 1] ^= 0xFF;

        let mut batch = CheckpointBatch::new();
        batch.add(&proto, &committee, EPOCH).expect("batch add (queues miller)");
        let err = batch.finalize().expect_err("batch finalize must fail");
        eprintln!("batch tampered error: {err}");
    }

    #[test]
    fn tampered_signature_fails() {
        let committee = get_committee();
        let proto     = get_checkpoint_proto();

        // Parse to get the sig bytes, rebuild proto with sig inverted.
        let parsed = parse_proto(&proto).expect("parse");
        let mut tampered = proto.clone();

        // Find the sig in the proto by scanning for the exact 48-byte sequence.
        let sig = parsed.sig;
        let pos = tampered
            .windows(48)
            .position(|w| w == sig)
            .expect("sig bytes must appear in proto");
        for b in &mut tampered[pos..pos + 48] {
            *b ^= 0xFF;
        }

        let err = verify_checkpoint(&tampered, &committee, EPOCH)
            .expect_err("tampered signature must fail");
        eprintln!("tampered signature error: {err}");
    }
}
