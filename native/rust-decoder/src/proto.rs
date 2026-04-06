//! Minimal protobuf wire format parser.
//!
//! Walks checkpoint and gRPC envelope protos to extract the raw byte ranges we
//! need for native decoding without generated protobuf code.

pub struct ProtoBalanceChange<'a> {
    pub address: &'a str,
    pub coin_type: &'a str,
    pub amount: &'a str,
}

#[allow(dead_code)]
pub struct ProtoObject<'a> {
    pub object_id: &'a str,
    pub version: u64,
    pub bcs: &'a [u8],
}

/// A single transaction's BCS byte ranges extracted from the proto.
pub struct ProtoTransaction<'a> {
    pub digest: &'a str,
    pub effects_bcs: Option<&'a [u8]>,
    pub transaction_bcs: Option<&'a [u8]>,
    pub events_bcs: Option<&'a [u8]>,
    pub balance_changes: Vec<ProtoBalanceChange<'a>>,
}

/// Parsed checkpoint proto — just the BCS byte slices we need.
pub struct ProtoCheckpoint<'a> {
    pub sequence_number: Option<String>,
    pub digest: Option<String>,
    pub summary_bcs: Option<&'a [u8]>,
    pub transactions: Vec<ProtoTransaction<'a>>,
    pub objects: Vec<ProtoObject<'a>>,
}

pub struct ProtoSubscribeCheckpointsResponse<'a> {
    pub cursor: String,
    pub checkpoint_bytes: &'a [u8],
}

/// Parse a decompressed checkpoint protobuf, extracting BCS byte ranges.
pub fn parse_checkpoint(buf: &[u8]) -> Result<ProtoCheckpoint<'_>, &'static str> {
    let mut result = ProtoCheckpoint {
        sequence_number: None,
        digest: None,
        summary_bcs: None,
        transactions: Vec::new(),
        objects: Vec::new(),
    };

    let mut pos = 0;
    while pos < buf.len() {
        let (field_number, wire_type, new_pos) = read_tag(buf, pos)?;
        pos = new_pos;

        match (field_number, wire_type) {
            // field 1: sequence_number
            (1, 0) => {
                let (value, new_pos) = read_varint(buf, pos)?;
                pos = new_pos;
                result.sequence_number = Some(value.to_string());
            }
            // field 2: digest (raw bytes → base58 string)
            (2, 2) => {
                let (data, new_pos) = read_length_delimited(buf, pos)?;
                pos = new_pos;
                // Try as UTF-8 first (gRPC format), then encode as base58 (proto format)
                result.digest = std::str::from_utf8(data)
                    .ok()
                    .map(|s| s.to_string())
                    .or_else(|| Some(bs58::encode(data).into_string()));
            }
            // field 3: summary (length-delimited message)
            (3, 2) => {
                let (data, new_pos) = read_length_delimited(buf, pos)?;
                pos = new_pos;
                // Parse CheckpointSummary message to find bcs.value
                result.summary_bcs = extract_bcs_value(data);
            }
            // field 6: transactions (repeated, length-delimited)
            (6, 2) => {
                let (data, new_pos) = read_length_delimited(buf, pos)?;
                pos = new_pos;
                if let Some(tx) = parse_executed_transaction(data) {
                    result.transactions.push(tx);
                }
            }
            // field 7: objects (ObjectSet message)
            (7, 2) => {
                let (data, new_pos) = read_length_delimited(buf, pos)?;
                pos = new_pos;
                result.objects.extend(parse_object_set(data)?);
            }
            // Skip all other fields
            _ => {
                pos = skip_field(buf, pos, wire_type)?;
            }
        }
    }

    // Compute checkpoint digest from summary BCS if available and digest not already set
    if result.digest.is_none() {
        if let Some(summary_bcs) = result.summary_bcs {
            result.digest = Some(compute_checkpoint_digest(summary_bcs));
        }
    }

    Ok(result)
}

/// Compute checkpoint digest: BLAKE2b-256("CheckpointSummary::" + BCS), base58 encoded.
fn compute_checkpoint_digest(summary_bcs: &[u8]) -> String {
    use blake2::{digest::consts::U32, Blake2b, Digest};
    type Blake2b256 = Blake2b<U32>;
    let mut hasher = Blake2b256::new();
    hasher.update(b"CheckpointSummary::");
    hasher.update(summary_bcs);
    let hash = hasher.finalize();
    bs58::encode(&hash).into_string()
}

fn parse_object_set(buf: &[u8]) -> Result<Vec<ProtoObject<'_>>, &'static str> {
    let mut objects = Vec::new();
    let mut pos = 0;

    while pos < buf.len() {
        let (field_number, wire_type, new_pos) = read_tag(buf, pos)?;
        pos = new_pos;

        match (field_number, wire_type) {
            // field 1: objects (repeated Object message)
            (1, 2) => {
                let (data, new_pos) = read_length_delimited(buf, pos)?;
                pos = new_pos;
                if let Some(object) = parse_object(data) {
                    objects.push(object);
                }
            }
            _ => {
                pos = skip_field(buf, pos, wire_type)?;
            }
        }
    }

    Ok(objects)
}

fn parse_object(buf: &[u8]) -> Option<ProtoObject<'_>> {
    let mut object_id = "";
    let mut version = 0;
    let mut bcs = None;

    let mut pos = 0;
    while pos < buf.len() {
        let (field_number, wire_type, new_pos) = read_tag(buf, pos).ok()?;
        pos = new_pos;

        match (field_number, wire_type) {
            // field 1: bcs (Bcs message)
            (1, 2) => {
                let (data, new_pos) = read_length_delimited(buf, pos).ok()?;
                pos = new_pos;
                bcs = extract_bcs_value_field(data);
            }
            // field 2: object_id
            (2, 2) => {
                let (data, new_pos) = read_length_delimited(buf, pos).ok()?;
                pos = new_pos;
                object_id = std::str::from_utf8(data).unwrap_or("");
            }
            // field 3: version
            (3, 0) => {
                let (value, new_pos) = read_varint(buf, pos).ok()?;
                pos = new_pos;
                version = value;
            }
            _ => {
                pos = skip_field(buf, pos, wire_type).ok()?;
            }
        }
    }

    Some(ProtoObject {
        object_id,
        version,
        bcs: bcs?,
    })
}

/// Parse an ExecutedTransaction message.
fn parse_executed_transaction(buf: &[u8]) -> Option<ProtoTransaction<'_>> {
    let mut digest = "";
    let mut effects_bcs = None;
    let mut transaction_bcs = None;
    let mut events_bcs = None;
    let mut balance_changes = Vec::new();

    let mut pos = 0;
    while pos < buf.len() {
        let (field_number, wire_type, new_pos) = read_tag(buf, pos).ok()?;
        pos = new_pos;
        // eprintln!("  [tx] field={} wire={}", field_number, wire_type);

        match (field_number, wire_type) {
            // field 1: digest (string)
            (1, 2) => {
                let (data, new_pos) = read_length_delimited(buf, pos).ok()?;
                pos = new_pos;
                digest = std::str::from_utf8(data).unwrap_or("");
            }
            // field 2: transaction (message containing bcs)
            (2, 2) => {
                let (data, new_pos) = read_length_delimited(buf, pos).ok()?;
                pos = new_pos;
                transaction_bcs = extract_bcs_value(data);
            }
            // field 3: signatures — skip
            // field 4: effects (message containing bcs)
            (4, 2) => {
                let (data, new_pos) = read_length_delimited(buf, pos).ok()?;
                pos = new_pos;
                effects_bcs = extract_bcs_value(data);
            }
            // field 5: events (message containing bcs)
            (5, 2) => {
                let (data, new_pos) = read_length_delimited(buf, pos).ok()?;
                pos = new_pos;
                events_bcs = extract_bcs_value(data);
            }
            // field 8: balance_changes
            (8, 2) => {
                let (data, new_pos) = read_length_delimited(buf, pos).ok()?;
                pos = new_pos;
                if let Some(change) = parse_balance_change(data) {
                    balance_changes.push(change);
                }
            }
            // Skip other fields (signatures, timestamp, objects, etc.)
            _ => {
                pos = skip_field(buf, pos, wire_type).ok()?;
            }
        }
    }

    Some(ProtoTransaction {
        digest,
        effects_bcs,
        transaction_bcs,
        events_bcs,
        balance_changes,
    })
}

fn parse_balance_change(buf: &[u8]) -> Option<ProtoBalanceChange<'_>> {
    let mut address = "";
    let mut coin_type = "";
    let mut amount = "";

    let mut pos = 0;
    while pos < buf.len() {
        let (field_number, wire_type, new_pos) = read_tag(buf, pos).ok()?;
        pos = new_pos;

        match (field_number, wire_type) {
            (1, 2) => {
                let (data, new_pos) = read_length_delimited(buf, pos).ok()?;
                pos = new_pos;
                address = std::str::from_utf8(data).unwrap_or("");
            }
            (2, 2) => {
                let (data, new_pos) = read_length_delimited(buf, pos).ok()?;
                pos = new_pos;
                coin_type = std::str::from_utf8(data).unwrap_or("");
            }
            (3, 2) => {
                let (data, new_pos) = read_length_delimited(buf, pos).ok()?;
                pos = new_pos;
                amount = std::str::from_utf8(data).unwrap_or("");
            }
            _ => {
                pos = skip_field(buf, pos, wire_type).ok()?;
            }
        }
    }

    if address.is_empty() || coin_type.is_empty() {
        return None;
    }

    Some(ProtoBalanceChange {
        address,
        coin_type,
        amount,
    })
}

pub fn parse_subscribe_checkpoints_response(
    buf: &[u8],
) -> Result<ProtoSubscribeCheckpointsResponse<'_>, &'static str> {
    let mut cursor = None;
    let mut checkpoint_bytes = None;

    let mut pos = 0;
    while pos < buf.len() {
        let (field_number, wire_type, new_pos) = read_tag(buf, pos)?;
        pos = new_pos;

        match (field_number, wire_type) {
            (1, 0) => {
                let (value, new_pos) = read_varint(buf, pos)?;
                pos = new_pos;
                cursor = Some(value.to_string());
            }
            (2, 2) => {
                let (data, new_pos) = read_length_delimited(buf, pos)?;
                pos = new_pos;
                checkpoint_bytes = Some(data);
            }
            _ => {
                pos = skip_field(buf, pos, wire_type)?;
            }
        }
    }

    match checkpoint_bytes {
        Some(checkpoint_bytes) => Ok(ProtoSubscribeCheckpointsResponse {
            cursor: cursor.unwrap_or_default(),
            checkpoint_bytes,
        }),
        None => Err("subscribe response: missing checkpoint"),
    }
}

pub fn parse_get_checkpoint_response(buf: &[u8]) -> Result<&[u8], &'static str> {
    let mut pos = 0;
    while pos < buf.len() {
        let (field_number, wire_type, new_pos) = read_tag(buf, pos)?;
        pos = new_pos;

        match (field_number, wire_type) {
            (1, 2) => {
                let (data, _) = read_length_delimited(buf, pos)?;
                return Ok(data);
            }
            _ => {
                pos = skip_field(buf, pos, wire_type)?;
            }
        }
    }

    Err("get checkpoint response: missing checkpoint")
}

/// Extract bcs.value from a message that has a nested Bcs message.
/// Structure: outer message { field 1: Bcs { field 1: bytes value } }
fn extract_bcs_value(buf: &[u8]) -> Option<&[u8]> {
    let mut pos = 0;
    while pos < buf.len() {
        let (field_number, wire_type, new_pos) = read_tag(buf, pos).ok()?;
        pos = new_pos;

        if field_number == 1 && wire_type == 2 {
            // This is the bcs field (length-delimited message)
            let (bcs_msg, _) = read_length_delimited(buf, pos).ok()?;
            // Parse the Bcs message to find field 1 (value: bytes)
            let result = extract_bcs_value_field(bcs_msg);
            return result;
        } else {
            pos = skip_field(buf, pos, wire_type).ok()?;
        }
    }
    None
}

/// Extract field 2 (value: bytes) from a Bcs message.
/// Bcs proto: { name: string = 1; value: bytes = 2; }
fn extract_bcs_value_field(buf: &[u8]) -> Option<&[u8]> {
    let mut pos = 0;
    while pos < buf.len() {
        let (field_number, wire_type, new_pos) = read_tag(buf, pos).ok()?;
        pos = new_pos;

        if field_number == 2 && wire_type == 2 {
            let (data, _) = read_length_delimited(buf, pos).ok()?;
            return Some(data);
        } else {
            pos = skip_field(buf, pos, wire_type).ok()?;
        }
    }
    None
}

// --- Protobuf wire format primitives ---

fn read_varint(buf: &[u8], pos: usize) -> Result<(u64, usize), &'static str> {
    let mut value: u64 = 0;
    let mut shift = 0;
    let mut p = pos;
    loop {
        if p >= buf.len() {
            return Err("varint: unexpected end of input");
        }
        let byte = buf[p];
        p += 1;
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return Err("varint: too many bytes");
        }
    }
    Ok((value, p))
}

fn read_tag(buf: &[u8], pos: usize) -> Result<(u32, u32, usize), &'static str> {
    let (tag, new_pos) = read_varint(buf, pos)?;
    let field_number = (tag >> 3) as u32;
    let wire_type = (tag & 0x7) as u32;
    Ok((field_number, wire_type, new_pos))
}

fn read_length_delimited<'a>(buf: &'a [u8], pos: usize) -> Result<(&'a [u8], usize), &'static str> {
    let (len, new_pos) = read_varint(buf, pos)?;
    let len = len as usize;
    let end = new_pos + len;
    if end > buf.len() {
        return Err("length-delimited: data extends past buffer");
    }
    Ok((&buf[new_pos..end], end))
}

fn skip_field(buf: &[u8], pos: usize, wire_type: u32) -> Result<usize, &'static str> {
    match wire_type {
        0 => {
            // Varint
            let (_, new_pos) = read_varint(buf, pos)?;
            Ok(new_pos)
        }
        1 => {
            // 64-bit
            if pos + 8 > buf.len() {
                return Err("skip 64-bit: unexpected end");
            }
            Ok(pos + 8)
        }
        2 => {
            // Length-delimited
            let (_, new_pos) = read_length_delimited(buf, pos)?;
            Ok(new_pos)
        }
        5 => {
            // 32-bit
            if pos + 4 > buf.len() {
                return Err("skip 32-bit: unexpected end");
            }
            Ok(pos + 4)
        }
        _ => Err("unknown wire type"),
    }
}
