//! Minimal protobuf wire format parser.
//!
//! Walks the Checkpoint proto to extract BCS byte ranges for:
//! - CheckpointSummary (summary.bcs.value)
//! - Per-transaction: effects.bcs.value, transaction.bcs.value, events.bcs.value, digest

/// A single transaction's BCS byte ranges extracted from the proto.
pub struct ProtoTransaction<'a> {
    pub digest: &'a str,
    pub effects_bcs: Option<&'a [u8]>,
    pub transaction_bcs: Option<&'a [u8]>,
    pub events_bcs: Option<&'a [u8]>,
}

/// Parsed checkpoint proto — just the BCS byte slices we need.
pub struct ProtoCheckpoint<'a> {
    pub summary_bcs: Option<&'a [u8]>,
    pub transactions: Vec<ProtoTransaction<'a>>,
}

/// Parse a decompressed checkpoint protobuf, extracting BCS byte ranges.
pub fn parse_checkpoint(buf: &[u8]) -> Result<ProtoCheckpoint<'_>, &'static str> {
    let mut result = ProtoCheckpoint {
        summary_bcs: None,
        transactions: Vec::new(),
    };

    let mut pos = 0;
    while pos < buf.len() {
        let (field_number, wire_type, new_pos) = read_tag(buf, pos)?;
        pos = new_pos;

        match (field_number, wire_type) {
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
            // Skip all other fields
            _ => {
                pos = skip_field(buf, pos, wire_type)?;
            }
        }
    }

    Ok(result)
}

/// Parse an ExecutedTransaction message.
fn parse_executed_transaction(buf: &[u8]) -> Option<ProtoTransaction<'_>> {
    let mut digest = "";
    let mut effects_bcs = None;
    let mut transaction_bcs = None;
    let mut events_bcs = None;

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
            // Skip other fields (signatures, balance_changes, etc.)
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
    })
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
