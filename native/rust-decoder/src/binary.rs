//! Binary output format — zero-allocation writes directly into the FFI output buffer.
//!
//! Format: header with record counts, then packed records with length-prefixed strings.
//! All integers are little-endian. Strings are UTF-8 with u16 length prefix.
//! Optional strings use u16::MAX (0xFFFF) as the "null" sentinel.

use crate::extract::ExtractedCheckpoint;

const NULL_STR: u16 = 0xFFFF;

pub struct BinaryWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl<'a> BinaryWriter<'a> {
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    #[inline]
    pub fn write_u8(&mut self, v: u8) {
        self.buf[self.pos] = v;
        self.pos += 1;
    }

    #[inline]
    pub fn write_u16(&mut self, v: u16) {
        self.buf[self.pos..self.pos + 2].copy_from_slice(&v.to_le_bytes());
        self.pos += 2;
    }

    #[inline]
    pub fn write_u32(&mut self, v: u32) {
        self.buf[self.pos..self.pos + 4].copy_from_slice(&v.to_le_bytes());
        self.pos += 4;
    }

    #[inline]
    pub fn begin_string(&mut self) -> usize {
        let len_pos = self.pos;
        self.pos += 2;
        len_pos
    }

    #[inline]
    pub fn finish_string(&mut self, len_pos: usize) {
        let len = self.pos - len_pos - 2;
        self.buf[len_pos..len_pos + 2].copy_from_slice(&(len as u16).to_le_bytes());
    }

    #[inline]
    pub fn write_raw_byte(&mut self, b: u8) {
        self.buf[self.pos] = b;
        self.pos += 1;
    }

    #[inline]
    pub fn write_raw_bytes(&mut self, bytes: &[u8]) {
        self.buf[self.pos..self.pos + bytes.len()].copy_from_slice(bytes);
        self.pos += bytes.len();
    }

    #[inline]
    pub fn write_raw_str(&mut self, s: &str) {
        self.write_raw_bytes(s.as_bytes());
    }

    #[inline]
    pub fn write_str(&mut self, s: &str) {
        let len = s.len().min(u16::MAX as usize - 1);
        self.write_u16(len as u16);
        self.buf[self.pos..self.pos + len].copy_from_slice(&s.as_bytes()[..len]);
        self.pos += len;
    }

    #[inline]
    pub fn write_opt_str(&mut self, s: &Option<String>) {
        match s {
            Some(s) => self.write_str(s),
            None => self.write_u16(NULL_STR),
        }
    }

    #[inline]
    pub fn write_bool(&mut self, v: bool) {
        self.write_u8(if v { 1 } else { 0 });
    }

    #[inline]
    pub fn write_usize_as_u32(&mut self, v: usize) {
        self.write_u32(v as u32);
    }

    /// Patch a u32 at a previously written position.
    #[inline]
    pub fn write_u32_at(&mut self, pos: usize, v: u32) {
        self.buf[pos..pos + 4].copy_from_slice(&v.to_le_bytes());
    }

    /// Write null sentinel for an optional string.
    #[inline]
    pub fn write_null(&mut self) {
        self.write_u16(NULL_STR);
    }

    /// Write a byte slice as "0x"-prefixed hex, length-prefixed.
    #[inline]
    pub fn write_hex(&mut self, bytes: &[u8]) {
        let len = 2 + bytes.len() * 2;
        self.write_u16(len as u16);
        self.write_raw_byte(b'0');
        self.write_raw_byte(b'x');
        self.write_raw_hex_contents(bytes);
    }

    #[inline]
    pub fn write_base58(&mut self, bytes: &[u8]) {
        let len_pos = self.begin_string();
        let written = bs58::encode(bytes)
            .onto(&mut self.buf[self.pos..])
            .expect("base58 output buffer too small");
        self.pos += written;
        self.finish_string(len_pos);
    }

    #[inline]
    pub fn write_0x_hex(&mut self, bytes: &[u8]) {
        let len = 2 + bytes.len() * 2;
        self.write_u16(len as u16);
        self.write_raw_byte(b'0');
        self.write_raw_byte(b'x');
        self.write_raw_hex_contents(bytes);
    }

    #[inline]
    pub fn write_raw_hex_contents(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.buf[self.pos] = HEX_CHARS[(b >> 4) as usize];
            self.buf[self.pos + 1] = HEX_CHARS[(b & 0x0f) as usize];
            self.pos += 2;
        }
    }

    /// Write an optional byte slice as hex, or null.
    #[inline]
    #[allow(dead_code)]
    pub fn write_opt_hex(&mut self, bytes: Option<&[u8]>) {
        match bytes {
            Some(b) => self.write_hex(b),
            None => self.write_null(),
        }
    }

    /// Write u64 as decimal string, length-prefixed.
    #[inline]
    pub fn write_u64_dec(&mut self, v: u64) {
        // Max u64 is 20 digits. Write to a small stack buffer.
        let mut tmp = [0u8; 20];
        let s = format_u64(v, &mut tmp);
        self.write_str(s);
    }

    /// Write i128 as decimal string, length-prefixed.
    #[inline]
    #[allow(dead_code)]
    pub fn write_i128_dec(&mut self, v: i128) {
        let mut tmp = [0u8; 40];
        let s = format_i128(v, &mut tmp);
        self.write_str(s);
    }

    /// Write an optional u64 as decimal, or null.
    #[inline]
    pub fn write_opt_u64_dec(&mut self, v: Option<u64>) {
        match v {
            Some(n) => self.write_u64_dec(n),
            None => self.write_null(),
        }
    }

    /// Write Display impl as a length-prefixed string.
    #[inline]
    pub fn write_display<T: std::fmt::Display>(&mut self, v: &T) {
        let len_pos = self.pos;
        self.pos += 2;
        let start = self.pos;
        self.write_raw_display(v);
        let len = self.pos - start;
        self.buf[len_pos..len_pos + 2].copy_from_slice(&(len as u16).to_le_bytes());
    }

    /// Write Display impl into the current string without adding a length prefix.
    #[inline]
    pub fn write_raw_display<T: std::fmt::Display>(&mut self, v: &T) {
        use std::fmt::Write;
        let mut writer = BufWriter {
            buf: self.buf,
            pos: self.pos,
        };
        let _ = write!(writer, "{}", v);
        self.pos = writer.pos;
    }

    /// Write an empty string (length 0).
    #[inline]
    pub fn write_empty_str(&mut self) {
        self.write_u16(0);
    }
}

static HEX_CHARS: [u8; 16] = *b"0123456789abcdef";

struct BufWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl<'a> std::fmt::Write for BufWriter<'a> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let bytes = s.as_bytes();
        self.buf[self.pos..self.pos + bytes.len()].copy_from_slice(bytes);
        self.pos += bytes.len();
        Ok(())
    }
}

fn format_u64(v: u64, buf: &mut [u8; 20]) -> &str {
    if v == 0 {
        return "0";
    }
    let mut n = v;
    let mut i = 20;
    while n > 0 {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    unsafe { std::str::from_utf8_unchecked(&buf[i..]) }
}

#[allow(dead_code)]
fn format_i128(v: i128, buf: &mut [u8; 40]) -> &str {
    if v == 0 {
        return "0";
    }
    let negative = v < 0;
    let mut n = if negative {
        (v as i128).unsigned_abs()
    } else {
        v as u128
    };
    let mut i = 40;
    while n > 0 {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    if negative {
        i -= 1;
        buf[i] = b'-';
    }
    unsafe { std::str::from_utf8_unchecked(&buf[i..]) }
}

/// Serialize an ExtractedCheckpoint into the binary format.
/// Returns the number of bytes written.
pub fn write_binary(checkpoint: &ExtractedCheckpoint, output: &mut [u8]) -> usize {
    let mut w = BinaryWriter::new(output);

    // Header: record counts (10 × u32 = 40 bytes)
    w.write_u32(checkpoint.transactions.len() as u32);
    w.write_u32(checkpoint.object_changes.len() as u32);
    w.write_u32(checkpoint.dependencies.len() as u32);
    w.write_u32(checkpoint.commands.len() as u32);
    w.write_u32(checkpoint.system_transactions.len() as u32);
    w.write_u32(checkpoint.move_calls.len() as u32);
    w.write_u32(checkpoint.inputs.len() as u32);
    w.write_u32(checkpoint.unchanged_consensus_objects.len() as u32);
    w.write_u32(checkpoint.events.len() as u32);
    w.write_u32(checkpoint.balance_changes.len() as u32);

    // Checkpoint summary
    let cp = &checkpoint.checkpoint;
    w.write_str(&cp.sequence_number);
    w.write_str(&cp.epoch);
    w.write_str(&cp.timestamp);
    w.write_str(&cp.digest);
    w.write_opt_str(&cp.previous_digest);
    w.write_opt_str(&cp.content_digest);
    w.write_str(&cp.total_network_transactions);
    w.write_str(&cp.epoch_rolling_gas_cost_summary.computation_cost);
    w.write_str(&cp.epoch_rolling_gas_cost_summary.storage_cost);
    w.write_str(&cp.epoch_rolling_gas_cost_summary.storage_rebate);
    w.write_str(&cp.epoch_rolling_gas_cost_summary.non_refundable_storage_fee);

    // Transactions
    for tx in &checkpoint.transactions {
        w.write_str(&tx.digest);
        w.write_str(&tx.sender);
        w.write_bool(tx.success);
        w.write_str(&tx.computation_cost);
        w.write_str(&tx.storage_cost);
        w.write_str(&tx.storage_rebate);
        w.write_str(&tx.non_refundable_storage_fee);
        w.write_str(&tx.checkpoint_seq);
        w.write_str(&tx.timestamp);
        w.write_usize_as_u32(tx.move_call_count);
        w.write_str(&tx.epoch);
        w.write_opt_str(&tx.error_kind);
        w.write_opt_str(&tx.error_description);
        w.write_u8(tx.error_command_index.map(|v| v as u8).unwrap_or(0xFF));
        w.write_opt_str(&tx.error_abort_code);
        w.write_opt_str(&tx.error_module);
        w.write_opt_str(&tx.error_function);
        w.write_opt_str(&tx.events_digest);
        w.write_opt_str(&tx.lamport_version);
        w.write_usize_as_u32(tx.dependency_count);
    }

    // Object changes
    for oc in &checkpoint.object_changes {
        w.write_str(&oc.tx_digest);
        w.write_str(&oc.object_id);
        w.write_str(&oc.change_type);
        w.write_opt_str(&oc.object_type);
        w.write_opt_str(&oc.input_version);
        w.write_opt_str(&oc.input_digest);
        w.write_opt_str(&oc.input_owner);
        w.write_opt_str(&oc.input_owner_kind);
        w.write_opt_str(&oc.output_version);
        w.write_opt_str(&oc.output_digest);
        w.write_opt_str(&oc.output_owner);
        w.write_opt_str(&oc.output_owner_kind);
        w.write_bool(oc.is_gas_object);
        w.write_str(&oc.checkpoint_seq);
        w.write_str(&oc.timestamp);
    }

    // Dependencies
    for dep in &checkpoint.dependencies {
        w.write_str(&dep.tx_digest);
        w.write_str(&dep.depends_on_digest);
        w.write_str(&dep.checkpoint_seq);
        w.write_str(&dep.timestamp);
    }

    // Commands
    for cmd in &checkpoint.commands {
        w.write_str(&cmd.tx_digest);
        w.write_usize_as_u32(cmd.command_index);
        w.write_str(&cmd.kind);
        w.write_opt_str(&cmd.package);
        w.write_opt_str(&cmd.module);
        w.write_opt_str(&cmd.function);
        w.write_opt_str(&cmd.type_arguments);
        w.write_opt_str(&cmd.args);
        w.write_str(&cmd.checkpoint_seq);
        w.write_str(&cmd.timestamp);
    }

    // System transactions
    for sys in &checkpoint.system_transactions {
        w.write_str(&sys.tx_digest);
        w.write_str(&sys.kind);
        w.write_str(&sys.data);
        w.write_str(&sys.checkpoint_seq);
        w.write_str(&sys.timestamp);
    }

    // Move calls
    for mc in &checkpoint.move_calls {
        w.write_str(&mc.tx_digest);
        w.write_usize_as_u32(mc.call_index);
        w.write_str(&mc.package);
        w.write_str(&mc.module);
        w.write_str(&mc.function);
        w.write_str(&mc.checkpoint_seq);
        w.write_str(&mc.timestamp);
    }

    // Inputs
    for inp in &checkpoint.inputs {
        w.write_str(&inp.tx_digest);
        w.write_usize_as_u32(inp.input_index);
        w.write_str(&inp.kind);
        w.write_opt_str(&inp.object_id);
        w.write_opt_str(&inp.version);
        w.write_opt_str(&inp.digest);
        w.write_opt_str(&inp.mutability);
        w.write_opt_str(&inp.initial_shared_version);
        w.write_opt_str(&inp.pure_bytes);
        w.write_opt_str(&inp.amount);
        w.write_opt_str(&inp.coin_type);
        w.write_opt_str(&inp.source);
        w.write_str(&inp.checkpoint_seq);
        w.write_str(&inp.timestamp);
    }

    // Unchanged consensus objects
    for uco in &checkpoint.unchanged_consensus_objects {
        w.write_str(&uco.tx_digest);
        w.write_str(&uco.object_id);
        w.write_str(&uco.kind);
        w.write_opt_str(&uco.version);
        w.write_opt_str(&uco.digest);
        w.write_opt_str(&uco.object_type);
        w.write_str(&uco.checkpoint_seq);
        w.write_str(&uco.timestamp);
    }

    // Events
    for ev in &checkpoint.events {
        w.write_str(&ev.handler_name);
        w.write_str(&ev.checkpoint_seq);
        w.write_str(&ev.tx_digest);
        w.write_usize_as_u32(ev.event_seq);
        w.write_str(&ev.sender);
        w.write_str(&ev.timestamp);
        // event data as JSON string (events have user-defined schemas)
        let data_json = serde_json::to_string(&ev.data).unwrap_or_default();
        w.write_str(&data_json);
    }

    // Balance changes
    for balance_change in &checkpoint.balance_changes {
        w.write_str(&balance_change.tx_digest);
        w.write_str(&balance_change.checkpoint_seq);
        w.write_str(&balance_change.address);
        w.write_str(&balance_change.coin_type);
        w.write_str(&balance_change.amount);
        w.write_str(&balance_change.timestamp);
    }

    w.position()
}
