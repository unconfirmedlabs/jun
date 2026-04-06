//! Jun checkpoint decoder — Rust FFI for high-throughput checkpoint extraction.
//!
//! Takes compressed checkpoint bytes (.binpb.zst), decompresses, parses protobuf,
//! BCS-decodes using sui-types, extracts all records, returns JSON string.
//!
//! Called via Bun FFI from the JS pipeline.

use std::io::Read;

mod bcs_reader;
mod binary;
mod canonical;
mod direct;
mod extract;
mod proto;

/// FFI entry point: decode a compressed checkpoint and return JSON.
///
/// # Arguments
/// * `input_ptr` - Pointer to zstd-compressed protobuf checkpoint bytes
/// * `input_len` - Length of compressed bytes
/// * `output_ptr` - Pointer to output buffer (caller-allocated)
/// * `output_capacity` - Size of output buffer
///
/// # Returns
/// Bytes written to output buffer, or 0 on error.
#[no_mangle]
pub extern "C" fn decode_checkpoint(
    input_ptr: *const u8,
    input_len: u32,
    output_ptr: *mut u8,
    output_capacity: u32,
) -> u32 {
    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len as usize) };
    let output = unsafe { std::slice::from_raw_parts_mut(output_ptr, output_capacity as usize) };

    write_json_result(output, decode_checkpoint_inner(input))
}

#[no_mangle]
pub extern "C" fn decode_archive_checkpoint(
    input_ptr: *const u8,
    input_len: u32,
    output_ptr: *mut u8,
    output_capacity: u32,
) -> u32 {
    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len as usize) };
    let output = unsafe { std::slice::from_raw_parts_mut(output_ptr, output_capacity as usize) };

    write_json_result(output, canonical::decode_archive_checkpoint(input))
}

#[no_mangle]
pub extern "C" fn decode_checkpoint_proto(
    input_ptr: *const u8,
    input_len: u32,
    output_ptr: *mut u8,
    output_capacity: u32,
) -> u32 {
    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len as usize) };
    let output = unsafe { std::slice::from_raw_parts_mut(output_ptr, output_capacity as usize) };

    write_json_result(output, canonical::decode_checkpoint_proto(input))
}

#[no_mangle]
pub extern "C" fn decode_subscribe_checkpoints_response(
    input_ptr: *const u8,
    input_len: u32,
    output_ptr: *mut u8,
    output_capacity: u32,
) -> u32 {
    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len as usize) };
    let output = unsafe { std::slice::from_raw_parts_mut(output_ptr, output_capacity as usize) };

    write_json_result(
        output,
        canonical::decode_subscribe_checkpoints_response(input),
    )
}

#[no_mangle]
pub extern "C" fn decode_get_checkpoint_response(
    input_ptr: *const u8,
    input_len: u32,
    output_ptr: *mut u8,
    output_capacity: u32,
) -> u32 {
    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len as usize) };
    let output = unsafe { std::slice::from_raw_parts_mut(output_ptr, output_capacity as usize) };

    write_json_result(output, canonical::decode_get_checkpoint_response(input))
}

/// FFI entry point: decode compressed checkpoint to flat binary format.
/// Much faster than JSON — no serde_json, no JSON.parse on the JS side.
#[no_mangle]
pub extern "C" fn decode_checkpoint_binary(
    input_ptr: *const u8,
    input_len: u32,
    output_ptr: *mut u8,
    output_capacity: u32,
) -> u32 {
    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len as usize) };
    let output = unsafe { std::slice::from_raw_parts_mut(output_ptr, output_capacity as usize) };

    match decode_checkpoint_binary_inner(input, output) {
        Ok(n) => n as u32,
        Err(_) => 0,
    }
}

fn use_direct_binary() -> bool {
    USE_DIRECT.with(|v| *v)
}

thread_local! {
    static USE_DIRECT: bool = std::env::var("JUN_DIRECT_BINARY").map(|v| v != "0").unwrap_or(true);
}

pub(crate) fn decode_checkpoint_binary_inner(
    compressed: &[u8],
    output: &mut [u8],
) -> Result<usize, Box<dyn std::error::Error>> {
    use std::time::Instant;
    let t0 = Instant::now();
    let mut decoder = zstd::Decoder::new(compressed)?;
    // Typical checkpoint decompresses to ~4-8x compressed size
    let mut decompressed = Vec::with_capacity(compressed.len() * 6);
    decoder.read_to_end(&mut decompressed)?;
    let zstd_ns = t0.elapsed().as_nanos() as u64;

    let (written, profile) = if use_direct_binary() {
        direct::extract_and_write_binary(&decompressed, output)?
    } else {
        extract::extract_checkpoint_binary(&decompressed, output)?
    };

    PROFILE_ACCUM.with(|acc| {
        let mut a = acc.borrow_mut();
        a.zstd_ns += zstd_ns;
        a.proto_ns += profile.proto_parse_ns;
        a.bcs_effects_ns += profile.bcs_effects_ns;
        a.bcs_tx_data_ns += profile.bcs_tx_data_ns;
        a.bcs_events_ns += profile.bcs_events_ns;
        a.extract_ns += profile.extract_records_ns;
        a.balance_ns += profile.balance_changes_ns;
        a.binary_write_ns += t0.elapsed().as_nanos() as u64
            - zstd_ns
            - profile.proto_parse_ns
            - profile.bcs_effects_ns
            - profile.bcs_tx_data_ns
            - profile.bcs_events_ns
            - profile.extract_records_ns
            - profile.balance_changes_ns;
        a.count += 1;
        a.tx_count += profile.tx_count;
        if a.count % 5000 == 0 {
            a.print_and_reset();
        }
    });

    Ok(written)
}

use std::cell::RefCell;

#[derive(Default)]
struct ProfileAccumulator {
    zstd_ns: u64,
    proto_ns: u64,
    bcs_effects_ns: u64,
    bcs_tx_data_ns: u64,
    bcs_events_ns: u64,
    extract_ns: u64,
    balance_ns: u64,
    binary_write_ns: u64,
    count: usize,
    tx_count: usize,
}

impl ProfileAccumulator {
    fn print_and_reset(&mut self) {
        let total = self.zstd_ns
            + self.proto_ns
            + self.bcs_effects_ns
            + self.bcs_tx_data_ns
            + self.bcs_events_ns
            + self.extract_ns
            + self.balance_ns
            + self.binary_write_ns;
        let pct = |v: u64| -> f64 { v as f64 / total as f64 * 100.0 };
        let avg_us = |v: u64| -> f64 { v as f64 / self.count as f64 / 1000.0 };
        eprintln!(
            "[rust-profile] {} checkpoints ({} txs) | zstd {:.0}% ({:.0}µs) | proto {:.0}% ({:.0}µs) | bcs_effects {:.0}% ({:.0}µs) | bcs_tx_data {:.0}% ({:.0}µs) | bcs_events {:.0}% ({:.0}µs) | extract {:.0}% ({:.0}µs) | balance {:.0}% ({:.0}µs) | binary_write {:.0}% ({:.0}µs)",
            self.count, self.tx_count,
            pct(self.zstd_ns), avg_us(self.zstd_ns),
            pct(self.proto_ns), avg_us(self.proto_ns),
            pct(self.bcs_effects_ns), avg_us(self.bcs_effects_ns),
            pct(self.bcs_tx_data_ns), avg_us(self.bcs_tx_data_ns),
            pct(self.bcs_events_ns), avg_us(self.bcs_events_ns),
            pct(self.extract_ns), avg_us(self.extract_ns),
            pct(self.balance_ns), avg_us(self.balance_ns),
            pct(self.binary_write_ns), avg_us(self.binary_write_ns),
        );
        *self = Self::default();
    }
}

thread_local! {
    static PROFILE_ACCUM: RefCell<ProfileAccumulator> = RefCell::new(ProfileAccumulator::default());
}

fn decode_checkpoint_inner(compressed: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    let mut decoder = zstd::Decoder::new(compressed)?;
    let mut decompressed = Vec::with_capacity(compressed.len() * 6);
    decoder.read_to_end(&mut decompressed)?;
    let result = extract::extract_checkpoint(&decompressed)?;
    Ok(result)
}

fn write_json_result(output: &mut [u8], result: Result<String, Box<dyn std::error::Error>>) -> u32 {
    match result {
        Ok(json) => {
            let json_bytes = json.as_bytes();
            if json_bytes.len() > output.len() {
                return 0;
            }
            output[..json_bytes.len()].copy_from_slice(json_bytes);
            json_bytes.len() as u32
        }
        Err(_) => 0,
    }
}
