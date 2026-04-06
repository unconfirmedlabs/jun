//! Jun checkpoint decoder — Rust FFI for high-throughput checkpoint extraction.
//!
//! Takes checkpoint bytes (compressed or raw proto), deserializes via sui-types,
//! extracts all records, returns either JSON or flat binary format.
//!
//! Called via Bun FFI from the JS pipeline.

use std::io::Read;

mod binary;
mod canonical;
mod extract_checkpoint_data;
mod proto;

// Re-export for tests
pub(crate) use extract_checkpoint_data as extract;

#[cfg(test)]
mod parity_tests;

// ---------------------------------------------------------------------------
// FFI: JSON output (used by canonical gRPC response decoders)
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn decode_checkpoint(
    input_ptr: *const u8, input_len: u32,
    output_ptr: *mut u8, output_capacity: u32,
) -> u32 {
    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len as usize) };
    let output = unsafe { std::slice::from_raw_parts_mut(output_ptr, output_capacity as usize) };
    write_json_result(output, decode_compressed_json(input))
}

#[no_mangle]
pub extern "C" fn decode_archive_checkpoint(
    input_ptr: *const u8, input_len: u32,
    output_ptr: *mut u8, output_capacity: u32,
) -> u32 {
    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len as usize) };
    let output = unsafe { std::slice::from_raw_parts_mut(output_ptr, output_capacity as usize) };
    write_json_result(output, canonical::decode_archive_checkpoint(input))
}

#[no_mangle]
pub extern "C" fn decode_checkpoint_proto(
    input_ptr: *const u8, input_len: u32,
    output_ptr: *mut u8, output_capacity: u32,
) -> u32 {
    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len as usize) };
    let output = unsafe { std::slice::from_raw_parts_mut(output_ptr, output_capacity as usize) };
    write_json_result(output, canonical::decode_checkpoint_proto(input))
}

#[no_mangle]
pub extern "C" fn decode_subscribe_checkpoints_response(
    input_ptr: *const u8, input_len: u32,
    output_ptr: *mut u8, output_capacity: u32,
) -> u32 {
    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len as usize) };
    let output = unsafe { std::slice::from_raw_parts_mut(output_ptr, output_capacity as usize) };
    write_json_result(output, canonical::decode_subscribe_checkpoints_response(input))
}

#[no_mangle]
pub extern "C" fn decode_get_checkpoint_response(
    input_ptr: *const u8, input_len: u32,
    output_ptr: *mut u8, output_capacity: u32,
) -> u32 {
    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len as usize) };
    let output = unsafe { std::slice::from_raw_parts_mut(output_ptr, output_capacity as usize) };
    write_json_result(output, canonical::decode_get_checkpoint_response(input))
}

// ---------------------------------------------------------------------------
// FFI: Binary output (used by index-chain for both archive and live)
// ---------------------------------------------------------------------------

/// Decode zstd-compressed archive checkpoint to flat binary format (all record types).
#[no_mangle]
pub extern "C" fn decode_checkpoint_binary(
    input_ptr: *const u8, input_len: u32,
    output_ptr: *mut u8, output_capacity: u32,
) -> u32 {
    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len as usize) };
    let output = unsafe { std::slice::from_raw_parts_mut(output_ptr, output_capacity as usize) };
    match decode_compressed_binary(input, output, extract::ExtractMask::ALL) {
        Ok(n) => n as u32,
        Err(_) => 0,
    }
}

/// Decode zstd-compressed archive checkpoint with selective extraction.
/// Only extracts record types enabled in the mask bitfield.
#[no_mangle]
pub extern "C" fn decode_checkpoint_binary_selective(
    input_ptr: *const u8, input_len: u32,
    output_ptr: *mut u8, output_capacity: u32,
    enabled_mask: u32,
) -> u32 {
    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len as usize) };
    let output = unsafe { std::slice::from_raw_parts_mut(output_ptr, output_capacity as usize) };
    let mask = extract::ExtractMask::from_bits_truncate(enabled_mask);
    match decode_compressed_binary(input, output, mask) {
        Ok(n) => n as u32,
        Err(_) => 0,
    }
}

/// Decode a raw SubscribeCheckpointsResponse to flat binary format.
/// Input: raw proto bytes (not compressed).
/// Output: 8-byte cursor (u64 LE) + binary checkpoint data.
#[no_mangle]
pub extern "C" fn decode_subscribe_response_binary(
    input_ptr: *const u8, input_len: u32,
    output_ptr: *mut u8, output_capacity: u32,
) -> u32 {
    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len as usize) };
    let output = unsafe { std::slice::from_raw_parts_mut(output_ptr, output_capacity as usize) };
    match decode_subscription_binary(input, output) {
        Ok(n) => n as u32,
        Err(_) => 0,
    }
}

// ---------------------------------------------------------------------------
// Internal
// ---------------------------------------------------------------------------

fn decompress(compressed: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut decoder = zstd::Decoder::new(compressed)?;
    let mut decompressed = Vec::with_capacity(compressed.len() * 6);
    decoder.read_to_end(&mut decompressed)?;
    Ok(decompressed)
}

fn decode_compressed_json(compressed: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    let decompressed = decompress(compressed)?;
    extract::extract_checkpoint(&decompressed)
}

fn decode_compressed_binary(compressed: &[u8], output: &mut [u8], mask: extract::ExtractMask) -> Result<usize, Box<dyn std::error::Error>> {
    let decompressed = decompress(compressed)?;
    let (written, _) = extract::extract_checkpoint_binary_selective(&decompressed, output, mask)?;
    Ok(written)
}

fn decode_subscription_binary(response_bytes: &[u8], output: &mut [u8]) -> Result<usize, Box<dyn std::error::Error>> {
    let parsed = proto::parse_subscribe_checkpoints_response(response_bytes)?;
    let cursor: u64 = parsed.cursor.parse().unwrap_or(0);

    if output.len() < 8 {
        return Err("output buffer too small".into());
    }
    output[..8].copy_from_slice(&cursor.to_le_bytes());

    let (written, _) = extract::extract_checkpoint_binary(parsed.checkpoint_bytes, &mut output[8..])?;
    Ok(8 + written)
}

fn write_json_result(output: &mut [u8], result: Result<String, Box<dyn std::error::Error>>) -> u32 {
    match result {
        Ok(json) => {
            let bytes = json.as_bytes();
            if bytes.len() > output.len() { return 0; }
            output[..bytes.len()].copy_from_slice(bytes);
            bytes.len() as u32
        }
        Err(_) => 0,
    }
}
