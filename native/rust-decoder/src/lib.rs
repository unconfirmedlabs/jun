//! Jun checkpoint decoder — Rust FFI for high-throughput checkpoint extraction.
//!
//! Takes compressed checkpoint bytes (.binpb.zst), decompresses, parses protobuf,
//! BCS-decodes using sui-types, extracts all records, returns JSON string.
//!
//! Called via Bun FFI from the JS pipeline.

use std::io::Read;

mod extract;

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

    match decode_checkpoint_inner(input) {
        Ok(json) => {
            let json_bytes = json.as_bytes();
            if json_bytes.len() > output.len() {
                return 0; // output buffer too small
            }
            output[..json_bytes.len()].copy_from_slice(json_bytes);
            json_bytes.len() as u32
        }
        Err(_) => 0,
    }
}

fn decode_checkpoint_inner(compressed: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    // 1. Decompress zstd
    let mut decoder = zstd::Decoder::new(compressed)?;
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;

    // 2. Parse protobuf to extract BCS byte ranges
    // 3. BCS decode using sui-types
    // 4. Extract records
    // 5. Serialize to JSON
    let result = extract::extract_checkpoint(&decompressed)?;
    Ok(result)
}
