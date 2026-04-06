//! Jun checkpoint decoder — Rust FFI for high-throughput checkpoint extraction.
//!
//! Takes compressed checkpoint bytes (.binpb.zst), decompresses, parses protobuf,
//! BCS-decodes using sui-types, extracts all records, returns JSON string.
//!
//! Called via Bun FFI from the JS pipeline.

use std::io::Read;

mod binary;
mod canonical;
mod extract;
mod fetch;
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

/// Download, decompress, and decode a range of archive checkpoints.
///
/// Spawns a background thread with a tokio runtime. Results stream back via
/// `output_callback`. Completion signaled by calling callback with `seq = u64::MAX`.
/// Returns 0 immediately on success (thread spawned), negative on error.
#[no_mangle]
pub extern "C" fn download_and_decode_archive_checkpoint_range(
    archive_url_ptr: *const u8,
    archive_url_len: u32,
    from_checkpoint: u64,
    to_checkpoint: u64,
    concurrency: u32,
    output_callback: fetch::OutputCallback,
) -> i32 {
    if archive_url_ptr.is_null() {
        return -1;
    }
    if from_checkpoint > to_checkpoint {
        return -2;
    }

    let archive_url_bytes =
        unsafe { std::slice::from_raw_parts(archive_url_ptr, archive_url_len as usize) };
    let archive_url = match std::str::from_utf8(archive_url_bytes) {
        Ok(url) => url.to_string(),
        Err(_) => return -3,
    };

    match fetch::spawn_download_and_decode_range(
        archive_url,
        from_checkpoint,
        to_checkpoint,
        concurrency,
        output_callback,
    ) {
        Ok(()) => 0,
        Err(_) => -4,
    }
}

pub(crate) fn decode_checkpoint_binary_inner(
    compressed: &[u8],
    output: &mut [u8],
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut decoder = zstd::Decoder::new(compressed)?;
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    extract::extract_checkpoint_binary(&decompressed, output)
}

fn decode_checkpoint_inner(compressed: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    let mut decoder = zstd::Decoder::new(compressed)?;
    let mut decompressed = Vec::new();
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
