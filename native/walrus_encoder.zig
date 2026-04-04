//! C-ABI wrapper for the pure Zig Walrus blob encoder.
//! Loaded from TypeScript via bun:ffi.
//!
//! Output format (flat binary buffer):
//!   [0..32]   blob_id
//!   [32..40]  unencoded_length (u64 LE)
//!   [40]      encoding_type (u8)
//!   [41..73]  blob_hash
//!   [73..75]  n_shards (u16 LE)
//!   [75..77]  symbol_size (u16 LE)
//!   [77..79]  n_primary_source (u16 LE)
//!   [79..81]  n_secondary_source (u16 LE)
//!   Then for each sliver pair (n_shards entries):
//!     [32 bytes] primary_merkle_hash
//!     [32 bytes] secondary_merkle_hash
//!     [primary_sliver_bytes] primary sliver data
//!     [secondary_sliver_bytes] secondary sliver data

const std = @import("std");
const blob = @import("blob");

const HEADER_SIZE = 81; // fixed header before sliver pairs

/// Encode a blob into Walrus sliver pairs.
/// Returns bytes written to output, or 0 on error.
export fn walrus_encode(
    blob_ptr: [*]const u8,
    blob_len: u32,
    n_shards: u16,
    output_ptr: [*]u8,
    output_capacity: u32,
) u32 {
    const allocator = std.heap.page_allocator;
    const input = blob_ptr[0..blob_len];

    var result = blob.encodeBlob(allocator, input, n_shards) catch return 0;
    defer result.deinit();

    const config = blob.EncodingConfig.init(n_shards, blob_len);
    const primary_bytes = config.primarySliverBytes();
    const secondary_bytes = config.secondarySliverBytes();
    const pair_size = 64 + primary_bytes + secondary_bytes; // 2×32 hash + data
    const total_size = HEADER_SIZE + @as(usize, n_shards) * pair_size;

    if (total_size > output_capacity) return 0;

    const out = output_ptr[0..total_size];

    // Write header
    @memcpy(out[0..32], &result.blob_id);
    std.mem.writeInt(u64, out[32..40], result.metadata.unencoded_length, .little);
    out[40] = result.metadata.encoding_type;
    @memcpy(out[41..73], &result.metadata.blob_hash);
    std.mem.writeInt(u16, out[73..75], n_shards, .little);
    std.mem.writeInt(u16, out[75..77], config.symbol_size, .little);
    std.mem.writeInt(u16, out[77..79], config.n_primary_source, .little);
    std.mem.writeInt(u16, out[79..81], config.n_secondary_source, .little);

    // Write sliver pairs
    var pos: usize = HEADER_SIZE;
    for (result.sliver_pairs) |pair| {
        @memcpy(out[pos..][0..32], &pair.primary_hash);
        pos += 32;
        @memcpy(out[pos..][0..32], &pair.secondary_hash);
        pos += 32;
        @memcpy(out[pos..][0..primary_bytes], pair.primary[0..primary_bytes]);
        pos += primary_bytes;
        @memcpy(out[pos..][0..secondary_bytes], pair.secondary[0..secondary_bytes]);
        pos += secondary_bytes;
    }

    return @intCast(total_size);
}

/// Compute only the blob ID (32 bytes) without returning full sliver data.
/// Returns 1 on success, 0 on error.
export fn walrus_blob_id(
    blob_ptr: [*]const u8,
    blob_len: u32,
    n_shards: u16,
    out_id: [*]u8,
) u32 {
    const allocator = std.heap.page_allocator;
    const input = blob_ptr[0..blob_len];

    var result = blob.encodeBlob(allocator, input, n_shards) catch return 0;
    defer result.deinit();

    @memcpy(out_id[0..32], &result.blob_id);
    return 1;
}
