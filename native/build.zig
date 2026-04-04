const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.createModule(.{
        .root_source_file = b.path("checkpoint_processor.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });

    // Add zstd C source files for cross-compilation
    const zstd_files = [_][]const u8{
        "vendor/zstd/common/debug.c",
        "vendor/zstd/common/entropy_common.c",
        "vendor/zstd/common/error_private.c",
        "vendor/zstd/common/fse_decompress.c",
        "vendor/zstd/common/pool.c",
        "vendor/zstd/common/threading.c",
        "vendor/zstd/common/xxhash.c",
        "vendor/zstd/common/zstd_common.c",
        "vendor/zstd/decompress/huf_decompress.c",
        "vendor/zstd/decompress/zstd_ddict.c",
        "vendor/zstd/decompress/zstd_decompress.c",
        "vendor/zstd/decompress/zstd_decompress_block.c",
    };

    mod.addCSourceFiles(.{
        .files = &zstd_files,
        .flags = &.{"-DZSTD_DISABLE_ASM"},
    });

    mod.addIncludePath(b.path("vendor/zstd"));
    mod.addIncludePath(b.path("vendor/zstd/common"));

    const lib = b.addLibrary(.{
        .linkage = .dynamic,
        .name = "checkpoint_processor",
        .root_module = mod,
    });

    b.installArtifact(lib);
}
