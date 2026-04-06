# Build Rust Decoder

**Model: Use Sonnet for this task. Opus is unnecessary for builds.**

Build the native Rust checkpoint decoder. Required after any changes to `native/rust-decoder/src/`.

## Local Build

```bash
cd native/rust-decoder && cargo build --release 2>&1 | tail -10
```

Binary output: `native/rust-decoder/target/release/libjun_checkpoint_decoder.dylib` (macOS) or `.so` (Linux).

## Server Build

```bash
ssh claude "export PATH=\$HOME/.bun/bin:\$HOME/.cargo/bin:\$PATH && cd ~/jun/native/rust-decoder && cargo build --release 2>&1 | tail -5"
```

Takes ~20-30s. Watch for warnings — unused imports are fine, actual errors need fixing.

## Verify Native Decoder is Active

```bash
bun -e "const { isNativeCheckpointDecoderAvailable } = require('./src/checkpoint-native-decoder.ts'); console.log('Native decoder:', isNativeCheckpointDecoderAvailable())"
```

Should print `true`. If `false`, the `.dylib`/`.so` wasn't found at the expected paths:
- `native/rust-decoder/target/release/libjun_checkpoint_decoder.{dylib,so}`
- `native/rust-decoder/target/debug/libjun_checkpoint_decoder.{dylib,so}`

## Force JS Decoder (bypass native)

```bash
JUN_BCS_DECODER=js bun run src/cli.ts ...
```

## Key Source Files

| File | Purpose |
|------|---------|
| `native/rust-decoder/src/lib.rs` | FFI entry points, zstd decompress |
| `native/rust-decoder/src/extract.rs` | BCS decode + record extraction (struct-based) |
| `native/rust-decoder/src/direct.rs` | Direct binary write path (JUN_DIRECT_BINARY=1) |
| `native/rust-decoder/src/binary.rs` | BinaryWriter + binary format serialization |
| `native/rust-decoder/src/proto.rs` | Minimal protobuf parser for checkpoint format |
| `native/rust-decoder/src/canonical.rs` | gRPC response format decoders |
| `native/rust-decoder/Cargo.toml` | Dependencies (sui-types mainnet branch) |
