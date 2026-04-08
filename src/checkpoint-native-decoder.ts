import { dlopen, FFIType, ptr, suffix } from "bun:ffi";
import { existsSync } from "fs";
import { join } from "path";
import type { GrpcCheckpointResponse } from "./grpc.ts";

type NativeDecodeFn = (
  inputPtr: number,
  inputLen: number,
  outputPtr: number,
  outputCapacity: number,
) => number;

type NativeSelectiveDecodeFn = (
  inputPtr: number,
  inputLen: number,
  outputPtr: number,
  outputCapacity: number,
  enabledMask: number,
) => number;

type NativeCheckpointDecoder = {
  decode_archive_checkpoint: NativeDecodeFn;
  decode_checkpoint_proto: NativeDecodeFn;
  decode_subscribe_checkpoints_response: NativeDecodeFn;
  decode_get_checkpoint_response: NativeDecodeFn;
  decode_checkpoint_binary: NativeDecodeFn;
  decode_subscribe_response_binary: NativeDecodeFn;
  decode_checkpoint_binary_selective: NativeSelectiveDecodeFn;
};

/** Keys whose FFI signature is the standard 4-arg NativeDecodeFn (no mask argument). */
type NativeDecodeFnKey = Exclude<keyof NativeCheckpointDecoder, "decode_checkpoint_binary_selective">;
/** Keys whose FFI signature returns binary output (used in decodeBinary). */
type NativeBinaryDecodeFnKey = "decode_checkpoint_binary" | "decode_subscribe_response_binary";

/** Extraction mask bits — controls which record types the Rust decoder extracts. */
export const ExtractMask = {
  TRANSACTIONS:         1 << 0,
  MOVE_CALLS:           1 << 1,
  BALANCE_CHANGES:      1 << 2,
  OBJECT_CHANGES:       1 << 3,
  DEPENDENCIES:         1 << 4,
  INPUTS:               1 << 5,
  COMMANDS:             1 << 6,
  SYSTEM_TRANSACTIONS:  1 << 7,
  UNCHANGED_CONSENSUS:  1 << 8,
  CHECKPOINTS:          1 << 9,
  EVENTS:               1 << 10,
  ALL:                  0x7FF,
} as const;

const PLATFORM = `${process.platform}-${process.arch === "x64" ? "x64" : process.arch === "arm64" ? "arm64" : process.arch}`;
const LIB_NAME = `libjun_checkpoint_decoder.${suffix}`;
const LIB_PATHS = [
  // Prebuilt binaries (shipped with package — no Rust toolchain needed)
  join(import.meta.dir, "..", "native", "lib", `libjun_checkpoint_decoder.${PLATFORM}.${suffix}`),
  // Local dev build
  join(import.meta.dir, "..", "native", "rust-decoder", "target", "release", LIB_NAME),
  join(import.meta.dir, "..", "native", "rust-decoder", "target", "debug", LIB_NAME),
];

let nativeDecoder: NativeCheckpointDecoder | null = null;

for (const libPath of LIB_PATHS) {
  if (!existsSync(libPath)) continue;
  try {
    const lib = dlopen(libPath, {
      decode_archive_checkpoint: {
        args: [FFIType.ptr, FFIType.u32, FFIType.ptr, FFIType.u32],
        returns: FFIType.u32,
      },
      decode_checkpoint_proto: {
        args: [FFIType.ptr, FFIType.u32, FFIType.ptr, FFIType.u32],
        returns: FFIType.u32,
      },
      decode_subscribe_checkpoints_response: {
        args: [FFIType.ptr, FFIType.u32, FFIType.ptr, FFIType.u32],
        returns: FFIType.u32,
      },
      decode_get_checkpoint_response: {
        args: [FFIType.ptr, FFIType.u32, FFIType.ptr, FFIType.u32],
        returns: FFIType.u32,
      },
      decode_checkpoint_binary: {
        args: [FFIType.ptr, FFIType.u32, FFIType.ptr, FFIType.u32],
        returns: FFIType.u32,
      },
      decode_subscribe_response_binary: {
        args: [FFIType.ptr, FFIType.u32, FFIType.ptr, FFIType.u32],
        returns: FFIType.u32,
      },
      decode_checkpoint_binary_selective: {
        args: [FFIType.ptr, FFIType.u32, FFIType.ptr, FFIType.u32, FFIType.u32],
        returns: FFIType.u32,
      },
    });
    nativeDecoder = lib.symbols;
    break;
  } catch (err) {
    process.stderr.write(`[jun] native decoder load failed (${libPath}): ${err}\n`);
  }
}
if (!nativeDecoder && process.env.JUN_BCS_DECODER !== "js") {
  process.stderr.write(`[jun] native decoder unavailable — falling back to JS decoder (significantly slower)\n`);
}

const decoder = new TextDecoder();
const DEFAULT_CAPACITY = 16 * 1024 * 1024;
const MAX_CAPACITY = (Number(process.env.JUN_DECODER_MAX_CAPACITY_MB) || 128) * 1024 * 1024;
const BINARY_DEFAULT_CAPACITY = 64 * 1024 * 1024;
const BINARY_MAX_CAPACITY = (Number(process.env.JUN_DECODER_BINARY_MAX_CAPACITY_MB) || 256) * 1024 * 1024;

function reviveBytes(value: unknown): Uint8Array | undefined {
  if (!Array.isArray(value)) return undefined;
  return Uint8Array.from(value as number[]);
}

function hydrateResponse(response: GrpcCheckpointResponse): GrpcCheckpointResponse {
  for (const transaction of response.checkpoint.transactions ?? []) {
    for (const input of transaction.transaction?.inputs ?? []) {
      const pure = reviveBytes(input.pure);
      if (pure) input.pure = pure;
    }

    for (const event of transaction.events?.events ?? []) {
      const bytes = reviveBytes(event.contents?.value);
      if (bytes) event.contents.value = bytes;
    }
  }

  return response;
}

function decodeJson(symbol: NativeDecodeFnKey, input: Uint8Array): string | null {
  if (!nativeDecoder) return null;

  let capacity = DEFAULT_CAPACITY;
  while (capacity <= MAX_CAPACITY) {
    const outBuf = new Uint8Array(capacity);
    const bytesWritten = nativeDecoder[symbol](ptr(input), input.length, ptr(outBuf), outBuf.length);
    if (bytesWritten > 0) {
      return decoder.decode(outBuf.subarray(0, bytesWritten));
    }
    capacity *= 2;
  }

  throw new Error(`Native checkpoint decoder failed for ${symbol}: output exceeded ${MAX_CAPACITY} bytes (set JUN_DECODER_MAX_CAPACITY_MB to increase)`);
}

function decodeResponse(symbol: NativeDecodeFnKey, input: Uint8Array): GrpcCheckpointResponse | null {
  const json = decodeJson(symbol, input);
  if (!json) return null;
  return hydrateResponse(JSON.parse(json) as GrpcCheckpointResponse);
}

function decodeBinary(symbol: NativeBinaryDecodeFnKey, input: Uint8Array): Uint8Array | null {
  if (!nativeDecoder) return null;

  let capacity = BINARY_DEFAULT_CAPACITY;
  while (capacity <= BINARY_MAX_CAPACITY) {
    const outBuf = new Uint8Array(capacity);
    const bytesWritten = nativeDecoder[symbol](ptr(input), input.length, ptr(outBuf), outBuf.length);
    if (bytesWritten > 0) {
      return outBuf.subarray(0, bytesWritten);
    }
    capacity *= 2;
  }

  throw new Error(`Native checkpoint decoder failed for ${symbol}: output exceeded ${BINARY_MAX_CAPACITY} bytes (set JUN_DECODER_BINARY_MAX_CAPACITY_MB to increase)`);
}

export function isNativeCheckpointDecoderAvailable(): boolean {
  if (process.env.JUN_BCS_DECODER === "js") return false;
  return nativeDecoder !== null;
}

export function decodeArchiveCheckpointCompressedNative(
  input: Uint8Array,
): GrpcCheckpointResponse | null {
  return decodeResponse("decode_archive_checkpoint", input);
}

export function decodeArchiveCheckpointBinary(input: Uint8Array): Uint8Array | null {
  return decodeBinary("decode_checkpoint_binary", input);
}

export function decodeCheckpointProtoNative(
  input: Uint8Array,
): GrpcCheckpointResponse | null {
  return decodeResponse("decode_checkpoint_proto", input);
}

export function decodeSubscribeCheckpointsResponseNative(
  input: Uint8Array,
): GrpcCheckpointResponse | null {
  return decodeResponse("decode_subscribe_checkpoints_response", input);
}

/** Decode a raw SubscribeCheckpointsResponse to binary format.
 *  Returns: first 8 bytes = cursor (u64 LE), then binary checkpoint data.
 *  Same binary format as decodeArchiveCheckpointBinary. */
export function decodeSubscribeResponseBinary(input: Uint8Array): Uint8Array | null {
  return decodeBinary("decode_subscribe_response_binary", input);
}

/** Decode compressed checkpoint with selective extraction.
 *  Only extracts record types enabled in the mask (see ExtractMask). */
export function decodeArchiveCheckpointBinarySelective(input: Uint8Array, mask: number): Uint8Array | null {
  if (!nativeDecoder) return null;

  let capacity = BINARY_DEFAULT_CAPACITY;
  while (capacity <= BINARY_MAX_CAPACITY) {
    const outBuf = new Uint8Array(capacity);
    const bytesWritten = (nativeDecoder.decode_checkpoint_binary_selective as NativeSelectiveDecodeFn)(
      ptr(input), input.length, ptr(outBuf), outBuf.length, mask,
    );
    if (bytesWritten > 0) {
      return outBuf.subarray(0, bytesWritten);
    }
    capacity *= 2;
  }

  throw new Error(`Native checkpoint decoder failed for selective binary decode: output exceeded ${BINARY_MAX_CAPACITY} bytes (set JUN_DECODER_BINARY_MAX_CAPACITY_MB to increase)`);
}

export function decodeGetCheckpointResponseNative(
  input: Uint8Array,
): GrpcCheckpointResponse | null {
  return decodeResponse("decode_get_checkpoint_response", input);
}
