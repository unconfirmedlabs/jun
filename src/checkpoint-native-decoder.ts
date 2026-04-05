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

type NativeCheckpointDecoder = {
  decode_archive_checkpoint: NativeDecodeFn;
  decode_checkpoint_proto: NativeDecodeFn;
  decode_subscribe_checkpoints_response: NativeDecodeFn;
  decode_get_checkpoint_response: NativeDecodeFn;
};

const LIB_NAME = `libjun_checkpoint_decoder.${suffix}`;
const LIB_PATHS = [
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
    });
    nativeDecoder = lib.symbols;
    break;
  } catch {}
}

const decoder = new TextDecoder();
const DEFAULT_CAPACITY = 16 * 1024 * 1024;
const MAX_CAPACITY = 128 * 1024 * 1024;

let outputBuf = new Uint8Array(DEFAULT_CAPACITY);
let outputPtr = ptr(outputBuf);

function ensureCapacity(capacity: number): void {
  if (outputBuf.length >= capacity) return;
  outputBuf = new Uint8Array(capacity);
  outputPtr = ptr(outputBuf);
}

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

function decodeJson(symbol: keyof NativeCheckpointDecoder, input: Uint8Array): string | null {
  if (!nativeDecoder) return null;

  let capacity = outputBuf.length;
  while (capacity <= MAX_CAPACITY) {
    ensureCapacity(capacity);
    const bytesWritten = nativeDecoder[symbol](ptr(input), input.length, outputPtr, outputBuf.length);
    if (bytesWritten > 0) {
      return decoder.decode(outputBuf.subarray(0, bytesWritten));
    }
    capacity *= 2;
  }

  throw new Error(`Native checkpoint decoder failed for ${symbol}`);
}

function decodeResponse(symbol: keyof NativeCheckpointDecoder, input: Uint8Array): GrpcCheckpointResponse | null {
  const json = decodeJson(symbol, input);
  if (!json) return null;
  return hydrateResponse(JSON.parse(json) as GrpcCheckpointResponse);
}

export function isNativeCheckpointDecoderAvailable(): boolean {
  return nativeDecoder !== null;
}

export function decodeArchiveCheckpointCompressedNative(
  input: Uint8Array,
): GrpcCheckpointResponse | null {
  return decodeResponse("decode_archive_checkpoint", input);
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

export function decodeGetCheckpointResponseNative(
  input: Uint8Array,
): GrpcCheckpointResponse | null {
  return decodeResponse("decode_get_checkpoint_response", input);
}
