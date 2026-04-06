import { dlopen, FFIType, JSCallback, ptr, suffix, toArrayBuffer } from "bun:ffi";
import { existsSync } from "fs";
import { join } from "path";
import type { GrpcCheckpointResponse } from "./grpc.ts";

type NativeDecodeFn = (
  inputPtr: number,
  inputLen: number,
  outputPtr: number,
  outputCapacity: number,
) => number;

type NativeRangeDecodeFn = (
  archiveUrlPtr: number,
  archiveUrlLen: number,
  fromCheckpoint: bigint,
  toCheckpoint: bigint,
  concurrency: number,
  outputPtr: number,
  outputCapacity: number,
  outputCallback: number,
) => number;

type NativeCheckpointDecoder = {
  decode_archive_checkpoint: NativeDecodeFn;
  decode_checkpoint_proto: NativeDecodeFn;
  decode_subscribe_checkpoints_response: NativeDecodeFn;
  decode_get_checkpoint_response: NativeDecodeFn;
  decode_checkpoint_binary: NativeDecodeFn;
  download_and_decode_archive_checkpoint_range?: NativeRangeDecodeFn;
};

type NativeDecodeSymbol =
  | "decode_archive_checkpoint"
  | "decode_checkpoint_proto"
  | "decode_subscribe_checkpoints_response"
  | "decode_get_checkpoint_response"
  | "decode_checkpoint_binary";

const LIB_NAME = `libjun_checkpoint_decoder.${suffix}`;
const LIB_PATHS = [
  join(import.meta.dir, "..", "native", "rust-decoder", "target", "release", LIB_NAME),
  join(import.meta.dir, "..", "native", "rust-decoder", "target", "debug", LIB_NAME),
];

let nativeDecoder: NativeCheckpointDecoder | null = null;

const baseSymbols = {
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
} as const;

const extendedSymbols = {
  ...baseSymbols,
  download_and_decode_archive_checkpoint_range: {
    args: [FFIType.ptr, FFIType.u32, FFIType.u64, FFIType.u64, FFIType.u32, FFIType.ptr, FFIType.u32, FFIType.function],
    returns: FFIType.i32,
  },
} as const;

for (const libPath of LIB_PATHS) {
  if (!existsSync(libPath)) continue;
  try {
    try {
      const lib = dlopen(libPath, extendedSymbols);
      nativeDecoder = lib.symbols;
      break;
    } catch {
      const lib = dlopen(libPath, baseSymbols);
      nativeDecoder = lib.symbols;
      break;
    }
  } catch {}
}

const decoder = new TextDecoder();
const DEFAULT_CAPACITY = 16 * 1024 * 1024;
const MAX_CAPACITY = 128 * 1024 * 1024;
const BINARY_DEFAULT_CAPACITY = 64 * 1024 * 1024;
const BINARY_MAX_CAPACITY = 256 * 1024 * 1024;
const RANGE_OUTPUT_CAPACITY = 64 * 1024 * 1024;
const RANGE_DONE_SEQ = (1n << 64n) - 1n;

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

function decodeJson(symbol: NativeDecodeSymbol, input: Uint8Array): string | null {
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

function decodeResponse(symbol: NativeDecodeSymbol, input: Uint8Array): GrpcCheckpointResponse | null {
  const json = decodeJson(symbol, input);
  if (!json) return null;
  return hydrateResponse(JSON.parse(json) as GrpcCheckpointResponse);
}

function decodeBinary(symbol: NativeDecodeSymbol, input: Uint8Array): Uint8Array | null {
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

  throw new Error(`Native checkpoint decoder failed for ${symbol}`);
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

export function hasDownloadAndDecodeRange(): boolean {
  return isNativeCheckpointDecoderAvailable()
    && typeof nativeDecoder?.download_and_decode_archive_checkpoint_range === "function";
}

export function downloadAndDecodeRange(
  archiveUrl: string,
  from: bigint,
  to: bigint,
  concurrency: number,
  onCheckpoint: (seq: bigint, binary: Uint8Array) => void,
): Promise<void> {
  if (!nativeDecoder?.download_and_decode_archive_checkpoint_range) {
    return Promise.reject(new Error("native range downloader is unavailable"));
  }

  const urlBuf = new TextEncoder().encode(archiveUrl);
  const outBuf = new Uint8Array(RANGE_OUTPUT_CAPACITY);

  return new Promise((resolve, reject) => {
    let settled = false;
    let callbackError: Error | null = null;
    const callback = new JSCallback((seqValue: number | bigint, dataPtr: number, dataLen: number) => {
      const seq = typeof seqValue === "bigint" ? seqValue : BigInt(seqValue);

      if (seq === RANGE_DONE_SEQ) {
        if (!settled) {
          settled = true;
          callback.close();
          if (callbackError) {
            reject(callbackError);
          } else {
            resolve();
          }
        }
        return;
      }

      try {
        const view = new Uint8Array(
          toArrayBuffer(dataPtr as Parameters<typeof toArrayBuffer>[0], 0, dataLen),
          0,
          dataLen,
        );
        const binary = new Uint8Array(dataLen);
        binary.set(view);
        onCheckpoint(seq, binary);
      } catch (error) {
        callbackError = error instanceof Error ? error : new Error(String(error));
      }
    }, {
      args: [FFIType.u64, FFIType.ptr, FFIType.u32],
      returns: FFIType.void,
      threadsafe: true,
    });

    try {
      const callbackPtr = callback.ptr;
      if (!callbackPtr) {
        settled = true;
        callback.close();
        reject(new Error("native range downloader callback pointer is null"));
        return;
      }

      const rc = nativeDecoder.download_and_decode_archive_checkpoint_range!(
        ptr(urlBuf),
        urlBuf.length,
        from,
        to,
        concurrency,
        ptr(outBuf),
        outBuf.length,
        callbackPtr as unknown as number,
      );

      if (rc < 0) {
        if (!settled) {
          settled = true;
          callback.close();
          reject(new Error(`native range downloader failed to start (${rc})`));
        }
      }
    } catch (error) {
      if (!settled) {
        settled = true;
        callback.close();
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    }
  });
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
