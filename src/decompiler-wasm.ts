/**
 * Move bytecode decompiler — WASM implementation for browser environments.
 *
 * Loads the move_decompiler.wasm binary via fetch() and provides a
 * decompile(bytecode) function that returns reconstructed Move source.
 */

let wasmInstance: WebAssembly.Instance | null = null;
let wasmMemory: WebAssembly.Memory | null = null;

async function init() {
  if (wasmInstance) return;
  const res = await WebAssembly.instantiateStreaming(
    fetch("/move_decompiler.wasm"),
  );
  wasmInstance = res.instance;
  wasmMemory = wasmInstance.exports.memory as WebAssembly.Memory;
}

export async function decompile(bytecode: Uint8Array): Promise<string> {
  await init();
  const exports = wasmInstance!.exports as {
    decompile: (ptr: number, len: number) => number;
    get_input_ptr: () => number;
    get_output_ptr: () => number;
    get_max_input_size: () => number;
  };

  const maxInput = exports.get_max_input_size();
  if (bytecode.length > maxInput) {
    throw new Error(`Module too large: ${bytecode.length} bytes (max ${maxInput})`);
  }

  const inputPtr = exports.get_input_ptr();
  const mem = new Uint8Array(wasmMemory!.buffer);
  mem.set(bytecode, inputPtr);

  const outputLen = exports.decompile(inputPtr, bytecode.length);
  if (outputLen === 0) {
    throw new Error("Decompilation failed");
  }

  const outputPtr = exports.get_output_ptr();
  const output = new Uint8Array(wasmMemory!.buffer).slice(outputPtr, outputPtr + outputLen);
  return new TextDecoder().decode(output);
}
