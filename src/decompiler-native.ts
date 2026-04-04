/**
 * Move bytecode decompiler — native server implementation.
 *
 * Strategy (in order of preference):
 * 1. Shell out to the `move-decompile` CLI binary (fastest, no overhead)
 * 2. Load the WASM file from disk via Bun's WASM support (portable fallback)
 *
 * The CLI binary is expected at the sibling repo:
 *   ~/Documents/GitHub/unconfirmedlabs/move-decompiler-zig/zig-out/bin/move-decompile
 *
 * The WASM file is expected at:
 *   ~/Documents/GitHub/unconfirmedlabs/move-decompiler-zig/zig-out/bin/move_decompiler.wasm
 */
import { existsSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";

// ---------------------------------------------------------------------------
// Binary discovery
// ---------------------------------------------------------------------------

const DECOMPILER_DIR = join(
  process.env.HOME || process.env.USERPROFILE || "~",
  "Documents/GitHub/unconfirmedlabs/move-decompiler-zig/zig-out/bin",
);

const CLI_PATH = join(DECOMPILER_DIR, "move-decompile");
const WASM_PATH = join(DECOMPILER_DIR, "move_decompiler.wasm");

const hasCli = existsSync(CLI_PATH);
const hasWasm = existsSync(WASM_PATH);

// ---------------------------------------------------------------------------
// CLI-based decompilation
// ---------------------------------------------------------------------------

async function decompileViaCli(bytecode: Uint8Array): Promise<string> {
  // Write bytecode to a temp file, run CLI, read stdout
  const tmpFile = join(tmpdir(), `jun-decompile-${Date.now()}-${Math.random().toString(36).slice(2)}.mv`);
  try {
    await Bun.write(tmpFile, bytecode);
    const proc = Bun.spawn([CLI_PATH, tmpFile], {
      stdout: "pipe",
      stderr: "pipe",
    });
    const [stdout, stderr] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
    ]);
    const exitCode = await proc.exited;
    if (exitCode !== 0) {
      throw new Error(`move-decompile exited ${exitCode}: ${stderr.trim()}`);
    }
    return stdout;
  } finally {
    try { await Bun.file(tmpFile).exists() && (await import("fs/promises")).unlink(tmpFile); } catch {}
  }
}

// ---------------------------------------------------------------------------
// WASM-based decompilation (disk-loaded, for server)
// ---------------------------------------------------------------------------

let wasmInstance: WebAssembly.Instance | null = null;
let wasmMemory: WebAssembly.Memory | null = null;

async function initWasm() {
  if (wasmInstance) return;
  const wasmBytes = await Bun.file(WASM_PATH).arrayBuffer();
  const res = await WebAssembly.instantiate(wasmBytes);
  wasmInstance = res.instance;
  wasmMemory = wasmInstance.exports.memory as WebAssembly.Memory;
}

async function decompileViaWasm(bytecode: Uint8Array): Promise<string> {
  await initWasm();
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

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export async function decompile(bytecode: Uint8Array): Promise<string> {
  if (hasCli) return decompileViaCli(bytecode);
  if (hasWasm) return decompileViaWasm(bytecode);
  throw new Error(
    "No decompiler backend found. Build move-decompiler-zig:\n" +
    `  cd ~/Documents/GitHub/unconfirmedlabs/move-decompiler-zig && zig build\n` +
    `  Expected CLI at: ${CLI_PATH}\n` +
    `  Expected WASM at: ${WASM_PATH}`,
  );
}
