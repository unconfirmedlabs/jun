/**
 * Move bytecode decompiler — unified API with dual target.
 *
 * Automatically selects the native (CLI/WASM-from-disk) backend on server
 * and the browser WASM backend in browser environments.
 *
 * Usage:
 *   import { decompile, decompileModule } from "jun/decompiler";
 *   const source = await decompile(bytecodeBytes);
 *   const source2 = await decompileModule("0x2", "coin", "mainnet");
 */
import type { Network } from "./package-reader.ts";

// ---------------------------------------------------------------------------
// Backend selection
// ---------------------------------------------------------------------------

const isBrowser = typeof (globalThis as Record<string, unknown>).document !== "undefined";

async function getBackend(): Promise<{ decompile: (bytecode: Uint8Array) => Promise<string> }> {
  if (isBrowser) {
    return await import("./decompiler-wasm.ts");
  }
  return await import("./decompiler-native.ts");
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Decompile raw Move bytecode into reconstructed Move source.
 */
export async function decompile(bytecode: Uint8Array): Promise<string> {
  const backend = await getBackend();
  return backend.decompile(bytecode);
}

/**
 * Fetch a module from on-chain and decompile it.
 *
 * @param packageId - The Sui package object ID (0x...)
 * @param moduleName - The module name within the package
 * @param network - Network to fetch from (default: "mainnet")
 */
export async function decompileModule(
  packageId: string,
  moduleName: string,
  network: Network = "mainnet",
): Promise<string> {
  const { fetchPackageModules } = await import("./package-reader.ts");
  const modules = await fetchPackageModules(packageId, network);

  const mod = modules.find((m) => m.name === moduleName);
  if (!mod) {
    const available = modules.map((m) => m.name).join(", ");
    throw new Error(`Module "${moduleName}" not found in ${packageId}. Available: ${available}`);
  }

  return decompile(mod.bytes);
}
