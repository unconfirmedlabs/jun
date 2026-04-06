/**
 * Move bytecode decompiler — server-only, uses native CLI or WASM-from-disk.
 *
 * Usage:
 *   import { decompile, decompileModule } from "jun/decompiler";
 *   const source = await decompile(bytecodeBytes);
 *   const source2 = await decompileModule("0x2", "coin", "mainnet");
 */
import type { Network } from "./package-reader.ts";

/**
 * Decompile raw Move bytecode into reconstructed Move source.
 */
export async function decompile(bytecode: Uint8Array): Promise<string> {
  const backend = await import("./decompiler-native.ts");
  return backend.decompile(bytecode);
}

/**
 * Fetch a module from on-chain and decompile it.
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
