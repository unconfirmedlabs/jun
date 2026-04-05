/**
 * BCS provider — single import point for all BCS schemas.
 *
 * Swappable at startup via JUN_BCS_PROVIDER env var:
 *   "mysten"       → @mysten/bcs + @mysten/sui/bcs (default)
 *   "unconfirmed"  → @unconfirmed/bcs + @unconfirmed/sui-bcs
 *
 * All other modules import { bcs, suiBcs } from "./bcs-provider.ts"
 * instead of directly from the package. This keeps the swap to one file.
 *
 * Both packages are API-compatible and pass the same 244-test suite.
 * Types are inferred from @mysten/bcs since TypeScript resolves types
 * statically. The runtime implementation is determined by the env var.
 */

const provider = process.env.JUN_BCS_PROVIDER ?? "mysten";

// Dynamic import keyed on provider — Bun resolves at startup.
const bcsModule = provider === "unconfirmed"
  ? await import("@unconfirmed/bcs")
  : await import("@mysten/bcs");

const suiBcsModule = provider === "unconfirmed"
  ? await import("@unconfirmed/sui-bcs")
  : await import("@mysten/sui/bcs");

export const bcs = bcsModule.bcs;
export const suiBcs = suiBcsModule.bcs;
export { provider as bcsProvider };
