/**
 * Shared helper functions for jun CLI client commands.
 */

/** Object owner type matching the Sui SDK ObjectOwner discriminated union */
export type ObjectOwnerLike = {
  $kind: string;
  AddressOwner?: string;
  ObjectOwner?: string;
  Shared?: { initialSharedVersion: string };
  Immutable?: boolean;
  ConsensusAddressOwner?: { owner: string };
};

/** Format an object owner for display */
export function formatOwner(owner: ObjectOwnerLike): string {
  switch (owner.$kind) {
    case "AddressOwner": return owner.AddressOwner!;
    case "ObjectOwner": return owner.ObjectOwner!;
    case "Shared": return `Shared (v${owner.Shared!.initialSharedVersion})`;
    case "Immutable": return "Immutable";
    case "ConsensusAddressOwner": return `Consensus (${owner.ConsensusAddressOwner!.owner})`;
    default: return "unknown";
  }
}

/** Object shape accepted by printObject */
export type PrintableObject = {
  objectId: string;
  type?: string;
  version: string;
  digest: string;
  owner: ObjectOwnerLike;
  previousTransaction?: string | null;
  objectBcs?: Uint8Array;
  json?: Record<string, unknown> | null;
};

/** Pretty-print an object's fields (shared between `object` and `objects`) */
export function printObject(obj: PrintableObject) {
  console.log(`\n  object      ${obj.objectId}`);
  if (obj.type) console.log(`  type        ${obj.type}`);
  console.log(`  version     ${obj.version}`);
  console.log(`  digest      ${obj.digest}`);
  console.log(`  owner       ${formatOwner(obj.owner)}`);
  if (obj.previousTransaction) console.log(`  prev tx     ${obj.previousTransaction}`);
  if (obj.objectBcs) console.log(`  bcs         ${obj.objectBcs.length} bytes`);
  if (obj.json) {
    console.log(`\n  content:`);
    for (const [key, val] of Object.entries(obj.json)) {
      const display = typeof val === "object" ? JSON.stringify(val) : String(val);
      const truncated = display.length > 120 ? display.slice(0, 117) + "..." : display;
      console.log(`    ${key}: ${truncated}`);
    }
  }
}

/** JSON replacer that serializes BigInt and Uint8Array */
export function jsonReplacer(_: string, v: unknown) {
  if (v instanceof Uint8Array) return Buffer.from(v).toString("hex");
  return typeof v === "bigint" ? v.toString() : v;
}

/** Format raw MIST balance as human-readable with given decimals/symbol */
export function formatBalance(mist: bigint, decimals: number, symbol: string): string {
  const formatted = decimals > 0
    ? (Number(mist) / 10 ** decimals).toLocaleString(undefined, { minimumFractionDigits: Math.min(decimals, 4), maximumFractionDigits: Math.min(decimals, 4) })
    : mist.toLocaleString();
  return `${formatted} ${symbol}`;
}

/** CLI error handler — prints message and exits */
export function cliError(err: unknown): never {
  const msg = err instanceof Error ? err.message : String(err);
  console.error(`[jun] error: ${msg}`);
  process.exit(1);
}
