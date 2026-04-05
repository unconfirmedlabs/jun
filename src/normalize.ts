/**
 * Shared normalization utilities using @mysten/sui/utils.
 *
 * All Sui addresses, object IDs, and struct tags are normalized to their
 * canonical forms before storage, comparison, or filtering.
 */
import {
  normalizeSuiAddress,
  isValidSuiAddress,
  normalizeStructTag,
} from "@mysten/sui/utils";

export { normalizeSuiAddress, isValidSuiAddress, normalizeStructTag };

/**
 * Normalize a coin type string.
 * "0x2::sui::SUI" → "0x0000...0002::sui::SUI"
 * "0x0000...0002::sui::SUI" → "0x0000...0002::sui::SUI" (already normalized)
 */
export function normalizeCoinType(coinType: string): string {
  try {
    return normalizeStructTag(coinType);
  } catch {
    // If normalizeStructTag fails (malformed type), try manual normalization
    const parts = coinType.split("::");
    if (parts.length >= 3) {
      parts[0] = normalizeSuiAddress(parts[0]!);
    }
    return parts.join("::");
  }
}

/**
 * Normalize a Move event type string (may include generics).
 * "0x2::coin::Coin<0x2::sui::SUI>" → normalized form
 */
export function normalizeEventType(eventType: string): string {
  try {
    return normalizeStructTag(eventType);
  } catch {
    // Fallback for complex generics that normalizeStructTag can't handle
    return eventType;
  }
}

/**
 * Validate that a string is a valid Sui address (with or without 0x prefix).
 * Returns the normalized address if valid, throws if invalid.
 */
export function validateAndNormalizeAddress(address: string, context: string): string {
  const normalized = normalizeSuiAddress(address);
  if (!isValidSuiAddress(normalized)) {
    throw new Error(`Invalid Sui address in ${context}: "${address}"`);
  }
  return normalized;
}

/**
 * Extract and validate the package address from a Move type string.
 * "0xPKG::module::MyEvent" → validates 0xPKG..., returns normalized
 */
export function validateEventTypeAddress(eventType: string, context: string): void {
  const parts = eventType.split("::");
  if (parts.length < 3) {
    throw new Error(`Invalid Move type in ${context}: "${eventType}". Expected format: 0xPKG::module::StructName`);
  }
  validateAndNormalizeAddress(parts[0]!, context);
}

/**
 * Strip generic type parameters from a Move type string.
 * "0xPKG::module::Event<0x2::sui::SUI>" → "0xPKG::module::Event"
 */
export function stripGenerics(type: string): string {
  const index = type.indexOf("<");
  return index === -1 ? type : type.slice(0, index);
}

/**
 * Extract the top-level type parameters from a Move type string.
 * Splits on commas at depth 0 (respecting nested `<>`).
 *
 * "Event<0x2::sui::SUI>"                    → ["0x2::sui::SUI"]
 * "BetResults<UP_USD, Coinflip>"            → ["UP_USD", "Coinflip"]
 * "PositionClaimed<PositionName<A, B>, C>"  → ["PositionName<A, B>", "C"]
 * "Event"                                   → []
 */
export function extractTypeParams(type: string): string[] {
  const start = type.indexOf("<");
  if (start === -1) return [];

  // Find the matching closing `>` (last char, but verify)
  const inner = type.slice(start + 1, type.lastIndexOf(">"));
  if (!inner) return [];

  // Split on commas at depth 0
  const params: string[] = [];
  let depth = 0;
  let current = "";
  for (let i = 0; i < inner.length; i++) {
    const ch = inner[i]!;
    if (ch === "<") depth++;
    else if (ch === ">") depth--;
    else if (ch === "," && depth === 0) {
      params.push(current.trim());
      current = "";
      continue;
    }
    current += ch;
  }
  if (current.trim()) params.push(current.trim());
  return params;
}
