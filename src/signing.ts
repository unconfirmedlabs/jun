/**
 * jun/signing — Sui personal message signing and verification.
 *
 * @example Sign and verify a message
 * ```ts
 * import { signMessage, verifyMessage } from "jun/signing";
 *
 * const result = await signMessage("hello world");
 * const verified = await verifyMessage("hello world", result.signature, result.address);
 * ```
 */

export { signMessage, verifyMessage, loadKeypair } from "./message.ts";
export type { SignResult, VerifyResult } from "./message.ts";
