/**
 * Sui personal message signing and verification.
 *
 * Loads keypairs from the standard Sui keystore (~/.sui/sui_config/sui.keystore)
 * and uses the SDK's signPersonalMessage / verifyPersonalMessageSignature.
 */
import { Ed25519Keypair } from "@mysten/sui/keypairs/ed25519";
import { Secp256k1Keypair } from "@mysten/sui/keypairs/secp256k1";
import { Secp256r1Keypair } from "@mysten/sui/keypairs/secp256r1";
import type { Keypair } from "@mysten/sui/cryptography";
import { readFileSync } from "fs";
import { join } from "path";

// ─── Keystore ────────────────────────────────────────────────────────────────

function getSuiConfigDir(): string {
  const home = process.env.HOME || process.env.USERPROFILE;
  if (!home) throw new Error("Cannot determine home directory");
  return join(home, ".sui", "sui_config");
}

function getActiveAddress(): string {
  const configDir = getSuiConfigDir();
  const yaml = readFileSync(join(configDir, "client.yaml"), "utf-8");
  const match = yaml.match(/active_address:\s*"?(0x[0-9a-fA-F]+)"?/);
  if (!match) throw new Error("No active_address in ~/.sui/sui_config/client.yaml");
  return match[1];
}

function keypairFromBase64(key: string): Keypair {
  const bytes = Buffer.from(key, "base64");
  const flag = bytes[0];
  const secret = bytes.subarray(1);

  if (flag === 0) return Ed25519Keypair.fromSecretKey(secret);
  if (flag === 1) return Secp256k1Keypair.fromSecretKey(secret);
  if (flag === 2) return Secp256r1Keypair.fromSecretKey(secret);
  throw new Error(`Unknown key scheme flag: ${flag}`);
}

export function loadKeypair(address?: string): { keypair: Keypair; address: string } {
  const configDir = getSuiConfigDir();
  const targetAddress = address ?? getActiveAddress();

  const keystoreJson = readFileSync(join(configDir, "sui.keystore"), "utf-8");
  const keys: string[] = JSON.parse(keystoreJson);

  for (const key of keys) {
    try {
      const keypair = keypairFromBase64(key);
      const addr = keypair.getPublicKey().toSuiAddress();
      if (addr === targetAddress) {
        return { keypair, address: addr };
      }
    } catch {
      continue;
    }
  }

  throw new Error(`Address ${targetAddress} not found in keystore`);
}

// ─── Sign / Verify ───────────────────────────────────────────────────────────

const encoder = new TextEncoder();

export interface SignResult {
  message: string;
  address: string;
  signature: string;
}

export async function signMessage(message: string, address?: string): Promise<SignResult> {
  const { keypair, address: addr } = loadKeypair(address);
  const messageBytes = encoder.encode(message);
  const { signature } = await keypair.signPersonalMessage(messageBytes);
  return { message, address: addr, signature };
}

export interface VerifyResult {
  valid: boolean;
  address: string;
  scheme: string;
}

export async function verifyMessage(
  message: string,
  signature: string,
  address: string,
): Promise<VerifyResult> {
  const { verifyPersonalMessageSignature } = await import("@mysten/sui/verify");
  const messageBytes = encoder.encode(message);

  try {
    const publicKey = await verifyPersonalMessageSignature(messageBytes, signature, { address });
    return {
      valid: true,
      address,
      scheme: publicKey.flag() === 0 ? "ed25519" : publicKey.flag() === 1 ? "secp256k1" : "secp256r1",
    };
  } catch {
    return { valid: false, address, scheme: "unknown" };
  }
}
