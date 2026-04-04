/**
 * Fetch and parse Sui Move package modules from on-chain BCS data.
 *
 * Uses @mysten/sui gRPC client to fetch the package object, then parses
 * the BCS-encoded module map to extract individual module bytecodes.
 */
import { SuiGrpcClient } from "@mysten/sui/grpc";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface MoveModule {
  name: string;
  bytes: Uint8Array;
}

export type Network = "mainnet" | "testnet" | "devnet" | "localnet";

// ---------------------------------------------------------------------------
// Network URLs
// ---------------------------------------------------------------------------

const NETWORK_URLS: Record<Network, string> = {
  mainnet: "https://fullnode.mainnet.sui.io:443",
  testnet: "https://fullnode.testnet.sui.io:443",
  devnet: "https://fullnode.devnet.sui.io:443",
  localnet: "http://127.0.0.1:9000",
};

const clients = new Map<string, SuiGrpcClient>();

function getClient(network: Network): SuiGrpcClient {
  const url = NETWORK_URLS[network];
  let client = clients.get(url);
  if (!client) {
    client = new SuiGrpcClient({ network, baseUrl: url });
    clients.set(url, client);
  }
  return client;
}

// ---------------------------------------------------------------------------
// BCS parsing
// ---------------------------------------------------------------------------

/**
 * Parse a BCS-encoded Sui package object into a map of module name -> bytecode.
 *
 * Layout: 1 byte (object type) + 32 bytes (id) + 8 bytes (version) + ULEB128 map
 * Each map entry: ULEB128 name_len, UTF-8 name, ULEB128 bytes_len, bytes
 */
export function parsePackageBcs(data: Uint8Array): Map<string, Uint8Array> {
  let pos = 1 + 32 + 8; // skip object type + id + version

  function readULEB(): number {
    let result = 0,
      shift = 0;
    while (true) {
      const b = data[pos++]!;
      result |= (b & 0x7f) << shift;
      if (!(b & 0x80)) return result;
      shift += 7;
    }
  }

  const mapLen = readULEB();
  const modules = new Map<string, Uint8Array>();

  for (let i = 0; i < mapLen; i++) {
    const nameLen = readULEB();
    const name = new TextDecoder().decode(data.slice(pos, pos + nameLen));
    pos += nameLen;
    const bytesLen = readULEB();
    const bytes = data.slice(pos, pos + bytesLen);
    pos += bytesLen;
    modules.set(name, bytes);
  }

  return modules;
}

// ---------------------------------------------------------------------------
// Fetch
// ---------------------------------------------------------------------------

/**
 * Fetch all modules from a published Sui Move package.
 * Returns an array of { name, bytes } for each module in the package.
 */
export async function fetchPackageModules(
  packageId: string,
  network: Network = "mainnet",
): Promise<MoveModule[]> {
  return fetchPackageModulesWithClient(getClient(network), packageId);
}

/**
 * Fetch all modules using an existing SuiGrpcClient.
 * Use this when you already have a client (e.g. in browser apps).
 */
export async function fetchPackageModulesWithClient(
  client: SuiGrpcClient,
  packageId: string,
): Promise<MoveModule[]> {
  const { object } = await client.core.getObject({
    objectId: packageId,
    include: { objectBcs: true },
  });

  if (object.type !== "package") {
    throw new Error(`Object ${packageId} is not a package (type: ${object.type})`);
  }

  if (!object.objectBcs) {
    throw new Error("No BCS data returned — the node may not support objectBcs");
  }

  const moduleMap = parsePackageBcs(object.objectBcs);
  if (moduleMap.size === 0) {
    throw new Error("Package contains no modules");
  }

  return [...moduleMap.entries()].map(([name, bytes]) => ({ name, bytes }));
}
