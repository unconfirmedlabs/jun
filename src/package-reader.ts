/**
 * Fetch and parse Sui Move package modules from on-chain BCS data.
 *
 * Uses @mysten/sui gRPC client to fetch the package object, then parses
 * the BCS-encoded module map to extract individual module bytecodes.
 */
import { SuiGrpcClient } from "@mysten/sui/grpc";

// ---------------------------------------------------------------------------
// BCS primitives (local — BCS package layout is simple enough to inline)
// ---------------------------------------------------------------------------

/** Read a ULEB128 integer from a buffer. Returns [value, bytesConsumed]. */
function readUleb128(buf: Uint8Array, offset: number): [number, number] {
  let value = 0;
  let shift = 0;
  let pos = offset;
  for (;;) {
    const byte = buf[pos]!;
    value |= (byte & 0x7f) << shift;
    pos++;
    if ((byte & 0x80) === 0) break;
    shift += 7;
  }
  return [value, pos - offset];
}

/** Read a BCS string (ULEB128 length prefix + UTF-8 bytes). Returns [string, newOffset]. */
function readBcsString(buf: Uint8Array, offset: number): [string, number] {
  const [len, lenBytes] = readUleb128(buf, offset);
  offset += lenBytes;
  const str = new TextDecoder().decode(buf.slice(offset, offset + len));
  return [str, offset + len];
}

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

  const [mapLen, mapLenBytes] = readUleb128(data, pos);
  pos += mapLenBytes;

  const modules = new Map<string, Uint8Array>();

  for (let i = 0; i < mapLen; i++) {
    const [name, nameEnd] = readBcsString(data, pos);
    pos = nameEnd;
    const [bytesLen, bytesLenBytes] = readUleb128(data, pos);
    pos += bytesLenBytes;
    modules.set(name, data.slice(pos, pos + bytesLen));
    pos += bytesLen;
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
