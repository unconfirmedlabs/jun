/** Minimal Sui JSON-RPC helper. */

const DEFAULT_URL = "https://fullnode.mainnet.sui.io:443";

export async function jsonRpc(
  method: string,
  params: unknown[],
  url = DEFAULT_URL,
) {
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
  });
  const json = (await resp.json()) as { result?: unknown; error?: { message: string } };
  if (json.error) throw new Error(`RPC error: ${json.error.message}`);
  return json.result;
}
