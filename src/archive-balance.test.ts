import { test, expect, describe } from "bun:test";
import { computeBalanceChangesFromArchive } from "./archive-balance.ts";
import { getCheckpointType } from "./archive.ts";
import { zstdDecompressSync } from "zlib";

/**
 * Integration test: verify balance change computation against a known
 * testnet checkpoint that contains a coin::send_funds accumulator transfer.
 *
 * Checkpoint 321080294 on testnet:
 * - Sender 0xf3a64ba4... sent 1 SUI to 0x00fbdc88... via coin::send_funds
 * - Accumulator write: Merge 1,000,000,000 to recipient
 * - Coin object diff: sender balance decreased by 1,001,009,880 (1 SUI + gas)
 */
describe("computeBalanceChangesFromArchive", () => {
  test("detects accumulator write + coin diff from testnet checkpoint 321080294", async () => {
    const archiveUrl = "https://checkpoints.testnet.sui.io";
    const seq = 321080294n;

    const resp = await fetch(`${archiveUrl}/${seq}.binpb.zst`);
    expect(resp.ok).toBe(true);

    const compressed = new Uint8Array(await resp.arrayBuffer());
    const decompressed = zstdDecompressSync(Buffer.from(compressed));
    const Checkpoint = await getCheckpointType();
    const decoded = Checkpoint.decode(decompressed);
    const checkpointProto = Checkpoint.toObject(decoded, { longs: String, enums: String, defaults: false });

    const changes = computeBalanceChangesFromArchive(
      checkpointProto,
      seq,
      new Date("2026-04-02T00:52:04Z"),
      null, // no filter — all coin types
    );

    expect(changes.length).toBe(2);

    // Sort by amount for deterministic order
    changes.sort((a, b) => BigInt(a.amount) < BigInt(b.amount) ? -1 : 1);

    // Sender: lost 1 SUI + gas
    const sender = changes[0]!;
    expect(sender.address).toContain("f3a64ba4");
    expect(sender.coinType).toBe("0x2::sui::SUI");
    expect(sender.amount).toBe("-1001009880");

    // Recipient: gained 1 SUI via accumulator
    const recipient = changes[1]!;
    expect(recipient.address).toContain("fbdc88b0");
    expect(recipient.coinType).toBe("0x2::sui::SUI");
    expect(recipient.amount).toBe("1000000000");
  }, 30_000);

  test("filters by coin type", async () => {
    const archiveUrl = "https://checkpoints.testnet.sui.io";
    const seq = 321080294n;

    const resp = await fetch(`${archiveUrl}/${seq}.binpb.zst`);
    const compressed = new Uint8Array(await resp.arrayBuffer());
    const decompressed = zstdDecompressSync(Buffer.from(compressed));
    const Checkpoint = await getCheckpointType();
    const decoded = Checkpoint.decode(decompressed);
    const checkpointProto = Checkpoint.toObject(decoded, { longs: String, enums: String, defaults: false });

    // Filter for a coin type that doesn't exist in this checkpoint
    const changes = computeBalanceChangesFromArchive(
      checkpointProto,
      seq,
      new Date(),
      new Set(["0xdead::fake::COIN"]),
    );

    expect(changes.length).toBe(0);
  }, 30_000);

  test("SUI filter matches accumulator writes", async () => {
    const archiveUrl = "https://checkpoints.testnet.sui.io";
    const seq = 321080294n;

    const resp = await fetch(`${archiveUrl}/${seq}.binpb.zst`);
    const compressed = new Uint8Array(await resp.arrayBuffer());
    const decompressed = zstdDecompressSync(Buffer.from(compressed));
    const Checkpoint = await getCheckpointType();
    const decoded = Checkpoint.decode(decompressed);
    const checkpointProto = Checkpoint.toObject(decoded, { longs: String, enums: String, defaults: false });

    // Filter for SUI only
    const changes = computeBalanceChangesFromArchive(
      checkpointProto,
      seq,
      new Date(),
      new Set(["0x2::sui::SUI"]),
    );

    // Should find both sender and recipient
    expect(changes.length).toBe(2);
  }, 30_000);
});
