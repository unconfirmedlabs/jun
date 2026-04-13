/**
 * Integration test: auto-reload cycle using file:// config URL.
 *
 * Tests the full flow:
 *   1. Write initial config (SUI balance tracking)
 *   2. Start auto-reload timer with etag-based conditional fetch
 *   3. Modify config file to add WAL
 *   4. Verify reload fires and detects the change
 */
import { test, expect, describe, afterEach } from "bun:test";
import { fetchRemoteConfig, type FetchOptions } from "./remote-config.ts";
import { tmpdir } from "os";
import { join } from "path";
import { unlinkSync } from "fs";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const suiOnlyConfig = `
sources:
  grpcUrl: fullnode.testnet.sui.io:443

processors:
  balances:
    coinTypes:
      - "0x2::sui::SUI"

storage:
  postgres: postgres://localhost/jun_test
`;

const suiAndWalConfig = `
sources:
  grpcUrl: fullnode.testnet.sui.io:443

processors:
  balances:
    coinTypes:
      - "0x2::sui::SUI"
      - "0x9f992cc2430a1f442ca7a5ca7638169f5d5c00e0ebc3977a65e9ac6e497fe5ef::wal::WAL"

storage:
  postgres: postgres://localhost/jun_test
`;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("auto-reload with file://", () => {
  const tmpFile = join(tmpdir(), `jun-auto-reload-test-${Date.now()}.yml`);
  const configUrl = `file://${tmpFile}`;
  let timer: Timer | null = null;

  afterEach(() => {
    if (timer) clearInterval(timer);
    try { unlinkSync(tmpFile); } catch {}
  });


  test("skips reload when config unchanged (etag match)", async () => {
    await Bun.write(tmpFile, suiOnlyConfig);

    let fetchCount = 0;
    let reloadCount = 0;
    let lastEtag: string | undefined;

    timer = setInterval(async () => {
      fetchCount++;
      const result = await fetchRemoteConfig(configUrl, { etag: lastEtag });
      if (!result) return; // etag matched, no download
      lastEtag = result.etag;
      reloadCount++;
    }, 100);

    // Wait several ticks
    await Bun.sleep(450);

    // Should have fetched multiple times but only "reloaded" once (first fetch)
    expect(fetchCount).toBeGreaterThanOrEqual(3);
    expect(reloadCount).toBe(1); // only the initial fetch
  });

  test("multiple config changes are each picked up", async () => {
    await Bun.write(tmpFile, suiOnlyConfig);

    const contents: string[] = [];
    let lastEtag: string | undefined;

    timer = setInterval(async () => {
      const result = await fetchRemoteConfig(configUrl, { etag: lastEtag });
      if (!result) return;
      lastEtag = result.etag;
      contents.push(result.content);
    }, 100);

    await Bun.sleep(150);
    expect(contents).toHaveLength(1);

    // Change 1: add WAL
    await Bun.sleep(10);
    await Bun.write(tmpFile, suiAndWalConfig);
    await Bun.sleep(200);
    expect(contents).toHaveLength(2);
    expect(contents[1]).toContain("WAL");

    // Change 2: back to SUI only
    await Bun.sleep(10);
    await Bun.write(tmpFile, suiOnlyConfig);
    await Bun.sleep(200);
    expect(contents).toHaveLength(3);
    expect(contents[2]).not.toContain("WAL");
  });
});
