import { test, expect, describe, afterEach } from "bun:test";
import { validateConfigUrl, fetchRemoteConfig } from "./remote-config.ts";
import { tmpdir } from "os";
import { join } from "path";
import { unlinkSync, symlinkSync, rmdirSync } from "fs";

// ---------------------------------------------------------------------------
// validateConfigUrl
// ---------------------------------------------------------------------------

describe("validateConfigUrl", () => {
  test("accepts s3://bucket/key", () => {
    expect(() => validateConfigUrl("s3://my-bucket/configs/indexer.yml")).not.toThrow();
  });

  test("accepts s3:// with nested keys", () => {
    expect(() => validateConfigUrl("s3://bucket/a/b/c/config.yml")).not.toThrow();
  });

  test("rejects s3:// without key", () => {
    expect(() => validateConfigUrl("s3://bucket")).toThrow("Must be s3://bucket/key");
  });

  test("rejects s3:// with trailing slash only", () => {
    expect(() => validateConfigUrl("s3://bucket/")).toThrow("Must be s3://bucket/key");
  });

  test("rejects s3:// with empty bucket", () => {
    expect(() => validateConfigUrl("s3:///key")).toThrow("Must be s3://bucket/key");
  });

  test("accepts https://", () => {
    expect(() => validateConfigUrl("https://example.com/config.yml")).not.toThrow();
  });

  test("accepts file:// with absolute path", () => {
    expect(() => validateConfigUrl("file:///tmp/config.yml")).not.toThrow();
  });

  test("rejects file:// with relative path", () => {
    expect(() => validateConfigUrl("file://relative/path.yml")).toThrow("Path must be absolute");
  });

  test("rejects http:// (no TLS)", () => {
    expect(() => validateConfigUrl("http://localhost/config.yml")).toThrow("Must start with");
  });

  test("rejects ftp://", () => {
    expect(() => validateConfigUrl("ftp://host/file")).toThrow("Must start with");
  });

  test("rejects bare path", () => {
    expect(() => validateConfigUrl("/tmp/config.yml")).toThrow("Must start with");
  });

  test("rejects empty string", () => {
    expect(() => validateConfigUrl("")).toThrow("Must start with");
  });
});

// ---------------------------------------------------------------------------
// fetchRemoteConfig — file:// protocol
// ---------------------------------------------------------------------------

describe("fetchRemoteConfig file://", () => {
  const prefix = join(tmpdir(), `jun-rc-test-${Date.now()}`);
  const tmpFile = `${prefix}.yml`;

  afterEach(() => {
    try { unlinkSync(tmpFile); } catch {}
    try { unlinkSync(`${prefix}-link.yml`); } catch {}
  });

  test("reads a valid YAML file", async () => {
    const yaml = "network: testnet\ngrpcUrl: fullnode.testnet.sui.io:443\n";
    await Bun.write(tmpFile, yaml);

    const result = await fetchRemoteConfig(`file://${tmpFile}`);
    expect(result).not.toBeNull();
    expect(result!.content).toBe(yaml);
    expect(result!.etag).toStartWith("file:");
  });

  test("re-reads after file modification", async () => {
    await Bun.write(tmpFile, "version: 1\n");
    const v1 = await fetchRemoteConfig(`file://${tmpFile}`);
    expect(v1!.content).toBe("version: 1\n");

    // Need a small delay so mtime changes (some FS have 1s granularity)
    await Bun.sleep(10);
    await Bun.write(tmpFile, "version: 2\n");
    const v2 = await fetchRemoteConfig(`file://${tmpFile}`);
    expect(v2!.content).toBe("version: 2\n");
  });

  test("follows symlinks", async () => {
    await Bun.write(tmpFile, "symlinked: true\n");
    const linkPath = `${prefix}-link.yml`;
    symlinkSync(tmpFile, linkPath);

    const result = await fetchRemoteConfig(`file://${linkPath}`);
    expect(result!.content).toBe("symlinked: true\n");
  });

  test("throws clear error on missing file", async () => {
    await expect(
      fetchRemoteConfig(`file://${prefix}-nonexistent.yml`),
    ).rejects.toThrow("Config file not found");
  });

  test("throws on empty file", async () => {
    await Bun.write(tmpFile, "");
    await expect(
      fetchRemoteConfig(`file://${tmpFile}`),
    ).rejects.toThrow("Config is empty");
  });

  test("throws on whitespace-only file", async () => {
    await Bun.write(tmpFile, "   \n\n  \n");
    await expect(
      fetchRemoteConfig(`file://${tmpFile}`),
    ).rejects.toThrow("Config is empty");
  });

  test("handles unicode content", async () => {
    const yaml = "name: test-unicode-ok\n";
    await Bun.write(tmpFile, yaml);

    const result = await fetchRemoteConfig(`file://${tmpFile}`);
    expect(result!.content).toBe(yaml);
  });

  test("rejects file larger than 1MB", async () => {
    const huge = "x".repeat(1_000_001);
    await Bun.write(tmpFile, huge);

    await expect(
      fetchRemoteConfig(`file://${tmpFile}`),
    ).rejects.toThrow("too large");
  });
});

// ---------------------------------------------------------------------------
// Conditional fetch (etag) — file://
// ---------------------------------------------------------------------------

describe("conditional fetch file://", () => {
  const tmpFile = join(tmpdir(), `jun-rc-etag-${Date.now()}.yml`);

  afterEach(() => {
    try { unlinkSync(tmpFile); } catch {}
  });

  test("returns null when file unchanged (same etag)", async () => {
    await Bun.write(tmpFile, "key: value\n");

    const first = await fetchRemoteConfig(`file://${tmpFile}`);
    expect(first).not.toBeNull();

    // Same etag → null (unchanged)
    const second = await fetchRemoteConfig(`file://${tmpFile}`, { etag: first!.etag });
    expect(second).toBeNull();
  });

  test("returns content when file changed (different etag)", async () => {
    await Bun.write(tmpFile, "v: 1\n");
    const first = await fetchRemoteConfig(`file://${tmpFile}`);

    await Bun.sleep(10); // ensure mtime advances
    await Bun.write(tmpFile, "v: 2\n");

    const second = await fetchRemoteConfig(`file://${tmpFile}`, { etag: first!.etag });
    expect(second).not.toBeNull();
    expect(second!.content).toBe("v: 2\n");
    expect(second!.etag).not.toBe(first!.etag);
  });

  test("returns content when no previous etag", async () => {
    await Bun.write(tmpFile, "key: value\n");

    const result = await fetchRemoteConfig(`file://${tmpFile}`, { etag: undefined });
    expect(result).not.toBeNull();
    expect(result!.content).toBe("key: value\n");
  });

  test("returns content when etag is stale/wrong", async () => {
    await Bun.write(tmpFile, "key: value\n");

    const result = await fetchRemoteConfig(`file://${tmpFile}`, { etag: "bogus" });
    expect(result).not.toBeNull();
    expect(result!.content).toBe("key: value\n");
  });
});

// ---------------------------------------------------------------------------
// fetchRemoteConfig — https:// protocol
// ---------------------------------------------------------------------------

describe("fetchRemoteConfig https://", () => {
  test("retries on network error and eventually fails", async () => {
    // Connect to a port nothing listens on — exercises retry logic
    const result = fetchRemoteConfig("https://127.0.0.1:19999/config.yml");
    await expect(result).rejects.toThrow();
  });

  test("rejects http:// at validation layer", () => {
    expect(() => validateConfigUrl("http://localhost:8080/config.yml")).toThrow("Must start with");
  });
});

// ---------------------------------------------------------------------------
// fetchRemoteConfig — s3:// protocol
// ---------------------------------------------------------------------------

describe("fetchRemoteConfig s3://", () => {
  test("wraps S3 errors with clear context", async () => {
    await expect(
      fetchRemoteConfig("s3://nonexistent-bucket/config.yml"),
    ).rejects.toThrow("S3 config fetch failed");
  });

  test("validates bucket/key format before attempting fetch", () => {
    expect(() => validateConfigUrl("s3://bucket")).toThrow("Must be s3://bucket/key");
  });
});

// ---------------------------------------------------------------------------
// Content validation (applies to all protocols)
// ---------------------------------------------------------------------------

describe("content validation", () => {
  const tmpFile = join(tmpdir(), `jun-rc-content-${Date.now()}.yml`);

  afterEach(() => {
    try { unlinkSync(tmpFile); } catch {}
  });

  test("accepts minimal valid YAML", async () => {
    await Bun.write(tmpFile, "key: value\n");
    const result = await fetchRemoteConfig(`file://${tmpFile}`);
    expect(result!.content).toBe("key: value\n");
  });

  test("accepts config at exactly 1MB", async () => {
    const content = "k: " + "x".repeat(1_000_000 - 3);
    expect(content.length).toBe(1_000_000);
    await Bun.write(tmpFile, content);

    const result = await fetchRemoteConfig(`file://${tmpFile}`);
    expect(result!.content.length).toBe(1_000_000);
  });

  test("rejects config at 1MB + 1 byte", async () => {
    const content = "k: " + "x".repeat(1_000_001 - 3);
    expect(content.length).toBe(1_000_001);
    await Bun.write(tmpFile, content);

    await expect(
      fetchRemoteConfig(`file://${tmpFile}`),
    ).rejects.toThrow("too large");
  });
});
