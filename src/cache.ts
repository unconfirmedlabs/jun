/**
 * Local checkpoint cache at ~/.jun/cache/checkpoints/
 *
 * Caches compressed .binpb.zst files from the archive.
 * LRU eviction when cache exceeds configured max size.
 */
import { existsSync, mkdirSync, readdirSync, statSync, unlinkSync } from "fs";
import { join } from "path";
import { loadConfig } from "./config.ts";

function getCacheDir(): string {
  const home = process.env.HOME || process.env.USERPROFILE;
  if (!home) throw new Error("Cannot determine home directory");
  return join(home, ".jun", "cache", "checkpoints");
}

function ensureCacheDir(): string {
  const dir = getCacheDir();
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  return dir;
}

function cachePath(seq: bigint): string {
  return join(getCacheDir(), `${seq}.binpb.zst`);
}

/** Get cached compressed bytes, or null if not cached. */
export async function cacheGet(seq: bigint): Promise<Uint8Array | null> {
  const path = cachePath(seq);
  const file = Bun.file(path);
  if (!(await file.exists())) return null;
  return new Uint8Array(await file.arrayBuffer());
}

/** Write compressed bytes to cache, then evict if over limit. */
export async function cachePut(seq: bigint, compressed: Uint8Array): Promise<void> {
  const dir = ensureCacheDir();
  await Bun.write(join(dir, `${seq}.binpb.zst`), compressed);
  evictIfNeeded();
}

/** Evict oldest files if cache exceeds max size. */
function evictIfNeeded(): void {
  const config = loadConfig();
  const maxBytes = config.cacheMaxMb * 1_000_000;
  if (maxBytes <= 0) return;

  const dir = getCacheDir();
  if (!existsSync(dir)) return;

  // Get all cached files with stats
  const files = readdirSync(dir)
    .filter(f => f.endsWith(".binpb.zst"))
    .map(f => {
      const fullPath = join(dir, f);
      const stat = statSync(fullPath);
      return { path: fullPath, size: stat.size, mtimeMs: stat.mtimeMs };
    });

  let totalSize = files.reduce((sum, f) => sum + f.size, 0);
  if (totalSize <= maxBytes) return;

  // Sort by access time (oldest first) and evict
  files.sort((a, b) => a.mtimeMs - b.mtimeMs);
  for (const file of files) {
    if (totalSize <= maxBytes) break;
    unlinkSync(file.path);
    totalSize -= file.size;
  }
}

/** Get cache stats. */
export function cacheStats(): { files: number; sizeBytes: number; dir: string } {
  const dir = getCacheDir();
  if (!existsSync(dir)) return { files: 0, sizeBytes: 0, dir };

  const entries = readdirSync(dir).filter(f => f.endsWith(".binpb.zst"));
  let sizeBytes = 0;
  for (const f of entries) {
    sizeBytes += statSync(join(dir, f)).size;
  }
  return { files: entries.length, sizeBytes, dir };
}

/** Clear all cached checkpoints. */
export function cacheClear(): number {
  const dir = getCacheDir();
  if (!existsSync(dir)) return 0;

  const entries = readdirSync(dir).filter(f => f.endsWith(".binpb.zst"));
  for (const f of entries) unlinkSync(join(dir, f));
  return entries.length;
}
