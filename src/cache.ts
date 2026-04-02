/**
 * Local checkpoint cache at ~/.jun/cache/checkpoints/
 *
 * Caches compressed .binpb.zst files from the archive.
 * LRU eviction when cache exceeds configured max size.
 *
 * Cache size is tracked in memory to avoid O(n) filesystem scans on every write.
 * Only scans the directory once on first access to initialize the in-memory index.
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

// ---------------------------------------------------------------------------
// In-memory cache index — avoids O(n) filesystem scans on every write
// ---------------------------------------------------------------------------

interface CacheEntry {
  seq: bigint;
  size: number;
  mtimeMs: number;
}

let cacheIndex: Map<bigint, CacheEntry> | null = null;
let cacheTotalBytes = 0;
let cacheMaxBytes = 0;

/** Initialize in-memory index by scanning cache directory once. */
function ensureIndex(): Map<bigint, CacheEntry> {
  if (cacheIndex) return cacheIndex;

  const config = loadConfig();
  cacheMaxBytes = config.cacheMaxMb * 1_000_000;
  cacheIndex = new Map();
  cacheTotalBytes = 0;

  const dir = getCacheDir();
  if (!existsSync(dir)) return cacheIndex;

  for (const f of readdirSync(dir)) {
    if (!f.endsWith(".binpb.zst")) continue;
    const seqStr = f.replace(".binpb.zst", "");
    const seq = BigInt(seqStr);
    try {
      const stat = statSync(join(dir, f));
      cacheIndex.set(seq, { seq, size: stat.size, mtimeMs: stat.mtimeMs });
      cacheTotalBytes += stat.size;
    } catch {}
  }

  return cacheIndex;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/** Get cached compressed bytes, or null if not cached. */
export async function cacheGet(seq: bigint): Promise<Uint8Array | null> {
  const index = ensureIndex();
  if (!index.has(seq)) return null;

  try {
    return new Uint8Array(await Bun.file(cachePath(seq)).arrayBuffer());
  } catch {
    // File was deleted externally — remove from index
    const entry = index.get(seq);
    if (entry) {
      cacheTotalBytes -= entry.size;
      index.delete(seq);
    }
    return null;
  }
}

/** Write compressed bytes to cache, then evict if over limit. */
export async function cachePut(seq: bigint, compressed: Uint8Array): Promise<void> {
  const index = ensureIndex();
  const dir = ensureCacheDir();

  // Skip if already cached
  if (index.has(seq)) return;

  await Bun.write(join(dir, `${seq}.binpb.zst`), compressed);

  const entry: CacheEntry = { seq, size: compressed.length, mtimeMs: Date.now() };
  index.set(seq, entry);
  cacheTotalBytes += entry.size;

  // Evict if over limit (O(n) sort only when needed, not on every write)
  if (cacheMaxBytes > 0 && cacheTotalBytes > cacheMaxBytes) {
    evictToTarget();
  }
}

/** Evict oldest entries until cache is under 80% of max (avoids thrashing). */
function evictToTarget(): void {
  const index = ensureIndex();
  const target = cacheMaxBytes * 0.8; // Evict to 80% to avoid immediate re-eviction

  // Sort by mtime (oldest first) — only happens when over limit
  const entries = [...index.values()].sort((a, b) => a.mtimeMs - b.mtimeMs);

  for (const entry of entries) {
    if (cacheTotalBytes <= target) break;
    try {
      unlinkSync(cachePath(entry.seq));
    } catch {}
    cacheTotalBytes -= entry.size;
    index.delete(entry.seq);
  }
}

/** Get cache stats. */
export function cacheStats(): { files: number; sizeBytes: number; dir: string } {
  const index = ensureIndex();
  return { files: index.size, sizeBytes: cacheTotalBytes, dir: getCacheDir() };
}

/** Clear all cached checkpoints. */
export function cacheClear(): number {
  const dir = getCacheDir();
  if (!existsSync(dir)) return 0;

  const entries = readdirSync(dir).filter(f => f.endsWith(".binpb.zst"));
  for (const f of entries) unlinkSync(join(dir, f));

  // Reset in-memory index
  if (cacheIndex) {
    cacheIndex.clear();
    cacheTotalBytes = 0;
  }

  return entries.length;
}
