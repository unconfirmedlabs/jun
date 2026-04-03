/**
 * jun/remote-config — Fetch indexer config from S3, HTTPS, or local file.
 *
 * Supported protocols:
 *   - file:///path/to/config.yml  — read from local filesystem
 *   - s3://bucket/key             — read from S3 (R2, MinIO, AWS)
 *   - https://host/path           — read from HTTPS endpoint
 *
 * Conditional fetching: pass the `etag` from a previous fetch to skip
 * re-downloading unchanged configs. Uses S3 HEAD (stat), file mtime,
 * or HTTP If-None-Match.
 *
 * S3 credentials read from standard AWS env vars:
 * AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_URL (for R2/MinIO).
 *
 * @example
 * ```ts
 * // First fetch
 * const result = await fetchRemoteConfig("s3://bucket/config.yml");
 * // result = { content: "...", etag: "abc123" }
 *
 * // Subsequent fetch — skips download if unchanged
 * const next = await fetchRemoteConfig("s3://bucket/config.yml", { etag: result.etag });
 * // next = null (unchanged) or { content: "...", etag: "def456" }
 * ```
 */

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Max config size: 1 MB. Prevents OOM from misconfigured URLs. */
const MAX_CONFIG_BYTES = 1_000_000;

/** HTTPS fetch timeout: 30 seconds. */
const FETCH_TIMEOUT_MS = 30_000;

/** Max retries for transient HTTPS errors (5xx, network failures). */
const MAX_RETRIES = 3;

/** Base delay between retries (doubles each attempt). */
const RETRY_BASE_MS = 500;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface FetchResult {
  /** The config file content. */
  content: string;
  /** Opaque tag for conditional fetching. Pass back to skip re-download if unchanged. */
  etag: string;
}

export interface FetchOptions {
  /** ETag from a previous fetch. If the remote hasn't changed, returns null. */
  etag?: string;
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

export function validateConfigUrl(url: string): void {
  if (!url.startsWith("s3://") && !url.startsWith("https://") && !url.startsWith("file://")) {
    throw new Error(`Invalid config URL: "${url}". Must start with s3://, https://, or file://`);
  }

  if (url.startsWith("file://")) {
    const path = url.slice(7);
    if (!path.startsWith("/")) {
      throw new Error(`Invalid file:// URL: "${url}". Path must be absolute (start with /)`);
    }
  }

  if (url.startsWith("s3://")) {
    const path = url.slice(5);
    const slashIdx = path.indexOf("/");
    if (slashIdx < 1 || slashIdx === path.length - 1) {
      throw new Error(`Invalid s3:// URL: "${url}". Must be s3://bucket/key`);
    }
  }
}

/** Validate that fetched content is a non-empty string. */
function validateContent(content: string, url: string): void {
  if (!content || content.trim().length === 0) {
    throw new Error(`Config is empty: ${url}`);
  }
  if (content.length > MAX_CONFIG_BYTES) {
    throw new Error(`Config too large (${content.length} bytes, max ${MAX_CONFIG_BYTES}): ${url}`);
  }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Fetch config from a remote URL.
 *
 * Returns `null` if `options.etag` is provided and the remote content hasn't changed.
 * Otherwise returns `{ content, etag }`.
 */
export async function fetchRemoteConfig(url: string, options?: FetchOptions): Promise<FetchResult | null> {
  validateConfigUrl(url);

  let result: FetchResult | null;

  if (url.startsWith("file://")) {
    result = await fetchFile(url, options?.etag);
  } else if (url.startsWith("s3://")) {
    result = await fetchS3(url, options?.etag);
  } else {
    result = await fetchHttps(url, options?.etag);
  }

  if (result) {
    validateContent(result.content, url);
  }

  return result;
}

// ---------------------------------------------------------------------------
// file:// — uses mtime for conditional check
// ---------------------------------------------------------------------------

async function fetchFile(url: string, previousEtag?: string): Promise<FetchResult | null> {
  const filePath = url.slice(7);
  const file = Bun.file(filePath);

  if (!await file.exists()) {
    throw new Error(`Config file not found: ${filePath}`);
  }

  const size = file.size;
  if (size > MAX_CONFIG_BYTES) {
    throw new Error(`Config file too large (${size} bytes, max ${MAX_CONFIG_BYTES}): ${filePath}`);
  }

  // Use mtime as etag — skip read if unchanged
  const etag = `file:${file.lastModified}`;
  if (previousEtag && etag === previousEtag) {
    return null;
  }

  const content = await file.text();
  return { content, etag };
}

// ---------------------------------------------------------------------------
// s3:// — uses HEAD (stat) for conditional check
// ---------------------------------------------------------------------------

async function fetchS3(url: string, previousEtag?: string): Promise<FetchResult | null> {
  const path = url.slice(5); // "bucket/key"

  try {
    // HEAD request — returns etag without downloading body
    const stat = await Bun.s3.file(path).stat();

    if (previousEtag && stat.etag === previousEtag) {
      return null; // unchanged
    }

    if (stat.size > MAX_CONFIG_BYTES) {
      throw new Error(`Config too large (${stat.size} bytes, max ${MAX_CONFIG_BYTES}): ${url}`);
    }

    const content = await Bun.s3.file(path).text();
    return { content, etag: stat.etag };
  } catch (err: any) {
    // Re-throw our own errors
    if (err?.message?.includes("Config too large") || err?.message?.includes("Config is empty")) {
      throw err;
    }
    const msg = err?.message ?? String(err);
    throw new Error(`S3 config fetch failed for ${url}: ${msg}`);
  }
}

// ---------------------------------------------------------------------------
// https:// — uses If-None-Match for conditional check, retries on 5xx
// ---------------------------------------------------------------------------

async function fetchHttps(url: string, previousEtag?: string): Promise<FetchResult | null> {
  let lastError: Error | null = null;

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    if (attempt > 0) {
      const delay = RETRY_BASE_MS * Math.pow(2, attempt - 1);
      await new Promise(resolve => setTimeout(resolve, delay));
    }

    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

      const headers: Record<string, string> = {};
      if (previousEtag) {
        headers["If-None-Match"] = previousEtag;
      }

      try {
        const response = await fetch(url, { signal: controller.signal, headers });

        // 304 Not Modified — content unchanged
        if (response.status === 304) {
          return null;
        }

        if (response.ok) {
          // Check Content-Length before reading body
          const contentLength = response.headers.get("content-length");
          if (contentLength && parseInt(contentLength, 10) > MAX_CONFIG_BYTES) {
            throw new Error(`Config too large (${contentLength} bytes, max ${MAX_CONFIG_BYTES}): ${url}`);
          }

          const content = await response.text();
          const etag = response.headers.get("etag") ?? `https:${Bun.hash(content)}`;
          return { content, etag };
        }

        // Retry on 5xx (server errors) and 429 (rate limit)
        if (response.status >= 500 || response.status === 429) {
          lastError = new Error(`Config fetch failed: ${response.status} ${response.statusText} from ${url}`);
          continue;
        }

        // 4xx errors are not retryable
        throw new Error(`Config fetch failed: ${response.status} ${response.statusText} from ${url}`);
      } finally {
        clearTimeout(timeout);
      }
    } catch (err: any) {
      if (err?.name === "AbortError") {
        lastError = new Error(`Config fetch timed out after ${FETCH_TIMEOUT_MS}ms: ${url}`);
        continue;
      }

      // Network errors are retryable
      if (err?.message?.includes("fetch failed") || err?.code === "ECONNREFUSED" || err?.code === "ENOTFOUND") {
        lastError = new Error(`Config fetch network error: ${err.message} from ${url}`);
        continue;
      }

      // Non-retryable errors (like our own size/status errors)
      throw err;
    }
  }

  throw lastError ?? new Error(`Config fetch failed after ${MAX_RETRIES} retries: ${url}`);
}
