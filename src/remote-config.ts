/**
 * jun/remote-config — Fetch indexer config from S3 or HTTPS URL.
 *
 * S3 credentials read from standard AWS env vars:
 * AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_URL (for R2/MinIO).
 *
 * @example
 * ```bash
 * jun run --config-url s3://my-bucket/configs/indexer.yml
 * ```
 */

export async function fetchRemoteConfig(url: string): Promise<string> {
  if (url.startsWith("s3://")) {
    const path = url.slice(5); // "bucket/key"
    return await Bun.s3.file(path).text();
  }

  // HTTPS fallback
  const resp = await fetch(url);
  if (!resp.ok) {
    throw new Error(`Config fetch failed: ${resp.status} ${resp.statusText} from ${url}`);
  }
  return resp.text();
}
