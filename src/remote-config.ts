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

export function validateConfigUrl(url: string): void {
  if (!url.startsWith("s3://") && !url.startsWith("https://")) {
    throw new Error(`Invalid config URL: "${url}". Must start with s3:// or https://`);
  }
}

export async function fetchRemoteConfig(url: string): Promise<string> {
  validateConfigUrl(url);

  if (url.startsWith("s3://")) {
    const path = url.slice(5); // "bucket/key"
    return await Bun.s3.file(path).text();
  }

  // HTTPS
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Config fetch failed: ${response.status} ${response.statusText} from ${url}`);
  }
  return response.text();
}
