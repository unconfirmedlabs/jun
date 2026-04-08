// Bun worker — fetches a fixed checkpoint range with N concurrent requests.
// Spawned by prefetch.mjs. Receives config via postMessage, replies with
// { fetched, skipped } when done.
import { existsSync } from "fs";
import { join } from "path";

self.onmessage = async (event) => {
  const { from, to, cacheDir, archive, concurrency } = event.data;

  let next = BigInt(from);
  const TO = BigInt(to);
  let fetched = 0;
  let skipped = 0;

  async function fetchLoop() {
    while (true) {
      const seq = next++;
      if (seq > TO) break;
      const filename = `${seq}.binpb.zst`;
      const path = join(cacheDir, filename);
      if (existsSync(path)) { skipped++; continue; }
      try {
        const res = await fetch(`${archive}/${filename}`);
        if (res.ok) {
          await Bun.write(path, await res.arrayBuffer());
          fetched++;
        }
      } catch {}
    }
  }

  // Send intermediate progress every 10K fetches so main thread can display rate
  const progressInterval = setInterval(() => {
    postMessage({ progress: true, fetched, skipped });
  }, 2000);

  await Promise.all(Array.from({ length: concurrency }, fetchLoop));
  clearInterval(progressInterval);
  postMessage({ done: true, fetched, skipped });
};
