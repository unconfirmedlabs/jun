/**
 * Integration tests for jun broadcast (NATS).
 *
 * These tests connect to a live Sui gRPC fullnode and verify that
 * checkpoints flow through the NATS broadcast destination correctly.
 *
 * Requirements:
 *   - Network access to fullnode.mainnet.sui.io:443 (or override with GRPC_URL env var)
 *   - A running NATS server (set NATS_URL env var, or all tests are skipped)
 *
 * Run:
 *   NATS_URL=nats://localhost:4222 bun test tests/broadcast.integration.test.ts
 */

import { test, expect } from "bun:test";
import { createPipeline } from "../src/pipeline/pipeline.ts";
import { createGrpcLiveSource } from "../src/pipeline/sources/grpc.ts";
import { createNatsBroadcast } from "../src/pipeline/destinations/nats.ts";

const GRPC_URL = process.env.GRPC_URL ?? "fullnode.mainnet.sui.io:443";
const NATS_URL = process.env.NATS_URL;

/** Max time to wait for the first message from mainnet. Checkpoints arrive ~1-2s apart. */
const RECEIVE_TIMEOUT_MS = 30_000;
/** Total test timeout: receive window + pipeline teardown slack. */
const TEST_TIMEOUT_MS = RECEIVE_TIMEOUT_MS + 10_000;

/**
 * Races `receive` against a timeout. Rejects with a clear message if
 * no message arrives within `timeoutMs`.
 */
async function withReceiveTimeout<T>(timeoutMs: number, receive: () => Promise<T>): Promise<T> {
  return Promise.race([
    receive(),
    new Promise<never>((_, reject) =>
      setTimeout(
        () => reject(new Error(`No message received within ${timeoutMs}ms`)),
        timeoutMs,
      )
    ),
  ]);
}

/**
 * Stops the source and waits for the pipeline to drain, with a fallback
 * timeout so a stuck pipeline never blocks a test indefinitely.
 */
async function teardown(
  source: ReturnType<typeof createGrpcLiveSource>,
  pipelinePromise: Promise<void>,
): Promise<void> {
  await source.stop();
  await Promise.race([pipelinePromise, Bun.sleep(5_000)]);
}

// ─── NATS ─────────────────────────────────────────────────────────────────────

const natsTest = NATS_URL ? test : test.skip;

natsTest(
  "broadcast nats: publishes checkpoint to jun.checkpoints subject",
  async () => {
    const source = createGrpcLiveSource({ url: GRPC_URL });

    const pipeline = createPipeline()
      .source(source)
      .broadcast(createNatsBroadcast({ url: NATS_URL! }));

    const pipelinePromise = pipeline.run();

    // Give NATS connection time to establish before subscribing.
    await Bun.sleep(500);

    const received = await withReceiveTimeout(RECEIVE_TIMEOUT_MS, async () => {
      const { connect, StringCodec } = await import("nats");
      const nc = await connect({ servers: NATS_URL });
      const sc = StringCodec();

      return new Promise<Record<string, unknown>>((resolve, reject) => {
        const sub = nc.subscribe("jun.checkpoints");
        (async () => {
          for await (const msg of sub) {
            const data: Record<string, unknown> = JSON.parse(sc.decode(msg.data));
            sub.unsubscribe();
            await nc.close();
            resolve(data);
            return;
          }
        })().catch(reject);
      });
    });

    await teardown(source, pipelinePromise);

    expect(received).toMatchObject({
      sequence_number: expect.any(String),
      digest: expect.any(String),
      timestamp: expect.any(String),
      tx_count: expect.any(Number),
    });
    expect(Number(received.sequence_number)).toBeGreaterThan(0);
  },
  TEST_TIMEOUT_MS,
);

natsTest(
  "broadcast nats: publishes transactions to jun.transactions subject",
  async () => {
    const source = createGrpcLiveSource({ url: GRPC_URL });

    const pipeline = createPipeline()
      .source(source)
      .broadcast(createNatsBroadcast({ url: NATS_URL! }));

    const pipelinePromise = pipeline.run();
    await Bun.sleep(500);

    const received = await withReceiveTimeout(RECEIVE_TIMEOUT_MS, async () => {
      const { connect, StringCodec } = await import("nats");
      const nc = await connect({ servers: NATS_URL });
      const sc = StringCodec();

      return new Promise<Record<string, unknown>>((resolve, reject) => {
        const sub = nc.subscribe("jun.transactions");
        (async () => {
          for await (const msg of sub) {
            const data: Record<string, unknown> = JSON.parse(sc.decode(msg.data));
            sub.unsubscribe();
            await nc.close();
            resolve(data);
            return;
          }
        })().catch(reject);
      });
    });

    await teardown(source, pipelinePromise);

    expect(received).toMatchObject({
      digest: expect.any(String),
      sender: expect.any(String),
      success: expect.any(Boolean),
      checkpoint_seq: expect.any(String),
      timestamp: expect.any(String),
    });
  },
  TEST_TIMEOUT_MS,
);

natsTest(
  "broadcast nats: respects custom prefix",
  async () => {
    const source = createGrpcLiveSource({ url: GRPC_URL });

    const pipeline = createPipeline()
      .source(source)
      .broadcast(createNatsBroadcast({ url: NATS_URL!, prefix: "mychain" }));

    const pipelinePromise = pipeline.run();
    await Bun.sleep(500);

    const received = await withReceiveTimeout(RECEIVE_TIMEOUT_MS, async () => {
      const { connect, StringCodec } = await import("nats");
      const nc = await connect({ servers: NATS_URL });
      const sc = StringCodec();

      return new Promise<Record<string, unknown>>((resolve, reject) => {
        // Should arrive on mychain.checkpoints, NOT jun.checkpoints
        const sub = nc.subscribe("mychain.checkpoints");
        (async () => {
          for await (const msg of sub) {
            const data: Record<string, unknown> = JSON.parse(sc.decode(msg.data));
            sub.unsubscribe();
            await nc.close();
            resolve(data);
            return;
          }
        })().catch(reject);
      });
    });

    await teardown(source, pipelinePromise);

    expect(Number(received.sequence_number)).toBeGreaterThan(0);
  },
  TEST_TIMEOUT_MS,
);
