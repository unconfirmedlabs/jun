/**
 * Protobuf timestamp parsing -- shared conversion from gRPC Timestamp to JS Date.
 */

export type ProtoTimestamp = { seconds: string; nanos: number } | null | undefined;

/** Parse a protobuf Timestamp to a JS Date. Returns epoch 0 if null/undefined. */
export function parseTimestamp(ts: ProtoTimestamp): Date {
  if (!ts) return new Date(0);
  const ms = BigInt(ts.seconds) * 1000n + BigInt(Math.floor(ts.nanos / 1_000_000));
  return new Date(Number(ms));
}

/** Parse a protobuf Timestamp to milliseconds since epoch. Returns 0 if null/undefined. */
export function parseTimestampMs(ts: ProtoTimestamp): number {
  if (!ts) return 0;
  return Number(BigInt(ts.seconds) * 1000n + BigInt(Math.floor(ts.nanos / 1_000_000)));
}

/** Parse a protobuf Timestamp to an ISO string. Returns null if null/undefined. */
export function parseTimestampISO(ts: ProtoTimestamp): string | null {
  if (!ts) return null;
  const ms = Number(BigInt(ts.seconds) * 1000n + BigInt(Math.floor(ts.nanos / 1_000_000)));
  return new Date(ms).toISOString();
}
