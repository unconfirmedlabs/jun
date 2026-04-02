/**
 * Shared pino logger for jun.
 *
 * Log level controlled via LOG_LEVEL env var (default: "info").
 * Pipe through pino-pretty for human-readable output in development:
 *   LOG_LEVEL=debug bun run examples/sona.ts | bunx pino-pretty
 */
import pino from "pino";

export type Logger = pino.Logger;

export function createLogger(name = "jun"): Logger {
  return pino({
    name,
    level: process.env.LOG_LEVEL ?? "info",
  }, pino.destination(2)); // fd 2 = stderr
}
