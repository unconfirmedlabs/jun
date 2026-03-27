#!/usr/bin/env bun
/**
 * Fetch Sui gRPC proto files from MystenLabs/sui-apis.
 * Runs automatically on `bun install` via postinstall.
 */
import { $ } from "bun";
import { existsSync } from "fs";
import path from "path";

const PROTO_DIR = path.join(import.meta.dir, "..", "proto");
const REPO = "https://github.com/MystenLabs/sui-apis.git";

if (existsSync(path.join(PROTO_DIR, "sui", "rpc", "v2", "subscription_service.proto"))) {
  console.log("[jun] proto files already present, skipping fetch");
  process.exit(0);
}

console.log("[jun] fetching proto files from MystenLabs/sui-apis...");

const tmpDir = path.join(import.meta.dir, "..", ".tmp-sui-apis");

try {
  await $`git clone --depth 1 ${REPO} ${tmpDir}`.quiet();
  await $`rm -rf ${PROTO_DIR}`.quiet();
  await $`cp -r ${tmpDir}/proto ${PROTO_DIR}`.quiet();
  console.log("[jun] proto files ready");
} finally {
  await $`rm -rf ${tmpDir}`.quiet();
}
