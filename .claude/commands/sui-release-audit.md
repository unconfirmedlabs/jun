# Sui Release Audit

Given a Sui GitHub PR URL or release tag, determine whether any changes require updates to jun's Rust checkpoint decoder, pipeline, or BCS schemas.

## Usage

```
/sui-release-audit https://github.com/MystenLabs/sui/pull/XXXXX
/sui-release-audit v1.40.0
```

## Step 1: Fetch the PR or release

For a PR URL — fetch it directly and extract:
- Title and description
- All changed file paths
- Diff content for any relevant files (see criteria below)
- Whether the "Protocol" release notes checkbox is checked

For a release tag — fetch `https://github.com/MystenLabs/sui/releases/tag/<tag>` and look for protocol/format changes in the release notes, then follow any linked PRs.

## Step 2: Check against impact criteria

### HIGH IMPACT — requires decoder/pipeline changes

These file paths or crates indicate a breaking or additive change we must handle:

| Path pattern | What it affects |
|---|---|
| `crates/sui-storage/src/blob.rs` | Checkpoint blob encoding/framing |
| `crates/sui-archival/` | Archive file format or fetch protocol |
| `crates/sui-rpc-api/proto/` or any `.proto` file | gRPC streaming schema |
| `crates/sui-sdk-types/src/` | Core Sui types (BCS-encoded) |
| `crates/sui-types/src/effects/` | `TransactionEffects` BCS layout |
| `crates/sui-types/src/transaction.rs` | `TransactionData` BCS layout |
| `crates/sui-types/src/messages_checkpoint.rs` | `CheckpointContents` / `CheckpointSummary` |
| `crates/sui-types/src/event.rs` | `Event` BCS layout |
| `crates/sui-types/src/balance_change.rs` | `BalanceChange` extraction logic |
| `crates/sui-types/src/object.rs` | Object ownership/type encoding |
| `crates/sui-types/src/base_types.rs` | Core primitive types |
| Any file adding a new enum variant to BCS-serialized types | Ordinal shift risk |

Also flag if the PR description mentions:
- "protocol upgrade", "breaking change", "new epoch", "new effects version"
- "CheckpointContents v2", "TransactionEffects v3", etc.

### LOW IMPACT — no decoder changes needed

Changes in these areas do not affect checkpoint decoding:

- `crates/sui-kv-rpc/` or `crates/sui-kvstore/` — BigTable read path, not archive
- `crates/sui-graphql-rpc/` — GraphQL layer
- `crates/sui-json-rpc/` — deprecated JSON-RPC
- `crates/sui-indexer/` — downstream indexer, not source format
- `crates/sui-sdk/` — TypeScript/client SDK
- `crates/move-*/` — Move VM (only matters if event BCS layout changes)
- `crates/sui-benchmark/`, `crates/sui-test-*` — testing infra
- `apps/`, `sdk/`, `dapps/` — frontend/dapp code
- Consensus, validator, narwhal internals
- Metrics, logging, CLI tooling

## Step 3: For HIGH IMPACT files — read the diff carefully

For each high-impact file changed, determine:

1. **New enum variant added?**
   - BCS encodes enums by ordinal. New variants appended at the end are safe.
   - Variants inserted in the middle break existing decodings. Flag this.

2. **New field added to a struct?**
   - BCS structs are positional. New fields added at the end are safe if optional.
   - Fields inserted in the middle, or required fields added anywhere, break decoding.

3. **Field removed or reordered?**
   - Always breaking. Flag immediately.

4. **New transaction kind / effects version?**
   - Jun's decoder must handle all variants. Check if our Rust decoder's match arms cover the new variant.
   - File: `native/rust-decoder/src/` — look for exhaustive match on `TransactionKind`, `TransactionEffects`, etc.

5. **Proto field added?**
   - Proto3 additions are backward compatible as long as field numbers aren't reused. Usually safe.
   - Flag if field numbers are reassigned or message types change.

## Step 4: Check our decoder coverage

If a high-impact change is found, look at our Rust decoder source:

```
native/rust-decoder/src/
```

Check whether the new type variant or field is:
- Already handled (safe)
- Missing from a match arm (will panic or silently skip)
- A struct field we extract (needs a new extraction line)

## Step 5: Output a verdict

Always end with one of these:

---

### VERDICT: NO ACTION REQUIRED
**Why:** [one sentence — what changed and why it doesn't touch our path]

---

### VERDICT: MONITOR
**Why:** [change is additive/proto-compatible but touches relevant types]
**Watch for:** [what to look for in follow-up PRs]

---

### VERDICT: ACTION REQUIRED
**Impact:** [exactly what breaks]
**Files to update:**
- `native/rust-decoder/src/...` — [what to change]
- `src/pipeline/...` — [if pipeline types need updating]
**Priority:** high / medium (can wait until next epoch)

---

## Context: what our decoder does

Jun's Rust decoder (`native/rust-decoder/`) reads `.binpb.zst` checkpoint files from the Sui archive:

1. Decompresses zstd
2. Parses protobuf outer envelope (`CheckpointData` proto)
3. BCS-decodes inner payloads: `TransactionEffectsV1/V2`, `TransactionEvents`, balance changes
4. Extracts: transactions, move calls, balance changes, object changes, inputs, commands, system transactions, unchanged consensus objects, events, checkpoint summaries
5. Returns a binary format consumed by Bun workers

**BCS field order must match Move struct definitions exactly** — any reordering is a silent decode error (no runtime panic, wrong data).
