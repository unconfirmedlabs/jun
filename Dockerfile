# syntax=docker/dockerfile:1.7

# --- builder ---------------------------------------------------------------
# Pinned to a stable Rust that satisfies aws-sdk-s3's MSRV (1.91+ as of
# 2026-04). Bump in lockstep with workspace `rust-version` if it ever moves.
FROM rust:1.94-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates pkg-config libssl-dev cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/

# BuildKit cache mounts persist the cargo registry + target across builds
# without baking them into image layers.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --bin jun && \
    cp /build/target/release/jun /usr/local/bin/jun

# --- runtime ---------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/jun /usr/local/bin/jun

# No env defaults baked into the image: every run is fully driven by the
# fly machine's `--env`/secrets. See the README + each subcommand's
# `--help` for the full set. Subcommand is selected at machine spawn:
#
#   fly machine run <image> ingest --env JUN_INGEST_EPOCH=1103
#   fly machine run <image> replay --env JUN_EPOCH=1103
#   fly machine run <image> stream --env JUN_GRPC_URL=...
#
# Required for `ingest`: S3_ENDPOINT, S3_BUCKET, AWS_ACCESS_KEY_ID,
# AWS_SECRET_ACCESS_KEY, AWS_REGION, JUN_INGEST_EPOCH (or --epoch).
# Optional: CF_API_TOKEN, CF_ACCOUNT_ID, CF_D1_DATABASE_ID for D1 manifest.
ENTRYPOINT ["jun"]
