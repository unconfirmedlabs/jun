# syntax=docker/dockerfile:1.7

# --- builder ---------------------------------------------------------------
FROM rust:1.89-slim-bookworm AS builder

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

# Runtime tuning + dispatch flow entirely through env vars on the fly
# machine. Required:
#   JUN_FROM, JUN_TO                         # checkpoint range
#   JUN_CLICKHOUSE_URL                       # http://host:8123 or https
#   JUN_CLICKHOUSE_DATABASE                  # default: jun
# Optional:
#   JUN_CLICKHOUSE_USERNAME / _PASSWORD
#   JUN_ARCHIVE_PROXY / JUN_ARCHIVE_URL
#   JUN_CHUNK_CONCURRENCY  (default 8)
#   JUN_DECODE_CONCURRENCY (default 4)
#   JUN_BATCH_SIZE         (default 500)
#   JUN_WRITE_CONCURRENCY  (default 4)
#   JUN_MODE               (full | no-write | fetch-only)
#   RUST_LOG               (default info)
#
# Run `jun init` once separately on a single machine to create the
# database + tables before fanning out workers.
#
# `fly machine run <image> --env JUN_FROM=N --env JUN_TO=M ...` runs one
# replay window; the machine exits when the replay completes.
ENV RUST_LOG=info

# Default to `jun replay` (env-driven worker mode). Override at machine
# launch to run other subcommands:
#   fly machine run <image>                         # → jun replay  (env-driven)
#   fly machine run <image> init --clickhouse ...   # → jun init
ENTRYPOINT ["jun"]
CMD ["replay"]
