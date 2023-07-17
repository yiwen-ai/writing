FROM rust:1.71-slim-bookworm AS builder

WORKDIR /src
COPY config ./config
COPY crates ./crates
COPY src ./src
COPY Cargo.toml Cargo.lock ./

RUN apt-get update \
    && apt-get install -y pkg-config libssl-dev
RUN set -eux; \
    osArch="$(dpkg --print-architecture)"; \
    if [ "$osArch" = "aarch64" ]; then \
        export JEMALLOC_SYS_WITH_LG_PAGE=16; \
    fi \
    && cargo build --release

FROM debian:12-slim AS runtime

RUN apt-get update \
    && apt-get install -y ca-certificates tzdata curl \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /src/config ./config
COPY --from=builder /src/target/release/writing ./
ENV CONFIG_FILE_PATH=./config/config.toml

ENTRYPOINT ["./writing"]
