# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM amd64/rust:slim-bookworm AS builder
RUN apt-get update && apt-get install -y clang lld cmake gcc g++ libc6-dev pkg-config libssl-dev curl openssl

WORKDIR /src
COPY src ./src
COPY cmd ./cmd
COPY crates ./crates
COPY Cargo.toml Cargo.lock ./
RUN cargo build --release -p sync-to-publication-index
RUN ls target/release

FROM scratch AS exporter
WORKDIR /cmd
COPY --from=builder /src/target/release/sync-to-publication-index ./
