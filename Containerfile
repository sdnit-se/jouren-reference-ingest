# Cargo caches survive source edits and feature changes. Scenario C enables
# mixed-case-ids; normal releases leave FEATURES empty.
FROM docker.io/library/rust:1.98-bookworm AS builder
WORKDIR /build
COPY rust-toolchain.toml ./
RUN rustup show active-toolchain
COPY Cargo.toml Cargo.lock ./
ARG PROFILE=release
ARG FEATURES=""
COPY src ./src
COPY migrations ./migrations
RUN --mount=type=cache,id=ingest-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=ingest-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=ingest-target-${PROFILE},target=/build/target,sharing=locked \
    cargo build --locked --profile "${PROFILE}" --features "${FEATURES}" \
 && cp "target/${PROFILE}/telemetry-ingest" /telemetry-ingest

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /telemetry-ingest /telemetry-ingest
EXPOSE 8080
ENTRYPOINT ["/telemetry-ingest"]
