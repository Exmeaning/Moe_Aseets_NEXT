# syntax=docker/dockerfile:1.7
#
# Build & runtime image for moe-assets-gateway.
#
# Notes
#   - CGO_ENABLED=1: the SQLite driver is mattn/go-sqlite3 (C SQLite), several
#     times faster than the pure-Go driver on scan-heavy queries. The binary
#     is statically linked against musl so the distroless/static runtime image
#     still works without a libc.
#   - cgo cannot cross-compile without a per-target C toolchain, so the build
#     stage runs natively on each platform (BuildKit emulates foreign arches)
#     instead of the --platform=$BUILDPLATFORM trick.
#   - BuildKit cache mounts (`--mount=type=cache`) speed up rebuilds; they
#     require BuildKit which is the default on Docker >= 23.
#   - The final image runs as `nonroot` (UID/GID 65532) from distroless. The
#     /data volume is chowned by the volume driver / operator, not by the
#     image (distroless has no shell).
#
# Build:   docker build -t moe-assets-gateway:local .
# Run:     docker run --rm -p 8080:8080 -p 7420:7420 \
#              -e HARUKI_GW_HIP_BEARER_TOKEN=change-me \
#              -e HARUKI_GW_SEAWEED_FILER=http://host.docker.internal:8888 \
#              -v moe-gw-data:/data \
#              moe-assets-gateway:local

# --------------------------------------------------------------------
# Stage 1: build
# --------------------------------------------------------------------
FROM golang:1.25-alpine AS build

# C toolchain for mattn/go-sqlite3.
RUN apk add --no-cache build-base

# Optional version stamp; pass with --build-arg VERSION=$(git rev-parse --short HEAD).
ARG VERSION=dev

WORKDIR /src

# Fetch deps first so subsequent code edits reuse the module cache.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

# Copy the rest of the source.
COPY . .

# Build a statically linked binary (musl): distroless/static has no libc.
# `sqlite_omit_load_extension` drops the dlopen dependency static linking
# cannot satisfy; `-trimpath` gives reproducible paths; `-ldflags "-s -w"`
# strips the DWARF/symbol tables (~30% smaller).
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 \
    go build \
        -trimpath \
        -tags "osusergo,netgo,sqlite_omit_load_extension" \
        -ldflags '-s -w -extldflags "-static" -X main.version='"${VERSION}" \
        -o /out/gateway \
        ./cmd/gateway

# --------------------------------------------------------------------
# Stage 2: runtime
# --------------------------------------------------------------------
FROM gcr.io/distroless/static:nonroot AS runtime

# OCI labels for provenance / registry metadata.
LABEL org.opencontainers.image.title="moe-assets-gateway" \
      org.opencontainers.image.description="HIP/1 ingest + SeaweedFS reverse proxy for Project Sekai static assets" \
      org.opencontainers.image.source="https://github.com/Team-Haruki/moe-assets-gateway" \
      org.opencontainers.image.licenses="MIT"

WORKDIR /
COPY --from=build /out/gateway /gateway

# Ports:
#   8080  public HTTP read path (behind Zeabur ingress).
#   7420  HIP ingest; bind 127.0.0.1 in production if the client is in the
#         same pod, otherwise front it with TLS.
EXPOSE 8080
EXPOSE 7420

# SQLite lives here. Mount a persistent volume in production.
VOLUME ["/data"]

# distroless:nonroot maps to UID 65532.
USER nonroot:nonroot

# No shell in distroless — we invoke the binary directly.
ENTRYPOINT ["/gateway"]
