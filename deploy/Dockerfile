# Thin re-export so `docker compose -f deploy/docker-compose.yml build` still
# works. The canonical Dockerfile lives at the repository root; see it for
# comments and build-arg documentation.
#
# When docker-compose sets `context: ..` the effective build context is the
# repo root, so this file behaves identically to /Dockerfile.

# syntax=docker/dockerfile:1.7
FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/gateway ./cmd/gateway

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=build /out/gateway /gateway
EXPOSE 8080
EXPOSE 7420
VOLUME ["/data"]
USER nonroot:nonroot
ENTRYPOINT ["/gateway"]
