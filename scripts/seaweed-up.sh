#!/usr/bin/env bash
# Convenience wrapper to launch a single-node SeaweedFS for local development.
# Requires Docker.
set -euo pipefail
docker run --rm -p 8888:8888 -p 9333:9333 \
  -v "${PWD}/.seaweed-data:/data" \
  chrislusf/seaweedfs:latest \
  server -filer -dir=/data
