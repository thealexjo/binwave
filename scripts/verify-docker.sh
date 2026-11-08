#!/usr/bin/env bash
# Smoke-tests the Docker image: it runs as non-root, reports its version and validates
# the example config with secrets passed through the environment.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
image="${1:-binwave:dev}"

docker run --rm "$image" version

uid="$(docker image inspect -f '{{.Config.User}}' "$image")"
if [[ "$uid" != "65532" && "$uid" != "nonroot" && "$uid" != "65532:65532" ]]; then
	echo "image must run as non-root, got user '$uid'" >&2
	exit 1
fi

docker run --rm \
	-e BINWAVE_MYSQL_PASSWORD=x \
	-e BINWAVE_CLICKHOUSE_PASSWORD=y \
	-v "$root/deploy/config.example.yaml:/etc/binwave/config.yaml:ro" \
	"$image" check --config /etc/binwave/config.yaml

set +e
docker run --rm \
	-v "$root/deploy/config.example.yaml:/etc/binwave/config.yaml:ro" \
	"$image" check --config /etc/binwave/config.yaml >/dev/null 2>&1
code=$?
set -e
if [[ "$code" != "78" ]]; then
	echo "missing secrets must exit with 78 (EX_CONFIG), got $code" >&2
	exit 1
fi

echo "docker image OK"
