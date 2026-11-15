#!/usr/bin/env bash
# Smoke-tests the Docker image: it runs as non-root, reports its version, validates
# the example config with secrets passed through the environment, and starts on a new volume.
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

# binwave run creates its control socket in /run/binwave before it connects anywhere, so it must be
# writable by the user of the image in a read-only container, as the state volume must. No network:
# nothing is reached.
volume="binwave-verify-$$"
container=""
cleanup() {
	if [[ -n "$container" ]]; then docker rm -f "$container" >/dev/null 2>&1 || true; fi
	docker volume rm -f "$volume" >/dev/null 2>&1 || true
}
trap cleanup EXIT
container="$(docker run -d --network none --read-only \
	-e BINWAVE_MYSQL_PASSWORD=x \
	-e BINWAVE_CLICKHOUSE_PASSWORD=y \
	-v "$root/deploy/config.example.yaml:/etc/binwave/config.yaml:ro" \
	-v "$volume:/var/lib/binwave" \
	"$image" run)"
# served reads the whole log before it searches it: under pipefail, docker logs | grep -q can fail on SIGPIPE.
served() { local logs; logs="$(docker logs "$container" 2>&1)"; grep -q 'control channel is served' <<<"$logs"; }
for _ in $(seq 1 30); do
	if served; then
		break
	fi
	if [[ "$(docker inspect -f '{{.State.Running}}' "$container")" != "true" ]]; then
		break
	fi
	sleep 1
done
if ! served; then
	echo "binwave run did not serve its control socket on a new volume:" >&2
	docker logs "$container" >&2
	exit 1
fi

# The commands reach the service on the default socket, as with docker exec on a host.
if ! status="$(docker exec "$container" /usr/local/bin/binwave replication status 2>&1)" || ! grep -q '^state:' <<<"$status"; then
	echo "binwave replication status did not reach the service in the container:" >&2
	echo "$status" >&2
	exit 1
fi

echo "docker image OK"
