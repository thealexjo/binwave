#!/usr/bin/env bash
# Smoke-tests the Docker image: it runs as non-root, reports its version, validates
# the example config with secrets passed through the environment, starts on a new volume, and refuses
# a state volume it cannot write to.
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

# binwave run creates its control socket in /run/binwave and checks that it can save its position in
# the state volume before it connects anywhere, so both must be writable by the user of the image in a
# read-only container, on a new volume. The example config with the file store; no network: nothing is
# reached, and the service waits for the databases.
volume="binwave-verify-$$"
work="$(mktemp -d)"
container=""
cleanup() {
	if [[ -n "$container" ]]; then docker rm -f "$container" >/dev/null 2>&1 || true; fi
	docker volume rm -f "$volume" >/dev/null 2>&1 || true
	rm -rf "$work"
}
trap cleanup EXIT
sed 's|^  # store: clickhouse .*|  store: file|' "$root/deploy/config.example.yaml" >"$work/config.yaml"
chmod 0644 "$work/config.yaml"
if ! grep -q '^  store: file$' "$work/config.yaml"; then
	echo "the example config has no checkpoint.store line to set" >&2
	exit 1
fi
run() {
	docker run "$@" --network none --read-only \
		-e BINWAVE_MYSQL_PASSWORD=x \
		-e BINWAVE_CLICKHOUSE_PASSWORD=y \
		-v "$work/config.yaml:/etc/binwave/config.yaml:ro" \
		"$image" run
}
container="$(run -d -v "$volume:/var/lib/binwave")"
# waiting reads the whole log before it searches it: under pipefail, docker logs | grep -q can fail on SIGPIPE.
waiting() { local logs; logs="$(docker logs "$container" 2>&1)"; grep -q 'control channel is served' <<<"$logs" && grep -q 'cannot start yet, retrying' <<<"$logs"; }
for _ in $(seq 1 30); do
	if waiting; then
		break
	fi
	if [[ "$(docker inspect -f '{{.State.Running}}' "$container")" != "true" ]]; then
		break
	fi
	sleep 1
done
if ! waiting; then
	echo "binwave run did not serve its control socket and wait for the databases on a new volume:" >&2
	docker logs "$container" >&2
	exit 1
fi

# The commands reach the service on the default socket, as with docker exec on a host.
if ! status="$(docker exec "$container" /usr/local/bin/binwave replication status 2>&1)" || ! grep -q '^state:' <<<"$status"; then
	echo "binwave replication status did not reach the service in the container:" >&2
	echo "$status" >&2
	exit 1
fi

# A state volume the service cannot write to stops it at once with 78 (EX_CONFIG), not at the first save.
set +e
logs="$(run --rm -v "$volume:/var/lib/binwave:ro" 2>&1)"
code=$?
set -e
if [[ "$code" != "78" ]] || ! grep -q 'checkpoint.file_path' <<<"$logs"; then
	echo "a read-only state volume must exit with 78 naming checkpoint.file_path, got $code:" >&2
	echo "$logs" >&2
	exit 1
fi

echo "docker image OK"
