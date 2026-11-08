#!/usr/bin/env bash
# Validates deploy/systemd units with systemd-analyze.
# The template is instantiated and ExecStart is pointed at the freshly built binary,
# since the install path does not exist on build machines.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
binary="${1:-$root/bin/binwave}"

if [[ ! -x "$binary" ]]; then
	echo "binary $binary not found; run 'make build' first" >&2
	exit 1
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

sed "s|/usr/local/bin/binwave|$binary|" "$root/deploy/systemd/binwave@.service" >"$tmp/binwave@example.service"
sed "s|/usr/local/bin/binwave|$binary|" "$root/deploy/systemd/binwave.service" >"$tmp/binwave.service"

systemd-analyze verify "$tmp/binwave@example.service"
systemd-analyze verify "$tmp/binwave.service"
echo "systemd unit OK"
