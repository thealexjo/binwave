#!/usr/bin/env bash
set -euo pipefail

[[ ${DOCKER_DEBUG:-} == "true" ]] && set -x

# A shell is started as is, without the init scripts.
if [[ ${1:-} =~ ^(bash|sh)$ ]]; then
  exec "${@}"
fi

# Arguments that are not a command are those of binwave: `docker run <image> tables ...`.
if [[ $# -eq 0 || ${1} == -* ]] || ! command -v "${1}" >/dev/null 2>&1; then
  set -- binwave "${@}"
fi

###
### Globals
###

# Path to scripts to source
CONFIG_DIR="/docker-entrypoint.d"

init="$( find "${CONFIG_DIR}" -name '*.sh' -type f | sort -u )"
for f in ${init}; do
 # shellcheck disable=SC1090
 echo "[Entrypoint] running $f"
 . "${f}"
done


echo
echo '[Entrypoint] Init process done. Ready for the start-up.'
echo


# exec: binwave replaces the shell as PID 1 and receives SIGTERM from docker stop itself.
exec "${@}"
