#!/usr/bin/env bash
# Open the interactive AtalantaDB shell inside the master container.

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

fail() {
    echo "ERROR: $*" >&2
    exit 1
}

[[ -t 0 && -t 1 ]] || fail "The AtalantaDB shell requires an interactive terminal."
# shellcheck source=compose-common.sh
source "$script_dir/compose-common.sh"
ce_require_compose
container=$(ce_compose ps -q --status running atalanta-master) || fail "Cannot read the CE master status."
[[ -n "$container" ]] \
    || fail "atalanta-master is not running. Start AtalantaDB with ./1-start.sh first."

exec env "${ce_env_args[@]}" docker "${ce_compose_args[@]}" exec atalanta-master \
    bash -c 'cd /opt/atalanta/shell && exec ./atalanta-shell.sh'
