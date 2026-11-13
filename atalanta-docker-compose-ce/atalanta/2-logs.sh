#!/usr/bin/env bash
# Show logs from this CE deployment only; pass Docker Compose logs options.
set -Eeuo pipefail
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
export ATALANTA_CE_FULL=1
# shellcheck source=compose-common.sh
source "$script_dir/compose-common.sh"
ce_require_compose
if (($# == 0)); then set -- --tail 100; fi
exec env "${ce_env_args[@]}" docker "${ce_compose_args[@]}" logs "$@"
