#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir"

[[ -x ./adb ]] || { echo "ERROR: The CE core binary is missing or not executable: $script_dir/adb" >&2; exit 1; }
[[ -f ./atalanta.prop ]] || { echo "ERROR: The core configuration is missing: $script_dir/atalanta.prop" >&2; exit 1; }

# Run in the foreground; start-rs.sh owns the process and its log redirection.
exec ./adb atalanta.prop "$@"
