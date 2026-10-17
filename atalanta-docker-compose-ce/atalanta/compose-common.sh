#!/usr/bin/env bash
# Source this file from CE host helpers; it does not start or stop anything.

ce_compose_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ce_compose_args=(compose --env-file /dev/null --project-name atalanta-ce
                 --project-directory "$ce_compose_dir" --file "$ce_compose_dir/docker-compose.yml")
ce_env_args=(-u COMPOSE_FILE -u COMPOSE_PROJECT_NAME -u COMPOSE_PROFILES
             -u COMPOSE_ENV_FILES -u COMPOSE_PATH_SEPARATOR)

ce_fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
if ((BASH_VERSINFO[0] < 4)); then
    # macOS keeps bash 3.2 as /bin/bash; rerun the calling helper with a Homebrew bash when one is installed.
    for ce_bash in /opt/homebrew/bin/bash /usr/local/bin/bash; do
        if [[ -x "$ce_bash" && -z "${ADB_BASH_REEXEC:-}" ]]; then
            ADB_BASH_REEXEC=1 exec "$ce_bash" "$0" "$@"
        fi
    done
    ce_fail "Bash 4 or newer is required. On macOS run: brew install bash"
fi
unset ADB_BASH_REEXEC
ce_compose() { env "${ce_env_args[@]}" docker "${ce_compose_args[@]}" "$@"; }

ce_validate_ports() {
    local name value
    local -A used=()
    export ATALANTA_CE_WEB_UI_PORT="${ATALANTA_CE_WEB_UI_PORT:-8433}"
    export ATALANTA_CE_PROMETHEUS_PORT="${ATALANTA_CE_PROMETHEUS_PORT:-9190}"
    export ATALANTA_CE_GRAFANA_PORT="${ATALANTA_CE_GRAFANA_PORT:-3100}"
    export ATALANTA_CE_ZK1_PORT="${ATALANTA_CE_ZK1_PORT:-2281}"
    export ATALANTA_CE_ZK2_PORT="${ATALANTA_CE_ZK2_PORT:-2282}"
    export ATALANTA_CE_ZK3_PORT="${ATALANTA_CE_ZK3_PORT:-2283}"
    for name in ATALANTA_CE_WEB_UI_PORT ATALANTA_CE_PROMETHEUS_PORT ATALANTA_CE_GRAFANA_PORT \
                ATALANTA_CE_ZK1_PORT ATALANTA_CE_ZK2_PORT ATALANTA_CE_ZK3_PORT; do
        value="${!name}"
        [[ "$value" =~ ^[1-9][0-9]{0,4}$ ]] && ((value <= 65535)) \
            || ce_fail "$name must be a TCP port between 1 and 65535 without leading zeros."
        [[ -z "${used[$value]:-}" ]] || ce_fail "$name and ${used[$value]} use the same host port: $value"
        used[$value]="$name"
    done
}

ce_require_compose() {
    local command version first
    for command in env docker sort head; do
        command -v "$command" >/dev/null 2>&1 || ce_fail "$command is required in PATH."
    done
    ce_validate_ports
    version=$(ce_compose version --short) || ce_fail "Docker Compose V2 is not available."
    version="${version#v}"
    [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+ ]] || ce_fail "Cannot parse Docker Compose version: $version"
    first=$(printf '%s\n' 2.20.0 "$version" | sort -V | head -n 1)
    [[ "$first" == 2.20.0 ]] || ce_fail "Docker Compose 2.20.0 or newer is required."
    docker info >/dev/null 2>&1 || ce_fail "The Docker daemon is not available to the current user."
    ce_compose config --quiet || ce_fail "The CE Docker Compose configuration is invalid."
}
