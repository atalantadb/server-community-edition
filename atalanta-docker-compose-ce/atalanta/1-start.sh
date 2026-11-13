#!/usr/bin/env bash
# Start the AtalantaDB Docker Compose environment and verify its services.

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
start_timeout="${ATALANTA_DOCKER_START_TIMEOUT:-180}"
stability_period="${ATALANTA_DOCKER_STABILITY_PERIOD:-10}"

fail() {
    echo "ERROR: $*" >&2
    exit 1
}

case "$#:${1:-}" in
    0:) ;;
    1:--full) export ATALANTA_CE_FULL=1 ;;
    *) fail "Usage: $0 [--full]" ;;
esac

# shellcheck source=compose-common.sh
source "$script_dir/compose-common.sh"
[[ "$start_timeout" =~ ^[1-9][0-9]{0,5}$ ]] \
    || fail "ATALANTA_DOCKER_START_TIMEOUT must be a positive integer."
[[ "$stability_period" =~ ^[1-9][0-9]{0,5}$ ]] \
    || fail "ATALANTA_DOCKER_STABILITY_PERIOD must be a positive integer."

((stability_period < start_timeout)) || fail "The stability period must be shorter than the startup timeout."
ce_require_compose

service_list=$(ce_compose config --services) || fail "Cannot list the CE services."
[[ -n "$service_list" ]] || fail "No services were found in the CE Docker Compose configuration."
mapfile -t expected_services < <(printf '%s\n' "$service_list" | sort -u)
((${#expected_services[@]} > 0)) || fail "No services were found in the Docker Compose configuration."

echo "Starting the AtalantaDB Community Edition Docker environment..."
ce_compose up -d --no-build --pull missing \
    || fail "Docker Compose startup failed. Check access to the published GHCR images, or explicitly build CE images with build-adb-docker-ce.sh. No source build was requested."

deadline=$((SECONDS + start_timeout))
stable_since=-1
ready_error='The database readiness check has not completed.'
expected_rs=1
[[ "${ATALANTA_CE_FULL:-}" != 1 ]] || expected_rs=3
echo "Waiting up to ${start_timeout}s for ${#expected_services[@]} services to remain running for ${stability_period}s..."

while ((SECONDS < deadline)); do
    service_list=$(ce_compose ps --services --status running) || fail "Cannot read the CE container status."
    mapfile -t running_services < <(printf '%s\n' "$service_list" | sort -u)

    all_running=true
    for expected_service in "${expected_services[@]}"; do
        if ! printf '%s\n' "${running_services[@]}" | grep -Fqx -- "$expected_service"; then
            all_running=false
            break
        fi
    done

    if [[ "$all_running" == true ]]; then
        if ((stable_since < 0)); then
            stable_since=$SECONDS
        elif ((SECONDS - stable_since >= stability_period)); then
            probe_timeout=$((deadline - SECONDS))
            ((probe_timeout > 0)) || break
            ((probe_timeout <= 15)) || probe_timeout=15
            if ready_error=$(ce_compose exec -T atalanta-master bash -c \
                'cd /opt/atalanta/shell && exec timeout --kill-after=2 "$1" java -Xmx96m -Dlog4j.configuration=file:config/log4j.properties -cp atalanta-shell.jar com.light.atalanta.client.tools.ClusterReadiness "$2"' \
                readiness "${probe_timeout}s" "$expected_rs" 2>&1); then
                ce_compose ps || fail "Cannot read the CE container status."
                printf 'Everything is \033[0;32mOK\033[0m. AtalantaDB Community Edition is ready.\n'
                printf 'Web UI: http://localhost:%s/ (no login).\n' "$ATALANTA_CE_WEB_UI_PORT"
                exit 0
            else
                probe_status=$?
                ready_error="Readiness probe failed (exit $probe_status; 124 means timeout). ${ready_error:-No diagnostic output was received.}"
            fi
        fi
    else
        stable_since=-1
    fi

    sleep 1
done

echo "ERROR: AtalantaDB did not become ready within ${start_timeout}s." >&2
echo "Last database readiness result:" >&2
printf '%s\n' "$ready_error" | tail -n 20 >&2
echo "Services that are not running:" >&2
service_list=$(ce_compose ps --services --status running) || fail "Cannot read the CE container status."
mapfile -t running_services < <(printf '%s\n' "$service_list" | sort -u)
for expected_service in "${expected_services[@]}"; do
    if ! printf '%s\n' "${running_services[@]}" | grep -Fqx -- "$expected_service"; then
        echo "  $expected_service" >&2
    fi
done
ce_compose ps --all >&2 || true
echo "Inspect a failed service with: ./2-logs.sh --tail 100 <service>" >&2
exit 1
