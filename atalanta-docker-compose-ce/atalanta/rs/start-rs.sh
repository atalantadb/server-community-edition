#!/usr/bin/env bash
# Supervise the Java RS and native core together; either exiting stops the other.
set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
core_dir="$(cd -- "$script_dir/../core" && pwd)"
cd "$script_dir"

fail() {
    echo "ERROR: $*" >&2
    exit 1
}

[[ -f ./atalanta-rs.jar ]] || fail "The CE RegionServer JAR is missing."
[[ -x "$core_dir/adb" ]] || fail "The CE core binary is missing or not executable."
[[ -f "$core_dir/atalanta.prop" ]] || fail "The CE core configuration is missing."
command -v ss >/dev/null 2>&1 || fail "ss is required to check the core listener."
command -v java >/dev/null 2>&1 || fail "Java is not installed."

core_port="$(awk -F '[=[:space:]]+' '$1 == "rs_port" { print $2; exit }' "$core_dir/atalanta.prop")"
[[ "$core_port" =~ ^[0-9]+$ ]] && ((core_port > 0 && core_port <= 65535)) \
    || fail "rs_port in the core configuration must be a valid TCP port."
start_timeout="${ATALANTA_CE_CORE_START_TIMEOUT:-60}"
[[ "$start_timeout" =~ ^[1-9][0-9]*$ ]] || fail "ATALANTA_CE_CORE_START_TIMEOUT must be a positive integer."

core_pid=""
rs_pid=""
cleanup() {
    trap - EXIT
    trap '' TERM INT
    if [[ -n "$rs_pid" ]] && kill -0 "$rs_pid" 2>/dev/null; then
        kill -TERM "$rs_pid" 2>/dev/null || true
    fi
    if [[ -n "$core_pid" ]] && kill -0 "$core_pid" 2>/dev/null; then
        kill -TERM "$core_pid" 2>/dev/null || true
    fi
    if [[ -n "$rs_pid" ]]; then wait "$rs_pid" 2>/dev/null || true; fi
    if [[ -n "$core_pid" ]]; then wait "$core_pid" 2>/dev/null || true; fi
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

"$core_dir/start-core.sh" >> "$core_dir/core.log" 2>&1 &
core_pid=$!

deadline=$((SECONDS + start_timeout))
echo "Waiting for the CE core administrative listener on port $core_port..."
while ! ss -ltnH "sport = :$core_port" | grep -q .; do
    kill -0 "$core_pid" 2>/dev/null || fail "The CE core exited during startup. Check $core_dir/core.log."
    ((SECONDS < deadline)) || fail "The CE core did not start within ${start_timeout}s. Check $core_dir/core.log."
    sleep 1
done
kill -0 "$core_pid" 2>/dev/null || fail "The CE core exited during startup. Check $core_dir/core.log."

# Keep JMX for internal metrics, without credentials or encrypted transport.
java -Xmx2048m \
    -Dspring.profiles.active=default \
    -Dcom.sun.management.jmxremote \
    -Dcom.sun.management.jmxremote.ssl=false \
    -Dcom.sun.management.jmxremote.registry.ssl=false \
    -Dcom.sun.management.jmxremote.authenticate=false \
    -Dcom.sun.management.jmxremote.local.only=false \
    -Dcom.sun.management.jmxremote.port=8340 \
    -Dcom.sun.management.jmxremote.rmi.port=8340 \
    "-Djava.rmi.server.hostname=$(hostname)" \
    -jar atalanta-rs.jar "$@" &
rs_pid=$!

while kill -0 "$rs_pid" 2>/dev/null; do
    if ! kill -0 "$core_pid" 2>/dev/null; then
        status=0
        wait "$core_pid" || status=$?
        echo "ERROR: The CE core exited while the RegionServer was running (status $status). Check $core_dir/core.log." >&2
        ((status != 0)) || status=1
        exit "$status"
    fi
    sleep 1
done

status=0
wait "$rs_pid" || status=$?
exit "$status"
