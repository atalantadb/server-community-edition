#!/usr/bin/env bash
# Build the AtalantaDB Community Edition Docker images and optionally start the cluster.
set -Eeuo pipefail

project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
compose_dir="$project_dir/atalanta-docker-compose-ce/atalanta"
core_dir="$project_dir/adb-core"
core_stamp="$compose_dir/core/.ce-build-stamp.json"

fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
if ((BASH_VERSINFO[0] < 4)); then
    # macOS keeps bash 3.2 as /bin/bash; rerun with a Homebrew bash when one is installed.
    for candidate in /opt/homebrew/bin/bash /usr/local/bin/bash; do
        if [[ -x "$candidate" && -z "${ADB_BASH_REEXEC:-}" ]]; then
            ADB_BASH_REEXEC=1 exec "$candidate" "$0" "$@"
        fi
    done
    fail "Bash 4 or newer is required. On macOS run: brew install bash"
fi
unset ADB_BASH_REEXEC
warn() {
    if [[ -t 2 ]]; then printf '\033[0;33mWARNING\033[0m: %s\n' "$*" >&2;
    else printf 'WARNING: %s\n' "$*" >&2; fi
}
confirm() {
    local answer
    while true; do
        read -r -p "$* [y/N] " answer || fail "Could not read the user response."
        case "${answer,,}" in
            y|yes) return 0 ;;
            n|no|"") return 1 ;;
            *) echo "Please answer yes or no." ;;
        esac
    done
}
require_command() { command -v "$1" >/dev/null 2>&1 || fail "$1 is required in PATH."; }

# Lightweight public CE checks; no private export report or publisher paths are required.
validate_ce() {
    python3 -I -B - "$project_dir" "$1" "$core_stamp" <<'PY'
import hashlib
import json
from pathlib import Path
import struct
import sys
import xml.etree.ElementTree as ET

root, action, stamp = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3])
def require(ok, reason):
    if not ok:
        raise ValueError(reason)
def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
def fingerprint():
    core = root / 'adb-core'
    sources = {str(p.relative_to(core)): digest(p) for p in sorted(core.rglob('*'))
               if p.is_file() and p.suffix in ('.c', '.h', '.sh')}
    return {'sources': sources, 'binary_sha256': digest(core / 'adb')}
def check_core():
    raw = (root / 'adb-core/adb').read_bytes()
    require(len(raw) >= 20 and raw[:6] == b'\x7fELF\x02\x01'
            and struct.unpack_from('<H', raw, 18)[0] == 62,
            'The compiled adb must be a Linux x86_64 ELF binary.')
    require(b'COMMUNITY_EDITION' in raw and b'ENTERPRISE_EDITION' not in raw,
            'The compiled adb is not Community Edition.')
try:
    if action == 'sources':
        ns = {'m': 'http://maven.apache.org/POM/4.0.0'}
        pom = ET.parse(root / 'pom.xml').getroot()
        require(pom.findtext('m:version', '', ns).endswith('-ce'),
                'Run this script in an exported Community Edition project (version ending in -ce).')
        require(not any(p.text == 'auth-free' for p in pom.findall('m:profiles/m:profile/m:id', ns)),
                'Export ordinary Community sources before building Docker images.')
        source = (root / 'adb-core/main.c').read_text()
        require('COMMUNITY_EDITION' in source and 'ENTERPRISE_EDITION' not in source,
                'The native sources are not Community Edition.')
        require((root / 'adb-core/compile-CE.sh').is_file(), 'compile-CE.sh is missing.')
    elif action == 'reuse-core':
        check_core()
        require(json.loads(stamp.read_text()) == fingerprint(), 'No matching verified CE core build stamp.')
    elif action == 'stamp-core':
        check_core()
        stamp.write_text(json.dumps(fingerprint(), indent=2) + '\n')
    else:
        raise ValueError('Unknown validation action: ' + action)
except (OSError, ValueError, ET.ParseError) as ex:
    print('ERROR: CE validation failed: ' + str(ex), file=sys.stderr)
    sys.exit(1)
PY
}

positive_integer() {
    [[ "$2" =~ ^[1-9][0-9]{0,5}$ ]] || fail "$1 must be a positive integer below 1000000."
}
existing_parent() {
    local path="$1"
    [[ "$path" == /* ]] || fail "Disk-check paths must be absolute: $path"
    while [[ ! -e "$path" ]]; do path="$(dirname -- "$path")"; done
    printf '%s\n' "$path"
}

check_space() {
    local project_gb="${ADB_CE_DOCKER_MIN_PROJECT_GB:-5}"
    local maven_gb="${ADB_CE_DOCKER_MIN_MAVEN_GB:-5}"
    local tmp_gb="${ADB_CE_DOCKER_MIN_TMP_GB:-3}"
    local docker_gb="${ADB_CE_DOCKER_MIN_DOCKER_GB:-20}"
    local work_gb="${ADB_CE_DOCKER_MIN_DOCKER_WORK_GB:-5}"
    local docker_root images image base weight total=0 missing=0 installed=0 required
    local label path gb device available required_kb deficit ok=true row
    local -a requests
    local -A weights=([atalanta-ce-master]=1000 [atalanta-ce-rs]=1100 [atalanta-ce-web-ui]=1200
                     [grafana/grafana]=1920 [prom/prometheus]=381 [zookeeper]=461)
    local -A filesystem_need=() filesystem_free=() filesystem_path=()
    for row in project_gb maven_gb tmp_gb docker_gb work_gb; do positive_integer "$row" "${!row}"; done
    ((work_gb <= docker_gb)) || fail "The Docker work reserve cannot exceed the fresh-install reserve."
    docker_root=$(docker info --format '{{.DockerRootDir}}') || fail "Cannot determine Docker storage."
    [[ -n "$docker_root" ]] || fail "Docker returned an empty storage path."
    images=$(ce_compose config --images) || fail "Cannot list Compose images."
    images=$(printf '%s\n' "$images" | sort -u)
    [[ -n "$images" ]] || fail "Compose has no images."
    echo "Checking installed images (size credits are approximate, not peak-build measurements)..."
    while IFS= read -r image; do
        base="${image%:*}"
        weight="${weights[$image]:-${weights[$base]:-1024}}"
        total=$((total + weight))
        if docker image inspect "$image" >/dev/null 2>&1; then
            printf '  [installed] %s\n' "$image"; installed=$((installed + 1))
        else
            printf '  [missing]   %s\n' "$image"; missing=$((missing + weight))
        fi
    done <<< "$images"
    required=$((work_gb + ((docker_gb - work_gb) * missing + total - 1) / total))
    printf 'Docker reserve: %s GiB fresh; %s GiB now (%s installed images).\n' "$docker_gb" "$required" "$installed"
    requests=("Project|$project_dir|$project_gb"
              "Maven repository|${ADB_CE_MAVEN_REPOSITORY:-${HOME}/.m2/repository}|$maven_gb"
              "Temporary directory|${TMPDIR:-/tmp}|$tmp_gb"
              "Docker storage|$docker_root|$required")
    for row in "${requests[@]}"; do
        IFS='|' read -r label path gb <<< "$row"
        path=$(existing_parent "$path") || fail "Cannot find filesystem for $label."
        row=$(df -Pk -- "$path" | awk 'END {print $1, $4}') || fail "Cannot inspect free space at $path."
        read -r device available <<< "$row"
        [[ "$available" =~ ^[0-9]+$ && -n "$device" ]] || fail "Invalid df output for $path."
        printf '%-20s %s GiB available; %s GiB reserved (%s)\n' "$label:" "$((available / 1048576))" "$gb" "$path"
        filesystem_need[$device]=$(( ${filesystem_need[$device]:-0} + gb * 1048576 ))
        filesystem_free[$device]="$available"
        filesystem_path[$device]="$path"
    done
    # Several locations may share the same filesystem. Count their reserves
    # together instead of promising the same free bytes to every build stage.
    for device in "${!filesystem_need[@]}"; do
        required_kb=${filesystem_need[$device]}; available=${filesystem_free[$device]}
        printf 'Combined reserve on %s: %s GiB.\n' "$device" "$((required_kb / 1048576))"
        if ((available < required_kb)); then
            deficit=$(((required_kb - available + 1048575) / 1048576))
            warn "Not enough space on ${filesystem_path[$device]}. Free at least $deficit GiB."
            ok=false
        fi
    done
    if [[ "$ok" != true ]]; then
        warn "Continuing may fail and leave incomplete Maven or Docker artifacts."
        confirm "Continue despite insufficient disk space?" || fail "Build canceled. Free space and run this script again."
    fi
}

preflight() {
    local command
    require_command python3
    validate_ce sources || fail "Refusing to build outside a Community Edition source tree."
    if [[ -n "${ADB_CE_BUILD_VERIFIER:-}" ]]; then
        [[ "$ADB_CE_BUILD_VERIFIER" == /* && -f "$ADB_CE_BUILD_VERIFIER" && -r "$ADB_CE_BUILD_VERIFIER" ]] \
            || fail "ADB_CE_BUILD_VERIFIER must name a readable absolute publisher verification script."
    fi
    [[ -t 0 ]] || fail "An interactive terminal is required."
    [[ -f "$compose_dir/docker-compose.yml" && -f "$compose_dir/1-start.sh" && -f "$compose_dir/compose-common.sh" ]] \
        || fail "CE deployment files are missing."
    for command in docker mvn java df awk sort head dirname cp chmod cmp; do require_command "$command"; done
    # The same fixed CE project/file selection is used by all host helpers.
    # shellcheck source=atalanta-docker-compose-ce/atalanta/compose-common.sh
    source "$compose_dir/compose-common.sh"
    ce_require_compose
    docker info >/dev/null 2>&1 || fail "The local Docker daemon is not available."
    # Host paths are not meaningful for remote daemons; never report them as checked.
    [[ -z "${DOCKER_HOST:-}" || "$DOCKER_HOST" == unix://* ]] || fail "A local Docker daemon is required for disk-space checks."
    local endpoint context
    if [[ -z "${DOCKER_HOST:-}" || -n "${DOCKER_CONTEXT:-}" ]]; then
        context=$(docker context show) || fail "Cannot determine the Docker context."
        endpoint=$(docker context inspect "$context" --format '{{.Endpoints.docker.Host}}') || fail "Cannot inspect the Docker endpoint."
        [[ "$endpoint" == unix://* ]] || fail "Remote Docker contexts are not supported by disk-space checks."
    fi
    check_space
}

build_java() {
    # clean also removes AtalantaDB, including any native deployment data there.
    if [[ -e "$project_dir/AtalantaDB" ]]; then
        warn "Maven clean will DELETE $project_dir/AtalantaDB, including any logs and database data inside it. Docker named volumes are not affected."
        confirm "Have you stopped the native deployment and backed up anything needed; allow this deletion?" \
            || fail "Build canceled; preserve AtalantaDB before continuing."
    fi
    confirm "Build CE Java with Maven clean install?" || fail "Java build declined; unverified existing JARs will not be used."
    (cd "$project_dir" && mvn -T4 -Dfindbugs.skip -Dcheckstyle.skip -Dpmd.skip=true \
        -Dspotbugs.skip=true -Dmaven.test.skip=true -Dmaven.gitcommitid.skip=true \
        "-Dmaven.repo.local=${ADB_CE_MAVEN_REPOSITORY:-${HOME}/.m2/repository}" clean install) \
        || fail "CE Java build failed. Docker images were not built or started."
}

build_core() {
    local rebuild=true
    if [[ -x "$core_dir/adb" ]] && validate_ce reuse-core >/dev/null 2>&1; then
        if ! confirm "A verified CE core is already built. Rebuild it?"; then rebuild=false; fi
    elif [[ -e "$core_dir/adb" ]]; then
        warn "The existing core has no matching CE build stamp; a CE rebuild is required."
    fi
    if [[ "$rebuild" == true ]]; then
        if [[ "$(uname -s):$(uname -m)" != Linux:x86_64 ]]; then
            # macOS and Linux ARM hosts build the x86_64 core in the matching Linux container.
            confirm "Build adb-core with compile-CE.sh in an ubuntu:24.04 (amd64) container?" || fail "CE core build declined."
            docker run --rm --platform linux/amd64 -v "$core_dir:/src" -w /src ubuntu:24.04 bash -c \
                'apt-get update -qq && apt-get install -y -qq gcc liblzma-dev >/dev/null && bash ./compile-CE.sh' \
                || fail "CE core build in Docker failed."
        elif confirm "Build adb-core using compile-CE.sh (dependency installation may require sudo)?"; then
            if [[ -f "$core_dir/check-deps-and-compile-CE.sh" ]]; then
                (cd "$core_dir" && bash ./check-deps-and-compile-CE.sh) || fail "CE core build failed."
            else
                (cd "$core_dir" && bash ./compile-CE.sh) || fail "CE core build failed. Install its compiler dependencies first."
            fi
        else
            fail "CE core build declined."
        fi
        [[ -x "$core_dir/adb" ]] || fail "The CE build did not produce an executable adb."
        validate_ce stamp-core || fail "CE core validation failed; nothing was copied to Docker."
    fi
    confirm "Copy the verified CE core to $compose_dir/core/adb?" || fail "Core copy declined."
    cp -- "$core_dir/adb" "$compose_dir/core/adb" || fail "Could not copy the CE core."
    chmod +x "$compose_dir/core/adb" || fail "Could not make the CE core executable."
    cmp -s -- "$core_dir/adb" "$compose_dir/core/adb" || fail "The copied core differs from the verified binary."
}

verify_publisher_build() {
    if [[ -n "${ADB_CE_BUILD_VERIFIER:-}" ]]; then
        echo "Running publisher verification before Docker image assembly..."
        python3 -I -B "$ADB_CE_BUILD_VERIFIER" "$project_dir" \
            || fail "Publisher CE verification failed; no Docker images were built or started."
    fi
}

main() {
    if [[ "${1:-}" == --help && $# == 1 ]]; then
        echo "Usage: ./build-adb-docker-ce.sh"
        echo "Run interactively. Every build/start stage requires confirmation."
        echo "Disk reserves (GiB): ADB_CE_DOCKER_MIN_PROJECT_GB=5, ADB_CE_DOCKER_MIN_MAVEN_GB=5,"
        echo "ADB_CE_DOCKER_MIN_TMP_GB=3, ADB_CE_DOCKER_MIN_DOCKER_GB=20, ADB_CE_DOCKER_MIN_DOCKER_WORK_GB=5."
        echo "Optional: ADB_CE_MAVEN_REPOSITORY (absolute path), TMPDIR, ATALANTA_DOCKER_START_TIMEOUT."
        echo "Publisher-only: ADB_CE_BUILD_VERIFIER (absolute verification script; runs before Docker image assembly)."
        echo "CE host ports: ATALANTA_CE_WEB_UI_PORT=8433, ATALANTA_CE_PROMETHEUS_PORT=9190,"
        echo "ATALANTA_CE_GRAFANA_PORT=3100, ATALANTA_CE_ZK1_PORT=2281, ATALANTA_CE_ZK2_PORT=2282, ATALANTA_CE_ZK3_PORT=2283."
        return
    fi
    (($# == 0)) || fail "Unknown arguments. Use --help."
    echo "Running CE preflight checks..."
    preflight
    build_java
    build_core
    validate_ce reuse-core || fail "CE core changed after verification."
    cmp -s -- "$core_dir/adb" "$compose_dir/core/adb" || fail "The Docker core differs from the verified CE binary."
    verify_publisher_build
    confirm "Build the CE master, RegionServer and Web UI Docker images?" || fail "Docker build declined; no containers were started."
    ce_compose build atalanta-rs atalanta-master atalanta-web-ui \
        || fail "Docker image build failed; no containers were started."
    if confirm "Start the CE Docker environment now?"; then
        "$BASH" "$compose_dir/1-start.sh" || fail "CE startup failed. Check logs with: bash '$compose_dir/2-logs.sh' --tail 100"
    else
        echo "Build completed; containers were not started."
        printf 'To start later: bash %q\n' "$compose_dir/1-start.sh"
    fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then main "$@"; fi
