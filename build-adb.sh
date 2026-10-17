#!/usr/bin/env bash
# Build the Java modules, build adb-core, refresh the AtalantaDB binary,
# and then start the interactive setup script.

set -u
set -o pipefail

project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
core_dir="$project_dir/adb-core"
distribution_dir="$project_dir/AtalantaDB"
target_binary="$distribution_dir/bin/core/adb"

fail() {
    echo "ERROR: $*" >&2
    exit 1
}

run_maven_build() {
    command -v mvn >/dev/null 2>&1 || fail "mvn is not installed or is not in PATH."

    echo "Building Maven modules..."
    (
        cd "$project_dir" || exit 1
        mvn -T4 -Dfindbugs.skip -Dcheckstyle.skip -Dpmd.skip=true -Dspotbugs.skip=true -DskipTests=true -Dmaven.gitcommitid.skip=true clean install
    ) || fail "Maven build failed."
}

run_core_build() {
    local dependency_script

    # Use the dependency checker when present; the separately supplied CE core
    # may only provide compile-CE.sh.
    if [[ -f "$core_dir/check-build-deps.sh" ]]; then
        dependency_script="$core_dir/check-build-deps.sh"
    elif [[ -f "$core_dir/check-deps-and-compile-CE.sh" ]]; then
        dependency_script="$core_dir/check-deps-and-compile-CE.sh"
    elif [[ -f "$core_dir/compile-CE.sh" ]]; then
        dependency_script="$core_dir/compile-CE.sh"
    else
        fail "adb-core dependency/build script was not found."
    fi

    echo "Building adb-core with $(basename -- "$dependency_script")..."
    (
        cd "$core_dir" || exit 1
        bash "$dependency_script"
    ) || fail "adb-core build failed."
    [[ -f "$core_dir/adb" ]] || fail "adb-core build completed but $core_dir/adb was not created."
}

copy_core_binary() {
    [[ -d "$distribution_dir/bin/core" ]] || fail "Distribution directory is missing: $distribution_dir/bin/core"

    echo "Copying adb-core binary to AtalantaDB/bin/core..."
    cp -f "$core_dir/adb" "$target_binary" || fail "Could not copy adb-core binary to the distribution."
}


run_setup() {
    [[ -f "$distribution_dir/setup-adb.sh" ]] || fail "Setup script is missing: $distribution_dir/setup-adb.sh"

    echo "Starting setup..."
    (
        cd "$distribution_dir" || exit 1
        bash ./setup-adb.sh
    ) || fail "Setup failed."

    echo "Check ./AtalantaDB"
}

run_maven_build
run_core_build
copy_core_binary


chmod -x AtalantaDB/*.sh
chmod +x AtalantaDB/setup-adb.sh

rm AtalantaDB/bin/master2/atalanta-master.jar
rm AtalantaDB/bin/rs2/atalanta-rs.jar
rm AtalantaDB/bin/rs3/atalanta-rs.jar
rm AtalantaDB/bin/rs4/atalanta-rs.jar

run_setup


echo "Build and setup workflow completed."
