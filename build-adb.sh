#!/usr/bin/env bash
# Build the Java modules, build adb-core, refresh the AtalantaDB binary,
# and then start the interactive setup script.

set -u
set -o pipefail

project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
core_dir="$project_dir/adb-core"
distribution_dir="$project_dir/AtalantaDB"
target_binary="$distribution_dir/bin/core/adb"
core_compile_script=''
core_binary_source=''
core_platform=''
os_family=''
os_id=''
os_major=''
installer=''
prebuilt_name=''
native_packages=()
missing_native_packages=()
missing_build_tools=()

fail() {
    echo "ERROR: $*" >&2
    exit 1
}

confirm() {
    local answer
    while true; do
        read -r -p "$* [y/N] " answer || fail "Could not read the user response."
        case "${answer,,}" in
            y|yes) return 0 ;;
            n|no|'') return 1 ;;
            *) echo "Please answer yes or no." ;;
        esac
    done
}

detect_build_environment() {
    local release_file="${1:-/etc/os-release}"
    local ID='' ID_LIKE='' VERSION_ID='' PRETTY_NAME=''
    [[ "$(uname -s)" == Linux ]] || fail "This build script requires Linux."
    [[ -r "$release_file" ]] || fail "Cannot identify the operating system: $release_file is missing."
    . "$release_file" || fail "Could not read operating-system information."
    os_id="${ID,,}"
    ID_LIKE="${ID_LIKE,,}"
    os_major="${VERSION_ID%%.*}"
    case "$os_id $ID_LIKE" in
        ubuntu\ *|debian\ *|*' debian'*)
            os_family=debian installer=apt-get
            native_packages=(build-essential liblzma-dev)
            ;;
        rhel\ *|redhat\ *|rocky\ *|almalinux\ *|centos\ *|ol\ *|fedora\ *|*' rhel'*|*' centos'*|*' fedora'*)
            os_family=rhel installer=dnf
            command -v dnf >/dev/null 2>&1 || installer=yum
            native_packages=(gcc glibc-devel kernel-headers binutils xz-devel)
            ;;
        *) fail "Unsupported operating-system family: ${PRETTY_NAME:-$ID}." ;;
    esac
    core_platform="$os_family:$os_major:$(uname -m)"
    prebuilt_name=''
    if [[ "$os_id" == ubuntu ]]; then
        prebuilt_name=adb_ubuntu
    elif [[ "$os_major" =~ ^[89]$ ]]; then
        case "$os_id $ID_LIKE" in
            rhel\ *|redhat\ *|rocky\ *|almalinux\ *|centos\ *|ol\ *|*' rhel'*|*' centos'*)
                prebuilt_name="adb_rhel$os_major" ;;
        esac
    fi
    echo "Detected operating system: ${PRETTY_NAME:-$ID}; architecture: $(uname -m)"
}

select_compile_script() {
    local name
    for name in compile-CE.sh compile-CE.sh; do
        if [[ -f "$core_dir/$name" ]]; then
            core_compile_script="$core_dir/$name"
            return
        fi
    done
    fail "No native compile script was found in $core_dir."
}

package_installed() {
    if [[ "$os_family" == debian ]]; then
        [[ "$(dpkg-query -W -f='${db:Status-Status}' "$1" 2>/dev/null)" == installed ]]
    else
        rpm -q "$1" >/dev/null 2>&1
    fi
}

jdk_available() {
    local version
    command -v java >/dev/null 2>&1 && command -v javac >/dev/null 2>&1 || return 1
    version="$(javac -version 2>&1)" || return 1
    [[ "$version" =~ ^javac[[:space:]]+([0-9]+) ]] && ((BASH_REMATCH[1] >= 17)) || return 1
    version="$(java -version 2>&1)" || return 1
    [[ "$version" =~ version[[:space:]]+\"([0-9]+) ]] && ((BASH_REMATCH[1] >= 17))
}

check_build_packages() {
    local package
    missing_native_packages=() missing_build_tools=()
    for package in "${native_packages[@]}"; do
        package_installed "$package" || missing_native_packages+=("$package")
    done
    if ! jdk_available; then
        if [[ "$os_family" == debian ]]; then
            missing_build_tools+=(openjdk-17-jdk)
        else
            missing_build_tools+=(java-17-openjdk-devel)
        fi
    fi
    command -v mvn >/dev/null 2>&1 || missing_build_tools+=(maven)
}

show_install_commands() {
    echo "Installation commands:"
    if [[ "$installer" == apt-get ]]; then
        echo "  sudo apt-get update"
    fi
    printf '  sudo %s install -y' "$installer"
    printf ' %q' "$@"
    printf '\n'
}

install_missing_packages() {
    (($# > 0)) || return 0
    echo "The following packages need to be installed:"
    printf '  %s\n' "$@"
    show_install_commands "$@"
    echo "Installing these packages requires sudo and may prompt for your password."
    if ! confirm "Install the missing packages now?"; then
        fail "Run the installation commands shown above, then run this script again. No build was started."
    fi
    command -v sudo >/dev/null 2>&1 || fail "sudo is required to install the packages."
    command -v "$installer" >/dev/null 2>&1 || fail "$installer is not available."
    if [[ "$installer" == apt-get ]]; then
        sudo apt-get update || fail "Could not update the package index. No build was started."
    fi
    sudo "$installer" install -y "$@" || fail "Package installation failed. No build was started."
}

select_prebuilt_core() {
    local name candidate header dependencies
    [[ "$(uname -m)" == x86_64 ]] || fail "Bundled core binaries support Linux x86_64 only."
    [[ -n "$prebuilt_name" ]] \
        || fail "No bundled core is supported for this OS. Install the build packages and run this script again."
    name="$prebuilt_name"
    for candidate in "$core_dir/bin/$name" "$core_dir/$name"; do
        [[ -e "$candidate" || -L "$candidate" ]] || continue
        [[ -f "$candidate" && ! -L "$candidate" && -s "$candidate" && -r "$candidate" ]] \
            || fail "Invalid prebuilt core: $candidate"
        header="$(od -An -tx1 -N20 -- "$candidate" | tr -d ' \n')" || fail "Could not inspect $candidate."
        [[ "$header" == 7f454c460201* && "${header:36:4}" == 3e00 ]] \
            || fail "The prebuilt core is not a little-endian Linux x86_64 ELF binary: $candidate"
        if [[ "$core_compile_script" == */compile-CE.sh ]]; then
            grep -aFq COMMUNITY_EDITION "$candidate" && ! grep -aFq ENTERPRISE_EDITION "$candidate" \
                || fail "The prebuilt core is not Community Edition: $candidate"
        fi
        command -v ldd >/dev/null 2>&1 || fail "ldd is required to check the prebuilt core's runtime libraries."
        if ! dependencies="$(ldd "$candidate" 2>&1)" || [[ "$dependencies" == *'not found'* ]]; then
            printf '%s\n' "$dependencies" >&2
            fail "The prebuilt core is incompatible or its runtime libraries are missing. Install the required runtime libraries or build the core locally."
        fi
        core_binary_source="$candidate"
        echo "Using the prebuilt core: $core_binary_source"
        echo "Native compilation and installation of native build packages will be skipped."
        return
    done
    fail "Prebuilt core $name was not found in $core_dir/bin or $core_dir. Install the build packages or supply the matching binary, then run this script again."
}

prepare_build() {
    detect_build_environment "$@"
    select_compile_script
    core_binary_source=''
    if grep -Eq -- '-l(ssl|crypto)([^[:alnum:]_]|$)' "$core_compile_script"; then
        if [[ "$os_family" == debian ]]; then native_packages+=(libssl-dev); else native_packages+=(openssl-devel); fi
    fi
    grep -Eq '\bpython3\b' "$core_compile_script" && native_packages+=(python3)
    check_build_packages
    if ((${#missing_native_packages[@]} > 0)); then
        echo "WARNING: Building adb-core requires missing development packages:"
        printf '  %s\n' "${missing_native_packages[@]}"
        show_install_commands "${missing_native_packages[@]}"
        echo "Installing these packages requires sudo and may prompt for your password."
        echo "Alternatively, you can use the bundled core without installing native build packages."
        echo "JDK 17 or newer and Maven are still required to build the Java components."
        if confirm "Use a prebuilt core instead of compiling adb-core?"; then
            select_prebuilt_core
        fi
    else
        echo "All native build packages are installed."
    fi
    local package packages=("${missing_build_tools[@]}")
    if [[ -z "$core_binary_source" ]]; then
        for package in "${missing_native_packages[@]}"; do
            [[ " ${packages[*]} " == *" $package "* ]] || packages+=("$package")
        done
    fi
    install_missing_packages "${packages[@]}"
    check_build_packages
    ((${#missing_build_tools[@]} == 0)) \
        || fail "Required build tools are still unavailable. Check PATH and Java selection, then run this script again."
    # Maven uses JAVA_HOME in preference to the Java commands found in PATH.
    if [[ -n "${JAVA_HOME:-}" ]]; then
        local java_home_version
        [[ -x "$JAVA_HOME/bin/javac" ]] || fail "JAVA_HOME does not point to a JDK. No build was started."
        java_home_version="$("$JAVA_HOME/bin/javac" -version 2>&1)" \
            || fail "Could not run javac from JAVA_HOME. No build was started."
        [[ "$java_home_version" =~ ^javac[[:space:]]+([0-9]+) ]] && ((BASH_REMATCH[1] >= 17)) \
            || fail "JAVA_HOME must point to JDK 17 or newer. No build was started."
    fi
    [[ -n "$core_binary_source" || ${#missing_native_packages[@]} == 0 ]] \
        || fail "Native build packages are still missing. No build was started."
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
    if [[ -n "$core_binary_source" ]]; then
        echo "Skipping adb-core compilation; a prebuilt core was selected."
        return 0
    fi
    # Legacy compile scripts finish with `mv a.out adb`; never reuse a stale a.out after a compiler failure.
    rm -f -- "$core_dir/a.out" || fail "Could not remove the previous compiler output."
    echo "Building adb-core with $(basename -- "$core_compile_script")..."
    (
        cd "$core_dir" || exit 1
        bash "$core_compile_script"
    ) || fail "adb-core build failed."
    [[ -f "$core_dir/adb" ]] || fail "adb-core build completed but $core_dir/adb was not created."
    core_binary_source="$core_dir/adb"
}

copy_core_binary() {
    [[ -d "$distribution_dir/bin/core" ]] || fail "Distribution directory is missing: $distribution_dir/bin/core"

    # Always include all bundled variants, even when the active core was compiled locally.
    # Copy before installing adb so a bundled adb cannot overwrite the selected build.
    local binary destination
    if [[ -d "$core_dir/bin" ]]; then
        for binary in "$core_dir/bin/adb" "$core_dir/bin/adb_"*; do
            [[ -e "$binary" || -L "$binary" ]] || continue
            [[ -f "$binary" && ! -L "$binary" && -s "$binary" ]] \
                || fail "Invalid bundled core binary: $binary"
            destination="$distribution_dir/bin/core/$(basename -- "$binary")"
            [[ ! -L "$destination" && ( ! -e "$destination" || -f "$destination" ) ]] \
                || fail "Invalid bundled core destination: $destination"
            echo "Copying bundled core: $(basename -- "$binary")"
            cp -p -- "$binary" "$destination" || fail "Could not copy bundled core: $binary"
        done
    fi

    echo "Copying adb-core binary to AtalantaDB/bin/core..."
    install -m 0755 -- "$core_binary_source" "$target_binary" || fail "Could not copy adb-core binary to the distribution."
    local checksum
    checksum="$(sha256sum < "$target_binary")" || fail "Could not calculate the core checksum."
    printf '%s %s\n' "$core_platform" "${checksum%% *}" > "$distribution_dir/bin/core/.adb-platform" \
        || fail "Could not save the selected core platform."
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

main() {
    prepare_build
    run_maven_build
    run_core_build
    copy_core_binary


    chmod -x "$distribution_dir"/*.sh || fail "Could not update distribution script permissions."
    chmod +x "$distribution_dir/setup-adb.sh" || fail "Could not make setup executable."

    rm -f -- "$distribution_dir/bin/master2/atalanta-master.jar" \
        "$distribution_dir/bin/rs2/atalanta-rs.jar" "$distribution_dir/bin/rs3/atalanta-rs.jar" \
        "$distribution_dir/bin/rs4/atalanta-rs.jar" || fail "Could not prepare distribution JAR copies."

    run_setup


    echo "Build and setup workflow completed."
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
