#!/usr/bin/env bash
# Configure replication and ZooKeeper for this distribution and optionally start AtalantaDB.

set -u
set -o pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
env_file="$script_dir/99-env.sh"
zk_download_base="${ZOOKEEPER_DOWNLOAD_BASE_URL:-https://dlcdn.apache.org/zookeeper}"
zk_version=""
zk_release=""
zk_dir=""
zk_download_url=""

fail() {
    echo "ERROR: $*" >&2
    exit 1
}

check_core_runtime() {
    local binary="$1" header dependencies
    header="$(od -An -tx1 -N20 -- "$binary" | tr -d ' \n')" || fail "Could not inspect $binary."
    [[ "$header" == 7f454c460201* && "${header:36:4}" == 3e00 ]] \
        || fail "The bundled core is not a little-endian Linux x86_64 ELF binary: $binary"
    command -v ldd >/dev/null 2>&1 || fail "ldd is required to check the core's runtime libraries."
    if ! dependencies="$(ldd "$binary" 2>&1)" || [[ "$dependencies" == *'not found'* ]]; then
        printf '%s\n' "$dependencies" >&2
        fail "The core is incompatible or required runtime libraries are missing: $binary"
    fi
}

select_core_binary() {
    local release_file="${1:-/etc/os-release}"
    local ID='' ID_LIKE='' VERSION_ID=''
    local core_dir="$script_dir/bin/core" family='' variant='' label='' architecture
    local active="$core_dir/adb" stamp="$core_dir/.adb-platform"
    local platform='' checksum='' extra='' actual='' current='' answer='' candidate path temporary

    [[ "$(uname -s)" == Linux ]] || fail "Automatic core selection requires Linux."
    if [[ ! -r "$release_file" && "$release_file" == /etc/os-release ]]; then
        release_file=/usr/lib/os-release
    fi
    if [[ ! -r "$release_file" ]]; then
        echo "WARNING: Cannot identify the operating system; keeping the current core binary."
        return 0
    fi
    . "$release_file" || fail "Could not read operating-system information."
    case "${ID,,} ${ID_LIKE,,}" in
        ubuntu\ *) family=debian variant=adb_ubuntu label=Ubuntu ;;
        rhel\ *|redhat\ *|rocky\ *|almalinux\ *|centos\ *|ol\ *|*' rhel'*|*' centos'*)
            if [[ "$VERSION_ID" =~ ^(8|9)(\.|$) ]]; then
                family=rhel variant="adb_rhel${BASH_REMATCH[1]}" label="RHEL ${BASH_REMATCH[1]}"
            fi ;;
    esac
    if [[ -z "$variant" ]]; then
        echo "WARNING: No bundled core is defined for ${ID:-unknown} ${VERSION_ID}; keeping the current core binary."
        return 0
    fi
    architecture="$(uname -m)"
    local expected="$family:${VERSION_ID%%.*}:$architecture"
    candidate="$core_dir/$variant"
    for path in "$active" "$stamp"; do
        [[ ! -L "$path" && ( ! -e "$path" || ( -f "$path" && -s "$path" ) ) ]] \
            || fail "Core file is a symlink, empty or not a regular file: $path"
    done
    # Locally compiled binaries carry a platform/hash stamp; filenames alone are not proof.
    if [[ -f "$stamp" && -f "$active" ]] \
            && read -r platform checksum extra < "$stamp" \
            && [[ "$checksum" =~ ^[0-9a-f]{64}$ && -z "$extra" ]]; then
        actual="$(sha256sum < "$active")" || fail "Could not verify the selected core."
        if [[ "${actual%% *}" == "$checksum" ]]; then
            current="$platform"
        fi
    fi
    if [[ "$current" == "$expected" ]]; then
        echo "The installed core is already prepared for $label; no binary switch is needed."
        return 0
    fi
    [[ "$architecture" == x86_64 ]] || fail "The bundled $label core supports x86_64 only."
    [[ ! -L "$candidate" && ( ! -e "$candidate" || ( -f "$candidate" && -s "$candidate" ) ) ]] \
        || fail "Core file is a symlink, empty or not a regular file: $candidate"
    # A named binary can also identify old distributions without a platform stamp.
    for path in "$core_dir/adb_ubuntu" "$core_dir/adb_rhel8" "$core_dir/adb_rhel9"; do
        [[ -f "$active" && -f "$path" && ! -L "$path" ]] || continue
        if cmp -s -- "$active" "$path"; then
            if [[ "$path" == "$candidate" ]]; then
                echo "The installed core matches $variant; no binary switch is needed."
                return 0
            fi
            [[ -n "$current" ]] || current="$(basename -- "$path")"
        fi
    done
    echo "Detected operating system: $label (${VERSION_ID}); architecture: $architecture."
    if [[ -n "$current" ]]; then
        echo "WARNING: bin/core/adb was prepared for $current, not the current operating system."
    else
        echo "WARNING: The build platform of bin/core/adb cannot be verified."
    fi
    if [[ ! -f "$candidate" ]]; then
        [[ -z "$current" && -f "$active" ]] \
            || fail "$label core not found: $candidate. Install the matching core before running setup."
        echo "WARNING: $variant is not included; checking the existing core's runtime libraries instead."
        check_core_runtime "$active"
        return 0
    fi
    while true; do
        read -r -p "Replace bin/core/adb with $variant for $label? [y/N] " answer \
            || fail "Could not read the core replacement response."
        case "${answer,,}" in
            y|yes) break ;;
            n|no|'')
                fail "Setup stopped without replacing adb. Install a compatible core or run setup again and accept the replacement." ;;
            *) echo "Please answer yes or no." ;;
        esac
    done
    if [[ -f "$active" ]] && grep -aFq COMMUNITY_EDITION "$active"; then
        grep -aFq COMMUNITY_EDITION "$candidate" && ! grep -aFq ENTERPRISE_EDITION "$candidate" \
            || fail "The replacement core is not Community Edition: $candidate"
    fi
    check_core_runtime "$candidate"
    # Preserve both named variants and the previous active core, including locally built cores.
    if [[ -f "$active" ]]; then
        temporary="$(mktemp "$core_dir/.adb-backup.XXXXXXXX")" || fail "Could not create a core backup."
        if ! cp -p -- "$active" "$temporary" || ! mv -fT -- "$temporary" "$core_dir/adb.previous"; then
            rm -f -- "$temporary"
            fail "Could not preserve the previous core as adb.previous."
        fi
    fi
    temporary="$(mktemp "$core_dir/.adb-activate.XXXXXXXX")" || fail "Could not create a temporary core file."
    if ! install -m 0755 -- "$candidate" "$temporary" || ! mv -fT -- "$temporary" "$active"; then
        rm -f -- "$temporary"
        fail "Could not activate the $label core; the previous adb was not replaced."
    fi
    actual="$(sha256sum < "$active")" || fail "Could not calculate the core checksum."
    printf '%s %s\n' "$expected" "${actual%% *}" > "$stamp" || fail "Could not save the selected core platform."
    echo "$variant activated as bin/core/adb. Named core variants are preserved."
}

configure_replication_factor() {
    local answer factor group config_file key separator count
    local -a candidates=() core_files=() yaml_files=()

    while true; do
        read -r -p "Replication factor (1-3) [3]: " answer \
            || fail "Could not read the replication factor."
        factor="${answer:-3}"
        [[ "$factor" =~ ^[1-3]$ ]] && break
        echo "Please enter a replication factor between 1 and 3."
    done

    # Validate every target before editing; never touch the separate Docker configs.
    for group in core master rs; do
        count=0
        case "$group" in
            core)
                candidates=("$script_dir"/bin/core/atalanta*.prop)
                key=rf separator='='
                ;;
            master)
                candidates=("$script_dir"/bin/master*/config/master-configuration.y{ml,aml})
                key=replicationFactor separator=':'
                ;;
            rs)
                candidates=("$script_dir"/bin/rs*/config/region-server-configuration.y{ml,aml})
                key=replicationFactor separator=':'
                ;;
        esac
        for config_file in "${candidates[@]}"; do
            [[ -e "$config_file" || -L "$config_file" ]] || continue
            [[ -f "$config_file" && ! -L "$config_file" && -w "$config_file" ]] \
                || fail "Configuration is not a writable regular file: $config_file"
            [[ "$(grep -Ec "^[[:space:]]*$key[[:space:]]*$separator" "$config_file")" == 1 ]] \
                && grep -Eq "^[[:space:]]*$key[[:space:]]*$separator[[:space:]]*[0-9]+([[:space:]].*)?$" "$config_file" \
                || fail "Expected exactly one numeric $key setting in $config_file"
            if [[ "$group" == core ]]; then
                core_files+=("$config_file")
            else
                yaml_files+=("$config_file")
            fi
            ((count+=1))
        done
        ((count > 0)) || fail "No $group replication configuration files found in $script_dir/bin"
    done

    for config_file in "${core_files[@]}"; do
        sed -i -E "s|^([[:space:]]*rf[[:space:]]*=[[:space:]]*)[0-9]+|\1$factor|" "$config_file" \
            || fail "Could not update $config_file. Fix the error and run setup again."
    done
    for config_file in "${yaml_files[@]}"; do
        sed -i -E "s|^([[:space:]]*replicationFactor[[:space:]]*:[[:space:]]*)[0-9]+|\1$factor|" "$config_file" \
            || fail "Could not update $config_file. Fix the error and run setup again."
    done
    echo "Replication factor $factor configured for all core, master and RegionServer instances."
}

make_files_executable() {
    echo "Making distribution shell scripts executable..."
    find "$script_dir" -type f -name '*.sh' -exec chmod +x {} + || fail "Could not update shell-script permissions."

    if [[ -f "$script_dir/bin/core/adb" ]]; then
        chmod +x "$script_dir/bin/core/adb" || fail "Could not make adb executable."
    fi
}

is_port_in_use() {
    local port="$1"

    if command -v ss >/dev/null 2>&1; then
        ss -ltn "sport = :$port" | grep -q LISTEN
    elif command -v lsof >/dev/null 2>&1; then
        lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
    else
        return 1
    fi
}

set_zk_home() {
    local zk_bin_dir="$1"
    local escaped_dir

    [[ -f "$env_file" ]] || fail "Environment file not found: $env_file"
    escaped_dir=$(printf '%s' "$zk_bin_dir" | sed 's/[&|]/\\\\&/g')
    sed -i "s|^export ZK_HOME=.*|export ZK_HOME=\"$escaped_dir\"|" "$env_file" || fail "Could not update $env_file"
}

set_zk_endpoint() {
    local hostname="$1"
    local port="$2"
    local endpoint="$hostname:$port"
    local escaped_endpoint
    local config_file

    echo "Configuring AtalantaDB to use ZooKeeper at $endpoint..."
    escaped_endpoint=$(printf '%s' "$endpoint" | sed 's/[&|]/\\&/g')
    while IFS= read -r -d '' config_file; do
        sed -i "/<name>atalanta.zookeeper.connection.string<\\/name>/,/<\\/property>/ s|<value>[^<]*</value>|<value>$escaped_endpoint</value>|" "$config_file" \
            || fail "Could not update $config_file"
    done < <(find "$script_dir" -type f -path '*/config/atalanta-site.xml' -print0)

    if [[ -d "$script_dir/bin" ]]; then
        while IFS= read -r -d '' config_file; do
            sed -i -E "/^[[:space:]]*zooKeeperConfiguration:[[:space:]]*$/,/^[^[:space:]#]/ s|^([[:space:]]*connectionString:[[:space:]]*).*$|\\1$escaped_endpoint|" "$config_file" \
                || fail "Could not update $config_file"
        done < <(find "$script_dir/bin" -type f \( -name '*.yml' -o -name '*.yaml' \) -print0)
    fi
}

get_listener_pid() {
    local port="$1"
    local pid

    if command -v lsof >/dev/null 2>&1; then
        pid=$(lsof -t -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | head -n 1)
    elif command -v fuser >/dev/null 2>&1; then
        pid=$(fuser -n tcp "$port" 2>/dev/null | awk '{print $NF}')
    else
        return 1
    fi

    [[ "$pid" =~ ^[0-9]+$ ]] || return 1
    printf '%s\n' "$pid"
}

configure_running_zookeeper() {
    local pid work_dir zk_bin_dir answer

    pid=$(get_listener_pid 2181) || return 1
    if ! command -v pwdx >/dev/null 2>&1; then
        echo "A process is listening on ZooKeeper's default port 2181, but pwdx is unavailable."
        return 1
    fi

    work_dir=$(pwdx "$pid" 2>/dev/null | sed -n 's/^[0-9][0-9]*: //p')
    if [[ -z "$work_dir" ]]; then
        echo "A process is listening on port 2181, but its working directory could not be read."
        return 1
    fi

    if [[ -x "$work_dir/bin/zkServer.sh" ]]; then
        zk_bin_dir="$work_dir/bin"
    elif [[ -x "$work_dir/zkServer.sh" ]]; then
        zk_bin_dir="$work_dir"
    else
        echo "A process is listening on port 2181, but pwdx found no ZooKeeper bin directory: $work_dir"
        return 1
    fi

    echo "A ZooKeeper-compatible listener was found on port 2181 (PID $pid)."
    echo "Its working directory, obtained with pwdx, is: $work_dir"
    read -r -p "Use this running ZooKeeper instance? [y/N] " answer
    case "${answer,,}" in
        y|yes)
            set_zk_home "$zk_bin_dir"
            set_zk_endpoint "localhost" "2181"
            echo "The running ZooKeeper instance was configured."
            return 0
            ;;
        *)
            echo "The running ZooKeeper instance was not selected."
            return 1
            ;;
    esac
}

find_free_admin_port() {
    local port=8080
    while is_port_in_use "$port"; do
        ((port++))
    done
    printf '%s\n' "$port"
}

resolve_downloadable_zookeeper() {
    local listing versions version release url

    echo "Checking available ZooKeeper versions..."
    listing=$(wget -qO- "$zk_download_base/") || fail "Could not read the ZooKeeper release list from $zk_download_base/"
    versions=$(printf '%s\n' "$listing" \
        | grep -oE 'zookeeper-[0-9]+\.[0-9]+\.[0-9]+/' \
        | sed -E 's|^zookeeper-||; s|/$||' \
        | LC_ALL=C sort -Vru)
    [[ -n "$versions" ]] || fail "No stable ZooKeeper releases were found at $zk_download_base/"

    while IFS= read -r version; do
        [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || continue
        release="apache-zookeeper-${version}-bin"
        url="$zk_download_base/zookeeper-$version/$release.tar.gz"
        if wget -q --spider "$url"; then
            zk_version="$version"
            zk_release="$release"
            zk_dir="$script_dir/$release"
            zk_download_url="$url"
            echo "ZooKeeper $zk_version is available for download."
            return 0
        fi
    done <<< "$versions"

    fail "No downloadable ZooKeeper binary archive was found at $zk_download_base/"
}

configure_downloaded_zookeeper() {
    local client_port="$1"
    local admin_port

    admin_port=$(find_free_admin_port)
    mkdir -p "$zk_dir/data" || fail "Could not create ZooKeeper data directory."
    cat > "$zk_dir/conf/zoo.cfg" <<EOF
dataDir=$zk_dir/data
clientPort=$client_port
maxClientCnxns=32000
autopurge.snapRetainCount=3
autopurge.purgeInterval=1
admin.serverPort=$admin_port
EOF
    echo "ZooKeeper configuration created: client port $client_port, AdminServer port $admin_port."
}

download_and_start_zookeeper() {
    local client_port="$1"
    local archive

    command -v wget >/dev/null 2>&1 || fail "wget is required to find and download ZooKeeper."
    command -v tar >/dev/null 2>&1 || fail "tar is required to extract ZooKeeper."
    resolve_downloadable_zookeeper
    archive="$script_dir/$zk_release.tar.gz"

    if [[ ! -d "$zk_dir" ]]; then
        echo "Downloading ZooKeeper $zk_version..."
        wget -O "$archive" "$zk_download_url" || fail "ZooKeeper $zk_version download failed."
        echo "Extracting ZooKeeper..."
        tar -xzf "$archive" -C "$script_dir" || fail "ZooKeeper extraction failed."
    else
        echo "Using the existing ZooKeeper directory: $zk_dir"
    fi

    configure_downloaded_zookeeper "$client_port"
    chmod +x "$zk_dir/bin/zkServer.sh" || fail "Could not make zkServer.sh executable."
    set_zk_home "$zk_dir/bin"
    set_zk_endpoint "localhost" "$client_port"

    echo "Starting ZooKeeper on port $client_port..."
    "$zk_dir/bin/zkServer.sh" start || fail "ZooKeeper could not be started."
}

configure_existing_zookeeper() {
    local zk_bin_dir hostname port

    read -r -p "Enter the path to the ZooKeeper bin directory (contains zkServer.sh): " zk_bin_dir
    [[ -n "$zk_bin_dir" ]] || fail "A ZooKeeper bin directory is required."
    zk_bin_dir="${zk_bin_dir/#\~/$HOME}"
    if [[ -x "$zk_bin_dir/bin/zkServer.sh" ]]; then
        zk_bin_dir="$zk_bin_dir/bin"
    fi
    [[ -x "$zk_bin_dir/zkServer.sh" ]] || fail "zkServer.sh is not executable in: $zk_bin_dir"

    read -r -p "Enter the ZooKeeper hostname or IP address [localhost]: " hostname
    hostname="${hostname:-localhost}"
    read -r -p "Enter the ZooKeeper client port [2181]: " port
    port="${port:-2181}"
    [[ "$port" =~ ^[0-9]{1,5}$ ]] && ((port >= 1 && port <= 65535)) || fail "Invalid ZooKeeper port: $port"

    set_zk_home "$zk_bin_dir"
    set_zk_endpoint "$hostname" "$port"
    echo "Existing ZooKeeper configuration was saved."
}

configure_zookeeper() {
    local answer client_port

    if configure_running_zookeeper; then
        return 0
    fi

    read -r -p "Do you want to download and configure a local ZooKeeper instance? [y/N] " answer
    case "${answer,,}" in
        y|yes)
            while :; do
                read -r -p "Enter the ZooKeeper client port [2181]: " client_port
                client_port="${client_port:-2181}"
                [[ "$client_port" =~ ^[0-9]{1,5}$ ]] && ((client_port >= 1 && client_port <= 65535)) || {
                    echo "Please enter a valid port number between 1 and 65535."
                    continue
                }
                if is_port_in_use "$client_port"; then
                    echo "Port $client_port is already in use. Choose another port."
                    continue
                fi
                break
            done
            download_and_start_zookeeper "$client_port"
            ;;
        *)
            configure_existing_zookeeper
            ;;
    esac
}

start_atalanta() {
    local answer

    cp bin/master1/atalanta-master.jar bin/master2
    cp bin/rs1/atalanta-rs.jar bin/rs2
    cp bin/rs1/atalanta-rs.jar bin/rs3
    cp bin/rs1/atalanta-rs.jar bin/rs4

    read -r -p "Start AtalantaDB? [y/N] " answer
    case "${answer,,}" in
        y|yes)
            echo "Starting AtalantaDB..."
            (
                cd "$script_dir" || exit 1
                bash ./1-start-new.sh
            ) || fail "AtalantaDB startup failed."
            echo "AtalantaDB startup command completed."
            ;;
        *)
            echo "To start AtalantaDB later, run:"
            echo "  cd $script_dir && ./1-start-new.sh"
            ;;
    esac
}

main() {
    select_core_binary
    configure_replication_factor
    configure_zookeeper
    make_files_executable
    start_atalanta

    echo "Setup completed."
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
