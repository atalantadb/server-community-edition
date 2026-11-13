#!/usr/bin/env bash
# Check and optionally install packages required to build adb-core.

set -u

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

build_adb_core() {
    echo "Starting adb-core build..."
    (
        cd "$script_dir" || exit 1
        bash ./compile-CE.sh
    )
}

if [[ ! -r /etc/os-release ]]; then
    echo "Cannot identify the operating system: /etc/os-release is unavailable." >&2
    exit 2
fi

# shellcheck disable=SC1091
. /etc/os-release

os_id="${ID,,}"
os_like="${ID_LIKE:-}"
os_like="${os_like,,}"

family=""
installer=""
packages=()

if [[ "$os_id" == "ubuntu" || "$os_id" == "debian" || "$os_like" == *"debian"* ]]; then
    family="Debian/Ubuntu"
    installer="apt-get"
    packages=(build-essential libssl-dev liblzma-dev)
elif [[ "$os_id" =~ ^(rhel|redhat|centos|rocky|almalinux|fedora|ol)$ || "$os_like" == *"rhel"* || "$os_like" == *"fedora"* ]]; then
    family="Red Hat family"
    packages=(gcc glibc-devel kernel-headers openssl-devel xz-devel)
    if command -v dnf >/dev/null 2>&1; then
        installer="dnf"
    elif command -v yum >/dev/null 2>&1; then
        installer="yum"
    else
        echo "A Red Hat family system was detected, but neither dnf nor yum is available." >&2
        exit 2
    fi
else
    echo "Unsupported operating-system family: ${PRETTY_NAME:-$os_id}." >&2
    echo "Supported families: Debian/Ubuntu and Red Hat Enterprise Linux-compatible distributions." >&2
    exit 2
fi

missing=()
for package in "${packages[@]}"; do
    if [[ "$family" == "Debian/Ubuntu" ]]; then
        if ! dpkg-query -W -f='${db:Status-Status}' "$package" 2>/dev/null | grep -qx 'installed'; then
            missing+=("$package")
        fi
    elif ! rpm -q "$package" >/dev/null 2>&1; then
        missing+=("$package")
    fi
done

echo "Detected operating system: ${PRETTY_NAME:-$family}"
echo "Package family: $family"

if ((${#missing[@]} == 0)); then
    echo "All packages required to build adb-core are installed."
    build_adb_core
    exit $?
fi

echo "The following required packages are missing:"
printf '  %s\n' "${missing[@]}"

if [[ "$installer" == "apt-get" ]]; then
    echo "Installation command:"
    printf '  sudo apt-get update && sudo apt-get install -y'
    printf ' %q' "${missing[@]}"
    printf '\n'
else
    echo "Installation command:"
    printf '  sudo %s install -y' "$installer"
    printf ' %q' "${missing[@]}"
    printf '\n'
fi

echo "Installing packages requires sudo privileges and may prompt for your password."
read -r -p "Do you want to install the missing packages now? [y/N] " answer

case "${answer,,}" in
    y|yes)
        if [[ "$installer" == "apt-get" ]]; then
            sudo apt-get update && sudo apt-get install -y "${missing[@]}"
        else
            sudo "$installer" install -y "${missing[@]}"
        fi
        install_status=$?
        if ((install_status != 0)); then
            echo "Package installation failed; adb-core was not built." >&2
            exit "$install_status"
        fi
        build_adb_core
        exit $?
        ;;
    *)
        echo "To build adb-core successfully, run the installation command shown above and then run this script again."
        exit 1
        ;;
esac
