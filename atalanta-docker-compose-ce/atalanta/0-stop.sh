#!/usr/bin/env bash
# Stop the AtalantaDB Docker Compose environment without deleting its data.

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=compose-common.sh
source "$script_dir/compose-common.sh"
ce_require_compose

echo "Stopping the AtalantaDB Community Edition Docker environment..."
ce_compose stop || ce_fail "CE shutdown failed. Check the container logs."
ce_compose ps --all || ce_fail "Cannot read the CE container status."
echo "AtalantaDB Community Edition is stopped. Docker volumes and database data were preserved."
