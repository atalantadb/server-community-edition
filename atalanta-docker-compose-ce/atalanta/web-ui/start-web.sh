#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir"

exec java \
    -Datalanta.config=./config/master-configuration.yml \
    -jar atalanta-web-ui.jar \
    --server.port=8333 \
    --atalanta.client.config=./config/atalanta-site.xml "$@"
