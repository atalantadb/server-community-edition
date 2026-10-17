#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir"

# Use the container hostname, not the legacy "docker" profile's localhost.
exec java \
    -Dspring.profiles.active=default \
    -Dcom.sun.management.jmxremote \
    -Dcom.sun.management.jmxremote.port=1199 \
    -Dcom.sun.management.jmxremote.rmi.port=1199 \
    -Dcom.sun.management.jmxremote.ssl=false \
    -Dcom.sun.management.jmxremote.registry.ssl=false \
    -Dcom.sun.management.jmxremote.authenticate=false \
    -Dcom.sun.management.jmxremote.local.only=false \
    "-Djava.rmi.server.hostname=$(hostname)" \
    -Datalanta.config=./config/master-configuration.yml \
    -jar atalanta-master.jar "$@"
