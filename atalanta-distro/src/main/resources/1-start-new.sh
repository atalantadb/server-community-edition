#!/bin/bash

# set environment variables
source 99-env.sh

# stop DB
./0-stop.sh

# clean data files, logs and ZooKeeper node
cd $ATALANTA_SCRIPTS
./99-clean-data.sh
./99-clean-logs.sh

# start master 1 & 2
cd $ATALANTA_BIN/master1
./start-master.sh > /dev/null 2>&1
sleep 1
cd $ATALANTA_BIN/master2
./start-master.sh > /dev/null 2>&1
sleep 2

# start rs 1, 2, 3 & 4
cd $ATALANTA_BIN/rs1
./start-rs.sh > /dev/null 2>&1
cd $ATALANTA_BIN/rs2
./start-rs.sh > /dev/null 2>&1
cd $ATALANTA_BIN/rs3
./start-rs.sh > /dev/null 2>&1
cd $ATALANTA_BIN/rs4
./start-rs.sh > /dev/null 2>&1

cd $ATALANTA_HOME

# The start scripts launch their processes in the background. Wait until all
# expected service ports are listening before reporting a successful startup.
is_port_listening() {
  local port="$1"

  if command -v ss >/dev/null 2>&1; then
    ss -ltnH "sport = :$port" 2>/dev/null | grep -q .
  elif command -v lsof >/dev/null 2>&1; then
    lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
  else
    echo "ERROR: Neither ss nor lsof is available to verify service startup." >&2
    return 2
  fi
}

wait_for_services() {
  local timeout=60
  local deadline=$((SECONDS + timeout))
  local service port all_ready
  local -a services=(
    "master1:8301" "master2:8302"
    "rs1:8303" "rs2:8304" "rs3:8305" "rs4:8306"
    "core1:10011" "core2:10022" "core3:10033" "core4:10044"
  )

  echo "Waiting up to ${timeout}s for AtalantaDB services to start..."
  while ((SECONDS < deadline)); do
    all_ready=true
    for service in "${services[@]}"; do
      port="${service#*:}"
      if ! is_port_listening "$port"; then
        all_ready=false
        break
      fi
    done
    [[ "$all_ready" == true ]] && return 0
    sleep 1
  done

  echo "ERROR: AtalantaDB did not start successfully." >&2
  echo "The following services did not become available:" >&2
  for service in "${services[@]}"; do
    port="${service#*:}"
    if ! is_port_listening "$port"; then
      echo "  ${service%%:*} (port $port)" >&2
    fi
  done
  echo "Check the logs in: ./AtalantaDB/logs" >&2
  return 1
}

if wait_for_services; then
  echo -e "Everything is \033[0;32mOK\033[0m. Check: AtalantaDB/logs"
else
  exit 1
fi
