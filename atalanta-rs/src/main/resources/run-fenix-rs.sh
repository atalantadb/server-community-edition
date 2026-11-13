./start-rs.sh > /dev/null 2>&1

# In some environments the process PID is not shown unless the command is given explicitly:
#   /bin/bash "./fenix-rs-restarter.sh" "$@" > fenix-rs-restarter.log &
./fenix-rs-restarter.sh "$@" > fenix-rs-restarter.log &
echo "$(date +'%Y-%m-%d %H:%M:%S') - Fenix RS started"
