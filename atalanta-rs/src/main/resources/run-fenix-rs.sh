./start-rs.sh > /dev/null 2>&1

# В некоторых условиях не отображается PID процесса в случае, если не указать команду для запуска:
#   /bin/bash "./fenix-rs-restarter.sh" "$@" > fenix-rs-restarter.log &
./fenix-rs-restarter.sh "$@" > fenix-rs-restarter.log &
echo "$(date +'%Y-%m-%d %H:%M:%S') - Fenix RS started"