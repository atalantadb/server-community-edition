#!/bin/bash

# Функция для проверки доступности порта
find_free_port() {
  local start_port=$1
  local port=$start_port

  while true; do
    # Проверяем, занят ли порт
    if ! lsof -iTCP:$port -sTCP:LISTEN > /dev/null 2>&1; then
      echo $port
      return
    fi
    port=$((port + 1))
  done
}

# Начальный порт
BASE_PORT=8333

# Найти свободный порт
FREE_PORT=$(find_free_port $BASE_PORT)

echo "Using port $FREE_PORT for atalanta-web-ui"

# Завершение текущих процессов atalanta-web-ui
ps ax | grep atalanta-web-ui | grep -v grep | awk '{print $1}' | xargs kill > /dev/null 2>&1

# Запуск приложения на найденном порту
nohup java -jar atalanta-web-ui.jar \
  --server.port=$FREE_PORT \
  --atalanta.config=../master1/config/master-configuration.yml \
  --atalanta.client.config=../shell/config/atalanta-site.xml > /dev/null 2>&1 &

# Сохранение PID запущенного процесса
pgrep -fo atalanta-web-ui.jar > ./atalanta-web-ui.pid
echo $FREE_PORT > ./web-ui-port.txt
echo "atalanta-web-ui started on port $FREE_PORT (saved to ./web-ui-port.txt). PID saved in ./atalanta-web-ui.pid"

