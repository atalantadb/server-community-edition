startRsDelaySec=10
rsName="rs2"
while (( "$#" )); do
  if [[ $1 == "--start.rs.delay.sec" ]] || [[ $1 == "-d" ]]; then
    shift
    startRsDelaySec=$1
  fi
  if [[ $1 == "--rs.name" ]] || [[ $1 == "-n" ]]; then
    shift
    rsName=$1
  fi
  shift
done

echo "$(date +'%Y-%m-%d %H:%M:%S') - Check delay is: $startRsDelaySec"
echo "$(date +'%Y-%m-%d %H:%M:%S') - RS name is $rsName"

while true; do
  result=$(ps ax | grep rs | grep "$rsName")

  rsAlive=false
  while IFS= read -r line; do
    if ( grep -q "java"  <<< "$line") && (grep -q '\-jar'  <<< "$line") && (grep -q "atalanta-rs.jar"  <<< "$line"); then
      rsAlive=true
    fi
  done <<< "$result"

  echo "$(date +'%Y-%m-%d %H:%M:%S') - rsAlive=$rsAlive"
  if ! $rsAlive; then
    echo "$(date +'%Y-%m-%d %H:%M:%S') - RS was killed. RS will be started after ${startRsDelaySec}secs."
    sleep "$startRsDelaySec"
    ./start-rs.sh > /dev/null 2>&1
  fi

  sleep "$startRsDelaySec"
done
