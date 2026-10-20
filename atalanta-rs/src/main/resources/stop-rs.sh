# get RS name from arguments
rsName="rs"
while (( "$#" )); do
  if [[ $1 == "--rs.name" ]] || [[ $1 == "-n" ]]; then
    shift
    rsName=$1
  fi
  shift
done

# kill fenix restarter
ps ax | grep 'fenix-rs-restarter.sh' | grep -v 'grep' | awk '{print $1}' | xargs kill
echo "$(date +'%Y-%m-%d %H:%M:%S') - fenix process killed."

# kill RS
ps ax | grep 'rs' | grep "${rsName}" | grep -v 'grep' | awk '{print $1}' | xargs kill
echo "$(date +'%Y-%m-%d %H:%M:%S') - RS killed."
