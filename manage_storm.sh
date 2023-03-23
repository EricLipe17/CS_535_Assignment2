#!/bin/bash

set -euo pipefail

function usage() {
  cat <<EOF
manage_storm.sh [-s] [-k] [-z zookeeper] [-n nimbus] [-w workers.txt]
 -s
   start the storm cluster (do not use in tandem with -k)
 -k
   kill the storm cluster (do not use in tandem with -s)
 -z
   hostname of your zookeeper namenode (REQUIRED)
 -n
   hostname of your nimbus node (REQUIRED)
 -w
   path to a text file containing the hostnames of your worker nodes on separate lines (REQUIRED)
EOF
}

if [[ $# -ne 7 ]]; then
  echo "Missing/extra arguments. You should only use four switches with their arguments."
  usage
  exit 0
fi

while getopts "hsk:z:n:w:" arg; do
  # shellcheck disable=SC2220
  case $arg in
    h)
      usage
      exit 0
      ;;
    s)
      ZOOKEEPER_CMD="supervisord -c ~/stormConf/zk-supervisord.conf"
      NIMBUS_CMD="supervisord -c ~/stormConf/nimbus-supervisord.conf"
      WORKER_CMD="supervisord -c ~/stormConf/worker-supervisord.conf"
      ;;
    k)
      ZOOKEEPER_CMD="pkill supervisord"
      WORKER_CMD=$ZOOKEEPER_CMD
      NIMBUS_CMD=$ZOOKEEPER_CMD
      ;;
    z)
      ZOOKEEPER=$OPTARG
      ;;
    n)
      NIMBUS=$OPTARG
      ;;
    w)
      WORKERS=$OPTARG
      ;;
  esac
done

ssh "$ZOOKEEPER" "$ZOOKEEPER_CMD"
ssh "$NIMBUS" "$NIMBUS_CMD"

while read MACHINE; do
  ssh -n "$MACHINE" "$WORKER_CMD"
done < "$WORKERS"

