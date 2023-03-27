#!/bin/bash

set -euo pipefail

function usage() {
  cat <<EOF
manage_storm.sh [-w workers.txt]
 -w
   path to a text file containing the hostnames of your worker nodes on separate lines (REQUIRED)
EOF
}

while getopts "hw:" arg; do
  # shellcheck disable=SC2220
  case $arg in
    h)
      usage
      exit 0
      ;;
    w)
      WORKERS=$OPTARG
      ;;
  esac
done
while read MACHINE; do
  echo Listing logs for $MACHINE
  ssh -n "$MACHINE" "ls '/tmp/storm_ericlipe/workers-artifacts'" || true
done < "$WORKERS"
