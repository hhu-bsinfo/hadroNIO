#!/bin/bash

readonly BENCHMARK_MODE=${1}
readonly BIND_ADDRESS=${2}
readonly REMOTE_ADDRESS=${3}
readonly MESSAGE_COUNT=${4}
readonly MESSAGE_SIZE=${5}
readonly MIN_CONNECTIONS=${6}
readonly MAX_CONNECTIONS=${7}
readonly STEPPING=${8}

wait() {
  local seconds=$1

  for (( k=0; k<$seconds; k++ )) do
    printf "."
    sleep 1s
  done

  printf "\n"
}

port=3000

for (( i=MIN_CONNECTIONS; i<=MAX_CONNECTIONS; i=(i+STEPPING)/STEPPING*STEPPING )); do
    for j in {0..4}; do
        wait 30
        port=$((port + 1))
        ./bin/hadronio netty benchmark "${BENCHMARK_MODE}" -r "${REMOTE_ADDRESS}:${port}" -a "${BIND_ADDRESS}:${port}" -m "${MESSAGE_COUNT}" -l "${MESSAGE_SIZE}" -c "${i}"
    done
done
