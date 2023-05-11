#!/bin/bash

readonly BENCHMARK_MODE=${1}
readonly BIND_ADDRESS=${2}
readonly REMOTE_ADDRESS=${3}
readonly MESSAGE_COUNT=${4}
readonly MIN_MESSAGE_SIZE_EXPONENT=${5}
readonly MAX_MESSAGE_SIZE_EXPONENT=${6}
readonly CONNECTIONS=${7}

wait() {
  local seconds=$1

  for (( k=0; k<seconds; k++ )) do
    printf "."
    sleep 1s
  done

  printf "\n"
}

port=3000

for (( i=MIN_MESSAGE_SIZE_EXPONENT; i<=MAX_MESSAGE_SIZE_EXPONENT; i++ )); do
    message_size=$((2 ** i))
    message_count=${MESSAGE_COUNT}
    if [ "${i}" -ge 13 ]; then
      message_count=$((message_count / 2 ** ($i - 12) ))
    fi

    for j in {0..4}; do
        wait 30
        port=$((port + 1))
        ./bin/hadronio netty benchmark "${BENCHMARK_MODE}" -r "${REMOTE_ADDRESS}:${port}" -a "${BIND_ADDRESS}:${port}" -m "${message_count}" -l "${message_size}" -c "${CONNECTIONS}"
    done
done
