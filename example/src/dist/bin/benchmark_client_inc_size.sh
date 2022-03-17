#!/bin/bash

readonly BENCHMARK_MODE=${1}
readonly BIND_ADDRESS=${2}
readonly REMOTE_ADDRESS=${3}
readonly MESSAGE_COUNT=${4}
readonly MIN_MESSAGE_SIZE_EXPONENT=${5}
readonly MAX_MESSAGE_SIZE_EXPONENT=${6}
readonly CONNECTIONS=${7}
readonly PIN_THREADS=${8}

port=3000

for (( i=MIN_MESSAGE_SIZE_EXPONENT; i<=MAX_MESSAGE_SIZE_EXPONENT; i++ )); do
    message_size=$((2 ** i))
    message_count=${MESSAGE_COUNT}
    if [ "${i}" -ge 13 ]; then
      message_count=$((message_count / 2 ** ($i - 12) ))
    fi

    for j in {0..4}; do
        sleep 10s
        port=$((port + 1))
        ./bin/hadronio netty benchmark "${BENCHMARK_MODE}" -r "${REMOTE_ADDRESS}:${port}" -a "${BIND_ADDRESS}:${port}" -m "${message_count}" -l "${message_size}" -c "${CONNECTIONS}" $([ "${PIN_THREADS}" = "true" ] && echo "-p")
    done
done
