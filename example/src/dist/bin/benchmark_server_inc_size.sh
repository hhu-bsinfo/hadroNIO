#!/bin/bash

readonly BENCHMARK_MODE=${1}
readonly BIND_ADDRESS=${2}
readonly MESSAGE_COUNT=${3}
readonly MIN_MESSAGE_SIZE_EXPONENT=${4}
readonly MAX_MESSAGE_SIZE_EXPONENT=${5}
readonly CONNECTIONS=${6}
readonly RESULT_FILE=${7}
readonly BENCHMARK_NAME=${8}
readonly THRESHOLD=${9}

port=3000

for (( i=MIN_MESSAGE_SIZE_EXPONENT; i<=MAX_MESSAGE_SIZE_EXPONENT; i++ )); do
    message_size=$((2 ** i))
    message_count=${MESSAGE_COUNT}
    if [ "${i}" -ge 13 ]; then
      message_count=$((message_count / 2 ** (i - 12) ))
    fi

    for j in {0..4}; do
        port=$((port + 1))
        ./bin/hadronio netty benchmark "${BENCHMARK_MODE}" -s -a "${BIND_ADDRESS}:${port}" -m "${message_count}" -l "${message_size}" -c "${CONNECTIONS}" -o "${RESULT_FILE}" -n "${BENCHMARK_NAME}" -i "${j}" $([ "${BENCHMARK_MODE}" = "throughput" ] && echo "-t ${THRESHOLD}")
    done
done
