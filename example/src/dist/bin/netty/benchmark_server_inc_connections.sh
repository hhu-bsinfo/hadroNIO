#!/bin/bash

readonly BENCHMARK_MODE=${1}
readonly BIND_ADDRESS=${2}
readonly MESSAGE_COUNT=${3}
readonly MESSAGE_SIZE=${4}
readonly MIN_CONNECTIONS=${5}
readonly MAX_CONNECTIONS=${6}
readonly STEPPING=${7}
readonly THRESHOLD=${8}
readonly RESULT_FILE=${9}
readonly BENCHMARK_NAME=${10}

port=3000

for (( i=MIN_CONNECTIONS; i<=MAX_CONNECTIONS; i=(i+STEPPING)/STEPPING*STEPPING )); do
    for j in {0..4}; do
        port=$((port + 1))
        ./bin/hadronio netty benchmark "${BENCHMARK_MODE}" -s -a "${BIND_ADDRESS}:${port}" -m "${MESSAGE_COUNT}" -l "${MESSAGE_SIZE}" -c "${i}" -o "${RESULT_FILE}" -n "${BENCHMARK_NAME}" -i "${j}" $([ "${BENCHMARK_MODE}" = "throughput" ] && echo "-t ${THRESHOLD}")
    done
done
