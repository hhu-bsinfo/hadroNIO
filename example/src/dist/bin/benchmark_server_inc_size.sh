#!/bin/bash

readonly BENCHMARK_MODE=$1
readonly BIND_ADDRESS=$2
readonly MESSAGE_COUNT=$3
readonly CONNECTIONS=$4
readonly RESULT_FILE=$5
readonly BENCHMARK_NAME=$6
readonly THRESHOLDS=("1024" "1024" "1024" "1024" "1024" "1024" "1024" "256" "256" "256" "256" "256" "64" "64" "16" "4" "4" "4" "1" "1" "1")

port=3000

for i in {0..20}; do
    message_size=$((2 ** $i))
    if [ $i -ge 13 ]; then
        message_count=$((message_count / 2))
    fi

    for j in {0..4}; do
        port=$((port + 1))
        ./bin/hadronio netty benchmark "${BENCHMARK_MODE}" -s -a "${BIND_ADDRESS}:${port}" -m "${MESSAGE_COUNT}" -l "${message_size}" -c "${CONNECTIONS}" -o "${RESULT_FILE}" -n "${BENCHMARK_NAME}" -i "${j}" $([ "${BENCHMARK_MODE}" = "throughput" ] && echo "-t ${THRESHOLDS[$i]}")
    done
done
