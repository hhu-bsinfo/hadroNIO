#!/bin/bash

readonly BENCHMARK_MODE=$1
readonly BIND_ADDRESS=$2
readonly REMOTE_ADDRESS=$3
readonly MESSAGE_COUNT=$4
readonly CONNECTIONS=$5

port=3000

for i in {0..20}; do
    message_size=$((2 ** $i))
    if [ $i -ge 13 ]; then
        message_count=$((message_count / 2))
    fi

    for j in {0..4}; do
        sleep 10s
        port=$((port + 1))
        ./bin/hadronio netty benchmark "${BENCHMARK_MODE}" -r "${REMOTE_ADDRESS}:${port}" -a "${BIND_ADDRESS}:${port}" -m "${MESSAGE_COUNT}" -l "${message_size}" -c "${CONNECTIONS}"
    done
done
