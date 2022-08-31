#!/bin/bash

readonly BIND_ADDRESS=${1}
readonly REMOTE_ADDRESS=${2}
readonly RECORD_SIZE=${3}
readonly MIN_THREADS=${4}
readonly MAX_THREADS=${5}
readonly BENCHMARK_NAME=${6}
readonly RESULT_FILE=${7}
readonly WORKLOAD=${8}

port=3000

./bin/hadronio grpc kvs -b -r "${REMOTE_ADDRESS}" -a "${BIND_ADDRESS}" -w "${WORKLOAD}" -p LOAD

for (( i=MIN_THREADS; i<=MAX_THREADS; i++ )); do
    for j in {0..4}; do
        port=$((port + 1))
        ./bin/hadronio grpc kvs -b -r "${REMOTE_ADDRESS}" -a "${BIND_ADDRESS}:${port}" -w "${WORKLOAD}" -p RUN -m "${RECORD_SIZE}" -t "${i}" -o "${RESULT_FILE}" -n "${BENCHMARK_NAME}" -i "${j}"
    done
done
