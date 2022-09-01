#!/bin/bash

readonly BIND_ADDRESS=${1}
readonly ANSWER_SIZE=${2}
readonly ITERATIONS=${3}

port=3000

for (( i=0; i<ITERATIONS; i++ )); do
    for j in {0..4}; do
        port=$((port + 1))
        ./bin/hadronio grpc benchmark -s -a "${BIND_ADDRESS}:${port}" -as "${ANSWER_SIZE}"
    done
done
