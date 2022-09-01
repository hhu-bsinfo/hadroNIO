#!/bin/bash

readonly BIND_ADDRESS=${1}
readonly ITERATIONS=${2}

port=3000

for (( i=0; i<ITERATIONS; i++ )); do
    for j in {0..4}; do
        port=$((port + 1))
        ./bin/hadronio grpc kvs -s -a "${BIND_ADDRESS}:${port}"
    done
done
