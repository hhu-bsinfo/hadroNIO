#!/bin/bash

readonly BIND_ADDRESS=${1}
readonly ITERATIONS=${2}
readonly CONNECTIONS=${3}
readonly START_ITERATION=${4}

for (( i=START_ITERATION; i<ITERATIONS; i++ )); do
    for j in {0..4}; do
        ./bin/hadronio grpc kvs -s -a "${BIND_ADDRESS}" -c $((CONNECTIONS * i))
    done
done
