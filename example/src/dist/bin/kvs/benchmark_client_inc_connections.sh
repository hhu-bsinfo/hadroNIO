#!/bin/bash

readonly BIND_ADDRESS=${1}
readonly REMOTE_ADDRESS=${2}
readonly RECORD_SIZE=${3}
readonly MIN_THREADS=${4}
readonly MAX_THREADS=${5}
readonly STEPPING=${6}
readonly BENCHMARK_NAME=${7}
readonly RESULT_FILE=${8}
readonly WORKLOAD=${9}

wait() {
  local seconds=$1

  for (( i=0; i<$seconds; i++ )) do
    printf "."
    sleep 1s
  done

  printf "\n"
}

port=3000

for (( i=MIN_THREADS; i<=MAX_THREADS; i=(i+STEPPING)/STEPPING*STEPPING )); do
    for j in {0..4}; do
        wait 30
        port=$((port + 1))
        ./bin/hadronio grpc kvs -b -r "${REMOTE_ADDRESS}:${port}" -a "${BIND_ADDRESS}" -w "${WORKLOAD}" -p LOAD
        ./bin/hadronio grpc kvs -b -r "${REMOTE_ADDRESS}:${port}" -a "${BIND_ADDRESS}" -w "${WORKLOAD}" -p RUN -m "${RECORD_SIZE}" -t "${i}" -o "${RESULT_FILE}" -n "${BENCHMARK_NAME}" -i "${j}"
    done
done
