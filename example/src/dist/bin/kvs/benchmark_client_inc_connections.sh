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
readonly OPERATION_COUNT=${10}
readonly LOAD=${11}

wait() {
  local seconds=$1

  for (( k=0; k<seconds; k++ )) do
    printf "."
    sleep 1s
  done

  printf "\n"
}

for (( i=MIN_THREADS; i<=MAX_THREADS; i=(i+STEPPING)/STEPPING*STEPPING )); do
    sed -i "/^operationcount=/c\operationcount=$((OPERATION_COUNT * i))" "${WORKLOAD}"
    for j in {0..4}; do
        wait 10
        if [ "${LOAD}" == "true" ]; then
          ./bin/hadronio grpc kvs -b -r "${REMOTE_ADDRESS}" -a "${BIND_ADDRESS}" -w "${WORKLOAD}" -p LOAD
        fi
        ./bin/hadronio grpc kvs -b -r "${REMOTE_ADDRESS}" -a "${BIND_ADDRESS}" -w "${WORKLOAD}" -p RUN -m "${RECORD_SIZE}" -t "${i}" -o "${RESULT_FILE}" -n "${BENCHMARK_NAME}" -i "${j}" -l
    done
done
