#!/bin/bash

readonly BENCHMARK_MODE=${1}
readonly BIND_ADDRESS=${2}
readonly REMOTE_ADDRESS=${3}
readonly MESSAGE_COUNT=${4}
readonly REQUEST_SIZE=${5}
readonly ANSWER_SIZE=${6}
readonly MIN_CONNECTIONS=${7}
readonly MAX_CONNECTIONS=${8}
readonly STEPPING=${9}
readonly THRESHOLD=${9}
readonly RESULT_FILE=${10}
readonly BENCHMARK_NAME=${11}

wait() {
  local seconds=$1

  for (( k=0; k<seconds; k++ )) do
    printf "."
    sleep 1s
  done

  printf "\n"
}

port=3000

for (( i=MIN_CONNECTIONS; i<=MAX_CONNECTIONS; i=(i+STEPPING)/STEPPING*STEPPING )); do
    for j in {0..4}; do
        wait 30
        port=$((port + 1))
        ./bin/hadronio grpc kvs -r "${REMOTE_ADDRESS}:${port}" -a "${BIND_ADDRESS}" -m "${MESSAGE_COUNT}" -rs "${REQUEST_SIZE}" -as "${ANSWER_SIZE}" -c "${i}" -o "${RESULT_FILE}" -n "${BENCHMARK_NAME}" -i "${j}" $([ "${BENCHMARK_MODE}" = "latency" ] && echo "-b") $([ "${BENCHMARK_MODE}" = "throughput" ] && echo "-t ${THRESHOLD}")
    done
done
