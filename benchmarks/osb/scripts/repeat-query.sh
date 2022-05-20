#!/bin/bash

# Repeats a set of OSB benchmarks and save results to a file
# ./repeat-query.sh 10 results/run ${URL}
CYCLES=$1
OUTPUT_PATH=$2
URL=$3

for i in $(eval echo "{1..$CYCLES}")
do
   echo "Starting Run $i"
   opensearch-benchmark execute_test \
      --target-hosts $URL:80 \
      --workload-path ./workload.json \
      --workload-params ./params/no-train-params.json \
      --test-procedure=no-train-test \
      --pipeline benchmark-only \
      --results-file "${OUTPUT_PATH}_${i}.csv"
done
