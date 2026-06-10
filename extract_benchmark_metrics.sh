#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


# Script to extract average time and gc.alloc.rate.norm from JMH benchmark results
# Usage: ./extract_benchmark_metrics.sh input.csv > output.csv

INPUT_FILE="${1:-jmh-result.csv}"

if [[ ! -f "$INPUT_FILE" ]]; then
    echo "Error: File '$INPUT_FILE' not found" >&2
    exit 1
fi

# Print CSV header
echo '"Benchmark","Param: _distinctKeys","Param: _type","Average (ms/op)","GC Alloc Rate Norm (B/op)"'

# Process the CSV file using a more sophisticated approach
# Read line by line and properly parse CSV fields
tail -n +2 "$INPUT_FILE" | while IFS= read -r line; do
    # Use Python for proper CSV parsing
    python3 -c "
import csv
import sys

line = '''$line'''
reader = csv.reader([line])
for row in reader:
    if len(row) >= 9:
        benchmark = row[0]
        score = row[4]
        distinctKeys = row[7]
        paramType = row[8]

        # Check if this is the base benchmark (no suffix)
        if ':' not in benchmark:
            print(f'AVG|{benchmark}|{distinctKeys}|{paramType}|{score}')
        # Check if this is gc.alloc.rate.norm
        elif benchmark.endswith(':gc.alloc.rate.norm'):
            base_benchmark = benchmark.replace(':gc.alloc.rate.norm', '')
            print(f'NORM|{base_benchmark}|{distinctKeys}|{paramType}|{score}')
"
done | awk -F'|' '
{
    key = $2 "|" $3 "|" $4
    if ($1 == "AVG") {
        avg[key] = $5
        benchmark_name[key] = $2
        distinct_keys[key] = $3
        param_type[key] = $4
    } else if ($1 == "NORM") {
        norm[key] = $5
    }
}
END {
    for (key in avg) {
        if (key in norm) {
            printf "\"%s\",%s,%s,%s,%s\n", benchmark_name[key], distinct_keys[key], param_type[key], avg[key], norm[key]
        }
    }
}
' | sort -t',' -k2,2n -k3,3

