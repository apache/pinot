#!/bin/bash
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

set -uo pipefail

case "${RUN_TEST_SET:-}" in
  1|2) ;;
  *)
    echo "Unsupported RUN_TEST_SET value: ${RUN_TEST_SET:-unset}"
    exit 1
    ;;
esac

readonly module_dir="${GITHUB_WORKSPACE:-$(pwd)}/pinot-integration-tests"
readonly log_dir="${module_dir}/target-concurrent-logs"
readonly runtime_root="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/pinot-integration-${RUN_TEST_SET}-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-$$"
readonly lane_a_build_dir="${module_dir}/target-lane-a"
readonly lane_b_build_dir="${module_dir}/target-lane-b"
readonly lane_a_tmp_dir="${runtime_root}/lane-a"
readonly lane_b_tmp_dir="${runtime_root}/lane-b"

mkdir -p "${log_dir}" "${lane_a_tmp_dir}" "${lane_b_tmp_dir}" || exit 1

java -version
ifconfig
netstat -i

time_args=(-v)
if [ "$(uname -s)" = "Darwin" ]; then
  time_args=(-l)
fi

# The module-only Maven launchers are capped separately from their Surefire forks. Two 4 GiB forks plus two
# 512 MiB launchers leave memory for native allocations, embedded services, LocalStack, and the runner OS.
read -r -a inherited_maven_opts <<< "${MAVEN_OPTS:-}"
base_maven_opts=()
for inherited_maven_opt in "${inherited_maven_opts[@]}"; do
  case "${inherited_maven_opt}" in
    -B|-ntp|-Xms*|-Xmx*) ;;
    *) base_maven_opts+=("${inherited_maven_opt}") ;;
  esac
done
readonly base_lane_maven_opts="${base_maven_opts[*]} -Xmx512m"

run_lane() {
  local label="$1"
  local profile="$2"
  local build_dir="$3"
  local tmp_dir="$4"
  local port_base="$5"
  local zk_port_base="$6"
  local log_file="${log_dir}/${label}.log"
  local log_root="${build_dir}/logs"
  local lane_maven_opts="${base_lane_maven_opts} -Djava.io.tmpdir=${tmp_dir}"
  local start_time
  local end_time
  local -a pipeline_status
  local status=0

  mkdir -p "${log_root}" || return 1
  start_time="$(date +%s)"
  echo "[${label}] Starting profile ${profile} at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  (
    cd "${module_dir}" || exit 1
    LOG_ROOT="${log_root}" MAVEN_OPTS="${lane_maven_opts}" /usr/bin/time "${time_args[@]}" \
      mvn -B -ntp test jacoco:report@report \
      -P "github-actions,codecoverage,${profile}" \
      "-Dpinot.integration.test.build.directory=${build_dir}" \
      -Dpinot.integration.test.heap.min=1g \
      -Dpinot.integration.test.heap.max=4g \
      "-Dpinot.integration.test.active.processor.args=-XX:ActiveProcessorCount=2" \
      "-Dpinot.integration.test.tmp.directory=${tmp_dir}" \
      "-Dpinot.integration.test.port.base=${port_base}" \
      "-Dpinot.integration.test.zk.port.base=${zk_port_base}"
  ) 2>&1 \
    | sed -u "s/^/[${label}] /" \
    | tee "${log_file}"
  pipeline_status=("${PIPESTATUS[@]}")
  for pipeline_exit in "${pipeline_status[@]}"; do
    if [ "${pipeline_exit}" -ne 0 ]; then
      status="${pipeline_exit}"
      break
    fi
  done
  end_time="$(date +%s)"
  echo "[${label}] Finished with status ${status} after $((end_time - start_time)) seconds"
  return "${status}"
}

print_surefire_dumps() {
  local label="$1"
  local reports_dir="$2/surefire-reports"
  local dump_file

  if [ ! -d "${reports_dir}" ]; then
    echo "[${label}] Surefire reports directory not found: ${reports_dir}"
    return
  fi
  while IFS= read -r dump_file; do
    echo "===== BEGIN ${label}: ${dump_file} ====="
    sed "s/^/[${label}] /" "${dump_file}"
    echo "===== END ${label}: ${dump_file} ====="
  done < <(find "${reports_dir}" -maxdepth 1 -type f \
    \( -name "*.dump" -o -name "*.dumpstream" -o -name "*jvmRun*" \) | sort)
}

run_lane "lane-a" "integration-tests-set-${RUN_TEST_SET}-lane-a" \
  "${lane_a_build_dir}" "${lane_a_tmp_dir}" 20000 12000 &
lane_a_pid=$!
run_lane "lane-b" "integration-tests-set-${RUN_TEST_SET}-lane-b" \
  "${lane_b_build_dir}" "${lane_b_tmp_dir}" 24000 14000 &
lane_b_pid=$!

lane_a_status=0
lane_b_status=0
wait "${lane_a_pid}" || lane_a_status=$?
wait "${lane_b_pid}" || lane_b_status=$?

echo "Concurrent integration result: lane-a=${lane_a_status}, lane-b=${lane_b_status}"
echo "Lane A log: ${log_dir}/lane-a.log"
echo "Lane B log: ${log_dir}/lane-b.log"
echo "Lane A Surefire reports: ${lane_a_build_dir}/surefire-reports"
echo "Lane B Surefire reports: ${lane_b_build_dir}/surefire-reports"

if [ "${lane_a_status}" -ne 0 ]; then
  print_surefire_dumps "lane-a" "${lane_a_build_dir}"
fi
if [ "${lane_b_status}" -ne 0 ]; then
  print_surefire_dumps "lane-b" "${lane_b_build_dir}"
fi
if [ "${lane_a_status}" -ne 0 ] || [ "${lane_b_status}" -ne 0 ]; then
  exit 1
fi
