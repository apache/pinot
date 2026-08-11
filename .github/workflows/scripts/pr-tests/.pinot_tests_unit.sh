#!/bin/bash -x
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

# Java version
java -version

# Check network
ifconfig
netstat -i

# Unit Tests
#   - Both test sets run plain `mvn test` (no install, no -am): the modules were already built
#     and installed by .pinot_tests_build.sh, so only the modules listed here are tested.
#
# Parallelism / memory:
#   - UNIT_TEST_FORK_COUNT (default 3) sets surefire forkCount so test *classes* run in
#     separate parallel JVMs (reuseForks=false keeps one class per JVM). This is
#     process-level isolation, not TestNG intra-JVM threading, so tests that were unsafe
#     to run multi-threaded within a single JVM (e.g. pinot-plugins) are unaffected.
#     Cross-fork resource collisions (ZK/controller ports, temp dirs) are avoided by
#     offsetting per surefire.forkNumber; embedded Kafka clusters use ephemeral ports.
#     This is the main lever for shortening the unit-test phase. 3 forks on the 4-vCPU
#     runner keeps a core free for the Maven reactor / GC while test JVMs spend much of
#     their time blocked on ZK/Helix/socket startup, so the extra fork still pays off.
#   - UNIT_TEST_FORK_HEAP (default 2500m) caps per-fork heap so N forks fit in the
#     runner's memory (N * heap + the mvn JVM must stay under the runner's RAM).
#   - UNIT_TEST_RERUN_COUNT (default 0) retries a failing test before failing the build. Left at 0
#     because the load-sensitive flaky tests parallel forks exposed are fixed at the root cause
#     (SegmentPreProcessorTest mtime granularity, LuceneMutableTextIndexTest NRT-refresh wait). It
#     remains overridable as an escape hatch if a new flake appears, but is intentionally not a
#     standing default so real failures are never masked.
UNIT_TEST_FORK_COUNT="${UNIT_TEST_FORK_COUNT:-3}"
# 2500m/fork: 3 forks * 2500m + the 2g Maven JVM (~9.5g) stays well under the runner's 16g.
UNIT_TEST_FORK_HEAP="${UNIT_TEST_FORK_HEAP:-2500m}"
UNIT_TEST_RERUN_COUNT="${UNIT_TEST_RERUN_COUNT:-0}"
# Coverage adds ~30% to the test phase (JaCoCo agent per fork + aggregate report). Keep it on by
# default to preserve Codecov behavior; set RUN_CODECOVERAGE=false (e.g. on PRs) to trade coverage
# for a faster run.
RUN_CODECOVERAGE="${RUN_CODECOVERAGE:-true}"
# Fork-scope the JaCoCo exec file (jacoco-<forkNumber>.exec) so parallel forks don't append to
# one shared jacoco.exec and corrupt coverage. Only the unit lane sets this; other lanes keep
# the default empty suffix (target/jacoco.exec).
FORK_OPTS="-Dunit.test.fork.count=${UNIT_TEST_FORK_COUNT} -Dunit.test.fork.heap=${UNIT_TEST_FORK_HEAP} -Dunit.test.rerun.count=${UNIT_TEST_RERUN_COUNT} -Djacoco.exec.suffix=-\${surefire.forkNumber}"
if [ "$RUN_CODECOVERAGE" == "true" ]; then
  COVERAGE_PROFILE=",codecoverage"
else
  COVERAGE_PROFILE=""
fi
if [ "$RUN_TEST_SET" == "1" ]; then
  # pinot-segment-local's tests run in set #2 to balance pinot-core's longer test time in this
  # shard against set #2's longer build. It remains built in set #1 as a pinot-core dependency.
  # No -am on this command, so only the listed modules test.
  mvn test ${FORK_OPTS} \
      -pl 'pinot-spi' \
      -pl 'pinot-segment-spi' \
      -pl 'pinot-common' \
      -pl ':pinot-yammer' \
      -pl 'pinot-core' \
      -pl 'pinot-query-planner' \
      -pl 'pinot-query-runtime' \
      -P github-actions,no-integration-tests${COVERAGE_PROFILE} || exit 1
fi
if [ "$RUN_TEST_SET" == "2" ]; then
  mvn test ${FORK_OPTS} \
    -pl '!pinot-spi' \
    -pl '!pinot-segment-spi' \
    -pl '!pinot-common' \
    -pl '!pinot-core' \
    -pl '!pinot-query-planner' \
    -pl '!pinot-query-runtime' \
    -pl '!:pinot-yammer' \
    -P github-actions,no-integration-tests${COVERAGE_PROFILE} || exit 1
fi

# Aggregate coverage across all per-fork exec files (jacoco-*.exec) written under forkCount>1,
# while still matching the single-fork jacoco.exec produced by non-parallel runs. Skipped when
# coverage is disabled.
if [ "$RUN_CODECOVERAGE" == "true" ]; then
  mvn jacoco:report-aggregate@report -P codecoverage \
    -Djacoco.dataFileIncludes='**/target/jacoco-*.exec,**/target/jacoco.exec' || exit 1
fi
