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
#   - TEST_SET#1 runs install and test together so the module list must ensure no additional modules were tested
#     due to the -am flag (include dependency modules)
#
# Parallelism / memory:
#   - UNIT_TEST_FORK_COUNT (default 2) sets surefire forkCount so test *classes* run in
#     separate parallel JVMs (reuseForks=false keeps one class per JVM). This is
#     process-level isolation, not TestNG intra-JVM threading, so tests that were unsafe
#     to run multi-threaded within a single JVM (e.g. pinot-plugins) are unaffected.
#     Cross-fork resource collisions (ZK/controller ports, temp dirs) are avoided by
#     offsetting per surefire.forkNumber; embedded Kafka clusters use ephemeral ports.
#     This is the main lever for shortening the unit-test phase.
#   - UNIT_TEST_FORK_HEAP (default 3g) caps per-fork heap so N forks fit in the
#     runner's memory (N * heap + the mvn JVM must stay under the runner's RAM).
UNIT_TEST_FORK_COUNT="${UNIT_TEST_FORK_COUNT:-2}"
# 3g/fork: 2 forks * 3g + the 2g Maven JVM stays well under the runner's 16g while leaving
# heap headroom for memory-heavy modules (e.g. pinot-segment-local) that previously had 4g.
UNIT_TEST_FORK_HEAP="${UNIT_TEST_FORK_HEAP:-3g}"
FORK_OPTS="-Dunit.test.fork.count=${UNIT_TEST_FORK_COUNT} -Dunit.test.fork.heap=${UNIT_TEST_FORK_HEAP}"
if [ "$RUN_TEST_SET" == "1" ]; then
  mvn test ${FORK_OPTS} \
      -pl 'pinot-spi' \
      -pl 'pinot-segment-spi' \
      -pl 'pinot-common' \
      -pl ':pinot-yammer' \
      -pl 'pinot-core' \
      -pl 'pinot-query-planner' \
      -pl 'pinot-query-runtime' \
      -P github-actions,codecoverage,no-integration-tests || exit 1
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
    -P github-actions,codecoverage,no-integration-tests || exit 1
fi

# Aggregate coverage across all per-fork exec files (jacoco-*.exec) written under forkCount>1,
# while still matching the single-fork jacoco.exec produced by non-parallel runs.
mvn jacoco:report-aggregate@report -P codecoverage \
  -Djacoco.dataFileIncludes='**/target/jacoco-*.exec,**/target/jacoco.exec' || exit 1
