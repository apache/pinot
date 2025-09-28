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

# Ensure Maven uses tuned GC options in GitHub Actions
export MAVEN_OPTS="${MAVEN_OPTS:-} -XX:+UseG1GC -XX:-G1UseAdaptiveIHOP -XX:InitiatingHeapOccupancyPercent=60 -XX:G1PeriodicGCInterval=2000 -XX:MaxGCPauseMillis=200"

# Unit Tests
#   - TEST_SET#1 runs install and test together so the module list must ensure no additional modules were tested
#     due to the -am flag (include dependency modules)
#   - tests for pinot-plugins should not be ran multi-threaded
if [ "$RUN_TEST_SET" == "1" ]; then
  mvn test \
      -DargLine="-XX:+UseG1GC -XX:-G1UseAdaptiveIHOP -XX:InitiatingHeapOccupancyPercent=60 -XX:G1PeriodicGCInterval=2000 -XX:MaxGCPauseMillis=200" \
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
  mvn test \
    -DargLine="-XX:+UseG1GC -XX:-G1UseAdaptiveIHOP -XX:InitiatingHeapOccupancyPercent=60 -XX:G1PeriodicGCInterval=2000 -XX:MaxGCPauseMillis=200" \
    -pl '!pinot-spi' \
    -pl '!pinot-segment-spi' \
    -pl '!pinot-common' \
    -pl '!pinot-core' \
    -pl '!pinot-query-planner' \
    -pl '!pinot-query-runtime' \
    -pl '!:pinot-yammer' \
    -P github-actions,codecoverage,no-integration-tests || exit 1
fi

mvn jacoco:report-aggregate@report -P codecoverage || exit 1
