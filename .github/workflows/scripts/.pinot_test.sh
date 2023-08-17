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

if [ "$RUN_INTEGRATION_TESTS" != false ]; then
  # Integration Tests
  mvn clean install -DskipTests -Dcheckstyle.skip -Dspotless.skip -Denforcer.skip -Dlicense.skip -am -B \
    -pl 'pinot-integration-tests' -T 16 || exit 1
  cd pinot-integration-tests || exit 1
  if [ "$RUN_TEST_SET" == "1" ]; then
    mvn test \
        -P github-actions,custom-cluster-integration-test-suite || exit 1
    mvn test \
        -P github-actions,integration-tests-set-1 && exit 0 || exit 1
  fi
  if [ "$RUN_TEST_SET" == "2" ]; then
    mvn test \
        -P github-actions,integration-tests-set-2 && exit 0 || exit 1
  fi
else
  # Unit Tests
  #   - TEST_SET#1 runs install and test together so the module list must ensure no additional modules were tested
  #     due to the -am flag (include dependency modules)
  if [ "$RUN_TEST_SET" == "1" ]; then
    mvn clean install -DskipTests -am -B -T 16 \
        -Dcheckstyle.skip -Dspotless.skip -Denforcer.skip -Dlicense.skip || exit 1
    mvn test -T 16 \
        -P github-actions,no-integration-tests -Dcheckstyle.skip -Dspotless.skip -Denforcer.skip -Dlicense.skip \
        && exit 0 || exit 1
  fi
  if [ "$RUN_TEST_SET" == "2" ]; then
    mvn clean install -DskipTests -Dcheckstyle.skip -Dspotless.skip -Denforcer.skip -Dlicense.skip -T 16 || exit 1
    mvn test -am -B \
        -pl '!pinot-spi' \
        -pl '!pinot-segment-spi' \
        -pl '!pinot-common' \
        -pl '!pinot-segment-local' \
        -pl '!pinot-core' \
        -pl '!pinot-query-planner' \
        -pl '!pinot-query-runtime' \
        -P github-actions,no-integration-tests \
        -Dspotless.skip -Dcheckstyle.skip -Denforcer.skip -Dlicense.skip \
         && exit 0 || exit 1
  fi
fi

mvn clean > /dev/null
