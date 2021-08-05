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

# The overhead of building repo is about 12 min.
mvn install -DskipTests -T 1C

if [ "$RUN_INTEGRATION_TESTS" != false ]; then
  # Integration Tests
  if [ "$RUN_TEST_SET" == "1" ]; then
    mvn test -B -pl 'pinot-integration-tests' -Dtest='R*Test,B*Test' -P github-actions,integration-tests-only && exit 0 || exit 1
  fi
  if [ "$RUN_TEST_SET" == "2" ]; then
    mvn test -B -pl 'pinot-integration-tests' -Dtest='C*Test,M*Test' -P github-actions,integration-tests-only && exit 0 || exit 1
  fi
  if [ "$RUN_TEST_SET" == "3" ]; then
    mvn test -B -pl 'pinot-integration-tests' -Dtest='A*Test,L*Test,S*Test' -P github-actions,integration-tests-only && exit 0 || exit 1
  fi
  if [ "$RUN_TEST_SET" == "4" ]; then
    mvn test -B -pl 'pinot-integration-tests' -Dtest='!A*Test,!B*Test,!C*Test,!L*Test,!M*Test,!R*Test,!S*Test' -P github-actions,integration-tests-only && exit 0 || exit 1
  fi
else
  # Unit Tests
  if [ "$RUN_TEST_SET" == "1" ]; then
    mvn test -B -pl 'pinot-controller'  -pl 'pinot-broker'  -pl 'pinot-server'  -pl 'pinot-segment-local'  -P github-actions,no-integration-tests && exit 0 || exit 1
  fi
  if [ "$RUN_TEST_SET" == "2" ]; then
    mvn test -B -pl '!pinot-controller' -pl '!pinot-broker' -pl '!pinot-server' -pl '!pinot-segment-local' -P github-actions,no-integration-tests && exit 0 || exit 1
  fi
fi
