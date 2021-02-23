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

# Only run tests for JDK 8
if [ "$TRAVIS_JDK_VERSION" != 'oraclejdk8' ]; then
  echo 'Skip tests for version other than oracle jdk8.'
  exit 0
fi

passed=0

KAFKA_BUILD_OPTS=""
if [ "$KAFKA_VERSION" != '2.0' ] && [ "$KAFKA_VERSION" != '' ]; then
  KAFKA_BUILD_OPTS="-Dkafka.version=${KAFKA_VERSION}"
fi

# Only run integration tests if needed
if [ "$RUN_INTEGRATION_TESTS" != 'false' ]; then
  mvn test -B -P travis,integration-tests-only ${DEPLOY_BUILD_OPTS} ${KAFKA_BUILD_OPTS}
  if [ $? -eq 0 ]; then
    passed=1
  fi
else
  mvn test -B -P travis,no-integration-tests ${DEPLOY_BUILD_OPTS} ${KAFKA_BUILD_OPTS}
  if [ $? -eq 0 ]; then
    passed=1
  fi
fi

if [ $passed -eq 1 ]; then
  # Only send code coverage data if passed
  bash <(cat .codecov_bash)
  exit 0
else
  exit 1
fi
