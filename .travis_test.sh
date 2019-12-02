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

# ThirdEye related changes
git diff --name-only $TRAVIS_COMMIT_RANGE | egrep '^(thirdeye)'
if [ $? -eq 0 ]; then
  echo 'ThirdEye changes.'

  if [ "$TRAVIS_JDK_VERSION" != 'oraclejdk8' ]; then
    echo 'Skip ThirdEye tests for version other than oracle jdk8.'
    rm -rf ~/.m2/repository/com/linkedin/pinot ~/.m2/repository/com/linkedin/thirdeye
    exit 0
  fi

  if [ "$RUN_INTEGRATION_TESTS" == 'false' ]; then
    echo 'Skip ThirdEye tests when integration tests off'
    rm -rf ~/.m2/repository/com/linkedin/pinot ~/.m2/repository/com/linkedin/thirdeye
    exit 0
  fi

  cd thirdeye
  mvn test -B
  failed=$?
  # Remove Pinot/ThirdEye files from local Maven repository to avoid a useless cache rebuild
  rm -rf ~/.m2/repository/com/linkedin/pinot ~/.m2/repository/com/linkedin/thirdeye
  if [ $failed -eq 0 ]; then
    exit 0
  else
    exit 1
  fi
fi

# Only run tests for JDK 8
if [ "$TRAVIS_JDK_VERSION" != 'oraclejdk8' ]; then
  echo 'Skip tests for version other than oracle jdk8.'
  # Remove Pinot files from local Maven repository to avoid a useless cache rebuild
  rm -rf ~/.m2/repository/com/linkedin/pinot
  exit 0
fi

passed=0

KAFKA_BUILD_OPTS=""
if [ "$KAFKA_VERSION" != '0.9' ]; then
  git diff --name-only $TRAVIS_COMMIT_RANGE | egrep '^(pinot-connectors)'
  if [ $? -ne 0 ]; then
    echo "No Pinot Connector Changes, Skip tests for Kafka Connector: ${KAFKA_VERSION}."
    exit 0
  fi
  KAFKA_BUILD_OPTS="-Dkafka.version=${KAFKA_VERSION}"
fi

# Only run integration tests if needed
if [ "$RUN_INTEGRATION_TESTS" != 'false' ]; then
  mvn test -B -P travis,travis-integration-tests-only ${KAFKA_BUILD_OPTS}
  if [ $? -eq 0 ]; then
    passed=1
  fi
else
  mvn test -B -P travis,travis-no-integration-tests ${KAFKA_BUILD_OPTS}
  if [ $? -eq 0 ]; then
    passed=1
  fi
fi

# Remove Pinot files from local Maven repository to avoid a useless cache rebuild
rm -rf ~/.m2/repository/com/linkedin/pinot

if [ $passed -eq 1 ]; then
  # Only send code coverage data if passed
  bash <(cat .codecov_bash)
  exit 0
else
  exit 1
fi
