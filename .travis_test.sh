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

  if [ "$TEST_CATEGORY" == 'UNIT_TEST' ]; then
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
if [ "$KAFKA_VERSION" != '2.0' ]; then
  git diff --name-only $TRAVIS_COMMIT_RANGE | egrep '^(pinot-connectors)'
  if [ $? -ne 0 ]; then
    echo "No Pinot Connector Changes, Skip tests for Kafka Connector: ${KAFKA_VERSION}."
    exit 0
  fi
  KAFKA_BUILD_OPTS="-Dkafka.version=${KAFKA_VERSION}"
fi

# Only run integration tests if needed
if [ "$TEST_CATEGORY" == 'INTEGRATION_TEST' ]; then
  mvn test -B -P travis,travis-integration-tests-only ${KAFKA_BUILD_OPTS}
  if [ $? -eq 0 ]; then
    passed=1
  fi
fi

# Unit test
if [ "$TEST_CATEGORY" == 'UNIT_TEST' ]; then
  mvn test -B -P travis,travis-no-integration-tests ${KAFKA_BUILD_OPTS}
  if [ $? -eq 0 ]; then
    passed=1
  fi
fi

# Quickstart
if [ "$TEST_CATEGORY" == 'QUICKSTART' ]; then
  mvn clean install -DskipTests -Pbin-dist ${KAFKA_BUILD_OPTS}
  DIST_BIN_DIR=`ls -d pinot-distribution/target/apache-pinot-*/apache-pinot-*`
  cd $DIST_BIN_DIR

  # Test quick-start-batch
  passed_batch=0
  bin/quick-start-batch.sh &
  PID=$!
  sleep 60
  COUNT_STAR_RES=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"select count(*) from baseballStats limit 1","trace":false}' http://localhost:8000/query/sql | jq '.resultTable.rows[0][0]'`
  if [ ${COUNT_STAR_RES} -eq 97889 ]; then
    passed_batch=1
  fi
  kill $PID
  sleep 30

  # Test quick-start-streaming
  passed_streaming=0
  bin/quick-start-streaming.sh &
  PID=$!
  sleep 60
  COUNT_STAR_RES_1=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"select count(*) from meetupRsvp limit 1","trace":false}' http://localhost:8000/query/sql | jq '.resultTable.rows[0][0]'`
  sleep 5
  COUNT_STAR_RES_2=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"select count(*) from meetupRsvp limit 1","trace":false}' http://localhost:8000/query/sql | jq '.resultTable.rows[0][0]'`
  sleep 5
  COUNT_STAR_RES_3=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"select count(*) from meetupRsvp limit 1","trace":false}' http://localhost:8000/query/sql | jq '.resultTable.rows[0][0]'`
  if [ ${COUNT_STAR_RES_3} -gt ${COUNT_STAR_RES_2} ] && [ ${COUNT_STAR_RES_2} -gt ${COUNT_STAR_RES_1} ]; then
    passed_streaming=1
  fi
  kill $PID
  sleep 30
  if [ ${passed_batch} -eq 1 ] && [ ${passed_streaming} -eq 1 ]; then
    passed=1
  fi
fi

# Remove Pinot files from local Maven repository to avoid a useless cache rebuild
rm -rf ~/.m2/repository/com/linkedin/pinot

if [ $passed -eq 1 ]; then
  if [ -f ".codecov_bash" ]; then
    # Only send code coverage data if passed
    bash <(cat .codecov_bash)
  fi
  exit 0
else
  exit 1
fi
