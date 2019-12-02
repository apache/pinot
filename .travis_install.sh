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

# Ignore changes not related to pinot code
echo 'Changed files:'
git diff --name-only $TRAVIS_COMMIT_RANGE
if [ $? -ne 0 ]; then
  echo 'Commit range is invalid.'
  exit 1
fi

# ThirdEye related changes
git diff --name-only $TRAVIS_COMMIT_RANGE | egrep '^(thirdeye)'
noThirdEyeChange=$?

if [ $noThirdEyeChange -eq 0 ]; then
  echo 'ThirdEye changes.'
  if [ "$RUN_INTEGRATION_TESTS" == 'false' ]; then
    echo 'Skip ThirdEye build when integration tests off'
    exit 0
  fi
fi

KAFKA_BUILD_OPTS=""
if [ "$KAFKA_VERSION" != '0.9' ]; then
  KAFKA_BUILD_OPTS="-Dkafka.version=${KAFKA_VERSION}"
fi

if [ $noThirdEyeChange -ne 0 ]; then
  echo "Full Pinot build"
  echo "No ThirdEye changes"
  mvn clean install -B -DskipTests=true -Dmaven.javadoc.skip=true -Dassembly.skipAssembly=true ${KAFKA_BUILD_OPTS} || exit $?
fi

# Build ThirdEye for ThirdEye related changes
if [ $noThirdEyeChange -eq 0 ]; then
  echo "Partial Pinot build"
  echo "ThirdEye changes only"
  mvn install -B -DskipTests -Dmaven.javadoc.skip=true -Dassembly.skipAssembly=true -pl pinot-common,pinot-core,pinot-api -am
  cd thirdeye/thirdeye-hadoop
  mvn clean compile -B -DskipTests
  cd ../..
  cd thirdeye/thirdeye-pinot
  mvn clean compile -B -DskipTests
  cd ../..
  #
  # skip thirdeye-frontend as re-build happens on test
  #
  exit $?
fi
