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

KAFKA_BUILD_OPTS=""
if [ "$KAFKA_VERSION" != '2.0' ] && [ "$KAFKA_VERSION" != '' ]; then
  KAFKA_BUILD_OPTS="-Dkafka.version=${KAFKA_VERSION}"
fi

# Java version
java -version

echo "Full Pinot build"
if [ "$TRAVIS_JDK_VERSION" != 'oraclejdk8' ]; then
  # JDK 11 prints more logs exceeding Travis limits.
  mvn clean install -B -DskipTests=true -Pbin-dist -Dmaven.javadoc.skip=true ${DEPLOY_BUILD_OPTS} ${KAFKA_BUILD_OPTS} > /tmp/mvn_build_log
  if [ $? -eq 0 ]; then
    exit 0
  else
    tail -1000 /tmp/mvn_build_log
    exit 1
  fi
else
  mvn clean install -B -DskipTests=true -Pbin-dist -Dmaven.javadoc.skip=true ${DEPLOY_BUILD_OPTS} ${KAFKA_BUILD_OPTS} || exit $?
fi
