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

# Check ThirdEye related changes
COMMIT_BEFORE=$(jq -r ".pull_request.base.sha" "${GITHUB_EVENT_PATH}")
COMMIT_AFTER=$(jq -r ".pull_request.head.sha" "${GITHUB_EVENT_PATH}")
git fetch
git diff --name-only "${COMMIT_BEFORE}" "${COMMIT_AFTER}" | grep -E '^(thirdeye)'
if [ $? -eq 0 ]; then
  echo 'ThirdEye changes.'

  if [ "${RUN_INTEGRATION_TESTS}" == false ]; then
    echo 'Skip ThirdEye tests when integration tests off'
    exit 0
  fi

  cd thirdeye
  mvn test
  failed=$?
  if [ $failed -eq 0 ]; then
    exit 0
  else
    exit 1
  fi
fi

# Only run integration tests if needed
if [ "$RUN_INTEGRATION_TESTS" != false ]; then
  mvn test -B -P travis,travis-integration-tests-only && exit 0 || exit 1
else
  mvn test -B -P travis,travis-no-integration-tests && exit 0 || exit 1
fi
