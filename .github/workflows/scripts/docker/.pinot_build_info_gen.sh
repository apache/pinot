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
if [ -z "${PINOT_GIT_URL}" ]; then
  PINOT_GIT_URL="https://github.com/apache/pinot.git"
fi
if [ -z "${PINOT_BRANCH}" ]; then
  PINOT_BRANCH="master"
fi

# Get pinot commit id
ROOT_DIR=$(pwd)
rm -rf /tmp/pinot
git clone -b ${PINOT_BRANCH} --single-branch ${PINOT_GIT_URL} /tmp/pinot
cd /tmp/pinot
COMMIT_ID=$(git rev-parse --short HEAD)
VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
rm -rf /tmp/pinot
DATE=$(date +%Y%m%d)

if [ -z "${TAGS}" ]; then
  TAGS="${VERSION}-${COMMIT_ID}-${DATE},latest"
fi
echo "commit-id=${COMMIT_ID}" >>"$GITHUB_OUTPUT"
echo "tags=${TAGS}" >>"$GITHUB_OUTPUT"
