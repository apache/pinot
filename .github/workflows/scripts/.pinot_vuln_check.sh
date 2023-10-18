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

if [ -z "${DOCKER_IMAGE_NAME}" ]; then
  DOCKER_IMAGE_NAME="apachepinot/pinot"
fi
if [ -z "${PINOT_GIT_URL}" ]; then
  PINOT_GIT_URL="https://github.com/apache/pinot.git"
fi
if [ -z "${PINOT_BRANCH}" ]; then
  PINOT_BRANCH="master"
fi

cd ${DOCKER_FILE_BASE_DIR}

docker image prune --all --filter "until=1h" -f

docker build \
    --no-cache \
    --file Dockerfile \
    --build-arg PINOT_GIT_URL=${PINOT_GIT_URL} \
    --build-arg PINOT_BRANCH=${PINOT_BRANCH} \
    --tag ${DOCKER_IMAGE_NAME}:${PINOT_SHA} \
    .

docker image ls
