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
  DOCKER_IMAGE_NAME="apachepinot/pinot-presto"
fi
if [ -z "${PRESTO_GIT_URL}" ]; then
  PRESTO_GIT_URL="https://github.com/prestodb/presto.git"
fi
if [ -z "${PRESTO_BRANCH}" ]; then
  PINOT_BRANCH="master"
fi
if [ -z "${BUILD_PLATFORM}" ]; then
  BUILD_PLATFORM="linux/amd64"
fi

tags=()
declare -a tags=($(echo ${TAGS} | tr "," " "))

DOCKER_BUILD_TAGS=""
for tag in "${tags[@]}"
do
  echo "Plan to build docker images for: ${DOCKER_IMAGE_NAME}:${tag}"
  DOCKER_BUILD_TAGS+=" --tag ${DOCKER_IMAGE_NAME}:${tag} "
done


cd ${DOCKER_FILE_BASE_DIR}

docker image prune --all --filter "until=1h" -f

docker build \
    --no-cache \
    --platform=${BUILD_PLATFORM} \
    --file Dockerfile \
    --build-arg PRESTO_GIT_URL=${PRESTO_GIT_URL} --build-arg PRESTO_BRANCH=${PRESTO_BRANCH} \
    ${DOCKER_BUILD_TAGS} \
    .

for tag in "${tags[@]}"
do
  echo "Push docker image: ${DOCKER_IMAGE_NAME}:${tag}"
  docker push ${DOCKER_IMAGE_NAME}:${tag}
done
