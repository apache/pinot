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
if [ -z "${JDK_VERSION}" ]; then
  JDK_VERSION="11"
fi

tags=()
declare -a tags=($(echo ${TAGS} | tr "," " "))

cd ${DOCKER_FILE_BASE_DIR}
platformTag=${BUILD_PLATFORM/\//-}
DOCKER_BUILD_TAGS=""
for tag in "${tags[@]}"; do
  DOCKER_BUILD_TAGS+=" --tag ${DOCKER_IMAGE_NAME}:${tag}-${BASE_IMAGE_TAG}-${platformTag} "

  if [ "${BASE_IMAGE_TAG}" == "11-amazoncorretto" ]; then
    if [ "${tag}" == "latest" ]; then
      DOCKER_BUILD_TAGS+=" --tag ${DOCKER_IMAGE_NAME}:latest-${platformTag} "
    fi
  fi
done

echo "Building docker image for platform: ${BUILD_PLATFORM} with tags: ${DOCKER_BUILD_TAGS}"
docker build \
  --no-cache \
  --platform ${BUILD_PLATFORM} \
  --file Dockerfile \
  --build-arg PINOT_GIT_URL=${PINOT_GIT_URL} \
  --build-arg PINOT_BRANCH=${PINOT_BRANCH} \
  --build-arg JDK_VERSION=${JDK_VERSION} \
  ${DOCKER_BUILD_TAGS} \
  .

for tag in "${tags[@]}"; do
  docker push ${DOCKER_IMAGE_NAME}:${tag}-${BASE_IMAGE_TAG}-${platformTag}

  if [ "${BASE_IMAGE_TAG}" == "11-amazoncorretto" ]; then
    if [ "${tag}" == "latest" ]; then
      docker push ${DOCKER_IMAGE_NAME}:${tag}-${platformTag}
    fi
  fi
done
