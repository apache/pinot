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
# Compiles Pinot from source on amd64 and pushes the build image to DockerHub
# so that per-arch package jobs can pull it.

set -e
set -x

if [ -z "${DOCKER_IMAGE_NAME}" ]; then
  DOCKER_IMAGE_NAME="apachepinot/pinot"
fi
if [ -z "${PINOT_GIT_URL}" ]; then
  PINOT_GIT_URL="https://github.com/apache/pinot.git"
fi

cd ${DOCKER_FILE_BASE_DIR}

PINOT_BUILD_IMAGE_TAG=${BASE_IMAGE_TAG}-amd64
BUILD_IMAGE_REMOTE_TAG="${DOCKER_IMAGE_NAME}:build-${BASE_IMAGE_TAG}"

echo "Building docker image for platform: amd64 with tag: pinot-build:${PINOT_BUILD_IMAGE_TAG}"
docker build \
  --no-cache \
  --platform amd64 \
  --file Dockerfile.build \
  --build-arg PINOT_GIT_URL=${PINOT_GIT_URL} \
  --build-arg PINOT_BRANCH=${PINOT_BRANCH} \
  --build-arg JDK_VERSION=${JDK_VERSION} \
  --build-arg PINOT_BASE_IMAGE_TAG=${BASE_IMAGE_TAG} \
  --tag pinot-build:${PINOT_BUILD_IMAGE_TAG} \
  --tag ${BUILD_IMAGE_REMOTE_TAG} \
  .

echo "Pushing build image to DockerHub as ${BUILD_IMAGE_REMOTE_TAG}"
docker push ${BUILD_IMAGE_REMOTE_TAG}
