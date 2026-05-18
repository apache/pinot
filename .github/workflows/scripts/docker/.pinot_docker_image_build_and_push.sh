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
if [ -z "${PINOT_GIT_REF}" ]; then
  PINOT_GIT_REF="master"
fi
if [ -z "${BUILD_PLATFORM}" ]; then
  BUILD_PLATFORM="linux/arm64,linux/amd64"
fi
if [ -z "${JDK_VERSION}" ]; then
  JDK_VERSION="21"
fi
if [ -z "${PINOT_BASE_IMAGE_TAG}" ]; then
  PINOT_BASE_IMAGE_TAG="${JDK_VERSION}-amazoncorretto"
fi

# Get pinot commit id.
# Use clone + checkout instead of clone -b so that commit SHAs work in
# addition to branch/tag names.
ROOT_DIR=`pwd`
rm -rf /tmp/pinot
git clone --no-single-branch https://github.com/apache/pinot.git /tmp/pinot
cd /tmp/pinot
git checkout "${PINOT_GIT_REF}"
COMMIT_ID=`git rev-parse --short HEAD`
VERSION=`mvn help:evaluate -Dexpression=project.version -q -DforceStdout`
rm -rf /tmp/pinot
DATE=`date +%Y%m%d`
cd ${ROOT_DIR}

tags=()
if [ -z "${TAGS}" ]; then
  tags=("${VERSION}-${COMMIT_ID}-${DATE}")
  tags+=("latest")
else
  declare -a tags=($(echo ${TAGS} | tr "," " "))
fi

DOCKER_BUILD_TAGS=""
for tag in "${tags[@]}"
do
  echo "Plan to build and push docker images for: ${DOCKER_IMAGE_NAME}:${tag}"
  DOCKER_BUILD_TAGS+=" --tag ${DOCKER_IMAGE_NAME}:${tag} "
done

cd ${DOCKER_FILE_BASE_DIR}

echo "Building Pinot Docker image with JDK ${JDK_VERSION} and base image tag ${PINOT_BASE_IMAGE_TAG}"
docker buildx build \
    --no-cache \
    --platform=${BUILD_PLATFORM} \
    --file Dockerfile \
    --build-arg PINOT_GIT_REF=${PINOT_GIT_REF} \
    --build-arg JDK_VERSION=${JDK_VERSION} \
    --build-arg PINOT_BASE_IMAGE_TAG=${PINOT_BASE_IMAGE_TAG} \
    ${DOCKER_BUILD_TAGS} \
    --push \
    .
