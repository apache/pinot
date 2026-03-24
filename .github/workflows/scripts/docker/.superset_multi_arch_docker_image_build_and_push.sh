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
  DOCKER_IMAGE_NAME="apachepinot/pinot-superset"
fi
if [ -z "${SUPERSET_IMAGE_TAG}" ]; then
  SUPERSET_IMAGE_TAG="master-py310"
fi
if [ -z "${SUPERSET_ARM64_SUFFIX}" ]; then
  SUPERSET_ARM64_SUFFIX="arm"
fi

DATE=`date +%Y%m%d`
docker pull apache/superset:${SUPERSET_IMAGE_TAG}
COMMIT_ID=`docker images apache/superset:${SUPERSET_IMAGE_TAG} --format "{{.ID}}"`

tags=()
if [ -z "${TAGS}" ]; then
  tags=("${COMMIT_ID}-${DATE}")
  tags+=("latest")
else
  declare -a tags=($(echo ${TAGS} | tr "," " "))
fi

DOCKER_BUILD_AMD64_TAGS=""
DOCKER_BUILD_ARM64_TAGS=""
DOCKER_AMEND_TAGS_CMD=""
for tag in "${tags[@]}"
do
  echo "Plan to build and push docker images for: ${DOCKER_IMAGE_NAME}:${tag}"
  DOCKER_BUILD_AMD64_TAGS+=" --tag ${DOCKER_IMAGE_NAME}:${tag}-amd64 "
  DOCKER_BUILD_ARM64_TAGS+=" --tag ${DOCKER_IMAGE_NAME}:${tag}-arm64 "
  DOCKER_AMEND_TAGS_CMD=" --amend ${DOCKER_IMAGE_NAME}:${tag}-amd64 --amend ${DOCKER_IMAGE_NAME}:${tag}-arm64 "
done

cd ${DOCKER_FILE_BASE_DIR}

docker build \
    --no-cache \
    --platform=linux/amd64 \
    --file Dockerfile \
    --build-arg SUPERSET_IMAGE_TAG=${SUPERSET_IMAGE_TAG} \
    ${DOCKER_BUILD_AMD64_TAGS} \
    --push \
    .

docker build \
    --no-cache \
    --platform=linux/arm64 \
    --file Dockerfile \
    --build-arg SUPERSET_IMAGE_TAG=${SUPERSET_IMAGE_TAG}-${SUPERSET_ARM64_SUFFIX} \
    ${DOCKER_BUILD_ARM64_TAGS} \
    --push \
    .

for tag in "${tags[@]}"
do
  echo "Plan to push docker manifest for: ${DOCKER_IMAGE_NAME}:${tag}"
  docker manifest create ${DOCKER_IMAGE_NAME}:${tag} ${DOCKER_AMEND_TAGS_CMD}
  docker manifest push ${DOCKER_IMAGE_NAME}:${tag}
done