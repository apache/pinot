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
  SUPERSET_IMAGE_TAG="latest"
fi
if [ -z "${BUILD_PLATFORM}" ]; then
  BUILD_PLATFORM="linux/amd64"
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

DOCKER_BUILD_TAGS=""
for tag in "${tags[@]}"
do
  echo "Plan to build and push docker images for: ${DOCKER_IMAGE_NAME}:${tag}"
  DOCKER_BUILD_TAGS+=" --tag ${DOCKER_IMAGE_NAME}:${tag} "
done

cd ${DOCKER_FILE_BASE_DIR}

docker buildx build \
    --no-cache \
    --platform=${BUILD_PLATFORM} \
    --file Dockerfile \
    --build-arg SUPERSET_IMAGE_TAG=${SUPERSET_IMAGE_TAG} \
    ${DOCKER_BUILD_TAGS} \
    --push \
    .
