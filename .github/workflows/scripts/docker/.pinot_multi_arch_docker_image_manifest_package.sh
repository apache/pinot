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

set -x

if [ -z "${DOCKER_IMAGE_NAME}" ]; then
  DOCKER_IMAGE_NAME="apachepinot/pinot"
fi

tags=()
declare -a tags=($(echo ${TAGS} | tr "," " "))

platforms=()
declare -a platforms=($(echo ${BUILD_PLATFORM} | tr "," " "))

baseImageTags=()
declare -a baseImageTags=($(echo ${BASE_IMAGE_TAGS} | tr "," " "))

for tag in "${tags[@]}"; do
  for baseImageTag in "${baseImageTags[@]}"; do
    DOCKER_AMEND_TAGS_CMD=""
    for platform in "${platforms[@]}"; do
      platformTag=${platform/\//-}
      DOCKER_AMEND_TAGS_CMD+=" --amend ${DOCKER_IMAGE_NAME}:${tag}-${baseImageTag}-${platformTag} "
    done

    echo "Creating manifest for tag: ${tag}-${baseImageTag}"
    docker manifest create \
      ${DOCKER_IMAGE_NAME}:${tag}-${baseImageTag} \
      ${DOCKER_AMEND_TAGS_CMD}

    docker manifest push \
      ${DOCKER_IMAGE_NAME}:${tag}-${baseImageTag}

    if [ "${baseImageTag}" == "11-amazoncorretto" ]; then
      if [ "${tag}" == "latest" ]; then
        echo "Creating manifest for tag: latest"
        docker manifest create \
          ${DOCKER_IMAGE_NAME}:latest \
          ${DOCKER_AMEND_TAGS_CMD}

        docker manifest push \
          ${DOCKER_IMAGE_NAME}:latest
      fi
    fi
  done
done
