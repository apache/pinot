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

set -e
set -x

if [ -z "${DOCKER_IMAGE_NAME}" ]; then
  DOCKER_IMAGE_NAME="apachepinot/pinot"
fi
if [ -z "${TAGS}" ]; then
  echo "TAGS must be set" >&2
  exit 1
fi
if [ -z "${BUILD_PLATFORM}" ]; then
  echo "BUILD_PLATFORM must be set" >&2
  exit 1
fi

tags=()
declare -a tags=($(echo ${TAGS} | tr "," " "))

platforms=()
declare -a platforms=($(echo ${BUILD_PLATFORM} | tr "," " "))

for tag in "${tags[@]}"; do
  DOCKER_AMEND_TAGS_CMD=""
  for platform in "${platforms[@]}"; do
    platformTag=${platform/\//-}
    DOCKER_AMEND_TAGS_CMD+=" --amend ${DOCKER_IMAGE_NAME}:${tag}-${platformTag}"
  done

  echo "Creating manifest for tag: ${tag}"
  docker manifest create \
    ${DOCKER_IMAGE_NAME}:${tag} \
    ${DOCKER_AMEND_TAGS_CMD}

  docker manifest push \
    ${DOCKER_IMAGE_NAME}:${tag}
done
