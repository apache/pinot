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

if [ -z "${BUILD_PLATFORM}" ]; then
  exit 1
fi

if [ -z "${BASE_IMAGE_TYPE}" ]; then
  exit 1
fi

BASE_IMAGE_DIR="docker/images/pinot-base/pinot-base-${BASE_IMAGE_TYPE}"
JAVA_VERSION="${JDK_VERSION:-25}"
IMAGE="apachepinot/pinot-base-${BASE_IMAGE_TYPE}:${TAG}-${ARCH}"

docker build \
  --pull \
  --no-cache \
  --platform=${BUILD_PLATFORM} \
  --file "${BASE_IMAGE_DIR}/${OPEN_JDK_DIST}.dockerfile" \
  --tag "${IMAGE}" \
  --build-arg JAVA_VERSION="${JAVA_VERSION}" \
  --build-arg ARCH=${ARCH} \
  "${BASE_IMAGE_DIR}"

docker run --rm -i --platform "${BUILD_PLATFORM}" --entrypoint sh "${IMAGE}" -s \
  < docker/images/pinot/check-java-version.sh

docker push "${IMAGE}"
