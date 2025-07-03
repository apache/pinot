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
if [ -z "${PINOT_GIT_URL}" ]; then
  PINOT_GIT_URL="https://github.com/apache/pinot.git"
fi
if [ -z "${JDK_VERSION}" ]; then
  JDK_VERSION="11"
fi


cd ${DOCKER_FILE_BASE_DIR}
curl -L -o etc/jmx_prometheus_javaagent/jmx_prometheus_javaagent-0.18.0.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.18.0/jmx_prometheus_javaagent-0.18.0.jar

cd etc/jmx_prometheus_javaagent/
ln -s jmx_prometheus_javaagent-0.18.0.jar jmx_prometheus_javaagent.jar
cd ../..

PINOT_BUILD_IMAGE_TAG=${BASE_IMAGE_TAG}-amd64
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
  .

tags=()
declare -a tags=($(echo ${TAGS} | tr "," " "))

runtimeImages=()
declare -a runtimeImages=($(echo ${RUNTIME_IMAGE_TAGS} | tr "," " "))


for runtimeImage in "${runtimeImages[@]}"; do
  DOCKER_BUILD_TAGS=""
  for tag in "${tags[@]}"; do
    DOCKER_BUILD_TAGS+=" --tag ${DOCKER_IMAGE_NAME}:${tag}-${runtimeImage}-linux-amd64 "

    if [ "${runtimeImage}" == "17-amazoncorretto" ]; then
      if [ "${tag}" == "latest" ]; then
        DOCKER_BUILD_TAGS+=" --tag ${DOCKER_IMAGE_NAME}:latest-linux-amd64 "
      fi
    fi
  done

  echo "Building docker image for platform: amd64 with tags: ${DOCKER_BUILD_TAGS}"
  docker build \
    --no-cache \
    --platform amd64 \
    --file Dockerfile.package \
    --build-arg PINOT_BUILD_IMAGE_TAG=${PINOT_BUILD_IMAGE_TAG} \
    --build-arg PINOT_RUNTIME_IMAGE_TAG=${runtimeImage} \
    ${DOCKER_BUILD_TAGS} \
    .

  for tag in "${tags[@]}"; do
    docker push ${DOCKER_IMAGE_NAME}:${tag}-${runtimeImage}-linux-amd64

    if [ "${runtimeImage}" == "17-amazoncorretto" ]; then
      if [ "${tag}" == "latest" ]; then
        docker push ${DOCKER_IMAGE_NAME}:${tag}-linux-amd64
      fi
    fi
  done

  DOCKER_BUILD_TAGS=""
  for tag in "${tags[@]}"; do
    DOCKER_BUILD_TAGS+=" --tag ${DOCKER_IMAGE_NAME}:${tag}-${runtimeImage}-linux-arm64 "

    if [ "${runtimeImage}" == "17-amazoncorretto" ]; then
      if [ "${tag}" == "latest" ]; then
        DOCKER_BUILD_TAGS+=" --tag ${DOCKER_IMAGE_NAME}:latest-linux-arm64 "
      fi
    fi
  done

  echo "Building docker image for platform: arm64 with tags: ${DOCKER_BUILD_TAGS}"
  docker build \
    --no-cache \
    --platform arm64 \
    --file Dockerfile.package \
    --build-arg PINOT_BUILD_IMAGE_TAG=${PINOT_BUILD_IMAGE_TAG} \
    --build-arg PINOT_RUNTIME_IMAGE_TAG=${runtimeImage} \
    ${DOCKER_BUILD_TAGS} \
    .

  for tag in "${tags[@]}"; do
    docker push ${DOCKER_IMAGE_NAME}:${tag}-${runtimeImage}-linux-arm64

    if [ "${runtimeImage}" == "17-amazoncorretto" ]; then
      if [ "${tag}" == "latest" ]; then
        docker push ${DOCKER_IMAGE_NAME}:${tag}-linux-arm64
      fi
    fi
  done
done
