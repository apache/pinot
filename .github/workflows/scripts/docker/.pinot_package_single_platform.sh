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
# Packages Pinot for a single platform. Pulls the build image from DockerHub,
# tags it locally, then builds and pushes the platform-specific package image.

set -e

if [ -z "${DOCKER_IMAGE_NAME}" ]; then
  DOCKER_IMAGE_NAME="apachepinot/pinot"
fi
if [ -z "${BUILD_ARCH}" ]; then
  echo "BUILD_ARCH is required (amd64 or arm64)" >&2
  exit 1
fi
if [ -z "${DOCKER_FILE_BASE_DIR}" ]; then
  echo "DOCKER_FILE_BASE_DIR is required" >&2
  exit 1
fi

cd "${DOCKER_FILE_BASE_DIR}"

# Pull the commit-scoped build image from DockerHub and tag it locally.
# PINOT_BRANCH is the short commit SHA resolved by generate-build-info; including
# it in the tag prevents concurrent nightly runs from overwriting each other.
PINOT_BUILD_IMAGE_TAG="${BASE_IMAGE_TAG}-amd64"
BUILD_IMAGE_REMOTE_TAG="${DOCKER_IMAGE_NAME}:build-${BASE_IMAGE_TAG}-${PINOT_BRANCH}"
echo "Pulling build image: ${BUILD_IMAGE_REMOTE_TAG}"
docker pull --platform linux/amd64 "${BUILD_IMAGE_REMOTE_TAG}"
docker tag "${BUILD_IMAGE_REMOTE_TAG}" "pinot-build:${PINOT_BUILD_IMAGE_TAG}"

declare -a tags=($(echo "${TAGS}" | tr "," " "))
declare -a runtimeImages=($(echo "${RUNTIME_IMAGE_TAGS}" | tr "," " "))

for runtimeImage in "${runtimeImages[@]}"; do
  DOCKER_BUILD_TAGS=""
  for tag in "${tags[@]}"; do
    DOCKER_BUILD_TAGS+=" --tag ${DOCKER_IMAGE_NAME}:${tag}-${runtimeImage}-linux-${BUILD_ARCH} "

    # 21-ms-openjdk is the canonical default runtime; promote it as the bare
    # latest-linux-<arch> tag so multi-arch manifests can reference it.
    if [ "${runtimeImage}" == "21-ms-openjdk" ]; then
      if [ "${tag}" == "latest" ]; then
        DOCKER_BUILD_TAGS+=" --tag ${DOCKER_IMAGE_NAME}:latest-linux-${BUILD_ARCH} "
      fi
    fi
  done

  echo "Building docker image for platform: ${BUILD_ARCH} with tags: ${DOCKER_BUILD_TAGS}"
  docker build \
    --no-cache \
    --platform "linux/${BUILD_ARCH}" \
    --file Dockerfile.package \
    --build-arg "PINOT_BUILD_IMAGE_TAG=${PINOT_BUILD_IMAGE_TAG}" \
    --build-arg "PINOT_RUNTIME_IMAGE_TAG=${runtimeImage}" \
    ${DOCKER_BUILD_TAGS} \
    .

  for tag in "${tags[@]}"; do
    docker push "${DOCKER_IMAGE_NAME}:${tag}-${runtimeImage}-linux-${BUILD_ARCH}"

    if [ "${runtimeImage}" == "21-ms-openjdk" ]; then
      if [ "${tag}" == "latest" ]; then
        docker push "${DOCKER_IMAGE_NAME}:latest-linux-${BUILD_ARCH}"
      fi
    fi
  done
done
