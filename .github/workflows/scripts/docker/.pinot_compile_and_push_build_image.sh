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
#
# The build image is tagged by JDK version only (not by JDK distro) because
# JVM bytecode is identical regardless of the JDK vendor used to compile it.
# Both amazoncorretto and ms-openjdk runtime images share the same build image.

set -e

if [ -z "${DOCKER_IMAGE_NAME}" ]; then
  DOCKER_IMAGE_NAME="apachepinot/pinot"
fi
if [ -z "${DOCKER_FILE_BASE_DIR}" ]; then
  echo "DOCKER_FILE_BASE_DIR is required" >&2
  exit 1
fi
if [ -z "${JDK_VERSION}" ]; then
  echo "JDK_VERSION is required" >&2
  exit 1
fi
if [ -z "${PINOT_GIT_REF}" ]; then
  echo "PINOT_GIT_REF is required" >&2
  exit 1
fi

cd "${DOCKER_FILE_BASE_DIR}"

# Tag the build image by JDK version only — distro-agnostic.
# Include PINOT_GIT_REF (commit SHA) to avoid collisions between concurrent runs.
PINOT_BUILD_IMAGE_TAG="${JDK_VERSION}-amd64"
BUILD_IMAGE_REMOTE_TAG="${DOCKER_IMAGE_NAME}:build-${JDK_VERSION}-${PINOT_GIT_REF}"

# Use amazoncorretto as the compile-time base; the resulting JARs are identical
# to those produced by any other JDK vendor at the same version.
COMPILE_BASE_IMAGE_TAG="${JDK_VERSION}-amazoncorretto"

echo "Building docker image for platform: amd64 with tag: pinot-build:${PINOT_BUILD_IMAGE_TAG}"
docker build \
  --no-cache \
  --platform linux/amd64 \
  --file Dockerfile.build \
  --build-arg "PINOT_GIT_REF=${PINOT_GIT_REF}" \
  --build-arg "JDK_VERSION=${JDK_VERSION}" \
  --build-arg "PINOT_BASE_IMAGE_TAG=${COMPILE_BASE_IMAGE_TAG}" \
  --tag "pinot-build:${PINOT_BUILD_IMAGE_TAG}" \
  --tag "${BUILD_IMAGE_REMOTE_TAG}" \
  .

echo "Pushing build image to DockerHub as ${BUILD_IMAGE_REMOTE_TAG}"
docker push "${BUILD_IMAGE_REMOTE_TAG}"
