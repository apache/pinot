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

# The image contents come from JDK_VERSION while the published tag comes from TAG, so a default
# here would silently publish a mislabeled image (e.g. a JDK 21 image tagged 25-amazoncorretto).
# Require it explicitly, matching .pinot_compile_and_push_build_image.sh and
# .pinot_package_single_platform.sh.
if [ -z "${JDK_VERSION}" ]; then
  echo "JDK_VERSION is required" >&2
  exit 1
fi

cd docker/images/pinot-base/pinot-base-${BASE_IMAGE_TYPE}

docker build \
  --no-cache \
  --platform=${BUILD_PLATFORM} \
  --file ${OPEN_JDK_DIST}.dockerfile \
  --tag apachepinot/pinot-base-${BASE_IMAGE_TYPE}:${TAG}-${ARCH} \
  --build-arg JAVA_VERSION=${JDK_VERSION} \
  --build-arg ARCH=${ARCH} \
  .

docker push apachepinot/pinot-base-${BASE_IMAGE_TYPE}:${TAG}-${ARCH}
