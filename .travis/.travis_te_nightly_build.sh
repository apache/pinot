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

if [ -n "${DEPLOY_BUILD_OPTS}" ]; then
  echo "Deploying ThirdEye to bintray"
  # Generate new version number
  cd thirdeye/
  BUILD_VERSION=$(grep -E "<revision>(.*)</revision>" pom.xml | cut -d'>' -f2 | cut -d'<' -f1)
  echo "Current build version: $BUILD_VERSION${DEV_VERSION}"
  mvn versions:set -DnewVersion="$BUILD_VERSION${DEV_VERSION}" -q -B
  mvn versions:commit -q -B
  # Deploy ThirdEye backend to bintray
  mvn -pl '!thirdeye-frontend,!thirdeye-dist' deploy -s ../.travis/.ci.settings.xml -DskipTests -q -DaltDeploymentRepository=bintray-linkedin-maven::default::'https://api.bintray.com/maven/linkedin/maven/thirdeye/;publish=1;override=1' -e -X
  # Deploy ThirdEye frontend to NPM
  cd thirdeye-frontend/
  npm version ${BUILD_VERSION}${DEV_VERSION}
  npm-login-noninteractive
  npm publish
fi
