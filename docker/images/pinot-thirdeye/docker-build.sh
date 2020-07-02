#!/bin/bash
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


if [[ "$#" -gt 0 ]]
then
  DOCKER_TAG=$1
else
  DOCKER_TAG="thirdeye:latest"
  echo "Not specified a Docker Tag, using default tag: ${DOCKER_TAG}."
fi

if [[ "$#" -gt 1 ]]
then
  PINOT_BRANCH=$2
else
  PINOT_BRANCH=master
  echo "Not specified a Pinot branch to build, using default branch: ${PINOT_BRANCH}."
fi

if [[ "$#" -gt 2 ]]
then
  PINOT_GIT_URL=$3
else
  PINOT_GIT_URL="https://github.com/apache/incubator-pinot.git"
fi

echo "Trying to build Thirdeye docker image from Git URL: [ ${PINOT_GIT_URL} ] on branch: [ ${PINOT_BRANCH} ] and tag it as: [ ${DOCKER_TAG} ]."

docker build -t ${DOCKER_TAG} --build-arg PINOT_BRANCH=${PINOT_BRANCH} --build-arg PINOT_GIT_URL=${PINOT_GIT_URL} -f Dockerfile .
