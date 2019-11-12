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
  DOCKER_TAG="pinot-presto:latest"
  echo "Not specified a Docker Tag, using default tag: ${DOCKER_TAG}."
fi

if [[ "$#" -gt 1 ]]
then
  PRESTO_BRANCH=$2
else
  PRESTO_BRANCH=master
  echo "Not specified a Presto branch to build, using default branch: ${PRESTO_BRANCH}."
fi

if [[ "$#" -gt 2 ]]
then
  PRESTO_GIT_URL=$3
else
  PRESTO_GIT_URL="https://github.com/prestodb/presto.git"
fi

echo "Trying to build Pinot Presto docker image from Git URL: [ ${PRESTO_GIT_URL} ] on branch: [ ${PRESTO_BRANCH} ] and tag it as: [ ${DOCKER_TAG} ]."

docker build --no-cache -t ${DOCKER_TAG} --build-arg PRESTO_BRANCH=${PRESTO_BRANCH} --build-arg PRESTO_GIT_URL=${PRESTO_GIT_URL} -f Dockerfile .
