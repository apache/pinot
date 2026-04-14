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
  DOCKER_TAG="pinot:latest"
  echo "Not specified a Docker Tag, using default tag: ${DOCKER_TAG}."
fi

if [[ "$#" -gt 1 ]]
then
  PINOT_GIT_REF=$2
else
  PINOT_GIT_REF=master
  echo "Not specified a Pinot git ref to build, using default: ${PINOT_GIT_REF}."
fi

if [[ "$#" -gt 2 ]]
then
  KAFKA_VERSION=$3
else
  KAFKA_VERSION=3.0
fi

if [[ "$#" -gt 3 ]]
then
  JAVA_VERSION=$4
else
  JAVA_VERSION=11
fi

echo "Trying to build Pinot docker image on git ref: [ ${PINOT_GIT_REF} ] and tag it as: [ ${DOCKER_TAG} ]. Kafka Dependencies: [ ${KAFKA_VERSION} ]. Java Version: [ ${JAVA_VERSION} ]."

docker build --no-cache -t ${DOCKER_TAG} --build-arg PINOT_GIT_REF=${PINOT_GIT_REF} --build-arg KAFKA_VERSION=${KAFKA_VERSION} --build-arg JAVA_VERSION=${JAVA_VERSION} -f Dockerfile .
