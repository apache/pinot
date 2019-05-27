#!/bin/bash

if [[ "$#" -gt 0 ]]
then
  DOCKER_TAG=$1
else
  DOCKER_TAG="pinot:latest"
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

echo "Trying to build Pinot docker image from Git URL: [ ${PINOT_GIT_URL} ] on branch: [ ${PINOT_BRANCH} ] and tag it as: [ ${DOCKER_TAG} ]"

docker build --no-cache -t ${DOCKER_TAG} --build-arg PINOT_BRANCH=${PINOT_BRANCH} --build-arg PINOT_GIT_URL=${PINOT_GIT_URL} -f Dockerfile .
