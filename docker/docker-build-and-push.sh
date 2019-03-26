#!/bin/bash

sh docker-build.sh $@
if [[ "$#" -gt 0 ]]
then
  DOCKER_TAG=$1
else
  DOCKER_TAG=test
fi
echo "Trying to push docker image to ${DOCKER_TAG}"
docker push ${DOCKER_TAG}