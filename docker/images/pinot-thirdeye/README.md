<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# docker-pinot-thirdeye
This is a docker image of [Apache Thirdeye](https://github.com/apache/incubator-pinot/tree/master/thirdeye).

## How to build a docker image

There is a docker build script which will build a given Git repo/branch and tag the image.

Usage:

```SHELL
./docker-build.sh [Docker Tag] [Git Branch] [Pinot Git URL]
```

This script will check out Pinot Repo `[Pinot Git URL]` on branch `[Git Branch]` and build the docker image for that.

The docker image is tagged as `[Docker Tag]`.

`Docker Tag`: Name and tag your docker image. Default is `thirdeye:latest`.

`Git Branch`: The Pinot branch to build. Default is `master`.

`Pinot Git URL`: The Pinot Git Repo to build, users can set it to their own fork. Please note that, the URL is `https://` based, not `git://`. Default is the Apache Repo: `https://github.com/apache/incubator-pinot.git`.

* Example of building and tagging a snapshot on your own fork:
```SHELL
./docker-build.sh thirdeye:latest master https://github.com/apache/incubator-pinot.git
```

##Distributed setup

Once the thirdeye container is built, the frontend and backend images can be generated using a simple docker build
in their respective directories. These images are built using the `thirdeye` image as the base image.

```SHELL
# The frontend image launches the Dashboard Server
cd frontend
docker build --no-cache -t spyne/thirdeye-frontend -f Dockerfile .
cd ..

# The backend image launches the Anomaly Server
cd backend
docker build --no-cache -t spyne/thirdeye-frontend -f Dockerfile .
cd ..
```


## How to publish a docker image

Script `docker-push.sh` publishes a given docker image to your docker registry.

In order to push to your own repo, the image needs to be explicitly tagged with the repo name.

* Example of publishing a image to [apachepinot/thirdeye](https://cloud.docker.com/u/apachepinot/repository/docker/apachepinot/thirdeye) dockerHub repo.

```SHELL
docker tag thirdeye:latest apachepinot/thirdeye:latest
./docker-push.sh apachepinot/thirdeye:latest
```

Script `docker-build-and-push.sh` builds and publishes this docker image to your docker registry after build.

* Example of building and publishing a image to [apachepinot/thirdeye](https://cloud.docker.com/u/apachepinot/repository/docker/apachepinot/thirdeye) dockerHub repo.

```SHELL
./docker-build-and-push.sh apachepinot/thirdeye:latest master https://github.com/apache/incubator-pinot.git
```

## How to Run it

The entry point of docker image is `start-thirdeye.sh` script.

* Create an isolated bridge network in docker.

```SHELL
docker network create -d bridge pinot-demo
```

* Start Pinot Batch Quickstart

```SHELL
docker run \
    --network=pinot-demo \
    --name pinot-quickstart \
    -p 9000:9000 \
    -d apachepinot/pinot:latest QuickStart \
    -type batch
```

* Start ThirdEye companion

```SHELL
docker run \
    --network=pinot-demo \
    --name  thirdeye-backend \
    -p 1426:1426 \
    -p 1427:1427 \
    -d apachepinot/thirdeye:latest pinot-quickstart
```

