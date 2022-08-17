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

# docker-pinot-base
This is the base docker image to build [Apache Pinot](https://github.com/apache/pinot).

## How to build a docker image

Arguments:

`JAVA_VERSION`: The Java Build and Runtime image version. Default is `11`

`OPENJDK_IMAGE`: Base image to use for Pinot build and runtime, e.g. `arm64v8/openjdk`. Default is `openjdk`.

Usage:
```SHELL
docker build -t apachepinot/pinot-base-build:openjdk11 --no-cache --network=host --build-arg JAVA_VERSION=11 -f pinot-base-build/Dockerfile .
```

```SHELL
docker build -t apachepinot/pinot-base-runtime:openjdk11 --no-cache --network=host --build-arg JAVA_VERSION=11 -f pinot-base-runtime/Dockerfile .
```

Note that if you are not on arm64 machine, you can still build the image by turning on the experimental feature of docker, and add `--platform linux/arm64` into the `docker build ...` script, e.g.
```SHELL
docker build -t apachepinot/pinot-base-build:openjdk11-arm64v8 --platform linux/arm64 --no-cache --network=host --build-arg JAVA_VERSION=11 --build-arg OPENJDK_IMAGE=arm64v8/openjdk -f pinot-base-build/Dockerfile .
```
```SHELL
docker build -t apachepinot/pinot-base-runtime:openjdk11-arm64v8 --platform linux/arm64 --no-cache --network=host --build-arg JAVA_VERSION=11 --build-arg OPENJDK_IMAGE=arm64v8/openjdk -f pinot-base-runtime/Dockerfile .
```

## Publish the docker image

Here is the [Github Action task](https://github.com/apachepinot/pinot-fork/actions/workflows/build-pinot-docker-base-image.yml) to build and publish pinot base docker images.

This task can be triggered manually to build the cross platform(amd64 and arm64v8) base image.

The build shell is:
```SHELL
docker buildx build --no-cache --platform=linux/arm64,linux/amd64 --file Dockerfile --tag apachepinot/pinot-base-build:openjdk11 --push .
```