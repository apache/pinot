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

## Build and publish the docker image

Here is
the [Github Action task](https://github.com/apachepinot/pinot-fork/actions/workflows/build-pinot-docker-base-image.yml)
to build and publish pinot base docker images.

This task can be triggered manually to build the cross platform(amd64 and arm64v8) base image.

The build shell is:

For Amazon Corretto 11:

```SHELL
docker buildx build --no-cache --platform=linux/arm64,linux/amd64 --file pinot-base-build/amazoncorretto.dockerfile --tag apachepinot/pinot-base-build:11-amazoncorretto --push .
```

```SHELL
docker buildx build --no-cache --platform=linux/arm64,linux/amd64 --file pinot-base-runtime/amazoncorretto.dockerfile --tag apachepinot/pinot-base-runtime:11-amazoncorretto --push .
```

For MS OpenJDK, the build shell is:

```SHELL
docker buildx build --no-cache --platform=linux/arm64,linux/amd64 --file pinot-base-build/amazoncorretto.dockerfile --tag apachepinot/pinot-base-build:11-ms-openjdk --push .
```

```SHELL
docker buildx build --no-cache --platform=linux/arm64,linux/amd64 --file pinot-base-runtime/amazoncorretto.dockerfile --tag apachepinot/pinot-base-runtime:11-ms-openjdk --push .
```
