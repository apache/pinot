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

Use the [Pinot Base Docker Image Build and Publish workflow](https://github.com/apache/pinot/actions/workflows/build-pinot-base-docker-image.yml)
to build and publish base images for amd64 and arm64.

To roll out a JDK security update, publish the build and runtime base images,
then rebuild and publish the Pinot application images that use them. The Pinot
image builds check the JDK in their base images before compiling or packaging. For the
[November 2026 OpenJDK advisory](https://openjdk.org/groups/vulnerability/advisories/2026-11-17),
the base image workflow verifies OpenJDK 25.0.5.1 or newer within the Java 25 release line.

Use the workflow for official base image publishing. The following commands are
for manual builds:

For Amazon Corretto 25:

```SHELL
docker buildx build --pull --no-cache --build-arg JAVA_VERSION=25 --platform=linux/arm64,linux/amd64 --file pinot-base-build/amazoncorretto.dockerfile --tag apachepinot/pinot-base-build:25-amazoncorretto --push .
```

```SHELL
docker buildx build --pull --no-cache --build-arg JAVA_VERSION=25 --platform=linux/arm64,linux/amd64 --file pinot-base-runtime/amazoncorretto.dockerfile --tag apachepinot/pinot-base-runtime:25-amazoncorretto --push .
```

For MS OpenJDK, the build shell is:

```SHELL
docker buildx build --pull --no-cache --build-arg JAVA_VERSION=25 --platform=linux/arm64,linux/amd64 --file pinot-base-build/ms-openjdk.dockerfile --tag apachepinot/pinot-base-build:25-ms-openjdk --push .
```

```SHELL
docker buildx build --pull --no-cache --build-arg JAVA_VERSION=25 --platform=linux/arm64,linux/amd64 --file pinot-base-runtime/ms-openjdk.dockerfile --tag apachepinot/pinot-base-runtime:25-ms-openjdk --push .
```
