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

# Presto

Docker image for [Presto](https://github.com/prestodb/presto) with Pinot integration.

This docker build project is specialized for Pinot.

## How to build

```bash
./docker-build.sh
```

You can also build directly with `docker build` command by setting arguments:
```bash
docker build \
	--build-arg PRESTO_BRANCH=master \
	--tag apachepinot/pinot-presto:latest \
	--target build .
```
## How to push

```bash
docker push apachepinot/pinot-presto:latest
```

## Configuration

Follow the [instructions](https://prestodb.io/docs/current/installation/deployment.html) provided by Presto for writing your own configuration files under `etc` directory.

## Volumes

The image defines two data volumes: one for mounting configuration into the container, and one for data.

The configuration volume is located alternatively at `/home/presto/etc`, which contains all the configuration and plugins.

The data volume is located at `/home/presto/data`.

## Kubernetes Examples

Please refer to [`presto-coordinator.yaml`](../../../kubernetes/examples/helm/prest-coordinator.yaml) as k8s deployment example.
