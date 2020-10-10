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

# Superset

Docker image for [Superset](https://github.com/apache/incubator-superset) with Pinot integration.


## How to build

Pinot Superset image is built on top of [preset/superset](https://hub.docker.com/r/preset/superset) with Pinotdb driver.

Below command will build Superset image based on `preset/superset:0.37` then tag it as `apachepinot/pinot-superset:0.37`.

```bash
docker build --build-arg PRESET_SUPERSET_IMAGE_TAG=0.37 --tag apachepinot/pinot-superset:0.37 .
```

## How to push

```bash
docker push apachepinot/pinot-superset:0.37
```

## Configuration

Follow the [instructions](https://superset.incubator.apache.org/installation.html#configuration) provided by Apache Superset for writing your own `superset_config.py`.

Place this file in a local directory and mount this directory to `/etc/superset` inside the container. This location is included in the image's `PYTHONPATH`. Mounting this file to a different location is possible, but it will need to be in the `PYTHONPATH`.


## Volumes

The image defines two data volumes: one for mounting configuration into the container, and one for data (logs, SQLite DBs, &c).

The configuration volume is located alternatively at `/etc/superset` or `/app/superset`; either is acceptable. Both of these directories are included in the `PYTHONPATH` of the image. Mount any configuration (specifically the `superset_config.py` file) here to have it read by the app on startup.

The data volume is located at `/app/superset_home` and it is where you would mount your SQLite file (if you are using that as your backend), or a volume to collect any logs that are routed there.

## Kubernetes Examples

Please refer to [`superset.yaml`](../../../kubernetes/examples/helm/superset.yaml) as k8s deployment example.
