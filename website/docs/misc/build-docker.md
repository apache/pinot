---
title: Build Docker Images
sidebar_label: build-docker
description: Build Docker Images
---

The scripts to build Pinot related docker images is located at [here](https://github.com/apache/incubator-pinot/tree/master/docker/images)

You can access those scripts by running below command to checkout Pinot repo:

```bash
git clone git@github.com:apache/incubator-pinot.git pinot
cd pinot/docker/images
```

You can find current supported 3 images in this directory:

- Pinot: Pinot all-in-one distribution image
- Pinot-Presto: Presto image with Presto-Pinot Connector built-in.
- Pinot-Superset: Superset image with Pinot connector built-in.

## Pinot

This is a docker image of [Apache Pinot](https://github.com/apache/incubator-pinot)

### How to build a docker image

There is a docker build script which will build a given Git repo/branch and tag the image.

Usage:

```bash
./docker-build.sh [Docker Tag] [Git Branch] [Pinot Git URL]
```

This script will check out Pinot Repo `[Pinot Git URL]` on branch `[Git Branch]` and build the docker image for that. <br />
The docker image is tagged as `[Docker Tag]`. <br />
Docker Tag: Name and tag your docker image. Default is `pinot:latest`. <br />
Git Branch: The Pinot branch to build. Default is master. <br />
Pinot Git URL: The Pinot Git Repo to build, users can set it to their own fork. <br />
Please note that, the URL is `https://` based, not `git://`. <br />
Default is the [Apache Repo: https://github.com/apache/incubator-pinot.git](https://github.com/apache/incubator-pinot.git). <br />

- Example of building and tagging a snapshot on your own fork:

```bash
./docker-build.sh pinot_fork:snapshot-5.2 snapshot-5.2 https://github.com/your_own_fork/pinot.git
```

- Example of building a release version:

```bash
./docker-build.sh pinot:release-0.1.0 release-0.1.0 https://github.com/apache/incubator-pinot.git
```

- Example of building current master branch as a snapshot:

```bash
./docker-build.sh apachepinot/pinot:0.3.0-SNAPSHOT master
```

## How to publish a docker image

Script `docker-push.sh` publishes a given docker image to your docker registry.
In order to push to your own repo, the image needs to be explicitly tagged with the repo name.

- Example of publishing an image to `[apachepinot/pinot](https://hub.docker.com/u/apachepinot/repository/docker/apachepinot/pinot)` dockerHub repo.

```bash
./docker-push.sh apachepinot/pinot:latest
```

- You can also tag a built image, then push it.

```bash
docker tag pinot:release-0.1.0 apachepinot/pinot:release-0.1.0
docker push apachepinot/pinot:release-0.1.0
```

- Script docker-build-and-push.sh builds and publishes this docker image to your docker registry after build.

- Example of building and publishing an image to `[apachepinot/pinot](https://hub.docker.com/u/apachepinot/repository/docker/apachepinot/pinot)` dockerHub repo.

```bash
./docker-build-and-push.sh apachepinot/pinot:latest master https://github.com/apache/incubator-pinot.git
```

## Kubernetes Examples

Please refer to [Kubernetes Quickstart](../../docs/administration/installation/cloud/on-premise.md) for deployment examples.

[Kubernetes Quickstart](../../docs/administration/installation/cloud/on-premise.md)

## Pinot Presto

Docker image for Presto with Pinot integration.
This docker build project is specialized for Pinot.

### How to build

Usage:

```bash
./docker-build.sh [Docker Tag] [Git Branch] [Presto Git URL]
```

This script will check out Pinot Repo `[Pinot Git URL]` on branch `[Git Branch]` and build the docker image for that. <br />
The docker image is tagged as `[Docker Tag]`. <br />
Docker Tag: Name and tag your docker image. Default is `pinot:latest`. <br />
Git Branch: The Pinot branch to build. Default is master. <br />
Pinot Git URL: The Pinot Git Repo to build, users can set it to their own fork. <br />
Please note that, the URL is `https://` based, not `git://`. <br />
Default is the [Apache Repo: https://github.com/apache/incubator-pinot.git](https://github.com/apache/incubator-pinot.git). <br />

### How to push

```bash
docker push apachepinot/pinot-presto:latest
```

### Configuration

Follow the instructions provided by Presto for writing your own configuration files under etc directory.

### Volumes

The image defines two data volumes: one for mounting configuration into the container, and one for data.
The configuration volume is located alternatively at `/home/presto/etc`, which contains all the configuration and plugins.
The data volume is located at `/home/presto/data`.

### Kubernetes Examples

Please refer to presto-coordinator.yaml as k8s deployment example.

## Pinot Superset

Docker image for Superset with Pinot integration.
This docker build project is based on Project docker-superset and specialized for Pinot.

### How to build

Please modify file `Makefile` to change image and `superset_version` accordingly.
Below command will build docker image and tag it as `superset_version` and `latest`.

```bash
make latest
```

You can also build directly with `docker build` command by setting arguments:

```bash
docker build \
    --build-arg NODE_VERSION=latest \
    --build-arg PYTHON_VERSION=3.6 \
    --build-arg SUPERSET_VERSION=0.34.1 \
    --tag apachepinot/pinot-superset:0.34.1 \
    --target build .
```

How to push

```bash
make push
```

### Configuration

Follow the [instructions](https://superset.incubator.apache.org/installation.html#configuration) provided by Apache Superset for writing your own superset_config.py.

Place this file in a local directory and mount this directory to /etc/superset inside the container. This location 
is included in the image's `PYTHONPATH`. Mounting this file to a different location is possible, but it will need to be in the PYTHONPATH.

### Volumes

The image defines two data volumes: one for mounting configuration into the container, and one for data (logs, SQLite DBs, &c).

The configuration volume is located alternatively at `/etc/superset` or `/home/superset`; either is acceptable. Both of these directories are included in the PYTHONPATH of the image. Mount any configuration (specifically the superset_config.py file) here to have it read by the app on startup.

The data volume is located at `/var/lib/superset` and it is where you would mount your SQLite file (if you are using that as your backend), or a volume to collect any logs that are routed there. This location is used as the value of the `SUPERSET_HOME` environmental variable.

### Kubernetes Examples

Please refer to superset.yaml as k8s deployment example.
