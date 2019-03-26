# docker-pinot
This is a docker image of [Apache Pinot](https://github.com/apache/incubator-pinot).

## How to build it

There is a docker build script which will build a given Git repo/branch and tag the image.

Usage:

```SHELL
./docker-build.sh [Docker Tag] [Git Branch] [Pinot Git URL]
```

This script will check out Pinot Repo `[Pinot Git URL]` on branch `[Git Branch]` and build the docker image for that.

The docker image is tagged as `[Docker Tag]`.

`Docker Tag`: Name and tag your docker image. Default is `pinot:latest`.

`Git Branch`: The Pinot branch to build. Default is `master`.

`Pinot Git URL`: The Pinot Git Repo to build, users can set it to their own fork. Please note that, the URL is `https://` based, not `git://`. Default is the Apache Repo: `https://github.com/apache/incubator-pinot.git`.

Example of build and tag a snapshot on your own fork:
```SHELL
./docker-build.sh pinot_fork:snapshot-5.2 snapshot-5.2 https://github.com/your_own_fork/pinot.git
```

Example of publish a release version:
```SHELL
./docker-build.sh pinot:release-0.1.0 release-0.1.0 https://github.com/apache/incubator-pinot.git
```

There is also a `docker-build-and-push.sh` script to publish this docker image to your docker registry after build.

```SHELL
./docker-build-and-push.sh pinot:release-0.1.0 release-0.1.0 https://github.com/apache/incubator-pinot.git
```

## How to Run it

The entry point of docker image is `pinot-admin.sh` script.

### Bring up Zookeeper
Example of bring up a local zookeeper in docker:
```SHELL
docker pull zookeeper
docker run --name  pinot-zookeeper --restart always -p 2181:2181  zookeeper
```
You can extract the zookeeper host from:
```SHELL
docker inspect pinot-zookeeper|grep IPAddress
            "SecondaryIPAddresses": null,
            "IPAddress": "172.17.0.2",
                    "IPAddress": "172.17.0.2",
```
Please use local zookeeper path `172.17.0.2:2181` as `-zkAddress` parameter.

### Pinot Controller
Example of bring up a local controller:
```SHELL
docker run -p 9000:9000 pinot:release-0.1.0 StartController -zkAddress 172.17.0.2:2181
```

### Pinot Broker
Example of bring up a local broker:
```SHELL
docker run -p 8099:8099 pinot:release-0.1.0 StartBroker -zkAddress 172.17.0.2:2181
```

### Pinot Server
Example of bring up a local server:
```SHELL
docker run -p 8098:8098 pinot:release-0.1.0 StartServer -zkAddress 172.17.0.2:2181
```