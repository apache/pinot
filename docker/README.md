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
# docker-pinot
This is a docker image of [Winedepot Pinot](https://github.com/winedepot/pinot), which is a fork of [Apache Pinot](https://pinot.apache.org/)

## How to build it

Usage:

```SHELL
./docker-build-and-push.sh [Docker Tag] [Git Branch]
```

This script will check out winedepot Pinot Fork [Winedepot Pinot Fork](https://github.com/winedepot/pinot) on branch `[Git Branch]`.

The built docker image is tagged as `winedepot/pinot:[Docker Tag]` then published to [Dockerhub Repo winedepot/pinot](https://hub.docker.com/r/winedepot/pinot). 

Example of publish a snapshot:
```SHELL
./docker-build-and-push.sh dev-snapshot-20190325 develop
```

Example of publish a release version:
```SHELL
./docker-build-and-push.sh release-0.1.0 release-0.1.0
```

There is also a build only script which will only build the branch and tag the image.
```SHELL
./docker-build.sh dev-snapshot-20190325 develop
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
docker run -p 9000:9000 winedepot/pinot:release-0.1.0 StartController -zkAddress 172.17.0.2:2181
```

### Pinot Broker
Example of bring up a local broker:
```SHELL
docker run -p 8099:8099 winedepot/pinot:release-0.1.0 StartBroker -zkAddress 172.17.0.2:2181
```

### Pinot Server
Example of bring up a local server:
```SHELL
docker run -p 8098:8098 winedepot/pinot:release-0.1.0 StartServer -zkAddress 172.17.0.2:2181
```
