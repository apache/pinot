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
This is a docker image of [Apache Pinot](https://github.com/apache/incubator-pinot).

## How to build a docker image

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

* Example of building and tagging a snapshot on your own fork:
```SHELL
./docker-build.sh pinot_fork:snapshot-5.2 snapshot-5.2 https://github.com/your_own_fork/pinot.git
```

* Example of building a release version:
```SHELL
./docker-build.sh pinot:release-0.1.0 release-0.1.0 https://github.com/apache/incubator-pinot.git
```

## How to publish a docker image

Script `docker-push.sh` publishes a given docker image to your docker registry.

In order to push to your own repo, the image needs to be explicitly tagged with the repo name.

* Example of publishing a image to [apachepinot/pinot](https://cloud.docker.com/u/apachepinot/repository/docker/apachepinot/pinot) dockerHub repo.

```SHELL
./docker-push.sh apachepinot/pinot:latest
```

* Tag a built image, then push.
````SHELL
docker tag pinot:release-0.1.0 apachepinot/pinot:release-0.1.0
docker push apachepinot/pinot:release-0.1.0
````

Script `docker-build-and-push.sh` builds and publishes this docker image to your docker registry after build.

* Example of building and publishing a image to [apachepinot/pinot](https://cloud.docker.com/u/apachepinot/repository/docker/apachepinot/pinot) dockerHub repo.

```SHELL
./docker-build-and-push.sh apachepinot/pinot:latest master https://github.com/apache/incubator-pinot.git
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

## QuickStart


### Use docker compose to bring up Pinot stack

Please note that this quickstart is based on Kafka 2.0.

Below is a script to use docker compose to bring up zookeeper/kafka/pinot-controller/pinot-broker/pinot-server
```SHELL
docker-compose -f docker-compose.yml up
```

### Create table and load data from Kafka.

Below is the script to create airlineStats table
```SHELL
docker run --network=pinot_default apachepinot/pinot:0.3.0-SNAPSHOT AddTable -schemaFile examples/stream/airlineStats/airlineStats_schema.json -tableConfigFile examples/stream/airlineStats/docker/airlineStats_realtime_table_config.json -controllerHost pinot-controller -controllerPort 9000 -exec
```

Below is the script to ingest airplane stats data to Kafka
```SHELL
docker run --network=pinot_default apachepinot/pinot:0.3.0-SNAPSHOT StreamAvroIntoKafka -avroFile examples/stream/airlineStats/sample_data/airlineStats_data.avro -kafkaTopic flights-realtime -kafkaBrokerList kafka:9092 -zkAddress zookeeper:2181
```

In order to query pinot, try to open `localhost:9000` from your browser.
