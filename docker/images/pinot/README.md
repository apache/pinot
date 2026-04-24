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
This is a docker image of [Apache Pinot](https://github.com/apache/pinot).

## How to build a docker image

There is a docker build script which will build a given Git repo/branch and tag the image.

Usage:

```SHELL
./docker-build.sh [Docker Tag] [Git Branch] [Pinot Git URL] [Kafka Version] [Java Version] [JDK Version] [OpenJDK Image ]
```

This script will check out Pinot Repo `[Pinot Git URL]` on branch `[Git Branch]` and build the docker image for that.

The docker image is tagged as `[Docker Tag]`.

`Docker Tag`: Name and tag your docker image. Default is `pinot:latest`.

`Git Branch`: The Pinot branch to build. Default is `master`.

`Pinot Git URL`: The Pinot Git Repo to build, users can set it to their own fork. Please note that, the URL is `https://` based, not `git://`. Default is the Apache Repo: `https://github.com/apache/pinot.git`.

`Kafka Version`: The Kafka Version to build pinot with. Default is `2.0`

`Java Version`: The Java Build and Runtime image version. Default is `11`

`JDK Version`: The JDK parameter to build pinot, set as part of maven build option: `-Djdk.version=${JDK_VERSION}`. Default is `11`

`OpenJDK Image`: Base image to use for Pinot build and runtime. Default is `openjdk`.

* Example of building and tagging a snapshot on your own fork:
```SHELL
./docker-build.sh pinot_fork:snapshot-5.2 snapshot-5.2 https://github.com/your_own_fork/pinot.git
```

* Example of building a release version:
```SHELL
./docker-build.sh pinot:release-0.1.0 release-0.1.0 https://github.com/apache/pinot.git
```

### Build image for arm64

On Apple Silicon (M1/M2) or any arm64 host, pass `--platform linux/arm64`:

```SHELL
docker build -t pinot:latest --platform linux/arm64 --no-cache --network=host \
  --build-arg PINOT_GIT_REF=master --build-arg JDK_VERSION=11 -f Dockerfile .
```

On an x86 machine with QEMU installed, add `--platform linux/arm64` the same way:
```SHELL
docker build -t pinot:latest --platform linux/arm64 --no-cache \
  --build-arg PINOT_GIT_REF=master --build-arg JDK_VERSION=11 -f Dockerfile .
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
./docker-build-and-push.sh apachepinot/pinot:latest master https://github.com/apache/pinot.git
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

The included `docker-compose.yml` uses the official [apache/kafka](https://hub.docker.com/r/apache/kafka)
image running in KRaft mode (no ZooKeeper required for Kafka). ZooKeeper is still needed
for Pinot's own cluster coordination.

Below is a script to use docker compose to bring up zookeeper/kafka/pinot-controller/pinot-broker/pinot-server
```SHELL
docker-compose -f docker-compose.yml up
```

### Create table and load data from Kafka.

Below is the script to create airlineStats table
```SHELL
docker run --network=pinot_default apachepinot/pinot:latest AddTable -schemaFile examples/stream/airlineStats/airlineStats_schema.json -tableConfigFile examples/stream/airlineStats/docker/airlineStats_realtime_table_config.json -controllerHost pinot-controller -controllerPort 9000 -exec
```

Below is the script to ingest airplane stats data to Kafka
```SHELL
docker run --network=pinot_default apachepinot/pinot:latest StreamAvroIntoKafka -avroFile examples/stream/airlineStats/rawdata/airlineStats_data.avro -kafkaTopic flights-realtime -kafkaBrokerList kafka:9092
```

In order to query pinot, try to open `localhost:9000` from your browser.

## Developer Setup: All-in-One Without Demo Data

For more deployment options, see the
[Running Pinot in Docker](https://docs.pinot.apache.org/basics/getting-started/running-pinot-in-docker)
guide in the Pinot documentation.

The `QuickStart` command is convenient for trying Pinot, but it has two limitations for
ongoing development work:

1. It populates demo tables (e.g., `baseballStats`, `airlineStats`) on every start.
2. It deletes all data on container restart.

If you want a lightweight, all-in-one Pinot instance that starts empty and preserves your
data across restarts, use the `StartServiceManager` command instead. Unlike `QuickStart`,
`StartServiceManager` requires a separate ZooKeeper instance (it does not embed one).

### All-in-one with docker compose

Below is a minimal `docker-compose.yml` for a persistent, empty Pinot instance with
ZooKeeper:

```yaml
version: '3'
services:
  zookeeper:
    image: zookeeper:latest
    ports:
      - "2181:2181"
    environment:
      ZOO_MY_ID: 1
    volumes:
      - ./pinot-data/zookeeper/data:/data
      - ./pinot-data/zookeeper/datalog:/datalog
  pinot:
    image: apachepinot/pinot:latest
    command:
      - StartServiceManager
      - -zkAddress
      - zookeeper:2181
      - -clusterName
      - PinotCluster
      - -bootstrapServices
      - CONTROLLER
      - BROKER
      - SERVER
    ports:
      - "9000:9000"   # Controller UI and API
      - "8099:8099"   # Broker query endpoint
      - "8098:8098"   # Server
    volumes:
      - ./pinot-data/controller:/tmp/data/controller
      - ./pinot-data/server:/tmp/data/server
    depends_on:
      - zookeeper
```

If you also need Kafka for real-time stream ingestion, add it using the official
[apache/kafka](https://hub.docker.com/r/apache/kafka) image in KRaft mode (no ZooKeeper
needed for Kafka):

```yaml
  kafka:
    image: apache/kafka:latest
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENERS: PLAINTEXT://kafka:19092,EXTERNAL://0.0.0.0:9092,CONTROLLER://kafka:29093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:19092,EXTERNAL://kafka:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      CLUSTER_ID: pinot-kafka-cluster-001
```

### All-in-one with docker run

First, start ZooKeeper:

```SHELL
docker run -d --name pinot-zookeeper \
  -p 2181:2181 \
  -v $(pwd)/pinot-data/zookeeper/data:/data \
  -v $(pwd)/pinot-data/zookeeper/datalog:/datalog \
  zookeeper:latest
```

Then start Pinot with all services in one container:

```SHELL
docker run -d --name pinot-dev \
  --link pinot-zookeeper:zookeeper \
  -p 9000:9000 -p 8099:8099 -p 8098:8098 \
  -v $(pwd)/pinot-data/controller:/tmp/data/controller \
  -v $(pwd)/pinot-data/server:/tmp/data/server \
  apachepinot/pinot:latest \
  StartServiceManager \
    -zkAddress zookeeper:2181 \
    -clusterName PinotCluster \
    -bootstrapServices CONTROLLER BROKER SERVER
```

This starts all services in one process without creating any demo tables. Your data is
persisted on the host under `./pinot-data/` and survives container restarts.

### Using an existing ZooKeeper (e.g. from a Kafka stack)

If you already run ZooKeeper separately (e.g. as part of a Kafka stack), just point
`-zkAddress` at your existing instance:

```SHELL
docker run -d --name pinot-dev \
  --network my-network \
  -p 9000:9000 -p 8099:8099 -p 8098:8098 \
  -v $(pwd)/pinot-data/controller:/tmp/data/controller \
  -v $(pwd)/pinot-data/server:/tmp/data/server \
  apachepinot/pinot:latest \
  StartServiceManager \
    -zkAddress zookeeper:2181 \
    -clusterName PinotCluster \
    -bootstrapServices CONTROLLER BROKER SERVER
```

### Creating your own tables

Once the cluster is up, create tables via the Controller API or UI at
`http://localhost:9000`:

```SHELL
# Add a schema
curl -F schemaName=@myTable_schema.json http://localhost:9000/schemas

# Add a table
curl -i -X POST -H 'Content-Type: application/json' \
  -d @myTable_offline_table_config.json \
  http://localhost:9000/tables
```

### QuickStart vs StartServiceManager

| Feature | QuickStart | StartServiceManager |
|---|---|---|
| Starts all services in one process | Yes | Yes |
| Populates demo tables on startup | Yes (10+ tables) | No |
| Preserves data across restarts | No (deletes on stop) | Yes (with volumes) |
| Embedded ZooKeeper | Yes | No (requires external ZK) |
| Suitable for development | Limited | Recommended |
| Suitable for production | No | No (use separate containers) |
