---
title: Running Pinot in Docker
sidebar_label: Docker
description: Using Pinot on Docker
source_url: https://github.com/

---

Apache Pinot started maintaing the [`apachepinot/pinot`](https://hub.docker.com/r/apachepinot/pinot) Docker images
available on [Docker Hub](https://hub.docker.com/r/apachepinot/pinot/tags)

> <b>Prerequisites:</b> <br/> <p>&nbsp; <a href="https://hub.docker.com/editions/community/docker-ce-desktop-mac" target="_blank">Install Docker</a><br/> &nbsp;Try <a href="https://hub.docker.com/editions/community/docker-ce-desktop-mac" target="_blank">Kubernetes Quickstart</a> if you already have a minikube cluster or docker kubernetes setup.</p>

## Running

Create an isolated bridge network in docker

```bash
docker network create -d bridge pinot-demo
```

We'll be running a docker image apachepinot/pinot:latest to run a quick start, which does the following:

- Sets up the Pinot cluster QuickStartCluster
- Creates a sample table and loads sample data
  
There's 3 types of quick start

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs
  block={true}
  defaultValue="Batch"
  urlKey="arch"
  values={[{"label":"Batch","value":"Batch"},{"label":"Streaming","value":"Streaming"},{"label":"Hybrid","value":"Hybrid"}]}>

<TabItem value="Batch">

Batch

1. Starts Pinot deployment by starting:

   - Apache Zookeeper
   - Pinot Controller
   - Pinot Broker
   - Pinot Server

2. Creates a demo table:

   - baseballStats

3. Launches a standalone data ingestion job:

   - build one Pinot segment for a given csv data file for table baseballStats
   - push built segment to Pinot controller

4. Issues sample queries to Pinot

    ```bash
    docker run \
      --network=pinot-demo \
      --name pinot-quickstart \
      -p 9000:9000 \
      -d apachepinot/pinot:latest QuickStart \
      -type batch
    ```

</TabItem>

<TabItem value="Streaming">

Streaming:

1. Starts Pinot deployment by starting:

   - Apache Kafka
   - Apache Zookeeper
   - Pinot Controller
   - Pinot Broker
   - Pinot Server
  
1. Creates a demo table: **meetupRsvp**

1. Launches a **meetup** stream and publish data to a Kafka: **meetupRSVPEvents** to be subscribed by Pinot

1. Issues sample queries to Pinot

</TabItem>

<TabItem value="Hybrid">

Hybrid:

1. Starts Pinot deployment by starting:

   - Apache Kafka
   - Apache Zookeeper
   - Pinot Controller
   - Pinot Broker
   - Pinot Server

1. Creates a demo table: **airlineStats**

1. Launches a standalone data ingestion job:

   - build Pinot segments under a given directory of Avro files for table airlineStats
   - push built segments to Pinot controller

1. Launches a stream of flights stats and publish data to a Kafka topic airlineStatsEvents to be subscribed by Pinot

1. Issues sample queries to Pinot

```bash
# Stop previous container, if any, or use different network
docker run \
    --network=pinot-demo \
    --name pinot-quickstart \
    -p 9000:9000 \
    -d apachepinot/pinot:latest QuickStart \
    -type hybrid
```

</TabItem>

</Tabs>

That's it! We've spun up a Pinot cluster.

It may take a while for all Pinot components to start and for the sample data to be loaded. Use the below command to check the container logs

```bash
docker logs pinot-quickstart -f
```

You can head over to  Exploring Pinot to check out the data in the `baseballStats`, `meetupRSVP` or the `airlineStats` table.
