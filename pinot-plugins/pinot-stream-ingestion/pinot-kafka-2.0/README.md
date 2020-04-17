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
# Pinot connector for kafka 2.x

This is an implementation of the kafka stream for kafka versions 2.x The version used in this implementation is kafka 2.0.0.

A stream plugin for another version of kafka, or another stream, can be added in a similar fashion. Refer to documentation on (Pluggable Streams)[https://docs.pinot.apache.org/developers-and-contributors/extending-pinot/pluggable-streams] for the specfic interfaces to implement.

* How to build and release Pinot package with Kafka 2.x connector
```$xslt
mvn clean package -DskipTests -Pbin-dist -Dkafka.version=2.0
```

* How to use Kafka 2.x connector
Below is a sample `streamConfigs` used to create a realtime table with Kafka Stream(High) level consumer:
```$xslt
"streamConfigs": {
  "streamType": "kafka",
  "stream.kafka.consumer.type": "highLevel",
  "stream.kafka.topic.name": "meetupRSVPEvents",
  "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder",
  "stream.kafka.hlc.zk.connect.string": "localhost:2191/kafka",
  "stream.kafka.consumer.factory.class.name": "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
  "stream.kafka.zk.broker.url": "localhost:2191/kafka",
  "stream.kafka.hlc.bootstrap.server": "localhost:19092"
}
```

* Upgrade from Kafka 0.9 connector to Kafka 2.x connector:

  1. Update  table config:
 `stream.kafka.consumer.factory.class.name` from `org.apache.pinot.plugin.stream.kafka09.KafkaConsumerFactory` to `org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory`.

  1. If using Stream(High) level consumer, please also add config `stream.kafka.hlc.bootstrap.server` into `tableIndexConfig.streamConfigs`.
This config should be the URI of Kafka broker lists, e.g. `localhost:9092`.

* How to upgrade to Kafka version > `2.0.0`
This connector is also suitable for Kafka lib version higher than `2.0.0`.
In `pinot-connector-kafka-2.0/pom.xml` change the `kafka.lib.version` from `2.0.0` to `2.1.1` will make this Connector working with Kafka `2.1.1`.
