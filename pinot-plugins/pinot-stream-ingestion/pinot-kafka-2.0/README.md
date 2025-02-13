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

This is an implementation of the kafka stream for kafka versions 2.x.

A stream plugin for another version of kafka, or another stream, can be added in a similar fashion. Refer to documentation on [Stream Ingestion Plugin](https://docs.pinot.apache.org/developers/plugin-architecture/write-custom-plugins/write-your-stream) for the specific interfaces to implement.

* How to build and release Pinot package with Kafka 2.x connector
```$xslt
./mvnw clean package -DskipTests -Pbin-dist
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
