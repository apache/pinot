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
# Stream ingestion plugins

Shared stream connector dependencies belong in `pinot-stream-ingestion/pom.xml`; connector-specific dependencies belong
in the connector's own POM.

## Kafka 4 migration

`pinot-kafka-4.0` is the only Kafka implementation and the default for tables that omit the consumer factory. New table
configs can use:

```json
"stream.kafka.consumer.factory.class.name": "org.apache.pinot.plugin.stream.kafka40.KafkaConsumerFactory"
```

Existing `kafka20` and `kafka30` factory names, and the older `impl.kafka2` and `impl.kafka3` factory names, resolve directly
to the Kafka 4 implementation. Existing `plugins.include` entries and plugin-qualified factory names using
`pinot-kafka-2.0` or `pinot-kafka-3.0` also resolve to `pinot-kafka-4.0`. Custom deployments must package the Kafka 4 plugin;
the aliases do not retain the Kafka 3 client or its protocol support.

Kafka 4 clients require brokers version 2.1 or newer; upgrading the Pinot plugin does not require Kafka 4 brokers or a
broker-side ZooKeeper-to-KRaft migration. Review [Kafka's client upgrade guide](https://kafka.apache.org/42/getting-started/upgrade/)
for removed configuration/API support, especially OAuth callback class names and endpoint allowlists. Validate the
TLS/SASL/OAuth or MSK IAM settings used by each source against the replacement client.

Retain existing factory names during rolling upgrades, and avoid writing Kafka 4 factory names until every component
that consumes table configs has the Kafka 4 plugin. Validate partition discovery, ingestion from saved offsets, restart
and replay on representative sources before completing rollout. Before rolling back to an image without the Kafka 4
plugin, restore legacy factory names in any table configs changed after the upgrade. Existing offsets and stream config
keys are unchanged.
