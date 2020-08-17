..
.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..   http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.
..

.. warning::  The documentation is not up-to-date and has moved to `Apache Pinot Docs <https://docs.pinot.apache.org/>`_.

.. _pluggable-streams:

Pluggable Streams
=================

.. note::

  This section is a pre-read if you are planning to develop plug-ins for streams other than Kafka. Pinot supports Kafka out of the box.

Prior to commit `ba9f2d <https://github.com/apache/incubator-pinot/commit/ba9f2ddfc0faa42fadc2cc48df1d77fec6b174fb>`_, Pinot was only able to support consuming
from `Kafka <https://kafka.apache.org/documentation/>`_ stream.

Pinot now enables its users to write plug-ins to consume from pub-sub streams
other than Kafka. (Please refer to `Issue #2583 <https://github.com/apache/incubator-pinot/issues/2583>`_)

Some of the streams for which plug-ins can be added are:

* `Amazon kinesis <https://docs.aws.amazon.com/streams/latest/dev/building-enhanced-consumers-kcl.html>`_
* `Azure Event Hubs <https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-java-get-started-receive-eph>`_
* `LogDevice <https://code.fb.com/core-data/logdevice-a-distributed-data-store-for-logs/>`_
* `Pravega <http://pravega.io/docs/latest/javadoc/>`_
* `Pulsar <https://pulsar.apache.org/docs/en/client-libraries-java/>`_


You may encounter some limitations either in Pinot or in the stream system while developing plug-ins.
Please feel free to get in touch with us when you start writing a stream plug-in, and we can help you out.
We are open to receiving PRs in order to improve these abstractions if they do not work for a certain stream implementation.

Refer to `Consuming and Indexing rows in Realtime <https://cwiki.apache.org/confluence/display/PINOT/Consuming+and+Indexing+rows+in+Realtime>`_
for details on how Pinot consumes streaming data.

Requirements to support Stream Level (High Level) consumers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The stream should provide the following guarantees:

* Exactly once delivery (unless restarting from a checkpoint) for each consumer of the stream.
* (Optionally) support mechanism to split events (in some arbitrary fashion) so that each event in the stream is delivered exactly to one host out of set of hosts.
* Provide ways to save a checkpoint for the data consumed so far. If the stream is partitioned, then this checkpoint is a vector of checkpoints for events consumed from individual partitions.
* The checkpoints should be recorded only when Pinot makes a call to do so.
* The consumer should be able to start consumption from one of:

  * latest avaialble data
  * earliest available data
  * last saved checkpoint

Requirements to support Partition Level (Low Level) consumers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

While consuming rows at a partition level, the stream should support the following
properties:

* Stream should provide a mechanism to get the current number of partitions.
* Each event in a partition should have a unique offset that is not more than 64 bits long.
* Refer to a partition as a number not exceeding 32 bits long.
* Stream should provide the following mechanisms to get an offset for a given partition of the stream:

  * get the offset of the oldest event available (assuming events are aged out periodically) in the partition.
  * get the offset of the most recent event published in the partition
  * (optionally) get the offset of an event that was published at a specified time

* Stream should provide a mechanism to consume a set of events from a partition starting from a specified offset.
* Pinot assumes that the offsets of incoming events are monotonically increasing; *i.e.*, if Pinot
  consumes an event at offset ``o1``, then the offset ``o2`` of the following event should be such that
  ``o2 > o1``.

In addition, we have an operational requirement that the number of partitions should not be
reduced over time.

Stream plug-in implementation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
In order to add a new type of stream (say,Foo) implement the following classes:

#. FooConsumerFactory extends `StreamConsumerFactory <https://github.com/apache/incubator-pinot/blob/master/pinot-core/src/main/java/org/apache/pinot/core/realtime/stream/StreamConsumerFactory.java>`_
#. FooPartitionLevelConsumer implements `PartitionLevelConsumer <https://github.com/apache/incubator-pinot/blob/master/pinot-core/src/main/java/org/apache/pinot/core/realtime/stream/PartitionLevelConsumer.java>`_
#. FooStreamLevelConsumer implements `StreamLevelConsumer <https://github.com/apache/incubator-pinot/blob/master/pinot-core/src/main/java/org/apache/pinot/core/realtime/stream/StreamLevelConsumer.java>`_
#. FooMetadataProvider implements `StreamMetadataProvider <https://github.com/apache/incubator-pinot/blob/master/pinot-core/src/main/java/org/apache/pinot/core/realtime/stream/StreamMetadataProvider.java>`_
#. FooMessageDecoder implements `StreamMessageDecoder <https://github.com/apache/incubator-pinot/blob/master/pinot-core/src/main/java/org/apache/pinot/core/realtime/stream/StreamMessageDecoder.java>`_

Depending on stream level or partition level, your implementation needs to include StreamLevelConsumer or PartitionLevelConsumer.


The properties for the stream implementation are to be set in the table configuration, inside `streamConfigs <https://github.com/apache/incubator-pinot/blob/master/pinot-core/src/main/java/org/apache/pinot/core/realtime/stream/StreamConfig.java>`_ section.

Use the ``streamType`` property to define the stream type. For example, for the implementation of stream ``foo``, set the property ``"streamType" : "foo"``.

The rest of the configuration properties for your stream should be set with the prefix ``"stream.foo"``. Be sure to use the same suffix for: (see examples below):

* topic
* consumer type
* stream consumer factory
* offset
* decoder class name
* decoder properties
* connnection timeout
* fetch timeout

All values should be strings. For example:


.. code-block:: none

    "streamType" : "foo",
    "stream.foo.topic.name" : "SomeTopic",
    "stream.foo.consumer.type": "LowLevel",
    "stream.foo.consumer.factory.class.name": "fully.qualified.pkg.ConsumerFactoryClassName",
    "stream.foo.consumer.prop.auto.offset.reset": "largest",
    "stream.foo.decoder.class.name" : "fully.qualified.pkg.DecoderClassName",
    "stream.foo.decoder.prop.a.decoder.property" : "decoderPropValue",
    "stream.foo.connection.timeout.millis" : "10000", // default 30_000
    "stream.foo.fetch.timeout.millis" : "10000" // default 5_000


You can have additional properties that are specific to your stream. For example:

.. code-block:: none

  "stream.foo.some.buffer.size" : "24g"

In addition to these properties, you can define thresholds for the consuming segments:

* rows threshold
* time threshold

The properties for the thresholds are as follows:

.. code-block:: none

  "realtime.segment.flush.threshold.size" : "100000"
  "realtime.segment.flush.threshold.time" : "6h"


An example of this implementation can be found in the `KafkaConsumerFactory <https://github.com/apache/incubator-pinot/blob/master/pinot-core/src/main/java/org/apache/pinot/core/realtime/impl/kafka/KafkaConsumerFactory.java>`_, which is an implementation for the kafka stream.


Kafka 2.x Plugin
^^^^^^^^^^^^^^^^

Pinot provides stream plugin support for Kafka 2.x version.
Although the version used in this implementation is kafka 2.0.0, it's possible to compile it with higher kafka lib version, e.g. 2.1.1.

How to build and release Pinot package with Kafka 2.x connector
---------------------------------------------------------------

.. code-block:: none

  mvn clean package -DskipTests -Pbin-dist -Dkafka.version=2.0

How to use Kafka 2.x connector
------------------------------

- **Use Kafka Stream(High) Level Consumer**

Below is a sample ``streamConfigs`` used to create a realtime table with Kafka Stream(High) level consumer.

Kafka 2.x HLC consumer uses ``org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory`` in config ``stream.kafka.consumer.factory.class.name``.

.. code-block:: none

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


- **Use Kafka Partition(Low) Level Consumer**

Below is a sample table config used to create a realtime table with Kafka Partition(Low) level consumer:

.. code-block:: none

  {
    "tableName": "meetupRsvp",
    "tableType": "REALTIME",
    "segmentsConfig": {
      "timeColumnName": "mtime",
      "timeType": "MILLISECONDS",
      "segmentPushType": "APPEND",
      "segmentAssignmentStrategy": "BalanceNumSegmentAssignmentStrategy",
      "schemaName": "meetupRsvp",
      "replication": "1",
      "replicasPerPartition": "1"
    },
    "tenants": {},
    "tableIndexConfig": {
      "loadMode": "MMAP",
      "streamConfigs": {
        "streamType": "kafka",
        "stream.kafka.consumer.type": "LowLevel",
        "stream.kafka.topic.name": "meetupRSVPEvents",
        "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder",
        "stream.kafka.consumer.factory.class.name": "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
        "stream.kafka.zk.broker.url": "localhost:2191/kafka",
        "stream.kafka.broker.list": "localhost:19092"
      }
    },
    "metadata": {
      "customConfigs": {}
    }
  }

Please note:

1. Config ``replicasPerPartition`` under ``segmentsConfig`` is required to specify table replication.
#. Config ``stream.kafka.consumer.type`` should be specified as ``LowLevel`` to use partition level consumer. (The use of ``simple`` instead of ``LowLevel`` is deprecated)
#. Configs ``stream.kafka.zk.broker.url`` and ``stream.kafka.broker.list`` are required under ``tableIndexConfig.streamConfigs`` to provide kafka related information.

Here is another example which is used to read avro messages using the schema id from confluent schema registry.
The only difference between this and the KafkaAvroMessageDecoder is that the KafkaConfluentSchemaRegistryAvroMessageDecoder uses
confluent schema registry:

.. code-block:: none

  {
    "tableName": "meetupRsvp",
    "tableType": "REALTIME",
    "segmentsConfig": {
      "timeColumnName": "mtime",
      "timeType": "MILLISECONDS",
      "segmentPushType": "APPEND",
      "segmentAssignmentStrategy": "BalanceNumSegmentAssignmentStrategy",
      "schemaName": "meetupRsvp",
      "replication": "1",
      "replicasPerPartition": "1"
    },
    "tenants": {},
    "tableIndexConfig": {
      "loadMode": "MMAP",
      "streamConfigs": {
        "streamType": "kafka",
        "stream.kafka.consumer.type": "LowLevel",
        "stream.kafka.topic.name": "meetupRSVPEvents",
        "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder",
        "stream.kafka.consumer.factory.class.name": "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
        "stream.kafka.zk.broker.url": "localhost:2191/kafka",
        "stream.kafka.broker.list": "localhost:19092"
      }
    },
    "metadata": {
      "customConfigs": {}
    }
  }

Here is another example which uses SSL based authentication to talk with kafka
and schema-registry. Notice there are two sets of SSL options, ones starting with
`ssl.` are for kafka consumer and ones with `stream.kafka.decoder.prop.schema.registry.`
are for `SchemaRegistryClient` used by `KafkaConfluentSchemaRegistryAvroMessageDecoder`.


.. code-block:: none

  {
    "tableName": "meetupRsvp",
    "tableType": "REALTIME",
    "segmentsConfig": {
      "timeColumnName": "mtime",
      "timeType": "MILLISECONDS",
      "segmentPushType": "APPEND",
      "segmentAssignmentStrategy": "BalanceNumSegmentAssignmentStrategy",
      "schemaName": "meetupRsvp",
      "replication": "1",
      "replicasPerPartition": "1"
    },
    "tenants": {},
    "tableIndexConfig": {
      "loadMode": "MMAP",
      "streamConfigs": {
        "streamType": "kafka",
        "stream.kafka.consumer.type": "LowLevel",
        "stream.kafka.topic.name": "meetupRSVPEvents",
        "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder",
        "stream.kafka.consumer.factory.class.name": "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
        "stream.kafka.zk.broker.url": "localhost:2191/kafka",
        "stream.kafka.broker.list": "localhost:19092",

        "schema.registry.url": "",
        "security.protocol": "",
        "ssl.truststore.location": "",
        "ssl.keystore.location": "",
        "ssl.truststore.password": "",
        "ssl.keystore.password": "",
        "ssl.key.password": "",

        "stream.kafka.decoder.prop.schema.registry.rest.url": "",
        "stream.kafka.decoder.prop.schema.registry.ssl.truststore.location": "",
        "stream.kafka.decoder.prop.schema.registry.ssl.keystore.location": "",
        "stream.kafka.decoder.prop.schema.registry.ssl.truststore.password": "",
        "stream.kafka.decoder.prop.schema.registry.ssl.keystore.password": "",
        "stream.kafka.decoder.prop.schema.registry.ssl.keystore.type": "",
        "stream.kafka.decoder.prop.schema.registry.ssl.truststore.type": "",
        "stream.kafka.decoder.prop.schema.registry.ssl.key.password": "",
        "stream.kafka.decoder.prop.schema.registry.ssl.protocol": "",
      }
    },
    "metadata": {
      "customConfigs": {}
    }
  }

Upgrade from Kafka 0.9 connector to Kafka 2.x connector
-------------------------------------------------------

* Update table config for both high level and low level consumer:
  Update config: ``stream.kafka.consumer.factory.class.name`` from ``org.apache.pinot.plugin.stream.kafka09.KafkaConsumerFactory`` to ``org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory``.

* If using Stream(High) level consumer:
  Please also add config ``stream.kafka.hlc.bootstrap.server`` into ``tableIndexConfig.streamConfigs``.
  This config should be the URI of Kafka broker lists, e.g. ``localhost:9092``.


How to use this plugin with higher Kafka version?
-------------------------------------------------

This connector is also suitable for Kafka lib version higher than ``2.0.0``.
In ``pinot-connector-kafka-2.0/pom.xml`` change the ``kafka.lib.version`` from ``2.0.0`` to ``2.1.1`` will make this Connector working with Kafka ``2.1.1``.
