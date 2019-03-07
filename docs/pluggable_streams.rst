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

.. _pluggable-streams:

Pluggable Streams
=================

Prior to commit `ba9f2d <https://github.com/apache/incubator-pinot/commit/ba9f2ddfc0faa42fadc2cc48df1d77fec6b174fb>`_, Pinot was only able to support reading
from `Kafka <https://kafka.apache.org/documentation/>`_ stream.

Pinot now enables its users to write plug-ins to read from pub-sub streams
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
* Events with higher offsets should be more recent (the offsets of events need not be contiguous)

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
    "stream.foo.consumer.type": "lowlevel",
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


An example of this implementation can be found in the `KafkaConsumerFactory <org.apache.pinot.core.realtime.impl.kafka.KafkaConsumerFactory>`_, which is an implementation for the kafka stream.
