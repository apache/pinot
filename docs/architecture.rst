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

.. _pinot-architecture-section:

Architecture
============

.. _pinot-architecture-diagram:

.. figure:: img/pinot-architecture.png

   Pinot Architecture Overview

Terminology
-----------

*Table*
    A table is a logical abstraction to refer to a collection of related data. It consists of columns and rows (documents).
*Segment*
    Data in table is divided into (horizontal) shards referred to as segments.

Pinot Components
----------------

*Pinot Controller*
    Manages other pinot components (brokers, servers) as well as controls assignment of tables/segments to servers.
*Pinot Server*
    Hosts one or more segments and serves queries from those segments
*Pinot Broker*
    Accepts queries from clients and routes them to one or more servers, and returns consolidated response to the client.

Pinot leverages `Apache Helix <http://helix.apache.org>`_ for cluster management.
Helix is a cluster management framework to manage replicated, partitioned resources in a distributed system.
Helix uses Zookeeper to store cluster state and metadata.

Briefly, Helix divides nodes into three logical components based on their responsibilities:

*Participant*
    The nodes that host distributed, partitioned resources
*Spectator*
    The nodes that observe the current state of each Participant and use that information to access the resources.
    Spectators are notified of state changes in the cluster (state of a participant, or that of a partition in a participant).
*Controller*
    The node that observes and controls the Participant nodes. It is responsible for coordinating all transitions
    in the cluster and ensuring that state constraints are satisfied while maintaining cluster stability

Pinot Controller hosts Helix Controller, in addition to hosting REST APIs for Pinot cluster administration and data ingestion.
There can be multiple instances of Pinot controller for redundancy. If there are multiple controllers, Pinot expects that all
of them are configured with the same back-end storage system so that they have a common view of the segments (*e.g.* NFS).
Pinot can use other storage systems such as HDFS or `ADLS <https://azure.microsoft.com/en-us/services/storage/data-lake-storage/>`_.

Pinot Servers are modeled as Helix Participants, hosting Pinot tables (referred to as *resources* in helix terminology).
Segments of a table are modeled as Helix partitions (of a resource). Thus, a Pinot server hosts one or more helix partitions of one
or more helix resources (*i.e.* one or more segments of one or more tables).

Pinot Brokers are modeled as Spectators. They need to know the location of each segment of a table (and each replica of the
segments)
and route requests to the
appropriate server that hosts the segments of the table being queried. The broker ensures that all the rows of the table
are queried exactly once so as to return correct, consistent results for a query. The brokers (or servers) may optimize
to prune some of the segments as long as accuracy is not satisfied. In case of hybrid tables, the brokers ensure that
the overlap between realtime and offline segment data is queried exactly once.
Helix provides the framework by which spectators can learn the location (*i.e.* participant) in which each partition
of a resource resides. The brokers use this mechanism to learn the servers that host specific segments of a table.

Pinot Tables
------------

Pinot supports realtime, or offline, or hybrid tables. Data in Pinot tables is contained in the segments
belonging to that table. A Pinot table is modeled as a Helix resource.  Each segment of a table is modeled as a Helix Partition.

Table Schema defines column names and their metadata. Table configuration and schema is stored in zookeeper.

Offline tables ingest pre-built pinot-segments from external data stores, whereas Reatime tables
ingest data from streams (such as Kafka) and build segments.

A hybrid Pinot table essentially has both realtime as well as offline tables.
In such a table, offline segments may be pushed periodically (say, once a day). The retention on the offline table
can be set to a high value (say, a few years) since segments are coming in on a periodic basis, whereas the retention
on the realtime part can be small (say, a few days). Once an offline segment is pushed to cover a recent time period,
the brokers automatically switch to using the offline table for segments in *that* time period, and use realtime table
only to cover later segments for which offline data may not be available yet.

Note that the query does not know the existence of offline or realtime tables. It only specifies the table name
in the query.

See section on :ref:`table-config-section` for how to customize table configuration as per requirements.


Ingesting Offline data
^^^^^^^^^^^^^^^^^^^^^^
Segments for offline tables are constructed outside of Pinot, typically in Hadoop via map-reduce jobs
and ingested into Pinot via REST API provided by the Controller.
Pinot provides libraries to create Pinot segments out of input files in AVRO, JSON or CSV formats in a hadoop job, and push
the constructed segments to the controllers via REST APIs.

When an Offline segment is ingested, the controller looks up the table's configuration and assigns the segment
to the servers that host the table. It may assign multiple servers for each segment depending on the number of replicas
configured for that table.

Pinot supports different segment assignment strategies that are optimized for various use cases.

Once segments are assigned, Pinot servers get notified via Helix to "host" the segment. The servers download the segments
(as a cached local copy to serve queries) and load them into local memory. All segment data is maintained in memory as long
as the server hosts that segment.

Once the server has loaded the segment, Helix notifies brokers of the availability of these segments. The brokers
start include the new
segments for queries. Brokers support different routing strategies depending on the type of table, the segment assignment
strategy and the use case.

Data in offline segments are immutable (Rows cannot be added, deleted, or modified). However, segments may be replaced with modified data.

.. _ingesting-realtime-data:

Ingesting Realtime Data
^^^^^^^^^^^^^^^^^^^^^^^
Segments for realtime tables are constructed by Pinot servers with rows ingested from data streams such as Kafka.
Rows ingested from streams are made available for query processing as soon as they are ingested, thus enabling
applications such as those that need real-time charts on analytics.

In large scale installations, data in streams is typically split across multiple stream partitions. The underlying
stream may provide consumer implementations that allow applications to consume data from any subset of partitions,
including all partitions (or, just from one partition).

A pinot table can be configured to consume from streams in one of two modes:

    * ``LowLevel``: This is the preferred mode of consumption. Pinot creates independent partition-level consumers for
      each partition. Depending on the the configured number of replicas, multiple consumers may be created for
      each partition, taking care that no two replicas exist on the same server host. Therefore you need to provision
      *at least* as many hosts as the number of replcias configured.

    * ``HighLevel``: Pinot creates *one* stream-level consumer that consumes from all partitions. Each message consumed
      could be from any of the partitions of the stream. Depending on the configured number of replicas, multiple
      stream-level consumers are created, taking care that no two replicas exist on the same server host.  Therefore
      you need to provision exactly as many hosts as the number of replicas configured.

Of course, the underlying stream should support either mode of consumption in order for a Pinot table to use that
mode. Kafka has support for both of these modes. See :ref:`pluggable-streams` for more information on support of other
data streams in Pinot.

In either mode, Pinot servers store the ingested rows in volatile memory until either one of the following conditions are met:

    #. A certain number of rows are consumed
    #. The consumption has gone on for a certain length of time

(See :ref:`stream-config-description` on how to set these values, or have pinot compute them for you)

Upon reaching either one of these limits, the servers do the following:

    * Pause consumption
    * Persist the rows consumed so far into non-volatile storage
    * Continue consuming new rows into volatile memory again.

The persisted rows form what we call a *completed* segment (as opposed to a *consuming*
segment that resides in volatile memory).

In ``LowLevel`` mode, the completed segments are persisted the into local non-volatile store of pinot server
*as well as* the segment store of the pinot cluster (See :ref:`pinot-architecture-diagram`). This allows for
easy and automated mechanisms for replacing pinot servers, or expanding capacity, etc. Pinot has
`special mechanisms <https://cwiki.apache.org/confluence/display/PINOT/Consuming+and+Indexing+rows+in+Realtime#ConsumingandIndexingrowsinRealtime-Segmentcompletionprotocol>`_
that ensure that the completed segment is equivalent across all replicas.

During segment completion, one winner is chosen by the controller from all the replicas as the ``committer server``. The ``committer server`` builds the segment and uploads it to the controller. All the other ``non-committer servers`` follow one of these two paths:

1. If the in-memory segment is equivalent to the committed segment, the ``non-committer`` server also builds the segment locally and replaces the in-memory segment
2. If the in-memory segment is non equivalent to the committed segment, the ``non-committer`` server downloads the segment from the controller.

For more details on this protocol, please refer to `this doc <https://cwiki.apache.org/confluence/display/PINOT/Consuming+and+Indexing+rows+in+Realtime#ConsumingandIndexingrowsinRealtime-Segmentcompletionprotocol>`_.

In ``HighLevel`` mode, the servers persist the consumed rows into local store (and **not** the segment store). Since consumption of rows
can be from any partition, it is not possible to guarantee equivalence of segments across replicas.

See `Consuming and Indexing rows in Realtime <https://cwiki.apache.org/confluence/display/PINOT/Consuming+and+Indexing+rows+in+Realtime>`_ for details.


Pinot Segments
--------------

A segment is laid out in a columnar format so that it can be directly mapped into memory for serving queries.

Columns may be single or multi-valued. Column types may be
STRING, INT, LONG, FLOAT, DOUBLE or BYTES. Columns may be declared to be metric or dimension (or specifically as a time dimension)
in the schema. Columns can have default null value. For example, the default null value of a integer column can be 0.
Note: The default value of byte column has to be hex-encoded before adding to the schema.

Pinot uses dictionary encoding to store values as a dictionary ID. Columns may be configured to be "no-dictionary" column in which
case raw values are stored. Dictionary IDs are encoded using minimum number of bits for efficient storage (*e.g.* a column with cardinality
of 3 will use only 3 bits for each dictionary ID).

There is a forward index built for each column and compressed appropriately for efficient memory use.  In addition, optional inverted indices can be
configured for any set of columns. Inverted indices, while take up more storage, offer better query performance.

Specialized indexes like Star-Tree index is also supported.

