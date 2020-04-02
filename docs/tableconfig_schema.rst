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

.. _table-config-section:

Table Config
============

Sample table config and descriptions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A sample table config is shown below which has sub-sections collasped. The sub-sections will be described individually in following sections. Further links to feature specific documention will be included where available.

``tableName`` - Should only contain alpha-numeric characters, hyphens ('-'), or underscores ('_'). Though using a double-underscore ('__') is not allowed and reserved for other features within Pinot.

``tableType`` - Indicates the type of the table. There are some settings specific to each type. This will be clarified below as each sub-section is explained.

  * Allowed values:

    * ``OFFLINE`` - An offline table is used to host data which might be periodically uploaded - daily, weekly, etc. More information on `Offline Tables <architecture.html#ingesting-offline-data>`_
    * ``REALTIME`` - A realtime table is used to consume data from incoming data streams and serve this data in a near-realtime manner. More information on `Realtime Tables <architecture.html#ingesting-realtime-data>`_

.. code-block:: none

    {
      "tableName": "myPinotTable",
      "tableType": "REALTIME"
      "segmentsConfig": {...},
      "tableIndexConfig": {...},
      "tenants": {...},
      "routing": {...},
      "task": {...},
      "metadata": {...}
    }

Some sections are required, otherwise the table config will be rejected by pinot-controller. The required sections are:

* ``tableName``
* ``tabletype``
* ``"segmentsConfig": {...}``
* ``"tableIndexConfig": {...}``
* ``"tenants": {...}``
* ``"metadata": {...}``

In case of realtime tables, the ``streamConfig`` section is required.

Segments Config Section
~~~~~~~~~~~~~~~~~~~~~~~

The ``segmentsConfig`` section has information about configuring the following:

* Segment Retention - with the ``retentionTimeUnit`` and ``retentionTimeValue`` options. Retention is only applicable to tables of type ``APPEND``.

  * Allowed values:

    * ``retentionTimeUnit`` - ``DAYS``
    * ``retentionTimeValue`` - Positive integers

* ``segmentPushFrequency`` - to indicate how frequently segments are uploaded. Ignored for tables of type ``REALTIME``.

  * Allowed values - ``daily``, ``hourly``

* ``segmentPushType`` - Indicates the type of push to the table. Ignored for tables of type ``REALTIME``.

  * Allowed values:

    * ``APPEND`` means new data will be pushed and appended to the current data in the table, all realtime tables *must* be explicity set to ``APPEND``.
    * ``REFRESH`` will refresh the entire dataset contained within the table. Segment retention is ignored when set to ``REFRESH``.

* ``replication`` - Number of replicas of data in a table, used for tables of type ``OFFLINE`` and tables of type ``REALIME`` if ``stream.<consumername>.consumer.type`` is set to ``HighLevel`` (See :ref:`stream-config-description`)

  * Allowed values - Positive integers

* ``replicasPerPartition`` - Number of replicas that consume a single partition of streaming data of a table, used for tables of type ``REALTIME`` if ``stream.<consumername>.consumer.type`` is set to ``LowLevel`` (See :ref:`stream-config-description`)

  * Allowed values - Positive integers

* Time column - using ``timeColumnName`` and ``timeType``, this must match what's configured in the preceeding schema. This is a special column that Pinot uses to manage retention (removing old segments), split query between ``REALTIME`` and ``OFFLINE`` tables in a hybrid table, etc.

  * Allowed values - String, this must match the ``timeFieldSpec`` section in the schema

* Segment assignment strategy - Described more on the page `Customizing Pinot <customizations.html#segment-assignment-strategies>`_


.. code-block:: none

    "segmentsConfig": {
      "retentionTimeUnit": "DAYS",
      "retentionTimeValue": "5",
      "segmentPushFrequency": "daily",
      "segmentPushType": "APPEND",
      "replication": "3",
      "replicasPerPartition": "3",
      "schemaName": "myPinotSchmea",
      "timeColumnName": "daysSinceEpoch",
      "timeType": "DAYS",
      "segmentAssignmentStrategy": "BalanceNumSegmentAssignmentStrategy"
    },

* Completion Config

  You can also add a ``completionConfig`` section under the ``segmentsConfig`` section. Completion config holds information related to realtime segment completion. There is just one field in this config as of now, which is the ``completionMode``. The value of the ``completioMode`` decides how non-committers servers should replace the in-memory segment during realtime segment completion. Refer to the `Architecture <architecture.html#ingesting-realtime-data>`_ for description about committer server and non-committer servers.

  By default, if the in memory segment in the non-winner server is equivalent to the committed segment, then the non-committer server builds and replaces the segment, else it download the segment from the controller.

  Currently, the supported value for ``completionMode`` is

  * ``DOWNLOAD``: In certain scenarios, segment build can get very memory intensive. It might become desirable to enforce the non-committer servers to just download the segment from the controller, instead of building it again. Setting this completionMode ensures that the non-committer servers always download the segment.


For example:

.. code-block:: none

    "segmentsConfig": {
      ..
      ..
      "completionConfig": {
        "completionMode": "DOWNLOAD"
      }
    },

Table Index Config Section
~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``tableIndexConfig`` section has information about how to configure:

* ``invertedIndexColumns`` - Indicates a list of real column names as specified in the schema to create inverted indexes for. More info on indexes can be found on the `Index Techniques <index_techniques.html>`_ page.

  * Allowed values - String; string must match the column name in the corresponding schema

* ``noDictionaryColumns`` - Indicates a list of real column names as specified in the schema. Column names present will **not** have a dictionary created. More info on indexes can be found on the `Index Techniques <index_techniques.html>`_ page.

  * Allowed values - String; string must match the column name in the corresponding schema

* ``sortedColumn`` - Indicates a list of real column names as specified in the schema. Data should be sorted based on the column names provided. This field needs to be set only for realtime tables. For offline, if the data at source is sorted, we will create a sorted index automatically. More info on indexes can be found on the `Index Techniques <index_techniques.html>`_ page.

  * Allowed values - String; string must match the column name in the corresponding schema

* ``aggregateMetrics`` - Switch for the aggregate metrics feature. This feature will aggregate realtime stream data as it is consumed, where applicable, in order to reduce segment sizes. We sum the metric column values of all rows that have the same value for dimension columns and create one row in a realtime segment for all such rows. This feature is only available on REALTIME tables.

  * Allowed values - ``true`` to enable, ``false`` to disable.

.. todo::

  Create a separate section to describe this feature and design, then link to it from this config description

* ``segmentPartitionConfig`` - Configures the Data Partitioning Strategy. Further documentation on this feather available in the `Data Partitioning Strategies <customizations.html#data-partitioning-strategies>`_ section.
* ``loadMode`` - indicates how data will be loaded on pinot-server. either ``"MMAP"`` or ``"HEAP"`` can be configured.

  * Allowed values:

    * ``MMAP`` - Configures pinot-server to load data segments to off-heap memory.
    * ``HEAP`` - Configures pinot-server to load data directly into direct memory.

* ``streamConfigs`` - This section is where the bulk of the settings specific to only ``REALTIME`` tables are found. See :ref:`stream-config-description`

.. code-block:: none

    "tableIndexConfig": {
      "invertedIndexColumns": [],
      "noDictionaryColumns" : [],
      "sortedColumn": [
        "nameOfSortedColumn"
      ],
      "noDictionaryColumns": [
        "nameOfNoDictionaryColumn"
      ],
      "aggregateMetrics": "true",
      "segmentPartitionConfig": {
        "columnPartitionMap": {
          "contentId": {
            "functionName": "murmur",
            "numPartitions": 32
          }
        }
      },
      "loadMode": "MMAP",
      "lazyLoad": "false",
      "segmentFormatVersion": "v3",
      "streamConfigs": {}
    },

Tenants Section
~~~~~~~~~~~~~~~

The ``tenants`` section has two main config fields in it. These fields are used to configure which tenants are used within Helix.

.. code-block:: none

    "tenants": {
      "broker": "brokerTenantName",
      "server": "serverTenantName"
    },


In the above example, 

* The table will be served by brokers that have been tagged as ``brokerTenantName_BROKER`` in Helix.

* The offline segments for the table will be hosted in pinot servers tagged in helix as ``serverTenantName_OFFLINE``

* The realtime segments (both consuming as well as completed ones) will be hosted in pinot servers tagged in helix as ``serverTeantName_REALTIME``.


You can also add a ``tagOverrideConfig`` section under the ``tenants`` section. Currently, the only override allowed is to add additional tags for 
consuming and completed segments. For example:

.. code-block:: none

    "tenants": {
      "broker": "brokerTenantName",
      "server": "serverTenantName",
      "tagOverrideConfig" : {
        "realtimeConsuming" : "serverTenantName_REALTIME"
        "realtimeCompleted" : "serverTenantName_OFFLINE"
      }
    }


In the above example, the consuming segments will stil be assigned to ``serverTenantName_REALTIME`` hosts, but once they are completed, the
segments will be moved to ``serverTeantnName_OFFLINE``. It is possible to specify the full name of *any* tag in this section (so, for example, you
could decide that completed segments for this table should be in pinot servers tagged as ``allTables_COMPLETED``).

See :ref:`ingesting-realtime-data` section for more details on consuming and completed segments.


Routing Section
~~~~~~~~~~~~~~~

The ``routing`` section contains configurations on how which routingTableBuilder will be used and to pass options specific to that builder. There is more information in the `Routing Strategies <customizations.html#routing-strategies>`_ section.

.. code-block:: none

    "routing": {
      "routingTableBuilderName": "PartitionAwareRealtime",
      "routingTableBuilderOptions": {}
    },

Metadata Section
~~~~~~~~~~~~~~~~

The ``metadata`` section is used for passing special key-value pairs into Pinot which will be stored with the table config inside of Pinot. There's more info in the `Custom Configs <customizations.html#custom-configs>`_ section.

.. code-block:: none

    "metadata": {
      "customConfigs": {
        "specialConfig": "testValue",
        "anotherSpecialConfig": "value"
      }
    }

.. _stream-config-description:

StreamConfigs Section
~~~~~~~~~~~~~~~~~~~~~

This section is specific to tables of type ``REALTIME`` and is ignored if the table type is any other.
See section on :ref:`ingesting-realtime-data` for an overview of how realtime ingestion works.

Here is a minimal example of what the ``streamConfigs`` section may look like:

.. code-block:: none

    "streamConfigs" : {
      "realtime.segment.flush.threshold.size": "0",
      "realtime.segment.flush.threshold.time": "24h",
      "realtime.segment.flush.desired.size": "150M",
      "streamType": "kafka",
      "stream.kafka.consumer.type": "LowLevel",
      "stream.kafka.topic.name": "ClickStream",
      "stream.kafka.consumer.prop.auto.offset.reset" : "largest"
    }


The ``streamType`` field is mandatory. In this case, it is set to ``kafka``.
StreamType of ``kafka`` is supported natively in Pinot.
You can use default decoder classes and consumer factory classes.
Pinot allows you to use other stream types with their own consumer factory
and decoder classes (or, even other decoder and consumer factory for ``kafka`` if
your installation formats kafka messages differently).

If you are considering adding support for streams other than Kafka,
please see section :ref:`pluggable-streams`.

There are some configurations that are generic to all stream types, and others that
are specific to stream types.

Configuration generic to all stream types
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* ``realtime.segment.flush.threshold.size``: Maximum number of rows to consume before persisting the consuming segment.

   Note that in the example above, it is set to
   ``0``. In this case, Pinot automatically computes the row limit using the value of ``realtime.segment.flush.desired.size``
   described below. If the consumer type is ``HighLevel``, then this value will be the maximum per consuming
   segment. If the consumer type is ``LowLevel`` then this value will be divided across all consumers being hosted
   on any one pinot-server.
   
   Default is ``5000000``.

* ``realtime.segment.flush.threshold.time``: Maximum elapsed time after which a consuming segment should be persisted.

   The value can be set as a human readable string, such as ``"1d"``,
   ``"4h30m"``, etc. This value should be set such that it is not below the retention of
   messages in the underlying stream, but is not so long that it may cause the server to run out of memory.

   Default is ``"6h"``


* ``realtime.segment.flush.desired.size``:  Desired size of the completed segments.

   This setting is supported only if consumer type is set to ``LowLevel``.
   This value can be set as a human readable string such as ``"150M"``, or ``"1.1G"``, etc.
   This value is used when ``realtime.segment.flush.threshold.size``
   is set to 0. Pinot learns and then estimates the number of rows that need to be consumed so that the 
   persisted segment
   is approximately of this size. The learning phase starts by setting the number of rows to 100,000
   (can be changed with the setting ``realtime.segment.flush.autotune.initialRows``).
   and increasing to reach the desired segment size. Segment size may go over the desired size significantly
   during the learning phase.
   Pinot corrects the estimation as it
   goes along, so it is not guaranteed that the resulting completed segments are of the exact size
   as configured. You should set this value to optimize the performance of queries (i.e. neither
   too small nor too large)

   Default is ``"200M"``

* ``realtime.segment.flush.autotune.initialRows``: Initial number of rows for learning.

   This value is used only if ``realtime.segment.flush.threshold.size`` is set o ``0`` and the consumer
   type is ``LowLevel``. See ``realtime.segment.flush.desired.size`` above.

   Default is ``"100K"``


Configuration specific to stream types
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All of these configuaration items have the prefix ``stream.<streamType>``. In the example above,
the prefix is ``stream.kafka``.

Important ones to note here are:

* ``stream.kafka.consumer.type``: This should have a value of ``LowLevel`` (recommended) or ``HighLevel``.
  
    Be sure to set the value of ``replicasPerPartition`` correctly as described before in your table configuration.

* ``stream.kafka.topic.name``: Name of the topic from which to consume.

* ``stream.kafka.consumer.prop.auto.offset.reset``: Indicates where to start consumption from in the stream.

   If the consumer type is ``LowLevel``, This configuration is used only when the table is first provisioned.
   In ``HighLevel`` consumer type, it will also be used when new servers are rolled in, or when existing servers
   are replaced with new ones.
   You can specify values such as ``smallest`` or ``largest``, or even ``3d`` if your stream supports it.
   If you specify ``largest``, the consumption starts from the most recent events in the data stream.
   This is the recommended way to create a new table.

   If you specify ``smallest`` then the consumption starts from the earliest event avaiable in the
   data stream.

All the configurations that are prefixed with the streamtype are expected to be used by the underlying
stream. So, you can set any of the configurations described in the
`Kafka configuraton page <https://kafka.apache.org/documentation/#consumerconfigs>`_ can be set using
the prefix ``stream.kafka`` and Kafka should pay attention to it.

More options are explained in the `Pluggable Streams <pluggable_streams.html#pluggable-streams>`_ section.
