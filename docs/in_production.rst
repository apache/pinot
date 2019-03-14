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

Running Pinot in production
===========================

Installing Pinot
----------------

Requirements
~~~~~~~~~~~~

* Java 8+
* Several nodes with enough memory
* A working installation of Zookeeper

Recommended environment
~~~~~~~~~~~~~~~~~~~~~~~

* Shared storage infrastructure (such as NFS)
* Regular Zookeeper backups
* HTTP load balancers (such as nginx/haproxy)

Deploying Pinot
---------------

In general, when deploying Pinot services, it is best to adhere to a specific ordering in which the various components should be deployed. This deployment order is recommended incase of the scenario that there might be protocol or other significant differences, the deployments go out in a predictable order in which failure  due to these changes can be avoided.

The ordering is as follows:

#. pinot-controller
#. pinot-broker
#. pinot-server
#. pinot-minion

Direct deployment of Pinot
~~~~~~~~~~~~~~~~~~~~~~~~~~

Deployment of Pinot on Kubernetes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Introduction to table configs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using tables is how Pinot serves and organizes data. There are many settings in the table config which will influence how Pinot operates. The first and most significant distinction is using an offline versus a realtime table.

An offline table in Pinot is used to host data which might be periodically uploaded - daily, weekly, etc. A realtime table, however, is used to consume data from incoming data streams and serve this data in a near-realtime manner. This might also be referred to as nearline or just plain 'realtime'.

In this section a sample table configuration will be shown and all sections will be explained and if applicable have appropriate sections linked to for further explanation of those corresponding Pinot features.

Sample table config and descriptions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A sample table config is shown below which has sub-sections collasped. The sub sections will be described individually in following sections.

The ``tableName`` should only contain alpha-numeric characters, hyphens ('-'), or underscores ('_'). Though using a double-underscore ('__') is not allowed and reserved for other features within Pinot.

The ``tableType`` will indicate the type of the table, ``OFFLINE`` or ``REALTIME``. There are some settings specific to each type. This differentiation will be called out below as options are explained.

.. code-block:: none

    {
      "tableName": "myPinotTable",
      "tableType": "REALTIME"
      "segmentsConfig": {},
      "tableIndexConfig": {},
      "tenants": {},
      "routing": {},
      "task": {},
      "metadata": {}
    }

The ``segmentsConfig`` section has information about configuring

* Segment Retention - with the  ``retentionTimeUnit`` and ``retentionTimeValue`` options.
* Segment Push - Using ``segmentPushFrequency`` to indicate how frequently segments are uploaded.
* Replication - Using ``replication`` for offline tables and ``replicasPerPartition`` for realtime tables will indicate how many replicas of data will be present.
* Schema - The name of the schema that's been uploaded to the controller
* Time column - using ``timeColumnName`` and ``timeType``, this must match what's configured in the preceeding schema
* Segment assignment strategy - Described more on the page `Customizing Pinot <customizations.html#segment-assignment-strategies>`_


.. code-block:: none

    "segmentsConfig": {
      "retentionTimeUnit": "DAYS",
      "retentionTimeValue": "5",
      "segmentPushFrequency": "daily",
      "segmentPushType": "APPEND",
      "replication": "3",
      "replicasPerPartition": "3",
      "schemaName": "ugcGestureEvents",
      "timeColumnName": "daysSinceEpoch",
      "timeType": "DAYS",
      "segmentAssignmentStrategy": "BalanceNumSegmentAssignmentStrategy"
    },


The ``tableIndexConfig`` section has information about how to configure:

* Inverted Indexes - Using the ``invertedIndexColumns`` to specify a list of real column names as specified in the schema.
* No Dictionary Columns - Using the ``noDictionaryColumns`` to specify a list of real column names as specified in the schema. Column names present will NOT have a dictionary created. More info on indexes can be found on the `Index Techniques <index_techniques.html>`_ page.
* Sorted Column - Using the ``sortedColumn`` to specify a list of real column names as specified in the schema.
* Aggregate Metrics - Using ``aggregateMetrics`` set to ``"true"`` to enable the feature and ``"false"`` to disable. This feature is only available on REALTIME tables.
* Data Partitioning Strategy using the ``segmentPartitionConfig`` to configure based on documentation in the `Data Partitioning Strategies <customizations.html#data-partitioning-strategies>`_ section.
* Load Mode - Using ``loadMode`` either ``"MMAP"`` or ``"HEAP"`` can be configured.
* Lazy Loading of Data - Using ``lazyLoad`` this feature can be enabled by setting it to ``"true"`` and disabled by setting to ``"false"``
* Segment Format Version - Using the ``segmentFormatVersion`` field, this can be set to ``"v1"``, ``"v2"``, or ``"v3"``.
* Stream Configs - This section is where the bulk of the settings specific to only REALTIME tables are found. These options are explained in detail in the `Pluggable Streams <pluggable_streams.html#pluggable-streams>`_ page.

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

The ``tenants`` section has two config fields in it. These fields are used to configure which tenants are used within Helix.

.. code-block:: none

    "tenants": {
      "broker": "ugcAnalytics",
      "server": "ugcAnalytics"
    },

The ``routing`` section contains configurations on how which routingTableBuilder will be used and to pass options specific to that builder. There is more information in the `Routing Strategies <customizations.html#routing-strategies>`_ section.

.. code-block:: none

    "routing": {
      "routingTableBuilderName": "PartitionAwareRealtime",
      "routingTableBuilderOptions": {}
    },

The ``metadata`` section is used for passing special key-value pairs into Pinot which will be stored with the table config inside of Pinot. There's more info in the `Custom Configs <customizations.html#custom-configs>`_ section.

.. code-block:: none

    "metadata": {
      "customConfigs": {
        "specialConfig": "testValue",
        "anotherSpecialConfig": "value"
      }
    }

Managing Pinot
--------------

Creating tables
~~~~~~~~~~~~~~~

Updating tables
~~~~~~~~~~~~~~~

Uploading data
~~~~~~~~~~~~~~

Configuring realtime data ingestion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Monitoring Pinot
~~~~~~~~~~~~~~~~

In order for Pinot to provide effective service there is a core set of metrics which should be monitored to ensure service stability, fault tolerance and acceptable response times. In the section following, there are service level metrics which are recommended to be monitored.

More info on metrics collection and viewing a complete set of available metric is available in the `Metrics <customizations.html#metrics>`_ section.

Pinot Server

* Missing Segments
* Page In Activity - this is especially relevant for HDD nodes
* Query latency - latency from the time a server receives a request to when it sends a response
* Query Execution Exceptions -
* Realtime Consumption Status - (Across a single replica)
* Realtime Consumption Status - (Across a single partition)
* Realtime Highest Offset Consumed -

Pinot Broker

* Incoming QPS
* Incoming Error Rate %
* Dropped Requests
* Partial Responses
* Total Query Latency -
* Jetty Thread Utilization
* Table QPS quota exceeded -
* Table QPS quota usage percent -

Pinot Controller

* Missing Segment Count
* Segments in Error State
* Last push delay
* Percent of replicas up
* Table quota usage percent


