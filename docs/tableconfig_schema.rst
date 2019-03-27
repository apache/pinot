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

Segments Config Section
~~~~~~~~~~~~~~~~~~~~~~~

The ``segmentsConfig`` section has information about configuring the following:

* Segment Retention - with the ``retentionTimeUnit`` and ``retentionTimeValue`` options. Retention is only applicable to tables of type ``APPEND``.

  * Allowed values:

    * ``retentionTimeUnit`` - ``DAYS``
    * ``retentionTimeValue`` - Positive integers

* ``segmentPushFrequency`` - to indicate how frequently segments are uploaded.

  * Allowed values - ``daily``, ``hourly``

* ``segmentPushType`` - Indicates the type of push to the table.

  * Allowed values:

    * ``APPEND`` means new data will be pushed and appended to the current data in the table, all realtime tables *must* be explicity set to ``APPEND``.
    * ``REFRESH`` will refresh the entire dataset contained within the table. Segment retention is ignored when set to ``REFRESH``.

* ``replication`` - Number of replicas of data in a table, used for offline tables only.

  * Allowed values - Positive integers

* ``replicasPerPartition`` - Number of of data in a table, used for realtime LLC tables only.

  * Allowed values - Positive integers

* Time column - using ``timeColumnName`` and ``timeType``, this must match what's configured in the preceeding schema

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

* ``segmentPartitionConfig`` - Cofigures the Data Partitioning Strategy. Further documentation on this feather available in the `Data Partitioning Strategies <customizations.html#data-partitioning-strategies>`_ section.
* ``loadMode`` - indicates how data will be loaded on pinot-server. either ``"MMAP"`` or ``"HEAP"`` can be configured.

  * Allowed values:

    * ``MMAP`` - Configures pinot-server to load data segments to off-heap memory.
    * ``HEAP`` - Configures pinot-server to load data directly into direct memory.

* ``streamConfigs`` - This section is where the bulk of the settings specific to only REALTIME tables are found. These options are explained in detail in the `Pluggable Streams <pluggable_streams.html#pluggable-streams>`_ page.

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
      "broker": "brokerTenant",
      "server": "serverTenant"
    },

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
