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
^^^^^^^^^^^^

* Missing Segments - `NUM_MISSING_SEGMENTS <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/metrics/ServerMeter.java>`_

  * Number of missing segments that the broker queried for (expected to be on the server) but the server didn't have. This can be due to retention or stale routing table.

* Query latency - `TOTAL_QUERY_TIME <https://github.com/apache/incubator-pinot/blob/ce2d9ee9dc73b2d7273a63a4eede774eb024ea8f/pinot-common/src/main/java/org/apache/pinot/common/metrics/ServerQueryPhase.java>`_

  * The number of exception which might have occurred during query execution

* Query Execution Exceptions - `QUERY_EXECUTION_EXCEPTIONS <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/metrics/ServerMeter.java>`_

  * The number of exception which might have occurred during query execution

* Realtime Consumption Status - `LLC_PARTITION_CONSUMING <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/metrics/ServerGauge.java>`_

  * This gives a binary 1 or 0 based on whether low-level consumption is healthy (1) or not (0). It's important to ensure at least a single replica of each partition is consuming

* Realtime Highest Offset Consumed - `HIGHEST_STREAM_OFFSET_CONSUMED <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/metrics/ServerGauge.java>`_

  * The highest offset which has been consumed so far.

Pinot Broker
^^^^^^^^^^^^

* Incoming QPS (per broker) - `QUERIES <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/metrics/BrokerMeter.java>`_

  * The rate which an individual broker is receiving queries. Units are in QPS.

* Dropped Requests - `REQUEST_DROPPED_DUE_TO_SEND_ERROR <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/metrics/BrokerMeter.java>`_, `REQUEST_DROPPED_DUE_TO_CONNECTION_ERROR <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/metrics/BrokerMeter.java>`_, `REQUEST_DROPPED_DUE_TO_ACCESS_ERROR <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/metrics/BrokerMeter.java>`_

  * These multiple metrics will indicate if a query is dropped, ie the processing of that query has been forfeited for some reason.

* Partial Responses - `BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/metrics/BrokerMeter.java>`_

  * Indicates a count of partial responses. A partial response is when at least 1 of the requested servers fails to respond to the query.

* Table QPS quota exceeded - `QUERY_QUOTA_EXCEEDED <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/metrics/BrokerMeter.java>`_

  * Binary metric which will indicate when the configured QPS quota for a table is exceeded (1) or if there is capacity remaining (0).

* Table QPS quota usage percent - `QUERY_QUOTA_CAPACITY_UTILIZATION_RATE <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/metrics/BrokerGauge.java>`_

  * Percentage of the configured QPS quota being utilized.

Pinot Controller
^^^^^^^^^^^^^^^^

* Missing Segment Count -
* Segments in Error State -
* Last push delay -
* Percent of replicas up - `PERCENT_OF_REPLICAS <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/metrics/ControllerGauge.java#L33>_`
* Table quota usage percent - `TABLE_STORAGE_QUOTA_UTILIZATION <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/metrics/ControllerGauge.java#L61>`_


