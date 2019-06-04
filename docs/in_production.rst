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

Running Pinot in Production
===========================

Requirements
~~~~~~~~~~~~

You will need the following in order to run pinot in production:

* Hardware for controller/broker/servers as per your load
* Working installation of Zookeeper that Pinot can use. We recommend setting aside a path within zookpeer and including that path in pinot.controller.zkStr. Pinot will create its own cluster under this path (cluster name decided by pinot.controller.helixClusterName)
* Shared storage mounted on controllers (if you plan to have multiple controllers for the same cluster). Alternatively, an implementation of PinotFS that the Pinot hosts have access to.
* HTTP load balancers for spraying queries across brokers (or other mechanism to balance queries)
* HTTP load balancers for spraying controller requests (e.g. segment push, or other controller APIs) or other mechanisms for distribution of these requests.

Deploying Pinot
~~~~~~~~~~~~~~~

In general, when deploying Pinot services, it is best to adhere to a specific ordering in which the various components should be deployed. This deployment order is recommended in case of the scenario that there might be protocol or other significant differences, the deployments go out in a predictable order in which failure  due to these changes can be avoided.

The ordering is as follows:

#. pinot-controller
#. pinot-broker
#. pinot-server
#. pinot-minion

Managing Pinot
~~~~~~~~~~~~~~

Creating tables
---------------

Updating tables
---------------

Uploading data
--------------

Configuring realtime data ingestion
-----------------------------------

Monitoring Pinot
~~~~~~~~~~~~~~~~

Pinot exposes several metrics to monitor the service and ensure that pinot users are not experiencing issues. In this section we discuss some of the key metrics that are useful to monitor. A full list of metrics is available in the `Metrics <customizations.html#metrics>`_ section.

Pinot Server
------------

* Missing Segments - `NUM_MISSING_SEGMENTS <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/metrics/ServerMeter.java>`_

  * Number of missing segments that the broker queried for (expected to be on the server) but the server didn't have. This can be due to retention or stale routing table.

* Query latency - `TOTAL_QUERY_TIME <https://github.com/apache/incubator-pinot/blob/ce2d9ee9dc73b2d7273a63a4eede774eb024ea8f/pinot-common/src/main/java/org/apache/pinot/common/metrics/ServerQueryPhase.java>`_

  * Total time to take from receiving to finishing executing the query.

* Query Execution Exceptions - `QUERY_EXECUTION_EXCEPTIONS <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/metrics/ServerMeter.java>`_

  * The number of exception which might have occurred during query execution.

* Realtime Consumption Status - `LLC_PARTITION_CONSUMING <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/metrics/ServerGauge.java>`_

  * This gives a binary value based on whether low-level consumption is healthy (1) or unhealthy (0). It's important to ensure at least a single replica of each partition is consuming.

* Realtime Highest Offset Consumed - `HIGHEST_STREAM_OFFSET_CONSUMED <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/metrics/ServerGauge.java>`_

  * The highest offset which has been consumed so far.

Pinot Broker
------------

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
----------------

Many of the controller metrics include a table name and thus are dynamically generated in the code. The metrics below point to the classes which generate the corresponding metrics.

To get the real metric name, the easiest route is to spin up a controller instance, create a table with the desired name and look through the generated metrics.

.. todo::

  Give a more detailed explanation of how metrics are generated, how to identify real metrics names and where to find them in the code.

* Percent Segments Available - `PERCENT_SEGMENTS_AVAILABLE <https://github.com/apache/incubator-pinot/blob/ce2d9ee9dc73b2d7273a63a4eede774eb024ea8f/pinot-common/src/main/java/org/apache/pinot/common/metrics/ControllerGauge.java>`_

  * Percentage of complete online replicas in external view as compared to replicas in ideal state.

* Segments in Error State - `SEGMENTS_IN_ERROR_STATE <https://github.com/apache/incubator-pinot/blob/ce2d9ee9dc73b2d7273a63a4eede774eb024ea8f/pinot-common/src/main/java/org/apache/pinot/common/metrics/ControllerGauge.java>`_

  * Number of segments in an ``ERROR`` state for a given table.

* Last push delay - Generated in the `ValidationMetrics <https://github.com/apache/incubator-pinot/blob/ce2d9ee9dc73b2d7273a63a4eede774eb024ea8f/pinot-common/src/main/java/org/apache/pinot/common/metrics/ValidationMetrics.java>`_ class.

  * The time in hours since the last time an offline segment has been pushed to the controller.

* Percent of replicas up - `PERCENT_OF_REPLICAS <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/metrics/ControllerGauge.java>`_

  * Percentage of complete online replicas in external view as compared to replicas in ideal state.

* Table storage quota usage percent - `TABLE_STORAGE_QUOTA_UTILIZATION <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/metrics/ControllerGauge.java>`_

  * Shows how much of the table's storage quota is currently being used, metric will a percentage of a the entire quota.


