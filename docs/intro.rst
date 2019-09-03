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

About Pinot
===========

Pinot is a realtime distributed OLAP datastore, which is used at LinkedIn to deliver scalable real time analytics with low latency. It can ingest data
from offline data sources (such as Hadoop and flat files) as well as streaming events (such as Kafka). Pinot is designed to scale horizontally,
so that it can scale to larger data sets and higher query rates as needed.

What is it for (and not)?
-------------------------

Pinot is well suited for analytical use cases on immutable append-only data that require low latency between an event being ingested and it being available to be queried.

Key Features
------------

* A column-oriented database with various compression schemes such as Run Length, Fixed Bit Length
* Pluggable indexing technologies - Sorted Index, Bitmap Index, Inverted Index
* Ability to optimize query/execution plan based on query and segment metadata .
* Near real time ingestion from streams and batch ingestion from Hadoop
* SQL like language that supports selection, aggregation, filtering, group by, order by, distinct queries on data.
* Support for multivalued fields
* Horizontally scalable and fault tolerant

Because of the design choices we made to achieve these goals, there are certain limitations in Pinot:

* Pinot is not a replacement for database i.e it cannot be used as source of truth store, cannot mutate data
* Not a replacement for search engine i.e Full text search, relevance not supported
* Query cannot span across multiple tables.

Pinot works very well for querying time series data with lots of Dimensions and Metrics. For example:

.. code-block:: sql

    SELECT sum(clicks), sum(impressions) FROM AdAnalyticsTable
      WHERE ((daysSinceEpoch >= 17849 AND daysSinceEpoch <= 17856)) AND accountId IN (123456789)
      GROUP BY daysSinceEpoch TOP 100

.. code-block:: sql

    SELECT sum(impressions) FROM AdAnalyticsTable
      WHERE (daysSinceEpoch >= 17824 and daysSinceEpoch <= 17854) AND adveriserId = '1234356789'
      GROUP BY daysSinceEpoch,advertiserId TOP 100

.. code-block:: sql

    SELECT sum(cost) FROM AdAnalyticsTable GROUP BY advertiserId TOP 50

