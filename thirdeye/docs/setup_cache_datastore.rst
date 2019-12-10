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

.. _setup_cache_datastore:

Setting Up Custom Centralized Cache
====================================

There are a few steps to getting started with your own centralized cache.

Firstly, you will need to setup your data store. This can be locally or on your server cluster, if you have one.
Some data stores that we considered before picking Couchbase are Redis, Cassandra, and Memcached.

Then, you will need to add the client for your data source to ThirdEye's build. For the most part, this can be done by adding the client package's info to pom.xml for Maven.

Lastly, you will need to make your own DAO class. ThirdEye provides an interface for users who want to use their own data store of choice.
This is the `CacheDAO <https://github.com/apache/incubator-pinot/blob/master/thirdeye/thirdeye-pinot/src/main/java/org/apache/pinot/thirdeye/detection/cache/CacheDAO.java>`__ interface.

The CacheDAO interface has two methods:

.. code-block:: java

	ThirdEyeCacheResponse tryFetchExistingTimeSeries(ThirdEyeCacheRequest request) throws Exception;
	void insertTimeSeriesDataPoint(TimeSeriesDataPoint point);

Your DAO will need to implement these methods, and handle connections to your centralized cache. Also keep performance in mind, and you may need to design your own document schema.
The schema that ThirdEye uses for Couchbase can be found in the code in the CouchbaseCacheDAO class.
