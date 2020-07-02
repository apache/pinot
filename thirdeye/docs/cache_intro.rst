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

.. _cache_intro:

Intro to Caching in ThirdEye
=============================

By default, ThirdEye uses an in-memory cache to reduce the number of fetch requests it needs to make to the data source.
The in-memory cache is set to use no more than 1/3 of the JVM's available memory, and can be disabled completely by setting
"useInMemoryCache" to be "false" in cache-config.yml. See :ref:`cache-config.yml`. The code for the in-memory cache can be found in:

``org.apache.pinot.thirdeye.detection.DefaultDataProvider``

ThirdEye also provides optional support for using an external, centralized cache, either as a standalone cache or as an L2 cache.
Currently, ThirdEye comes with a connector for using Couchbase as a centralized cache right out of the box. Details for this
connector can be found in :doc:`couchbase`.

If you'd like to use a different data store for your centralized cache instead, see :doc:`setup_cache_datastore`.
