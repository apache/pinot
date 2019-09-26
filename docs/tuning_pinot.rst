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

.. _tuning-pinot:

Tuning Pinot
============

This section provides information on various options to tune Pinot cluster for storage and query efficiency.
Unlike Key-Value store, tuning Pinot sometimes can be tricky because the cost of query can vary depending on the
workload and data characteristics.

If you want to improve query latency for your use case, you can refer to ``Index Techniques`` section. If your
use case faces the scalability issue after tuning index, you can refer ``Optimizing Scatter and Gather`` for
improving query throughput for Pinot cluster. If you have identified a performance issue on the specific component
(broker or server), you can refer to the ``Tuning Broker`` or ``Tuning Server`` section.


.. toctree::
   :maxdepth: 1

   index_techniques
   star-tree/star-tree
   tuning_scatter_and_gather
   tuning_realtime_performance
