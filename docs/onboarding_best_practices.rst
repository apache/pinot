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

.. _onboarding-best-practices:

Onboarding Best Practices
==========================

Here's a checklist of things to consider before you begin the process of modelling your data and onboarding to Pinot

This has been split up into 2 sections: 

1) Data Preparation 
2) Querying Pinot



Data Preparation
^^^^^^^^^^^^^^^^^
These are the best practices and considerations when preparing your data schema and data format

Considerations common to offline and realtime
**********************************************

1. Pre-aggregations
###################
Pre aggregate the data as much as the application logic allows. This means **rolling up the metric values for the unique dimensions and time column combinations**. This is beneficial as we will reduce the size of the data being stored in Pinot, as well as avoid aggregations to be done in Pinot for every query, hence improving query performance. 

- For offline, perform the aggregations in your data preparation hadoop/spark job. 
- For realtime, a samza job can be used to aggregate data at intervals based on the freshness requirements of the usecase.

2. Time column
###################
Think about what **granularity of time column** is needed for your application. This will usually be *HOURS, DAYS, (sometimes 15 MINUTES)*. It is not recommended to have the time column in MILLISECONDS or SECONDS granularity. If time granularity is in milliseconds, the cardinality of the time column becomes very high in realtime systems, causing the time column dictionary to get very big. Bucket your time column to the coarsest granularity possible (hoursSinceEpoch, daysSinceEpoch). A greater level of pre-aggregations can be achieved with coarser time granularity.

If you are unsure of the granularity, or feel that the chosen granularity might have to be made finer at a later point, you can keep the column in *MILLISECONDS*, however, round off the column to a higher granularity (round off millis to nearest day for example). This way you still have flexibility later on to change the bucketing.

3. Segment Push Modes
######################
Pinot has 2 modes of segment push. *APPEND and REFRESH*.

- APPEND - every data push is an incremental one, and the data received is appended to the existing data.  
- REFRESH - The existing data is completely replaced by the new data pushed. The new segment names need to be exactly the same for the existing segments to be replaced

In general, if you push a segment with the same name, it will overwrite the previous segment (in either mode).

4. Prefer long over string where possible
##########################################
Strings in Java are inefficient. We end up serializing/deserializing them during query execution a lot of time.

Instead of storing a long string value, store an id, and use a stateless utility function for conversion. Similarly, for attributes of a dimension (*for example, companyId -> company details*) store only the id in Pinot, and use another key value store/pinot table for the lookup. This is applicable only if Pinot cannot handle desired latency/throughput, even after all other possible optimizations. The reason is that, inside Pinot, we don’t know the relation between different columns. If used in a query like group by (group by companyId, companyName, companySize)  we will try to gather all the combinations of these columns in order to get the group-by results. This is quite expensive inside Pinot, but with key-value store service (or even keep a separate Pinot table), this will be simply a value lookup, which is way cheaper, and can definitely give you better performance

5. Sorted column
###################
Sort data on column if it will always be present in the queries. **Sorted index performs superior than inverted index.** 

- For offline, Pinot cannot sort the data in segments that are pushed to pinot. This needs to be done prior to the segment creation, in the data preparation job. If the data is sorted before segment creation, the segment creation will detect and maintain this sorted column. 
- For realtime, set the column in “sortedColumn” in table config, and it will be sorted in Pinot when data is converted to a Pinot segment.

6. Multi value columns
#######################
Pinot supports multi value columns. Arrays in an avro will be treated as multi value columns, by setting `“singleValueField” : “false”` in the dimensionFieldSpec in Pinot schema. However, in general they are more costly for query execution, and ensure that they are being used only when needed.

7. Inverted indexes
###################
For columns that will be used fairly frequently in your queries, enable inverted indexes. Inverted indexes are **more efficient and effective for high cardinality columns.** 

8. Partitioning
###################
- For offline, if the data has been partitioned, Pinot can leverage this information for querying. This can be done by setting the partitioning function, number of partitions and column name in the segmentPartitionConfig in the table config 
- If the data is partitioned when splitting into the stream partitions, Pinot can leverage this information when querying, and prune segments accordingly, effectively speeding up queries. This can be done by setting the partitioning function, number of partitions and column name in the segmentPartitionConfig.

9. Number of segments
######################
Avoid creating too many small Pinot segments. Pinot’s query execution happens at the segment level, which can suffer from a substantial query planning overhead if faced with too many small segments.This can be controlled by the number of input files fed to the segment creation job. In realtime, this can be controlled by tuning the rows, time or size thresholds as described in :ref:`pluggable-streams`




Realtime specific considerations
*********************************

1. Time column
###################
Time column is a must for hybrid table

2. Num partitions
###################
When creating the stream topic (typically for your output of Samza job),  keep the number of partitions to a minimum. If partitions are increased later, Pinot is able to automatically and seamlessly start consuming from them.

3. Retention
###################
The retention of the stream topic should be set depending on the lag in your offline push. In hybrid tables, depending on the push frequency (set in table config), Pinot will start using the offline data 

- 1 day after offline data becomes available, if push frequency daily 
- 1 hour after offline data becomes available, if push frequency is hourly

Generally a stream retention of 4-5 days is sufficient, as this is enough time for the offline flow to generate data. The retention of the realtime table should no be more than the stream retention.

4. Pre aggregations
###################
Consider reducing the number of stream events per second, by aggregating in a samza job. Keep the aggregation window reasonable (5 minutes, 15 minutes, 1 hour depending on the events per second). If the aggregation window is large (say 6 hours), a burst of events will be sent to Pinot periodically after long gaps of silence. This kind of ingestion pattern is not ideal for Pinot consumption, and a steady stream of events is preferred. 

5. Metrics aggregation in Pinot realtime
#########################################################
If aggregation is not possible at source (due to freshness concerns), pinot can aggregate after events are received, by enabling aggregateMetrics: true in table config. Note that Pinot supports only “sum” aggregations in realtime.

6. Aggregation buckets
#####################################
Keep the aggregation bucket size the same across realtime and offline. If aggregation buckets are not consistent across realtime and offline, there can be undesirable results for data which spans the boundary. For example, if in realtime you have MILLISECONDS time column rounded off to the nearest hour via the samza job, follow the same model in offline flow, and round off time column to the nearest hour in the pre aggregations. This means, the samza job will produce a new timestamp every hour, whereas the offline segment for the day will have 24 unique timestamps. 




Querying Pinot
^^^^^^^^^^^^^^
`PQL doc <https://pinot.readthedocs.io/en/latest/pql_examples.html>`_

1. Apply **time filters** to your queries where applicable, to avoid fetching too much data

2. Apply **reasonable LIMIT and TOP** to your query results, based on what application needs

3. Pinot supports **distinct count**. However it is an expensive operation. Consider using **distinctCountHLL**, which is an approximate distinct count

4. Implement **caching layer on the application side**, to reduce the amount of repeated queries being sent to Pinot

5. For **report generation** style applications, it might be better to **split your query** into smaller time periods

6. At Pinot table side, **hard QPS quota limits** can be applied `tableConfig -> quotaConfig -> maxQueriesPerSecond`. If this quota is exceed, a 429 exception will be thrown with  message 

.. code-block:: none

    "Request <request id> exceeds query quota for table:<table name>, query:<query>"

7. Pinot doesn’t have query throttling. It is recommended to have an **application side query throttling mechanism**

8. Even if it is a low QPS usecase (ie < 1 qps) having distinct count queries, top K queries, etc can greatly impact the query cost

9. **Count, max, min queries** with no filters on OFFLINE only or REALTIME only tables, will be answered only from **metadata**, and hence will be very efficient. For example, if you have a hybrid (OFFLINE + REALTIME) table, and you want to query 

.. code-block:: none

    “select max(timestamp) from table” // full scan
    //it will be more efficient to rewrite query as
    “select max(timestamp) from table_REALTIME // answered from metadata

If you have an OFFLINE only table, max, min, count(*) with no filters will be answered directly from metadata, without any special considerations


