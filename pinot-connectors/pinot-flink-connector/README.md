<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# Flink-Pinot Connector

Flink connector to write data to Pinot directly. This is useful for backfilling or bootstrapping tables,
including the upsert tables. You can read more about the motivation and design in this [design proposal](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=177045634).

## Quick Start
```java
StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
execEnv.setParallelism(2);
DataStream<Row> srcDs = execEnv.fromCollection(data).returns(TEST_TYPE_INFO).keyBy(r -> r.getField(0));

PinotControllerClient client = new PinotControllerClient();
// fetch Pinot schema
Schema schema = PinotConnectionUtils.getSchema(client, "starbucksStores");
// fetch Pinot table config
TableConfig tableConfig = PinotConnectionUtils.getTableConfig(client, "starbucksStores", "OFFLINE");
// create Flink Pinot Sink
srcDs.addSink(new PinotSinkFunction<>(new PinotRowRecordConverter(TEST_TYPE_INFO), tableConfig, schema));
execEnv.execute();
```

For more examples, please see `src/main/java/org/apache/pinot/connector/flink/FlinkQuickStart.java`

## Notes for backfilling upsert table
 - To correctly partition the output segments by the primary key, the Flink job *must* also include the partitionByKey operator before the Sink operator
 - The parallelism of the job *must* be set the same as the number of partitions of the Pinot table, so that the sink in each task executor can generate the segment of same partitions.
 - It’s important to plan the resource usage to avoid capacity issues such as out of memory. In particular, Pinot sink has an in-memory buffer of records, and it flushes when the threshold is reached. Currently, the threshold on the number of records is supported via the config of `segmentFlushMaxNumRecords`. In the future, we could add other types of threshold such as the memory usage of the buffer.
