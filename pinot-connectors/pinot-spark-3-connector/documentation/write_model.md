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
# Write Model

> [!CAUTION]
> This feature is experimental and the API may change in future releases.

Spark Connector also has experimental support for writing Pinot segments from Spark DataFrames.
Currently, only append mode is supported and the schema of the DataFrame should match the schema of the Pinot table.

```scala
// create sample data
val data = Seq(
  ("ORD", "Florida", 1000, true, 1722025994),
  ("ORD", "Florida", 1000, false, 1722025994),
  ("ORD", "Florida", 1000, false, 1722025994),
  ("NYC", "New York", 20, true, 1722025994),
)

val airports = spark.createDataFrame(data)
  .toDF("airport", "state", "distance", "active", "ts")
  .repartition(2)

airports.write.format("pinot")
  .mode("append")
  .option("table", "airlineStats")
  .option("tableType", "OFFLINE")
  .option("segmentNameFormat", "{table}_{partitionId:03}")
  .option("invertedIndexColumns", "airport")
  .option("noDictionaryColumns", "airport,state")
  .option("bloomFilterColumns", "airport")
  .option("timeColumnName", "ts")
  .save("myPath")
```

For more details, refer to the implementation at `org.apache.pinot.connector.spark.v3.datasource.PinotDataWriter`.
