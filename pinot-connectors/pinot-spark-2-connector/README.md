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
# Spark-Pinot Connector

Spark-pinot connector to read and write data from/to Pinot.

Detailed read model documentation is here; [Spark-Pinot Connector Read Model](documentation/read_model.md)


## Features
- Query realtime, offline or hybrid tables
- Distributed, parallel scan
- SQL support instead of PQL
- Column and filter push down to optimize performance
- Overlap between realtime and offline segments is queried exactly once for hybrid tables
- Schema discovery 
  - Dynamic inference
  - Static analysis of case class

## Quick Start
```scala
import org.apache.spark.sql.SparkSession

val spark: SparkSession = SparkSession
      .builder()
      .appName("spark-pinot-connector-test")
      .master("local")
      .getOrCreate()

import spark.implicits._

val data = spark.read
  .format("pinot")
  .option("table", "airlineStats")
  .option("tableType", "offline")
  .load()
  .filter($"DestStateName" === "Florida")

data.show(100)
```

For more examples, see `src/test/scala/example/ExampleSparkPinotConnectorTest.scala` 

Spark-Pinot connector uses Spark `DatasourceV2 API`. Please check the Databricks presentation for DatasourceV2 API;

https://www.slideshare.net/databricks/apache-spark-data-source-v2-with-wenchen-fan-and-gengliang-wang

## Future Works
- Add integration tests for read operation
- Streaming endpoint support for read operation
- Add write support(pinot segment write logic will be changed in later versions of pinot)
