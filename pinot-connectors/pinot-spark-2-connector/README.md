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

Spark-pinot connector to read data from Pinot.

Detailed read model documentation is here; [Spark-Pinot Connector Read Model](documentation/read_model.md)


## Features
- Query realtime, offline or hybrid tables
- Distributed, parallel scan
- Streaming reads using gRPC (optional)
- SQL support instead of PQL
- Column and filter push down to optimize performance
- Overlap between realtime and offline segments is queried exactly once for hybrid tables
- Schema discovery 
  - Dynamic inference
  - Static analysis of case class
- Supports query options

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

## Examples

There are more examples included in `src/test/scala/.../ExampleSparkPinotConnectorTest.scala`.
You can run the examples locally (e.g. using your IDE) in standalone mode by starting a local Pinot cluster. See: https://docs.pinot.apache.org/basics/getting-started/running-pinot-locally

You can also run the tests in _cluster mode_ using following command:
```shell
export SPARK_CLUSTER=<YOUR_YARN_OR_SPARK_CLUSTER>

# Edit the ExampleSparkPinotConnectorTest to get rid of `.master("local")` and rebuild the jar before running this command
spark-submit \
    --class org.apache.pinot.connector.spark.datasource.ExampleSparkPinotConnectorTest \
    --jars ./target/pinot-spark-2-connector-0.13.0-SNAPSHOT-shaded.jar \
    --master $SPARK_CLUSTER \
    --deploy-mode cluster \
  ./target/pinot-spark-2-connector-0.13.0-SNAPSHOT-tests.jar
```

Spark-Pinot connector uses Spark `DatasourceV2 API`. Please check the Databricks presentation for DatasourceV2 API;

https://www.slideshare.net/databricks/apache-spark-data-source-v2-with-wenchen-fan-and-gengliang-wang

## Future Works
- Add integration tests for read operation
- Streaming endpoint support for read operation
- Add write support(pinot segment write logic will be changed in later versions of pinot)
