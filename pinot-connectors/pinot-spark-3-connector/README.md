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
- HTTPS/TLS support for secure connections

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

## HTTPS Configuration

The connector supports HTTPS/TLS connections to Pinot clusters. To enable HTTPS, use the following configuration options:

```scala
val data = spark.read
  .format("pinot")
  .option("table", "airlineStats")
  .option("tableType", "offline")
  .option("useHttps", "true")
  .option("keystorePath", "/path/to/keystore.jks")
  .option("keystorePassword", "keystorePassword")
  .option("truststorePath", "/path/to/truststore.jks")
  .option("truststorePassword", "truststorePassword")
  .load()
```

### HTTPS Configuration Options

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| `useHttps` | Enable HTTPS connections | No | `false` |
| `keystorePath` | Path to client keystore file (JKS format) | No | None |
| `keystorePassword` | Password for the keystore | No | None |
| `truststorePath` | Path to truststore file (JKS format) | No | None |
| `truststorePassword` | Password for the truststore | No | None |

**Note:** If no truststore is provided when HTTPS is enabled, the connector will trust all certificates (not recommended for production use).

## Authentication Support

The connector supports custom authentication headers for secure access to Pinot clusters:

```scala
// Using Bearer token authentication
val data = spark.read
  .format("pinot")
  .option("table", "airlineStats")
  .option("tableType", "offline")
  .option("authToken", "my-jwt-token")  // Automatically adds "Authorization: Bearer my-jwt-token"
  .load()

// Using custom authentication header
val data = spark.read
  .format("pinot")
  .option("table", "airlineStats")
  .option("tableType", "offline")
  .option("authHeader", "Authorization")
  .option("authToken", "Bearer my-custom-token")
  .load()

// Using API key authentication
val data = spark.read
  .format("pinot")
  .option("table", "airlineStats")
  .option("tableType", "offline")
  .option("authHeader", "X-API-Key")
  .option("authToken", "my-api-key")
  .load()
```

### Authentication Configuration Options

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| `authHeader` | Custom authentication header name | No | `Authorization` (when `authToken` is provided) |
| `authToken` | Authentication token/value | No | None |

**Note:** If only `authToken` is provided without `authHeader`, the connector will automatically use `Authorization: Bearer <token>`.

## Examples

There are more examples included in `src/test/scala/.../ExampleSparkPinotConnectorTest.scala`.
You can run the examples locally (e.g. using your IDE) in standalone mode by starting a local Pinot cluster. See: https://docs.pinot.apache.org/basics/getting-started/running-pinot-locally

You can also run the tests in _cluster mode_ using following command:
```shell
export SPARK_CLUSTER=<YOUR_YARN_OR_SPARK_CLUSTER>

# Edit the ExampleSparkPinotConnectorTest to get rid of `.master("local")` and rebuild the jar before running this command
spark-submit \
    --class org.apache.pinot.connector.spark.v3.datasource.ExampleSparkPinotConnectorTest \
    --jars ./target/pinot-spark-3-connector-0.13.0-SNAPSHOT-shaded.jar \
    --master $SPARK_CLUSTER \
    --deploy-mode cluster \
  ./target/pinot-spark-3-connector-0.13.0-SNAPSHOT-tests.jar
```

Spark-Pinot connector uses Spark `DatasourceV2 API`. Please check the Databricks presentation for DatasourceV2 API;

https://www.slideshare.net/databricks/apache-spark-data-source-v2-with-wenchen-fan-and-gengliang-wang

## Future Works
- Add integration tests for read operation
- Add write support(pinot segment write logic will be changed in later versions of pinot)
