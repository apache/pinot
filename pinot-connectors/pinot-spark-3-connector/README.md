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

## Security Configuration

You can secure both HTTP and gRPC using a unified switch or explicit flags.

- Unified: set `secureMode=true` to enable HTTPS and gRPC TLS together (recommended)
- Explicit: set `useHttps` for REST and `grpc.use-plain-text=false` for gRPC

### Quick examples

```scala
// Unified secure mode (enables HTTPS + gRPC TLS by default)
val data = spark.read
  .format("pinot")
  .option("table", "airlineStats")
  .option("tableType", "offline")
  .option("secureMode", "true")
  .load()

// Explicit HTTPS only (gRPC remains plaintext by default)
val data = spark.read
  .format("pinot")
  .option("table", "airlineStats")
  .option("tableType", "offline")
  .option("useHttps", "true")
  .load()

// Explicit gRPC TLS only (REST remains HTTP by default)
val data = spark.read
  .format("pinot")
  .option("table", "airlineStats")
  .option("tableType", "offline")
  .option("grpc.use-plain-text", "false")
  .load()
```

### HTTPS Configuration

When HTTPS is enabled (either via `secureMode=true` or `useHttps=true`), you can configure keystore/truststore as needed:

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
| `secureMode` | Unified switch to enable HTTPS and gRPC TLS | No | `false` |
| `useHttps` | Enable HTTPS connections (overrides `secureMode` for REST) | No | `false` |
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

## Pinot Proxy Support

The connector supports Pinot Proxy for secure cluster access where the proxy is the only exposed endpoint. When proxy is enabled, all HTTP requests to controllers/brokers and gRPC requests to servers are routed through the proxy.

### Proxy Configuration Examples

```scala
// Basic proxy configuration
val data = spark.read
  .format("pinot")
  .option("table", "airlineStats")
  .option("tableType", "offline")
  .option("controller", "pinot-proxy:8080")  // Proxy endpoint
  .option("proxy.enabled", "true")
  .load()

// Proxy with authentication
val data = spark.read
  .format("pinot")
  .option("table", "airlineStats")
  .option("tableType", "offline")
  .option("controller", "pinot-proxy:8080")
  .option("proxy.enabled", "true")
  .option("authToken", "my-proxy-token")
  .load()

// Proxy with gRPC configuration
val data = spark.read
  .format("pinot")
  .option("table", "airlineStats")
  .option("tableType", "offline")
  .option("controller", "pinot-proxy:8080")
  .option("proxy.enabled", "true")
  .option("grpc.proxy-uri", "pinot-proxy:8094")  // gRPC proxy endpoint
  .load()
```

### Proxy Configuration Options

| Option | Description | Required | Default |
|--------|-------------|----------|----------|
| `proxy.enabled` | Use Pinot Proxy for controller and broker requests | No | `false` |

**Note:** When proxy is enabled, the connector adds `FORWARD_HOST` and `FORWARD_PORT` headers to route requests to the actual Pinot services.

## gRPC Configuration

The connector supports comprehensive gRPC configuration for secure and optimized communication with Pinot servers.

### gRPC Configuration Examples

```scala
// Basic gRPC configuration
val data = spark.read
  .format("pinot")
  .option("table", "airlineStats")
  .option("tableType", "offline")
  .option("grpc.port", "8091")
  .option("grpc.max-inbound-message-size", "256000000")  // 256MB
  .load()

// gRPC with TLS (explicit)
val data = spark.read
  .format("pinot")
  .option("table", "airlineStats")
  .option("tableType", "offline")
  .option("grpc.use-plain-text", "false")
  .option("grpc.tls.keystore-path", "/path/to/grpc-keystore.jks")
  .option("grpc.tls.keystore-password", "keystore-password")
  .option("grpc.tls.truststore-path", "/path/to/grpc-truststore.jks")
  .option("grpc.tls.truststore-password", "truststore-password")
  .load()

// gRPC with proxy
val data = spark.read
  .format("pinot")
  .option("table", "airlineStats")
  .option("tableType", "offline")
  .option("proxy.enabled", "true")
  .option("grpc.proxy-uri", "pinot-proxy:8094")
  .load()
```

### gRPC Configuration Options

| Option | Description | Required | Default |
|--------|-------------|----------|----------|
| `grpc.port` | Pinot gRPC port | No | `8090` |
| `grpc.max-inbound-message-size` | Max inbound message bytes when init gRPC client | No | `128MB` |
| `grpc.use-plain-text` | Use plain text for gRPC communication (overrides `secureMode` for gRPC) | No | `true` |
| `grpc.tls.keystore-type` | TLS keystore type for gRPC connection | No | `JKS` |
| `grpc.tls.keystore-path` | TLS keystore file location for gRPC connection | No | None |
| `grpc.tls.keystore-password` | TLS keystore password | No | None |
| `grpc.tls.truststore-type` | TLS truststore type for gRPC connection | No | `JKS` |
| `grpc.tls.truststore-path` | TLS truststore file location for gRPC connection | No | None |
| `grpc.tls.truststore-password` | TLS truststore password | No | None |
| `grpc.tls.ssl-provider` | SSL provider | No | `JDK` |
| `grpc.proxy-uri` | Pinot Rest Proxy gRPC endpoint URI | No | None |

**Note:** When using gRPC with proxy, the connector automatically adds `FORWARD_HOST` and `FORWARD_PORT` metadata headers for proper request routing.

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

## Security Best Practices

### Production HTTPS Configuration
- Always use HTTPS in production environments
- Store certificates in secure locations with appropriate file permissions
- Use proper certificate validation with valid truststore
- Rotate certificates regularly

### Production Authentication
- Use service accounts with minimal required permissions
- Store authentication tokens securely (environment variables, secret management systems)
- Implement token rotation policies
- Monitor authentication failures

### Production gRPC Configuration
- Enable TLS for gRPC communication in production
- Use certificate-based authentication when possible
- Configure appropriate message size limits based on your data
- Use connection pooling for high-throughput scenarios

### Production Proxy Configuration
### Data Types

- BIG_DECIMAL values are mapped to Spark `Decimal(38,18)` with HALF_UP rounding to match the declared schema. Ensure your data fits this precision/scale or cast accordingly in Spark.

- Ensure proxy endpoints are properly secured
- Monitor proxy health and performance
- Implement proper request routing and load balancing
- Use authentication for proxy access

## Future Works
- Add integration tests for read operation
- Add write support(pinot segment write logic will be changed in later versions of pinot)
