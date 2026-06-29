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

# Pinot Admin Client

The Pinot Admin Client provides a Java API for all administrative operations available through the Pinot controller REST
APIs. This client allows you to programmatically manage Pinot clusters, including tables, schemas, instances, segments,
tenants, and tasks.

## Features

- **Complete API Coverage**: Codifies all Pinot controller REST APIs
- **Authentication Support**: Supports Basic, Bearer, and custom authentication
- **Async Operations**: Provides both synchronous and asynchronous methods
- **Comprehensive Exception Handling**: Specific exception types for different error scenarios
- **Type Safety**: Strongly typed API with proper error handling

## Architecture

The admin client consists of:

1. **PinotAdminClient**: Main entry point that provides access to all service clients
2. **PinotAdminTransport**: HTTP transport layer for communicating with the controller
3. **Service Clients**:
    - `TableAdminClient`: Table operations (CRUD, status, metadata)
    - `SchemaAdminClient`: Schema operations (CRUD, validation)
    - `InstanceAdminClient`: Instance operations (CRUD, state management)
    - `SegmentAdminClient`: Segment operations (query, management)
    - `TenantAdminClient`: Tenant operations (CRUD, configuration)
    - `TaskAdminClient`: Task management operations
4. **Exception Classes**: Specialized exceptions for different error types
5. **Authentication Utilities**: Support for various authentication methods

## Usage Examples

### Basic Usage

```java
import org.apache.pinot.client.admin.*;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;

// Create client without authentication
try(PinotAdminClient adminClient = new PinotAdminClient("localhost:9000")){
// List all tables
List<String> tables = adminClient.getTableClient().listTables(null, null, null);

// Get a specific table configuration
String config = adminClient.getTableClient().getTableConfig("myTable");

// Get a typed table configuration
TableConfig offlineConfig = adminClient.getTableClient().getTableConfigObjectForType("myTable", TableType.OFFLINE);

// List schemas
List<String> schemas = adminClient.getSchemaClient().listSchemaNames();
}
```

### With Authentication

```java
import org.apache.pinot.client.admin.*;
import java.util.Map;
import java.util.Properties;

// Create client with basic authentication
Properties properties = new Properties();
properties.setProperty("pinot.admin.request.timeout.ms", "30000");

PinotAdminClient adminClient = new PinotAdminClient(
    "localhost:9000",
    properties,
    PinotAdminAuthentication.AuthType.BASIC,
    Map.of("username", "admin", "password", "password")
);

try {
  // Create a new schema
  String schemaConfig =
      "{\"schemaName\":\"mySchema\",\"dimensionFieldSpecs\":[{\"name\":\"id\",\"dataType\":\"INT\"}]}";
  String result = adminClient.getSchemaClient().createSchema(schemaConfig);
} finally {
  adminClient.close();
}
```

### Async Operations

```java
import org.apache.pinot.client.admin.*;
import java.util.List;
import java.util.concurrent.CompletableFuture;

// Assume adminClient is already created
CompletableFuture<List<String>> tablesFuture =
    adminClient.getTableClient().listTablesAsync(null, null, null);
CompletableFuture<List<String>> schemasFuture =
    adminClient.getSchemaClient().listSchemaNamesAsync();

try {
  List<String> tables = tablesFuture.get();
  List<String> schemas = schemasFuture.get();
} catch (Exception e) {
  // Handle exceptions
}
```

## Service Clients

### Table Operations

```java
TableAdminClient tableClient = adminClient.getTableClient();

// List tables with filters
List<String> tables = tableClient.listTables("offline", null, "name");

// Create a table
String result = tableClient.createTable(tableConfigJson, "ALL");

// Update table configuration
String updateResult = tableClient.updateTableConfig("myTable", newConfigJson);

// Get table status
String status = tableClient.getTableStatus("myTable");

// Rebalance a table
String rebalanceResult = tableClient.rebalanceTable("myTable", true, "default", 1);
```

### Creating Tables With Modern Config Fields

Table create and update endpoints surface deprecated table-config properties via a `deprecationWarnings` field on
the success response. The validator is in soft-launch mode: requests with deprecated keys succeed (HTTP 200) and the
controller logs a warning. A future release will promote older deprecations to a `400 BAD_REQUEST` rejection, so
clients should treat any value in `deprecationWarnings` as a migration TODO and update their payloads accordingly.

Common migrations:

- `segmentsConfig.segmentPushType` -> `ingestionConfig.batchIngestionConfig.segmentIngestionType`
- `segmentsConfig.segmentPushFrequency` -> `ingestionConfig.batchIngestionConfig.segmentIngestionFrequency`
- `tableIndexConfig.streamConfigs` -> `ingestionConfig.streamIngestionConfig.streamConfigMaps`
- `fieldConfigList[].indexType` -> `fieldConfigList[].indexTypes`

### Wire-shape changes (rolling upgrade)

The same migration touches the controller's REST response shape. Several deprecated fields are no longer emitted
when at their Java default. Old clients reading `GET /tables/{name}` or `GET /tableConfigs/{name}` MUST tolerate
the absent fields:

- `fieldConfigList[].indexType` is preserved in the response shape for back-compat, but the controller emits a
  `deprecationWarnings` entry pointing callers to `indexTypes` instead.
- The following boolean getters are now annotated with `@JsonInclude(NON_DEFAULT)`; the field disappears from the
  response when the value is `false` (the type default): `upsertConfig.enableSnapshot`, `dedupConfig.enablePreload`,
  `indexingConfig.createInvertedIndexDuringSegmentGeneration`,
  `instanceAssignmentConfigMap.*.replicaGroupPartitionConfig.minimizeDataMovement`.

The new `deprecationWarnings` field on `ConfigSuccessResponse` and `CopyTableResponse` is annotated with
`@JsonInclude(NON_EMPTY)` and the response classes carry `@JsonIgnoreProperties(ignoreUnknown = true)`, so:

- New clients reading old controllers: succeed (no `deprecationWarnings`, treated as empty).
- Old clients reading new controllers: succeed (unknown field is ignored).

Rolling-upgrade label: this PR changes wire shape (field elision and new optional field) but no field name or
type is changed; existing clients that parse leniently round-trip cleanly. Strict-parsing clients should set
`FAIL_ON_UNKNOWN_PROPERTIES=false` or upgrade in lockstep with the controller.

Sample REALTIME table config for create:

```json
{
  "tableName": "events",
  "tableType": "REALTIME",
  "segmentsConfig": {
    "timeColumnName": "ts",
    "replication": "1"
  },
  "tenants": {
    "broker": "DefaultTenant",
    "server": "DefaultTenant"
  },
  "tableIndexConfig": {
    "loadMode": "MMAP"
  },
  "fieldConfigList": [
    {
      "name": "userId",
      "encodingType": "DICTIONARY",
      "indexTypes": [
        "INVERTED"
      ]
    }
  ],
  "ingestionConfig": {
    "batchIngestionConfig": {
      "segmentIngestionType": "APPEND",
      "segmentIngestionFrequency": "DAILY"
    },
    "streamIngestionConfig": {
      "streamConfigMaps": [
        {
          "streamType": "kafka",
          "stream.kafka.topic.name": "events",
          "stream.kafka.consumer.type": "lowlevel",
          "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder"
        }
      ]
    }
  },
  "metadata": {}
}
```

A create or update request that includes deprecated fields succeeds (HTTP 200) with a populated
`deprecationWarnings` array on the response body. Each entry names the offending JSON path and the replacement
field to use. The response shape is identical for the `/tables/.../validate` and `/tableConfigs/.../validate`
endpoints, so the same parser can surface warnings from any of them.

### Schema Operations

```java
SchemaAdminClient schemaClient = adminClient.getSchemaClient();

// List all schemas
List<String> schemas = schemaClient.listSchemaNames();

// Get schema configuration as JSON
String schema = schemaClient.getSchema("mySchema");

// Get a typed schema object
Schema schemaObject = schemaClient.getSchemaObject("mySchema");

// Create a new schema
String createResult = schemaClient.createSchema(schemaConfigJson);

// Validate schema before creating
String validation = schemaClient.validateSchema(schemaConfigJson);
```

### Instance Operations

```java
InstanceAdminClient instanceClient = adminClient.getInstanceClient();

// List all instances
List<String> instances = instanceClient.listInstances();

// Get instance information
String instanceInfo = instanceClient.getInstance("Server_192.168.1.1_8098");

// Enable/disable instance
String result = instanceClient.setInstanceState("Server_192.168.1.1_8098", true);

// Update instance configuration
String updateResult = instanceClient.updateInstance("Server_192.168.1.1_8098", configJson);
```

### Segment Operations

```java
SegmentAdminClient segmentClient = adminClient.getSegmentClient();

// List segments for a table
List<String> segments = segmentClient.listSegments("myTable_OFFLINE", false);

// Get segment metadata
Map<String, Object> metadata = segmentClient.getSegmentMetadata("myTable_OFFLINE", "segmentName", null);

// Delete a segment
String deleteResult = segmentClient.deleteSegment("myTable_OFFLINE", "segmentName", "7d");
```

### Tenant Operations

```java
TenantAdminClient tenantClient = adminClient.getTenantClient();

// List all tenants
List<String> tenants = tenantClient.listTenants();

// Create a tenant
String createResult = tenantClient.createTenant(tenantConfigJson);

// Get tenant metadata
String metadata = tenantClient.getTenantMetadata("myTenant", "SERVER");

// Rebalance tenant tables
String rebalanceResult = tenantClient.rebalanceTenant("myTenant", 2, null, null, "default");
```

### Task Operations

```java
TaskAdminClient taskClient = adminClient.getTaskClient();

// List task types
Set<String> taskTypes = taskClient.listTaskTypes();

// Get tasks for a specific type
Set<String> tasks = taskClient.getTasks("SegmentReloadTask");

// Get task state
TaskState state = taskClient.getTaskState("taskName");

// Get task debug information
String debugInfo = taskClient.getTaskDebugInfo("taskName", 0, null);
```

## Authentication

The client supports multiple authentication methods:

### Basic Authentication

```java
PinotAdminClient client = new PinotAdminClient("localhost:9000", properties,
    PinotAdminAuthentication.AuthType.BASIC,
    Map.of("username", "admin", "password", "password"));
```

### Bearer Token Authentication

```java
PinotAdminClient client = new PinotAdminClient("localhost:9000", properties,
    PinotAdminAuthentication.AuthType.BEARER,
    Map.of("token", "your-jwt-token"));
```

### Custom Authentication

```java
Map<String, String> customHeaders = Map.of(
    "X-API-Key", "your-api-key",
    "X-Tenant-Id", "tenant-123"
);

PinotAdminClient client = new PinotAdminClient("localhost:9000", properties,
    PinotAdminAuthentication.AuthType.CUSTOM, customHeaders);
```

## Exception Handling

The client provides specific exception types:

- `PinotAdminException`: General admin operation errors
- `PinotAdminAuthenticationException`: Authentication failures
- `PinotAdminNotFoundException`: Resource not found errors
- `PinotAdminValidationException`: Validation failures

```java
try {
  adminClient.getTableClient().getTableConfig("nonexistent");
} catch (PinotAdminNotFoundException e) {
  System.out.println("Table not found: " + e.getMessage());
} catch (PinotAdminAuthenticationException e) {
  System.out.println("Authentication failed: " + e.getMessage());
} catch (PinotAdminException e) {
  System.out.println("Admin operation failed: " + e.getMessage());
}
```

## Configuration

The client can be configured through properties:

```java
Properties properties = new Properties();
properties.setProperty("pinot.admin.request.timeout.ms", "60000");
properties.setProperty("pinot.admin.scheme", "https");

// Create client with configuration
PinotAdminClient client = new PinotAdminClient("localhost:9000", properties);
```

## Building

The admin client is part of the `pinot-java-client` module. Build it along with the rest of Pinot:

```bash
mvn clean compile -DskipTests
```

## Testing

Run the integration tests:

```bash
mvn test -Dtest=PinotAdminClientTest
```

Note: Integration tests require a running Pinot cluster.

## Contributing

When adding new API methods:

1. Add the method to the appropriate service client
2. Include proper exception handling
3. Add async version if the operation is long-running
4. Update documentation and examples
5. Add corresponding tests

## API Coverage

The admin client currently covers all major Pinot controller APIs:

- ✅ Table management (CRUD, status, metadata, rebalance)
- ✅ Schema management (CRUD, validation)
- ✅ Instance management (CRUD, state management)
- ✅ Segment operations (query, metadata, deletion)
- ✅ Tenant management (CRUD, configuration, rebalance)
- ✅ Task management (monitoring, debugging)

This provides a complete programmatic interface for Pinot cluster administration.
