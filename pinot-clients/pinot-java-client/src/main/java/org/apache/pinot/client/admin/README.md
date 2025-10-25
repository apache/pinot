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
    - `PinotTableAdminClient`: Table operations (CRUD, status, metadata)
    - `PinotSchemaAdminClient`: Schema operations (CRUD, validation)
    - `PinotInstanceAdminClient`: Instance operations (CRUD, state management)
    - `PinotSegmentAdminClient`: Segment operations (query, management)
    - `PinotTenantAdminClient`: Tenant operations (CRUD, configuration)
    - `PinotTaskAdminClient`: Task management operations
4. **Exception Classes**: Specialized exceptions for different error types
5. **Authentication Utilities**: Support for various authentication methods

## Usage Examples

### Basic Usage

```java
import org.apache.pinot.client.admin.*;

// Create client without authentication
try(PinotAdminClient adminClient = new PinotAdminClient("localhost:9000")){
// List all tables
List<String> tables = adminClient.getTableClient().listTables(null, null, null);

// Get a specific table configuration
String config = adminClient.getTableClient().getTableConfig("myTable");

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
PinotTableAdminClient tableClient = adminClient.getTableClient();

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

### Schema Operations

```java
PinotSchemaAdminClient schemaClient = adminClient.getSchemaClient();

// List all schemas
List<String> schemas = schemaClient.listSchemaNames();

// Get schema configuration
String schema = schemaClient.getSchema("mySchema");

// Create a new schema
String createResult = schemaClient.createSchema(schemaConfigJson);

// Validate schema before creating
String validation = schemaClient.validateSchema(schemaConfigJson);
```

### Instance Operations

```java
PinotInstanceAdminClient instanceClient = adminClient.getInstanceClient();

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
PinotSegmentAdminClient segmentClient = adminClient.getSegmentClient();

// List segments for a table
List<String> segments = segmentClient.listSegments("myTable_OFFLINE", false);

// Get segment metadata
Map<String, Object> metadata = segmentClient.getSegmentMetadata("myTable_OFFLINE", "segmentName", null);

// Delete a segment
String deleteResult = segmentClient.deleteSegment("myTable_OFFLINE", "segmentName", "7d");
```

### Tenant Operations

```java
PinotTenantAdminClient tenantClient = adminClient.getTenantClient();

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
PinotTaskAdminClient taskClient = adminClient.getTaskClient();

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
