/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.client.admin;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Example demonstrating how to use PinotAdminClient.
 */
public class PinotAdminClientExample {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotAdminClientExample.class);

  private PinotAdminClientExample() {
  }

  public static void main(String[] args) {
    // Example 1: Basic usage without authentication
    try (PinotAdminClient adminClient = new PinotAdminClient("localhost:9000")) {
      exampleBasicUsage(adminClient);
    } catch (Exception e) {
      LOGGER.error("Error in basic usage example", e);
    }

    // Example 2: Usage with basic authentication
    try {
      Properties properties = new Properties();
      properties.setProperty("pinot.admin.request.timeout.ms", "30000");

      try (PinotAdminClient adminClient = new PinotAdminClient("localhost:9000", properties,
          PinotAdminAuthentication.AuthType.BASIC,
          Map.of("username", "admin", "password", "password"))) {
        exampleWithAuthentication(adminClient);
      }
    } catch (Exception e) {
      LOGGER.error("Error in authentication example", e);
    }

    // Example 3: Usage with bearer token authentication
    try {
      Properties properties = new Properties();
      try (PinotAdminClient adminClient = new PinotAdminClient("localhost:9000", properties,
          PinotAdminAuthentication.AuthType.BEARER,
          Map.of("token", "your-bearer-token"))) {
        exampleWithBearerAuth(adminClient);
      }
    } catch (Exception e) {
      LOGGER.error("Error in bearer auth example", e);
    }
  }

  private static void exampleBasicUsage(PinotAdminClient adminClient)
      throws PinotAdminException {
    LOGGER.info("=== Basic Usage Example ===");

    try {
      // List tables
      var tables = adminClient.getTableClient().listTables(null, null, null);
      LOGGER.info("Tables: {}", tables);

      // List schemas
      var schemas = adminClient.getSchemaClient().listSchemaNames();
      LOGGER.info("Schemas: {}", schemas);

      // List instances
      var instances = adminClient.getInstanceClient().listInstances();
      LOGGER.info("Instances: {}", instances);

      // List tenants
      var tenants = adminClient.getTenantClient().listTenants();
      LOGGER.info("Tenants: {}", tenants);

      // List task types
      var taskTypes = adminClient.getTaskClient().listTaskTypes();
      LOGGER.info("Task types: {}", taskTypes);
    } catch (PinotAdminException e) {
      LOGGER.error("Admin operation failed", e);
    }
  }

  private static void exampleWithAuthentication(PinotAdminClient adminClient)
      throws IOException {
    LOGGER.info("=== Authentication Example ===");

    try {
      // Get a specific table configuration
      String tableConfig = adminClient.getTableClient().getTableConfig("myTable");
      LOGGER.info("Table config: {}", tableConfig);

      TableConfig offlineTableConfig =
          adminClient.getTableClient().getTableConfigObjectForType("myTable", TableType.OFFLINE);
      LOGGER.info("Typed table config: {}", offlineTableConfig.getTableName());

      Schema schema = adminClient.getSchemaClient().getSchemaObject("myTable");
      LOGGER.info("Typed schema: {}", schema.getSchemaName());

      // Validate a schema
      String schemaConfig =
          "{\"schemaName\":\"testSchema\",\"dimensionFieldSpecs\":[{\"name\":\"id\",\"dataType\":\"INT\"}]}";
      String validationResult = adminClient.getSchemaClient().validateSchema(schemaConfig);
      LOGGER.info("Schema validation: {}", validationResult);
    } catch (PinotAdminAuthenticationException e) {
      LOGGER.error("Authentication failed", e);
    } catch (PinotAdminNotFoundException e) {
      LOGGER.error("Resource not found", e);
    } catch (PinotAdminException e) {
      LOGGER.error("Admin operation failed", e);
    }
  }

  private static void exampleWithBearerAuth(PinotAdminClient adminClient)
      throws PinotAdminException {
    LOGGER.info("=== Bearer Authentication Example ===");

    try {
      // Create a new schema
      String schemaConfig =
          "{\"schemaName\":\"exampleSchema\",\"dimensionFieldSpecs\":[{\"name\":\"id\",\"dataType\":\"INT\"}]}";
      String createResult = adminClient.getSchemaClient().createSchema(schemaConfig);
      LOGGER.info("Schema creation: {}", createResult);

      // Get instance information
      var liveInstances = adminClient.getInstanceClient().listLiveInstances();
      LOGGER.info("Live instances: {}", liveInstances);
    } catch (PinotAdminAuthenticationException e) {
      LOGGER.error("Authentication failed", e);
    } catch (PinotAdminValidationException e) {
      LOGGER.error("Validation failed", e);
    } catch (PinotAdminException e) {
      LOGGER.error("Admin operation failed", e);
    }
  }
}
