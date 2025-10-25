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

import java.util.Map;
import java.util.Properties;


/**
 * Example demonstrating how to use PinotAdminClient.
 */
public class PinotAdminClientExample {
  private PinotAdminClientExample() {
  }

  public static void main(String[] args) {
    // Example 1: Basic usage without authentication
    try (PinotAdminClient adminClient = new PinotAdminClient("localhost:9000")) {
      exampleBasicUsage(adminClient);
    } catch (Exception e) {
      System.err.println("Error in basic usage example: " + e.getMessage());
    }

    // Example 2: Usage with basic authentication
    try {
      Properties properties = new Properties();
      properties.setProperty("pinot.admin.request.timeout.ms", "30000");

      PinotAdminClient adminClient = new PinotAdminClient("localhost:9000", properties,
          PinotAdminAuthentication.AuthType.BASIC,
          Map.of("username", "admin", "password", "password"));

      exampleWithAuthentication(adminClient);
      adminClient.close();
    } catch (Exception e) {
      System.err.println("Error in authentication example: " + e.getMessage());
    }

    // Example 3: Usage with bearer token authentication
    try {
      Properties properties = new Properties();
      PinotAdminClient adminClient = new PinotAdminClient("localhost:9000", properties,
          PinotAdminAuthentication.AuthType.BEARER,
          Map.of("token", "your-bearer-token"));

      exampleWithBearerAuth(adminClient);
      adminClient.close();
    } catch (Exception e) {
      System.err.println("Error in bearer auth example: " + e.getMessage());
    }
  }

  private static void exampleBasicUsage(PinotAdminClient adminClient)
      throws PinotAdminException {
    System.out.println("=== Basic Usage Example ===");

    try {
      // List tables
      var tables = adminClient.getTableClient().listTables(null, null, null);
      System.out.println("Tables: " + tables);

      // List schemas
      var schemas = adminClient.getSchemaClient().listSchemaNames();
      System.out.println("Schemas: " + schemas);

      // List instances
      var instances = adminClient.getInstanceClient().listInstances();
      System.out.println("Instances: " + instances);

      // List tenants
      var tenants = adminClient.getTenantClient().listTenants();
      System.out.println("Tenants: " + tenants);

      // List task types
      var taskTypes = adminClient.getTaskClient().listTaskTypes();
      System.out.println("Task types: " + taskTypes);
    } catch (PinotAdminException e) {
      System.out.println("Admin operation failed: " + e.getMessage());
    }
  }

  private static void exampleWithAuthentication(PinotAdminClient adminClient)
      throws PinotAdminException {
    System.out.println("=== Authentication Example ===");

    try {
      // Get a specific table configuration
      String tableConfig = adminClient.getTableClient().getTableConfig("myTable");
      System.out.println("Table config: " + tableConfig);

      // Validate a schema
      String schemaConfig =
          "{\"schemaName\":\"testSchema\",\"dimensionFieldSpecs\":[{\"name\":\"id\",\"dataType\":\"INT\"}]}";
      String validationResult = adminClient.getSchemaClient().validateSchema(schemaConfig);
      System.out.println("Schema validation: " + validationResult);
    } catch (PinotAdminAuthenticationException e) {
      System.out.println("Authentication failed: " + e.getMessage());
    } catch (PinotAdminNotFoundException e) {
      System.out.println("Resource not found: " + e.getMessage());
    } catch (PinotAdminException e) {
      System.out.println("Admin operation failed: " + e.getMessage());
    }
  }

  private static void exampleWithBearerAuth(PinotAdminClient adminClient)
      throws PinotAdminException {
    System.out.println("=== Bearer Authentication Example ===");

    try {
      // Create a new schema
      String schemaConfig =
          "{\"schemaName\":\"exampleSchema\",\"dimensionFieldSpecs\":[{\"name\":\"id\",\"dataType\":\"INT\"}]}";
      String createResult = adminClient.getSchemaClient().createSchema(schemaConfig);
      System.out.println("Schema creation: " + createResult);

      // Get instance information
      var liveInstances = adminClient.getInstanceClient().listLiveInstances();
      System.out.println("Live instances: " + liveInstances);
    } catch (PinotAdminAuthenticationException e) {
      System.out.println("Authentication failed: " + e.getMessage());
    } catch (PinotAdminValidationException e) {
      System.out.println("Validation failed: " + e.getMessage());
    } catch (PinotAdminException e) {
      System.out.println("Admin operation failed: " + e.getMessage());
    }
  }
}
