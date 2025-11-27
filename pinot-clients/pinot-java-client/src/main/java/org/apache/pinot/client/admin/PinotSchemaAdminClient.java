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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Client for schema administration operations.
 * Provides methods to create, update, delete, and manage Pinot schemas.
 */
public class PinotSchemaAdminClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSchemaAdminClient.class);

  private final PinotAdminTransport _transport;
  private final String _controllerAddress;
  private final Map<String, String> _headers;

  public PinotSchemaAdminClient(PinotAdminTransport transport, String controllerAddress,
      Map<String, String> headers) {
    _transport = transport;
    _controllerAddress = controllerAddress;
    _headers = headers;
  }

  /**
   * Lists all schema names in the cluster.
   *
   * @return List of schema names
   * @throws PinotAdminException If the request fails
   */
  public List<String> listSchemaNames()
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/schemas", null, _headers);
    return _transport.parseStringArray(response, "schemas");
  }

  /**
   * Gets information about all schemas including field count details.
   *
   * @return Schema information as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getSchemasInfo()
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/schemas/info", null, _headers);
    return response.toString();
  }

  /**
   * Gets a specific schema by name.
   *
   * @param schemaName Name of the schema
   * @return Schema configuration as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getSchema(String schemaName)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/schemas/" + schemaName, null, _headers);
    return response.toString();
  }

  /**
   * Deletes a schema by name.
   *
   * @param schemaName Name of the schema to delete
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String deleteSchema(String schemaName)
      throws PinotAdminException {
    JsonNode response = _transport.executeDelete(_controllerAddress, "/schemas/" + schemaName, null, _headers);
    return response.toString();
  }

  /**
   * Updates an existing schema.
   *
   * @param schemaName Name of the schema to update
   * @param schemaConfig New schema configuration as JSON string
   * @param reloadTables Whether to reload tables using this schema
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String updateSchema(String schemaName, String schemaConfig, boolean reloadTables)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("reloadTables", String.valueOf(reloadTables));

    JsonNode response = _transport.executePut(_controllerAddress, "/schemas/" + schemaName, schemaConfig,
        queryParams, _headers);
    return response.toString();
  }

  /**
   * Updates an existing schema (without reloading tables).
   *
   * @param schemaName Name of the schema to update
   * @param schemaConfig New schema configuration as JSON string
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String updateSchema(String schemaName, String schemaConfig)
      throws PinotAdminException {
    return updateSchema(schemaName, schemaConfig, false);
  }

  /**
   * Creates a new schema.
   *
   * @param schemaConfig Schema configuration as JSON string
   * @param force Whether to force creation even if schema exists
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String createSchema(String schemaConfig, boolean force)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("force", String.valueOf(force));

    JsonNode response = _transport.executePost(_controllerAddress, "/schemas", schemaConfig, queryParams, _headers);
    return response.toString();
  }

  /**
   * Creates a new schema (non-force creation).
   *
   * @param schemaConfig Schema configuration as JSON string
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String createSchema(String schemaConfig)
      throws PinotAdminException {
    return createSchema(schemaConfig, false);
  }

  /**
   * Validates a schema configuration without applying it.
   *
   * @param schemaConfig Schema configuration to validate as JSON string
   * @param force Whether to force validation even if schema exists
   * @return Validation response
   * @throws PinotAdminException If the request fails
   */
  public String validateSchema(String schemaConfig, boolean force)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("force", String.valueOf(force));

    JsonNode response = _transport.executePost(_controllerAddress, "/schemas/validate", schemaConfig,
        queryParams, _headers);
    return response.toString();
  }

  /**
   * Validates a schema configuration without applying it (non-force validation).
   *
   * @param schemaConfig Schema configuration to validate as JSON string
   * @return Validation response
   * @throws PinotAdminException If the request fails
   */
  public String validateSchema(String schemaConfig)
      throws PinotAdminException {
    return validateSchema(schemaConfig, false);
  }

  /**
   * Gets field specification metadata.
   *
   * @return Field specification metadata as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getFieldSpecMetadata()
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/schemas/fieldSpec", null, _headers);
    return response.toString();
  }

  // Async versions of key methods

  /**
   * Lists all schema names in the cluster (async).
   */
  public CompletableFuture<List<String>> listSchemaNamesAsync() {
    return _transport.executeGetAsync(_controllerAddress, "/schemas", null, _headers)
        .thenApply(response -> _transport.parseStringArraySafe(response, "schemas"));
  }

  /**
   * Gets a specific schema by name (async).
   */
  public CompletableFuture<String> getSchemaAsync(String schemaName) {
    return _transport.executeGetAsync(_controllerAddress, "/schemas/" + schemaName, null, _headers)
        .thenApply(JsonNode::toString);
  }

  /**
   * Creates a new schema (async).
   */
  public CompletableFuture<String> createSchemaAsync(String schemaConfig, boolean force) {
    Map<String, String> queryParams = Map.of("force", String.valueOf(force));

    return _transport.executePostAsync(_controllerAddress, "/schemas", schemaConfig, queryParams, _headers)
        .thenApply(JsonNode::toString);
  }

  /**
   * Creates a new schema (async, non-force).
   */
  public CompletableFuture<String> createSchemaAsync(String schemaConfig) {
    return createSchemaAsync(schemaConfig, false);
  }
}
