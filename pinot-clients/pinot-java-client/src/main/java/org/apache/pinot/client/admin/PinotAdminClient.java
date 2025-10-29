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
import org.apache.pinot.client.PinotClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Main admin client for Pinot controller operations.
 * Provides access to all administrative APIs for managing Pinot clusters.
 */
public class PinotAdminClient implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotAdminClient.class);

  private final PinotAdminTransport _transport;
  private final String _controllerAddress;
  private final Map<String, String> _headers;

  // Service clients
  private PinotTableAdminClient _tableClient;
  private PinotSchemaAdminClient _schemaClient;
  private PinotInstanceAdminClient _instanceClient;
  private PinotSegmentAdminClient _segmentClient;
  private PinotTenantAdminClient _tenantClient;
  private PinotTaskAdminClient _taskClient;

  /**
   * Creates a PinotAdminClient with the specified controller address.
   *
   * @param controllerAddress The address of the Pinot controller (e.g., "localhost:9000")
   * @throws PinotClientException If the client cannot be initialized
   */
  public PinotAdminClient(String controllerAddress)
      throws PinotClientException {
    this(controllerAddress, new Properties());
  }

  /**
   * Creates a PinotAdminClient with the specified controller address and properties.
   *
   * @param controllerAddress The address of the Pinot controller (e.g., "localhost:9000")
   * @param properties Configuration properties for the client
   * @throws PinotClientException If the client cannot be initialized
   */
  public PinotAdminClient(String controllerAddress, Properties properties)
      throws PinotClientException {
    this(controllerAddress, properties, null);
  }

  /**
   * Creates a PinotAdminClient with the specified controller address, properties, and authentication headers.
   *
   * @param controllerAddress The address of the Pinot controller (e.g., "localhost:9000")
   * @param properties Configuration properties for the client
   * @param authHeaders Authentication headers for admin operations
   * @throws PinotClientException If the client cannot be initialized
   */
  public PinotAdminClient(String controllerAddress, Properties properties, Map<String, String> authHeaders)
      throws PinotClientException {
    _controllerAddress = controllerAddress;
    _transport = new PinotAdminTransport(properties, authHeaders);
    _headers = authHeaders != null ? authHeaders : Map.of();
    LOGGER.info("Created Pinot admin client for controller at {}", controllerAddress);
  }

  /**
   * Creates a PinotAdminClient with authentication configuration.
   *
   * @param controllerAddress The address of the Pinot controller (e.g., "localhost:9000")
   * @param properties Configuration properties for the client
   * @param authType Authentication type
   * @param authConfig Authentication configuration
   * @throws PinotClientException If the client cannot be initialized
   * @throws PinotAdminAuthenticationException If authentication configuration is invalid
   */
  public PinotAdminClient(String controllerAddress, Properties properties,
      PinotAdminAuthentication.AuthType authType, Map<String, String> authConfig)
      throws PinotClientException, PinotAdminAuthenticationException {
    _controllerAddress = controllerAddress;
    Map<String, String> authHeaders = PinotAdminAuthentication.createAuthHeaders(authType, authConfig);
    _transport = new PinotAdminTransport(properties, authHeaders);
    _headers = authHeaders;
    LOGGER.info("Created Pinot admin client for controller at {} with {} authentication",
        controllerAddress, authType);
  }

  // Package-private constructor for tests to inject a mocked transport
  PinotAdminClient(String controllerAddress, PinotAdminTransport transport, Map<String, String> headers) {
    _controllerAddress = controllerAddress;
    _transport = transport;
    _headers = headers != null ? headers : Map.of();
  }

  /**
   * Gets the table administration client.
   *
   * @return Table administration operations
   */
  public PinotTableAdminClient getTableClient() {
    if (_tableClient == null) {
      _tableClient = new PinotTableAdminClient(_transport, _controllerAddress, _headers);
    }
    return _tableClient;
  }

  /**
   * Gets the schema administration client.
   *
   * @return Schema administration operations
   */
  public PinotSchemaAdminClient getSchemaClient() {
    if (_schemaClient == null) {
      _schemaClient = new PinotSchemaAdminClient(_transport, _controllerAddress, _headers);
    }
    return _schemaClient;
  }

  /**
   * Gets the instance administration client.
   *
   * @return Instance administration operations
   */
  public PinotInstanceAdminClient getInstanceClient() {
    if (_instanceClient == null) {
      _instanceClient = new PinotInstanceAdminClient(_transport, _controllerAddress, _headers);
    }
    return _instanceClient;
  }

  /**
   * Gets the segment administration client.
   *
   * @return Segment administration operations
   */
  public PinotSegmentAdminClient getSegmentClient() {
    if (_segmentClient == null) {
      _segmentClient = new PinotSegmentAdminClient(_transport, _controllerAddress, _headers);
    }
    return _segmentClient;
  }

  /**
   * Gets the tenant administration client.
   *
   * @return Tenant administration operations
   */
  public PinotTenantAdminClient getTenantClient() {
    if (_tenantClient == null) {
      _tenantClient = new PinotTenantAdminClient(_transport, _controllerAddress, _headers);
    }
    return _tenantClient;
  }

  /**
   * Gets the task administration client.
   *
   * @return Task administration operations
   */
  public PinotTaskAdminClient getTaskClient() {
    if (_taskClient == null) {
      _taskClient = new PinotTaskAdminClient(_transport, _controllerAddress, _headers);
    }
    return _taskClient;
  }

  @Override
  public void close()
      throws IOException {
    try {
      _transport.close();
    } catch (PinotClientException e) {
      throw new IOException("Failed to close admin client transport", e);
    }
  }
}
