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
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
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
  private TableAdminClient _tableClient;
  private SchemaAdminClient _schemaClient;
  private InstanceAdminClient _instanceClient;
  private SegmentAdminClient _segmentClient;
  private TenantAdminClient _tenantClient;
  private TaskAdminClient _taskClient;
  private LogicalTableAdminClient _logicalTableClient;
  private ClusterAdminClient _clusterClient;
  private RebalanceAdminClient _rebalanceClient;
  private QueryWorkloadAdminClient _queryWorkloadClient;
  private QueryAdminClient _queryClient;
  private UserAdminClient _userClient;
  private BrokerAdminClient _brokerClient;
  private ZookeeperAdminClient _zookeeperClient;
  private FileIngestClient _fileIngestClient;

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
  public PinotAdminClient(String controllerAddress, Properties properties, @Nullable Map<String, String> authHeaders)
      throws PinotClientException {
    this(controllerAddress, properties, authHeaders, null);
  }

  /**
   * Creates a PinotAdminClient with the specified controller address, properties, authentication headers, and SSL
   * context.
   *
   * @param controllerAddress The address of the Pinot controller (e.g., "localhost:9000")
   * @param properties Configuration properties for the client
   * @param authHeaders Authentication headers for admin operations
   * @param sslContext Optional SSL context used when connecting over HTTPS
   * @throws PinotClientException If the client cannot be initialized
   */
  public PinotAdminClient(String controllerAddress, Properties properties, @Nullable Map<String, String> authHeaders,
      @Nullable SSLContext sslContext)
      throws PinotClientException {
    _controllerAddress = controllerAddress;
    _transport = new PinotAdminTransport(properties, authHeaders, sslContext);
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
  PinotAdminClient(String controllerAddress, PinotAdminTransport transport, @Nullable Map<String, String> headers) {
    _controllerAddress = controllerAddress;
    _transport = transport;
    _headers = headers != null ? headers : Map.of();
  }

  /**
   * Gets the table administration client.
   *
   * @return Table administration operations
   */
  public TableAdminClient getTableClient() {
    if (_tableClient == null) {
      _tableClient = new TableAdminClient(_transport, _controllerAddress, _headers);
    }
    return _tableClient;
  }

  /**
   * Gets the schema administration client.
   *
   * @return Schema administration operations
   */
  public SchemaAdminClient getSchemaClient() {
    if (_schemaClient == null) {
      _schemaClient = new SchemaAdminClient(_transport, _controllerAddress, _headers);
    }
    return _schemaClient;
  }

  /**
   * Gets the instance administration client.
   *
   * @return Instance administration operations
   */
  public InstanceAdminClient getInstanceClient() {
    if (_instanceClient == null) {
      _instanceClient = new InstanceAdminClient(_transport, _controllerAddress, _headers);
    }
    return _instanceClient;
  }

  /**
   * Gets the broker administration client.
   *
   * @return Broker administration operations
   */
  public BrokerAdminClient getBrokerClient() {
    if (_brokerClient == null) {
      _brokerClient = new BrokerAdminClient(_transport, _controllerAddress, _headers);
    }
    return _brokerClient;
  }


  public ZookeeperAdminClient getZookeeperClient() {
    if (_zookeeperClient == null) {
      _zookeeperClient = new ZookeeperAdminClient(_transport, _controllerAddress, _headers);
    }
    return _zookeeperClient;
  }

  /**
   * Gets the file ingestion client.
   */
  public FileIngestClient getFileIngestClient() {
    if (_fileIngestClient == null) {
      _fileIngestClient = new FileIngestClient(_transport, _controllerAddress, _headers);
    }
    return _fileIngestClient;
  }


  /**
   * Gets the segment administration client.
   *
   * @return Segment administration operations
   */
  public SegmentAdminClient getSegmentClient() {
    if (_segmentClient == null) {
      _segmentClient = new SegmentAdminClient(_transport, _controllerAddress, _headers);
    }
    return _segmentClient;
  }

  /**
   * Gets the tenant administration client.
   *
   * @return Tenant administration operations
   */
  public TenantAdminClient getTenantClient() {
    if (_tenantClient == null) {
      _tenantClient = new TenantAdminClient(_transport, _controllerAddress, _headers);
    }
    return _tenantClient;
  }

  /**
   * Gets the task administration client.
   *
   * @return Task administration operations
   */
  public TaskAdminClient getTaskClient() {
    if (_taskClient == null) {
      _taskClient = new TaskAdminClient(_transport, _controllerAddress, _headers);
    }
    return _taskClient;
  }

  /**
   * Gets the logical table administration client.
   */
  public LogicalTableAdminClient getLogicalTableClient() {
    if (_logicalTableClient == null) {
      _logicalTableClient = new LogicalTableAdminClient(_transport, _controllerAddress, _headers);
    }
    return _logicalTableClient;
  }

  /**
   * Gets the cluster administration client.
   */
  public ClusterAdminClient getClusterClient() {
    if (_clusterClient == null) {
      _clusterClient = new ClusterAdminClient(_transport, _controllerAddress, _headers);
    }
    return _clusterClient;
  }

  /**
   * Gets the rebalance administration client.
   */
  public RebalanceAdminClient getRebalanceClient() {
    if (_rebalanceClient == null) {
      _rebalanceClient = new RebalanceAdminClient(_transport, _controllerAddress, _headers);
    }
    return _rebalanceClient;
  }

  public UserAdminClient getUserClient() {
    if (_userClient == null) {
      _userClient = new UserAdminClient(_transport, _controllerAddress, _headers);
    }
    return _userClient;
  }

  /**
   * Gets the query workload administration client.
   */
  public QueryWorkloadAdminClient getQueryWorkloadClient() {
    if (_queryWorkloadClient == null) {
      _queryWorkloadClient = new QueryWorkloadAdminClient(_transport, _controllerAddress, _headers);
    }
    return _queryWorkloadClient;
  }

  /**
   * Gets the query administration client.
   */
  public QueryAdminClient getQueryClient() {
    if (_queryClient == null) {
      _queryClient = new QueryAdminClient(_transport, _controllerAddress, _headers);
    }
    return _queryClient;
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

  /**
   * Exposes controller host:port for helper utilities.
   */
  public String getControllerAddress() {
    return _controllerAddress;
  }

  /**
   * Returns the controller base URL, e.g., http://host:port
   */
  public String getControllerBaseUrl() {
    return _transport.getScheme() + "://" + _controllerAddress;
  }

  /**
   * Returns the v2 segment upload URL.
   */
  public String getSegmentUploadUrl() {
    return getControllerBaseUrl() + "/v2/segments";
  }
}
