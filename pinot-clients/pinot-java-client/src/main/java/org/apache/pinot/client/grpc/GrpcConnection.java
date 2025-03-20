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
package org.apache.pinot.client.grpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.client.BrokerResponse;
import org.apache.pinot.client.BrokerSelector;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.PinotClientException;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.client.SimpleBrokerSelector;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.common.utils.grpc.BrokerGrpcQueryClient;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A grpc connection to Pinot, normally created through calls to the {@link org.apache.pinot.client.ConnectionFactory}.
 */
public class GrpcConnection {
  public static final String FAIL_ON_EXCEPTIONS = "failOnExceptions";
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcConnection.class);

  private final BrokerSelector _brokerSelector;
  private final boolean _failOnExceptions;
  private final BrokerStreamingQueryClient _grpcQueryClient;

  public GrpcConnection(Properties properties, List<String> brokerList) {
    this(properties, new SimpleBrokerSelector(brokerList));
    LOGGER.info("Created connection to broker list {}", brokerList);
  }

  GrpcConnection(Properties properties, BrokerSelector brokerSelector) {
    _brokerSelector = brokerSelector;
    // Convert Properties properties to a Map
    Map<String, Object> propertiesMap = new HashMap<>();
    properties.forEach((key, value) -> propertiesMap.put(key.toString(), value));
    _grpcQueryClient = new BrokerStreamingQueryClient(new GrpcConfig(new PinotConfiguration(propertiesMap)));
    // Default fail Pinot query if response contains any exception.
    _failOnExceptions = Boolean.parseBoolean(properties.getProperty(FAIL_ON_EXCEPTIONS, "TRUE"));
  }

  /**
   * Creates a prepared statement, to escape query parameters.
   *
   * @param query The query for which to create a prepared statement
   * @return A prepared statement for this connection
   */
  public GrpcPreparedStatement prepareStatement(String query) {
    return new GrpcPreparedStatement(this, query);
  }

  /**
   * Executes a query.
   *
   * @param query The query to execute
   * @return The result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public ResultSetGroup execute(String query)
      throws PinotClientException, IOException {
    return execute(query, new HashMap<>());
  }

  /**
   * Executes a query.
   *
   * @param query The query to execute
   * @param metadataMap The query metadata
   * @return The result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public ResultSetGroup execute(String query, Map<String, String> metadataMap)
      throws PinotClientException, IOException {
    BrokerResponse brokerResponse = BrokerResponse.fromJson(getJsonResponse(query, metadataMap));
    if (!brokerResponse.getExceptions().isEmpty() && _failOnExceptions) {
      throw new PinotClientException("Query had processing exceptions: \n" + brokerResponse.getExceptions());
    }
    return new ResultSetGroup(brokerResponse);
  }

  /**
   * Executes a query.
   *
   * @param query The query to execute
   * @return The result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public GrpcResultSetGroup executeGrpc(String query)
      throws PinotClientException, IOException {
    return executeGrpc(query, new HashMap<>());
  }

  /**
   * Executes a query.
   *
   * @param query The query to execute
   * @param metadataMap The query metadata
   * @return The result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public GrpcResultSetGroup executeGrpc(String query, Map<String, String> metadataMap)
      throws PinotClientException, IOException {
    Iterator<Broker.BrokerResponse> brokerResponseIterator = executeWithIterator(query, metadataMap);
    return new GrpcResultSetGroup(brokerResponseIterator);
  }

  /**
   * Executes a query asynchronously.
   *
   * @param query The query to execute
   * @return A future containing the result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public CompletableFuture<ResultSetGroup> executeAsync(String query)
      throws PinotClientException {
    return executeAsync(query, new HashMap<>());
  }

  /**
   * Executes a query asynchronously.
   *
   * @param query The query to execute
   * @return A future containing the result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public CompletableFuture<ResultSetGroup> executeAsync(String query, Map<String, String> metadataMap)
      throws PinotClientException {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return new ResultSetGroup(BrokerResponse.fromJson(getJsonResponse(query, metadataMap)));
      } catch (IOException e) {
        throw new PinotClientException("Failed to execute query: " + query, e);
      }
    });
  }

  /**
   * Executes a query.
   *
   * @param query The query to execute
   * @param metadataMap The query metadata
   * @return The JsonNode result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public JsonNode getJsonResponse(String query, Map<String, String> metadataMap)
      throws IOException {
    Iterator<org.apache.pinot.common.proto.Broker.BrokerResponse> response = executeWithIterator(query, metadataMap);
    // Process metadata
    ObjectNode brokerResponseJson = JsonUtils.newObjectNode();
    if (response.hasNext()) {
      brokerResponseJson.setAll(GrpcUtils.extractMetadataJson(response.next()));
    }
    // Directly return when there is exception
    if (brokerResponseJson.has("exceptions") && !brokerResponseJson.get("exceptions").isEmpty()) {
      return brokerResponseJson;
    }
    // Process schema
    JsonNode schemaJsonNode = null;
    if (response.hasNext()) {
      schemaJsonNode = GrpcUtils.extractSchemaJson(response.next());
    }
    // Process rows
    ArrayNode rows = JsonUtils.newArrayNode();
    while (response.hasNext()) {
      rows.addAll(GrpcUtils.extractRowsJson(response.next()));
    }
    if (schemaJsonNode != null && rows != null) {
      ObjectNode resultTable = JsonUtils.newObjectNode();
      resultTable.putIfAbsent("dataSchema", schemaJsonNode);
      resultTable.putIfAbsent("rows", rows);
      brokerResponseJson.putIfAbsent("resultTable", resultTable);
    }

    return brokerResponseJson;
  }

  /**
   * Executes a query asynchronously.
   *
   * @param query The query to execute
   * @return A future containing the result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public Iterator<Broker.BrokerResponse> executeWithIterator(String query)
      throws PinotClientException {
    return executeWithIterator(query, new HashMap<>());
  }

  /**
   * Executes a query asynchronously.
   *
   * @param query The query to execute
   * @return A future containing the result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public Iterator<Broker.BrokerResponse> executeWithIterator(String query, Map<String, String> metadata)
      throws PinotClientException {
    String[] tableNames = Connection.resolveTableName(query);
    String brokerHostPort = _brokerSelector.selectBroker(tableNames);
    if (brokerHostPort == null) {
      throw new PinotClientException("Could not find broker to query " + ((tableNames == null) ? "with no tables"
          : "for table(s): " + Arrays.asList(tableNames)));
    }
    String brokerHost = brokerHostPort.split(":")[0];
    int brokerPort = Integer.parseInt(brokerHostPort.split(":")[1]);
    Broker.BrokerRequest brokerRequest =
        Broker.BrokerRequest.newBuilder().setSql(query).putAllMetadata(metadata).build();
    return _grpcQueryClient.submit(brokerHost, brokerPort, brokerRequest);
  }

  /**
   * Close the connection for further processing
   *
   * @throws PinotClientException when connection is already closed
   */
  public void close()
      throws PinotClientException {
    _grpcQueryClient.shutdown();
    _brokerSelector.close();
  }

  /**
   * Provides access to the underlying grpc clients for this connection.
   * There may be client metrics useful for monitoring and other observability goals.
   *
   * @return pinot client.
   */
  public BrokerStreamingQueryClient getGrpcQueryClient() {
    return _grpcQueryClient;
  }

  public List<String> getBrokerList() {
    return _brokerSelector.getBrokers();
  }

  public static class BrokerStreamingQueryClient {
    private final Map<String, BrokerGrpcQueryClient> _grpcQueryClientMap = new ConcurrentHashMap<>();
    private final GrpcConfig _config;

    public BrokerStreamingQueryClient(GrpcConfig config) {
      _config = config;
    }

    public Iterator<Broker.BrokerResponse> submit(String brokerHost, int brokerGrpcPort,
        Broker.BrokerRequest brokerRequest) {
      BrokerGrpcQueryClient client = getOrCreateGrpcQueryClient(brokerHost, brokerGrpcPort);
      return client.submit(brokerRequest);
    }

    private BrokerGrpcQueryClient getOrCreateGrpcQueryClient(String brokerHost, int brokerGrpcPort) {
      String hostnamePort = String.format("%s_%d", brokerHost, brokerGrpcPort);
      return _grpcQueryClientMap.computeIfAbsent(hostnamePort,
          k -> new BrokerGrpcQueryClient(brokerHost, brokerGrpcPort, _config));
    }

    public void shutdown() {
      for (BrokerGrpcQueryClient client : _grpcQueryClientMap.values()) {
        client.close();
      }
    }
  }
}
