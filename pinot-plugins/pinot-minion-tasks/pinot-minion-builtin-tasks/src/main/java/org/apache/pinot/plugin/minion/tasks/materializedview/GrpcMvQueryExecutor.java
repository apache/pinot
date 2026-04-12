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
package org.apache.pinot.plugin.minion.tasks.materializedview;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.compression.CompressionFactory;
import org.apache.pinot.common.compression.Compressor;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.response.encoder.ResponseEncoder;
import org.apache.pinot.common.response.encoder.ResponseEncoderFactory;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.grpc.BrokerGrpcQueryClient;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * gRPC-based implementation of {@link MvQueryExecutor}.
 *
 * <p>Features:
 * <ul>
 *   <li><b>Load balancing</b>: discovers all gRPC-enabled brokers via Helix and
 *       selects them in round-robin order.</li>
 *   <li><b>Connection reuse</b>: caches {@link BrokerGrpcQueryClient} instances per
 *       broker endpoint ({@code host_port}), following the same pattern used in
 *       {@code GrpcConnection.BrokerStreamingQueryClient}.</li>
 *   <li><b>Stale client cleanup</b>: when the broker list is refreshed, clients for
 *       brokers that are no longer in the cluster are closed and evicted.</li>
 * </ul>
 *
 * <p>Instances are thread-safe and intended to be long-lived (one per executor factory).
 */
public class GrpcMvQueryExecutor implements MvQueryExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcMvQueryExecutor.class);

  private final HelixManager _helixManager;
  private final GrpcConfig _grpcConfig;
  private final ConcurrentHashMap<String, BrokerGrpcQueryClient> _clientCache = new ConcurrentHashMap<>();
  private final AtomicInteger _roundRobinCounter = new AtomicInteger(0);

  public GrpcMvQueryExecutor(HelixManager helixManager, GrpcConfig grpcConfig) {
    _helixManager = helixManager;
    _grpcConfig = grpcConfig;
  }

  @Override
  public QueryResult executeQuery(String sql, Map<String, String> authHeaders)
      throws IOException {
    Pair<String, Integer> broker = selectBroker();
    LOGGER.info("Selected broker gRPC endpoint: {}:{}", broker.getLeft(), broker.getRight());

    BrokerGrpcQueryClient client = getOrCreateClient(broker.getLeft(), broker.getRight());

    Broker.BrokerRequest brokerRequest = Broker.BrokerRequest.newBuilder()
        .setSql(sql)
        .putAllMetadata(authHeaders)
        .build();
    Iterator<Broker.BrokerResponse> responseIterator = client.submit(brokerRequest);

    Preconditions.checkState(responseIterator.hasNext(), "Empty gRPC response for query: %s", sql);
    Broker.BrokerResponse metadataResponse = responseIterator.next();
    JsonNode metadataJson = JsonUtils.bytesToJsonNode(metadataResponse.getPayload().toByteArray());
    if (metadataJson.has("exceptions") && !metadataJson.get("exceptions").isEmpty()) {
      throw new RuntimeException("Query execution failed with exceptions: " + metadataJson.get("exceptions"));
    }

    Preconditions.checkState(responseIterator.hasNext(), "No schema in gRPC response for query: %s", sql);
    Broker.BrokerResponse schemaResponse = responseIterator.next();
    DataSchema dataSchema = DataSchema.fromBytes(schemaResponse.getPayload().asReadOnlyByteBuffer());

    List<Object[]> allRows = new ArrayList<>();
    while (responseIterator.hasNext()) {
      Broker.BrokerResponse dataResponse = responseIterator.next();
      Map<String, String> responseMetadata = dataResponse.getMetadataMap();

      String compressionAlgorithm = responseMetadata.getOrDefault(
          CommonConstants.Broker.Grpc.COMPRESSION, CommonConstants.Broker.Grpc.DEFAULT_COMPRESSION);
      Compressor compressor = CompressionFactory.getCompressor(compressionAlgorithm);

      String encodingType = responseMetadata.getOrDefault(
          CommonConstants.Broker.Grpc.ENCODING, CommonConstants.Broker.Grpc.DEFAULT_ENCODING);
      ResponseEncoder responseEncoder = ResponseEncoderFactory.getResponseEncoder(encodingType);

      byte[] respBytes = dataResponse.getPayload().toByteArray();
      int rowSize = Integer.parseInt(responseMetadata.get("rowSize"));
      byte[] uncompressedPayload;
      try {
        uncompressedPayload = compressor.decompress(respBytes);
      } catch (Exception e) {
        throw new RuntimeException("Failed to decompress gRPC response payload", e);
      }

      ResultTable resultTable = responseEncoder.decodeResultTable(uncompressedPayload, rowSize, dataSchema);
      allRows.addAll(resultTable.getRows());
    }

    LOGGER.info("gRPC query returned schema with {} columns and {} rows",
        dataSchema.getColumnNames().length, allRows.size());
    return new QueryResult(dataSchema, allRows);
  }

  /**
   * Discovers all gRPC-enabled brokers from Helix and selects one using round-robin.
   * Also evicts cached clients for brokers that are no longer present.
   */
  @VisibleForTesting
  Pair<String, Integer> selectBroker() {
    List<Pair<String, Integer>> brokerEndpoints = discoverBrokerEndpoints();
    Preconditions.checkState(!brokerEndpoints.isEmpty(),
        "No broker with gRPC enabled found in the cluster");

    evictStaleClients(brokerEndpoints);

    int index = Math.abs(_roundRobinCounter.getAndIncrement() % brokerEndpoints.size());
    return brokerEndpoints.get(index);
  }

  /**
   * Scans Helix instance configs for brokers that have a gRPC port configured.
   */
  private List<Pair<String, Integer>> discoverBrokerEndpoints() {
    List<Pair<String, Integer>> endpoints = new ArrayList<>();
    List<InstanceConfig> instanceConfigs = HelixHelper.getInstanceConfigs(_helixManager);
    for (InstanceConfig instanceConfig : instanceConfigs) {
      if (!InstanceTypeUtils.isBroker(instanceConfig.getInstanceName())) {
        continue;
      }
      String grpcPortStr = instanceConfig.getRecord()
          .getSimpleField(CommonConstants.Helix.Instance.GRPC_PORT_KEY);
      if (grpcPortStr != null) {
        int grpcPort = Integer.parseInt(grpcPortStr);
        if (grpcPort > 0) {
          endpoints.add(Pair.of(instanceConfig.getHostName(), grpcPort));
        }
      }
    }
    return endpoints;
  }

  /**
   * Closes and removes cached clients for brokers that are no longer in the cluster.
   */
  private void evictStaleClients(List<Pair<String, Integer>> currentEndpoints) {
    Set<String> activeKeys = currentEndpoints.stream()
        .map(p -> clientKey(p.getLeft(), p.getRight()))
        .collect(Collectors.toSet());

    for (String cachedKey : _clientCache.keySet()) {
      if (!activeKeys.contains(cachedKey)) {
        BrokerGrpcQueryClient removed = _clientCache.remove(cachedKey);
        if (removed != null) {
          LOGGER.info("Evicting stale gRPC client for broker: {}", cachedKey);
          removed.close();
        }
      }
    }
  }

  private BrokerGrpcQueryClient getOrCreateClient(String host, int port) {
    String key = clientKey(host, port);
    return _clientCache.computeIfAbsent(key,
        k -> new BrokerGrpcQueryClient(host, port, _grpcConfig));
  }

  private static String clientKey(String host, int port) {
    return host + "_" + port;
  }

  @VisibleForTesting
  int getCachedClientCount() {
    return _clientCache.size();
  }

  @Override
  public void close() {
    for (BrokerGrpcQueryClient client : _clientCache.values()) {
      try {
        client.close();
      } catch (Exception e) {
        LOGGER.warn("Error closing gRPC client", e);
      }
    }
    _clientCache.clear();
  }
}
