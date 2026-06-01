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
package org.apache.pinot.materializedview.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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


/// gRPC-based implementation of [MaterializedViewQueryExecutor].
///
/// Features:
///
///   - **Load balancing**: discovers all gRPC-enabled brokers via Helix and
///       selects them in round-robin order.
///   - **Connection reuse**: caches [BrokerGrpcQueryClient] instances per
///       broker endpoint (`host_port`), following the same pattern used in
///       `GrpcConnection.BrokerStreamingQueryClient`.
///   - **Stale client cleanup**: when the broker list is refreshed, clients for
///       brokers that are no longer in the cluster are closed and evicted.
///
///
/// Instances are thread-safe and intended to be long-lived (one per executor factory).
public class GrpcMaterializedViewQueryExecutor implements MaterializedViewQueryExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcMaterializedViewQueryExecutor.class);

  private final HelixManager _helixManager;
  private final GrpcConfig _grpcConfig;
  private final ConcurrentHashMap<String, BrokerGrpcQueryClient> _clientCache = new ConcurrentHashMap<>();
  private final AtomicInteger _roundRobinCounter = new AtomicInteger(0);

  public GrpcMaterializedViewQueryExecutor(HelixManager helixManager, GrpcConfig grpcConfig) {
    _helixManager = helixManager;
    _grpcConfig = grpcConfig;
  }

  @Override
  public QueryHandle executeQuery(String sql, Map<String, String> authHeaders)
      throws IOException {
    Pair<String, Integer> broker = selectBroker();
    LOGGER.info("Selected broker gRPC endpoint: {}:{}", broker.getLeft(), broker.getRight());

    BrokerGrpcQueryClient client = getOrCreateClient(broker.getLeft(), broker.getRight());

    Broker.BrokerRequest brokerRequest = Broker.BrokerRequest.newBuilder()
        .setSql(sql)
        .putAllMetadata(authHeaders)
        .build();
    Iterator<Broker.BrokerResponse> responseIterator = client.submit(brokerRequest);

    Preconditions.checkState(responseIterator.hasNext(),
        "gRPC broker %s:%d returned no response for query: %s. Check broker health and gRPC connectivity.",
        broker.getLeft(), broker.getRight(), sql);
    Broker.BrokerResponse metadataResponse = responseIterator.next();
    JsonNode metadataJson = JsonUtils.bytesToJsonNode(metadataResponse.getPayload().toByteArray());
    if (metadataJson.has("exceptions") && !metadataJson.get("exceptions").isEmpty()) {
      throw new IOException("Query execution failed with exceptions: " + metadataJson.get("exceptions"));
    }

    // The broker gRPC protocol always sends the schema frame after metadata, even for queries
    // that match zero rows.  A missing schema therefore indicates a real protocol error
    // (broker version mismatch, truncated stream, etc.) — fail loud with actionable diagnostics
    // so the operator can identify and fix the underlying issue rather than silently advancing
    // the watermark with no data.
    Preconditions.checkState(responseIterator.hasNext(),
        "gRPC broker %s:%d sent metadata but no schema frame for query: %s. "
            + "Indicates a broker protocol error (version mismatch or truncated stream). "
            + "Empty result sets still include a schema frame.",
        broker.getLeft(), broker.getRight(), sql);
    Broker.BrokerResponse schemaResponse = responseIterator.next();
    DataSchema dataSchema = DataSchema.fromBytes(schemaResponse.getPayload().asReadOnlyByteBuffer());

    return new GrpcQueryHandle(broker, dataSchema, responseIterator);
  }

  /// Streaming handle that decodes one gRPC data frame at a time. Heap residency is bounded by
  /// the size of the currently-buffered frame, NOT the total query result size.
  private static final class GrpcQueryHandle implements QueryHandle {
    private final Pair<String, Integer> _broker;
    private final DataSchema _dataSchema;
    private final Iterator<Broker.BrokerResponse> _responseIterator;
    private final FrameRowIterator _rowIterator;

    private GrpcQueryHandle(Pair<String, Integer> broker, DataSchema dataSchema,
        Iterator<Broker.BrokerResponse> responseIterator) {
      _broker = broker;
      _dataSchema = dataSchema;
      _responseIterator = responseIterator;
      _rowIterator = new FrameRowIterator();
    }

    @Override
    public DataSchema getDataSchema() {
      return _dataSchema;
    }

    @Override
    public Iterator<Object[]> rows() {
      return _rowIterator;
    }

    @Override
    public void close() {
      // Drain any remaining gRPC frames so the underlying call's server-streaming RPC is
      // properly terminated. The Helix-managed channel is shared and cached; not draining
      // here would leak the stream until the channel closes.
      while (_responseIterator.hasNext()) {
        _responseIterator.next();
      }
    }

    /// Iterator that pulls one gRPC data frame at a time and decodes its rows into a small
    /// buffered list; advances to the next frame only when the current one is exhausted.
    private final class FrameRowIterator implements Iterator<Object[]> {
      private List<Object[]> _currentFrameRows;
      private int _cursor;
      private long _totalRowsEmitted;
      private long _framesDecoded;

      @Override
      public boolean hasNext() {
        while (_currentFrameRows == null || _cursor >= _currentFrameRows.size()) {
          if (!_responseIterator.hasNext()) {
            return false;
          }
          decodeNextFrame();
        }
        return true;
      }

      @Override
      public Object[] next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        _totalRowsEmitted++;
        return _currentFrameRows.get(_cursor++);
      }

      private void decodeNextFrame() {
        Broker.BrokerResponse dataResponse = _responseIterator.next();
        Map<String, String> responseMetadata = dataResponse.getMetadataMap();
        String compressionAlgorithm = responseMetadata.getOrDefault(
            CommonConstants.Broker.Grpc.COMPRESSION, CommonConstants.Broker.Grpc.DEFAULT_COMPRESSION);
        Compressor compressor = CompressionFactory.getCompressor(compressionAlgorithm);
        String encodingType = responseMetadata.getOrDefault(
            CommonConstants.Broker.Grpc.ENCODING, CommonConstants.Broker.Grpc.DEFAULT_ENCODING);
        ResponseEncoder responseEncoder = ResponseEncoderFactory.getResponseEncoder(encodingType);

        byte[] respBytes = dataResponse.getPayload().toByteArray();
        String rowSizeStr = responseMetadata.get("rowSize");
        Preconditions.checkNotNull(rowSizeStr,
            "gRPC response metadata missing required 'rowSize' field");
        int rowSize = Integer.parseInt(rowSizeStr);
        byte[] uncompressedPayload;
        try {
          uncompressedPayload = compressor.decompress(respBytes);
        } catch (Exception e) {
          throw new RuntimeException("Failed to decompress gRPC response payload", e);
        }
        ResultTable resultTable;
        try {
          resultTable = responseEncoder.decodeResultTable(uncompressedPayload, rowSize, _dataSchema);
        } catch (IOException e) {
          throw new RuntimeException("Failed to decode gRPC response frame", e);
        }
        _currentFrameRows = resultTable.getRows();
        _cursor = 0;
        _framesDecoded++;
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("gRPC frame {}: decoded {} rows from broker {}:{}",
              _framesDecoded, _currentFrameRows.size(), _broker.getLeft(), _broker.getRight());
        }
      }
    }
  }

  /// Discovers all gRPC-enabled brokers from Helix and selects one using round-robin.
  /// Also evicts cached clients for brokers that are no longer present.
  @VisibleForTesting
  Pair<String, Integer> selectBroker() {
    List<Pair<String, Integer>> brokerEndpoints = discoverBrokerEndpoints();
    Preconditions.checkState(!brokerEndpoints.isEmpty(),
        "No broker with gRPC enabled found in the cluster");

    evictStaleClients(brokerEndpoints);

    int index = Math.abs(_roundRobinCounter.getAndIncrement() % brokerEndpoints.size());
    Pair<String, Integer> selected = brokerEndpoints.get(index);
    getOrCreateClient(selected.getLeft(), selected.getRight());
    return selected;
  }

  /// Scans Helix instance configs for brokers that have a gRPC port configured.
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

  /// Closes and removes cached clients for brokers that are no longer in the cluster.
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
