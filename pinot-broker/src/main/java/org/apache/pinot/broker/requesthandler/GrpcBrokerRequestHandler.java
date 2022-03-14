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
package org.apache.pinot.broker.requesthandler;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.broker.api.RequestStatistics;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.RoutingManager;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.grpc.GrpcQueryClient;
import org.apache.pinot.common.utils.grpc.GrpcRequestBuilder;
import org.apache.pinot.core.query.reduce.StreamingReduceService;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * The <code>GrpcBrokerRequestHandler</code> class communicates query request via GRPC.
 */
@ThreadSafe
public class GrpcBrokerRequestHandler extends BaseBrokerRequestHandler {

  private final GrpcQueryClient.Config _grpcConfig;
  private final StreamingReduceService _streamingReduceService;
  private final PinotStreamingQueryClient _streamingQueryClient;

  public GrpcBrokerRequestHandler(PinotConfiguration config, RoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
      BrokerMetrics brokerMetrics, TlsConfig tlsConfig) {
    super(config, routingManager, accessControlFactory, queryQuotaManager, tableCache, brokerMetrics);
    _grpcConfig = buildGrpcQueryClientConfig(config);

    // create streaming query client
    _streamingQueryClient = new PinotStreamingQueryClient(_grpcConfig);

    // create streaming reduce service
    _streamingReduceService = new StreamingReduceService(config);
  }

  @Override
  public void start() {
  }

  @Override
  public synchronized void shutDown() {
    _streamingReduceService.shutDown();
  }

  @Override
  protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
      BrokerRequest brokerRequest, @Nullable BrokerRequest offlineBrokerRequest, @Nullable Map<ServerInstance,
      List<String>> offlineRoutingTable, @Nullable BrokerRequest realtimeBrokerRequest, @Nullable Map<ServerInstance,
      List<String>> realtimeRoutingTable, long timeoutMs, ServerStats serverStats, RequestStatistics requestStatistics)
      throws Exception {
    assert offlineBrokerRequest != null || realtimeBrokerRequest != null;

    String rawTableName = TableNameBuilder.extractRawTableName(originalBrokerRequest.getQuerySource().getTableName());
    Map<ServerRoutingInstance, Iterator<Server.ServerResponse>> responseMap = new HashMap<>();
    // Making request to a streaming server response.

    if (offlineBrokerRequest != null) {
      // to offline servers.
      assert offlineRoutingTable != null;
      streamingQueryToPinotServer(requestId, _brokerId, rawTableName, TableType.OFFLINE, responseMap,
          offlineBrokerRequest, offlineRoutingTable, timeoutMs, true, 1);
    }
    if (realtimeBrokerRequest != null) {
      // to realtime servers.
      assert realtimeRoutingTable != null;
      streamingQueryToPinotServer(requestId, _brokerId, rawTableName, TableType.REALTIME, responseMap,
          realtimeBrokerRequest, realtimeRoutingTable, timeoutMs, true, 1);
    }
    BrokerResponseNative brokerResponse = _streamingReduceService.reduceOnStreamResponse(
        originalBrokerRequest, responseMap, timeoutMs, _brokerMetrics);
    return brokerResponse;
  }

  /**
   * Query pinot server for data table.
   */
  public void streamingQueryToPinotServer(final long requestId, final String brokerHost, final String rawTableName,
      final TableType tableType, Map<ServerRoutingInstance, Iterator<Server.ServerResponse>> responseMap,
      BrokerRequest brokerRequest, Map<ServerInstance, List<String>> routingTable, long connectionTimeoutInMillis,
      boolean ignoreEmptyResponses, int pinotRetryCount) {
    // Retries will all hit the same server because the routing decision has already been made by the pinot broker
    Map<ServerRoutingInstance, Iterator<Server.ServerResponse>> serverResponseMap = new HashMap<>();
    for (Map.Entry<ServerInstance, List<String>> routingEntry : routingTable.entrySet()) {
      ServerInstance serverInstance = routingEntry.getKey();
      List<String> segments = routingEntry.getValue();
      String serverHost = serverInstance.getHostname();
      int port = serverInstance.getGrpcPort();
      // TODO: enable throttling on per host bases.
      Iterator<Server.ServerResponse> streamingResponse = _streamingQueryClient.submit(serverHost, port,
          new GrpcRequestBuilder()
              .setSegments(segments)
              .setBrokerRequest(brokerRequest)
              .setEnableStreaming(true));
      responseMap.put(serverInstance.toServerRoutingInstance(tableType), streamingResponse);
    }
  }

  // return empty config for now
  private GrpcQueryClient.Config buildGrpcQueryClientConfig(PinotConfiguration config) {
    return new GrpcQueryClient.Config();
  }

  public static class PinotStreamingQueryClient {
    private final Map<String, GrpcQueryClient> _grpcQueryClientMap = new ConcurrentHashMap<>();
    private final GrpcQueryClient.Config _config;

    public PinotStreamingQueryClient(GrpcQueryClient.Config config) {
      _config = config;
    }

    public Iterator<Server.ServerResponse> submit(String host, int port, GrpcRequestBuilder requestBuilder) {
      GrpcQueryClient client = getOrCreateGrpcQueryClient(host, port);
      return client.submit(requestBuilder.build());
    }

    private GrpcQueryClient getOrCreateGrpcQueryClient(String host, int port) {
      String key = String.format("%s_%d", host, port);
      return _grpcQueryClientMap.computeIfAbsent(key, k -> new GrpcQueryClient(host, port, _config));
    }
  }
}
