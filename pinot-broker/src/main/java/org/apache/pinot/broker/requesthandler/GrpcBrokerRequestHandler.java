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

import io.grpc.ConnectivityState;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.grpc.ServerGrpcQueryClient;
import org.apache.pinot.common.utils.grpc.ServerGrpcRequestBuilder;
import org.apache.pinot.core.query.reduce.StreamingReduceService;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>GrpcBrokerRequestHandler</code> class communicates query request via GRPC.
 */
@ThreadSafe
public class GrpcBrokerRequestHandler extends BaseSingleStageBrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcBrokerRequestHandler.class);

  private final StreamingReduceService _streamingReduceService;
  private final PinotServerStreamingQueryClient _streamingQueryClient;
  private final FailureDetector _failureDetector;

  // TODO: Support TLS
  public GrpcBrokerRequestHandler(PinotConfiguration config, String brokerId, BrokerRoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
      FailureDetector failureDetector) {
    super(config, brokerId, routingManager, accessControlFactory, queryQuotaManager, tableCache);
    _streamingReduceService = new StreamingReduceService(config);
    _streamingQueryClient = new PinotServerStreamingQueryClient(GrpcConfig.buildGrpcQueryConfig(config));
    _failureDetector = failureDetector;
    _failureDetector.registerUnhealthyServerRetrier(this::retryUnhealthyServer);
  }

  @Override
  public void start() {
    super.start();
  }

  @Override
  public void shutDown() {
    super.shutDown();
    _streamingQueryClient.shutdown();
    _streamingReduceService.shutDown();
  }

  @Override
  protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
      BrokerRequest serverBrokerRequest, @Nullable BrokerRequest offlineBrokerRequest,
      @Nullable Map<ServerInstance, ServerRouteInfo> offlineRoutingTable,
      @Nullable BrokerRequest realtimeBrokerRequest,
      @Nullable Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable, long timeoutMs,
      ServerStats serverStats, RequestContext requestContext)
      throws Exception {
    // TODO: Add servers queried/responded stats
    assert offlineBrokerRequest != null || realtimeBrokerRequest != null;
    Map<ServerRoutingInstance, Iterator<Server.ServerResponse>> responseMap = new HashMap<>();
    if (offlineBrokerRequest != null) {
      assert offlineRoutingTable != null;
      sendRequest(requestId, TableType.OFFLINE, offlineBrokerRequest, offlineRoutingTable, responseMap,
          requestContext.isSampledRequest());
    }
    if (realtimeBrokerRequest != null) {
      assert realtimeRoutingTable != null;
      sendRequest(requestId, TableType.REALTIME, realtimeBrokerRequest, realtimeRoutingTable, responseMap,
          requestContext.isSampledRequest());
    }
    long reduceStartTimeNs = System.nanoTime();
    BrokerResponseNative brokerResponse =
        _streamingReduceService.reduceOnStreamResponse(originalBrokerRequest, responseMap, timeoutMs, _brokerMetrics);
    brokerResponse.setBrokerReduceTimeMs(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - reduceStartTimeNs));
    return brokerResponse;
  }

  /**
   * Query pinot server for data table.
   */
  private void sendRequest(long requestId, TableType tableType, BrokerRequest brokerRequest,
      Map<ServerInstance, ServerRouteInfo> routingTable,
      Map<ServerRoutingInstance, Iterator<Server.ServerResponse>> responseMap, boolean trace) {
    for (Map.Entry<ServerInstance, ServerRouteInfo> routingEntry : routingTable.entrySet()) {
      ServerInstance serverInstance = routingEntry.getKey();
      // TODO: support optional segments for GrpcQueryServer.
      List<String> segments = routingEntry.getValue().getSegments();
      // TODO: enable throttling on per host bases.
      try {
        String cid = QueryThreadContext.getCid() == null ? QueryThreadContext.getCid() : Long.toString(requestId);
        Iterator<Server.ServerResponse> streamingResponse = _streamingQueryClient.submit(serverInstance,
            new ServerGrpcRequestBuilder()
                .setRequestId(requestId)
                .setCid(cid)
                .setBrokerId(_brokerId)
                .setEnableTrace(trace)
                .setEnableStreaming(true)
                .setBrokerRequest(brokerRequest)
                .setSegments(segments).build());
        responseMap.put(serverInstance.toServerRoutingInstance(tableType, ServerInstance.RoutingType.GRPC),
            streamingResponse);
      } catch (Exception e) {
        LOGGER.warn("Failed to send request {} to server: {}", requestId, serverInstance.getInstanceId(), e);
        _failureDetector.markServerUnhealthy(serverInstance.getInstanceId());
      }
    }
  }

  public static class PinotServerStreamingQueryClient {
    private final Map<String, ServerGrpcQueryClient> _grpcQueryClientMap = new ConcurrentHashMap<>();
    private final GrpcConfig _config;

    public PinotServerStreamingQueryClient(GrpcConfig config) {
      _config = config;
    }

    public Iterator<Server.ServerResponse> submit(ServerInstance serverInstance, Server.ServerRequest serverRequest) {
      ServerGrpcQueryClient client = getOrCreateGrpcQueryClient(serverInstance);
      return client.submit(serverRequest);
    }

    private ServerGrpcQueryClient getOrCreateGrpcQueryClient(ServerInstance serverInstance) {
      String hostnamePort = String.format("%s_%d", serverInstance.getHostname(), serverInstance.getGrpcPort());
      return _grpcQueryClientMap.computeIfAbsent(hostnamePort,
          k -> new ServerGrpcQueryClient(serverInstance.getHostname(), serverInstance.getGrpcPort(), _config));
    }

    public void shutdown() {
      for (ServerGrpcQueryClient client : _grpcQueryClientMap.values()) {
        client.close();
      }
    }
  }

  /**
   * Check if a server that was previously detected as unhealthy is now healthy.
   */
  private FailureDetector.ServerState retryUnhealthyServer(String instanceId) {
    LOGGER.info("Checking gRPC connection to unhealthy server: {}", instanceId);
    ServerInstance serverInstance = _routingManager.getEnabledServerInstanceMap().get(instanceId);
    if (serverInstance == null) {
      LOGGER.info("Failed to find enabled server: {} in routing manager, skipping the retry", instanceId);
      return FailureDetector.ServerState.UNHEALTHY;
    }

    String hostnamePort = String.format("%s_%d", serverInstance.getHostname(), serverInstance.getGrpcPort());
    ServerGrpcQueryClient client = _streamingQueryClient._grpcQueryClientMap.get(hostnamePort);

    // Could occur if the cluster is only serving multi-stage queries
    if (client == null) {
      LOGGER.debug("No GrpcQueryClient found for server with instanceId: {}", instanceId);
      return FailureDetector.ServerState.UNKNOWN;
    }

    ConnectivityState connectivityState = client.getChannel().getState(true);
    if (connectivityState == ConnectivityState.READY) {
      LOGGER.info("Successfully connected to server: {}", instanceId);
      return FailureDetector.ServerState.HEALTHY;
    } else {
      LOGGER.info("Still can't connect to server: {}, current state: {}", instanceId, connectivityState);
      return FailureDetector.ServerState.UNHEALTHY;
    }
  }
}
