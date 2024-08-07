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
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.grpc.GrpcQueryClient;
import org.apache.pinot.common.utils.grpc.GrpcRequestBuilder;
import org.apache.pinot.core.query.reduce.StreamingReduceService;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerQueryRoutingContext;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.RequestContext;


/**
 * The <code>GrpcBrokerRequestHandler</code> class communicates query request via GRPC.
 */
@ThreadSafe
public class GrpcBrokerRequestHandler extends BaseSingleStageBrokerRequestHandler {
  private final StreamingReduceService _streamingReduceService;
  private final PinotStreamingQueryClient _streamingQueryClient;

  // TODO: Support TLS
  public GrpcBrokerRequestHandler(PinotConfiguration config, String brokerId, BrokerRoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache) {
    super(config, brokerId, routingManager, accessControlFactory, queryQuotaManager, tableCache);
    _streamingReduceService = new StreamingReduceService(config);
    _streamingQueryClient = new PinotStreamingQueryClient(GrpcConfig.buildGrpcQueryConfig(config));
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
      BrokerRequest serverBrokerRequest,
      Map<ServerInstance, List<ServerQueryRoutingContext>> queryRoutingTable, long timeoutMs,
      ServerStats serverStats, RequestContext requestContext)
      throws Exception {
    // TODO: Support failure detection
    // TODO: Add servers queried/responded stats
    assert !queryRoutingTable.isEmpty();
    Map<ServerRoutingInstance, Iterator<Server.ServerResponse>> responseMap = new HashMap<>();
    sendRequest(requestId, queryRoutingTable, responseMap, requestContext.isSampledRequest());
    long reduceStartTimeNs = System.nanoTime();
    BrokerResponseNative brokerResponse =
        _streamingReduceService.reduceOnStreamResponse(originalBrokerRequest, responseMap, timeoutMs, _brokerMetrics);
    brokerResponse.setBrokerReduceTimeMs(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - reduceStartTimeNs));
    return brokerResponse;
  }

  /**
   * Query pinot server for data table.
   */
  private void sendRequest(long requestId, Map<ServerInstance, List<ServerQueryRoutingContext>> queryRoutingTable,
      Map<ServerRoutingInstance, Iterator<Server.ServerResponse>> responseMap, boolean trace) {
    for (Map.Entry<ServerInstance, List<ServerQueryRoutingContext>> routingEntry : queryRoutingTable.entrySet()) {
      ServerInstance serverInstance = routingEntry.getKey();
      for (ServerQueryRoutingContext serverQueryRoutingContext : routingEntry.getValue()) {
        // TODO: support optional segments for GrpcQueryServer.
        String serverHost = serverInstance.getHostname();
        int port = serverInstance.getGrpcPort();
        // TODO: enable throttling on per host bases.
        Iterator<Server.ServerResponse> streamingResponse = _streamingQueryClient.submit(serverHost, port,
            new GrpcRequestBuilder().setRequestId(requestId).setBrokerId(_brokerId).setEnableTrace(trace)
                .setEnableStreaming(true).setBrokerRequest(serverQueryRoutingContext.getBrokerRequest())
                .setSegments(serverQueryRoutingContext.getRequiredSegmentsToQuery()).build());
        responseMap.put(serverInstance.toServerRoutingInstance(ServerInstance.RoutingType.GRPC), streamingResponse);
      }
    }
  }

  public static class PinotStreamingQueryClient {
    private final Map<String, GrpcQueryClient> _grpcQueryClientMap = new ConcurrentHashMap<>();
    private final GrpcConfig _config;

    public PinotStreamingQueryClient(GrpcConfig config) {
      _config = config;
    }

    public Iterator<Server.ServerResponse> submit(String host, int port, Server.ServerRequest serverRequest) {
      GrpcQueryClient client = getOrCreateGrpcQueryClient(host, port);
      return client.submit(serverRequest);
    }

    private GrpcQueryClient getOrCreateGrpcQueryClient(String host, int port) {
      String key = String.format("%s_%d", host, port);
      return _grpcQueryClientMap.computeIfAbsent(key, k -> new GrpcQueryClient(host, port, _config));
    }

    public void shutdown() {
      for (GrpcQueryClient client : _grpcQueryClientMap.values()) {
        client.close();
      }
    }
  }
}
