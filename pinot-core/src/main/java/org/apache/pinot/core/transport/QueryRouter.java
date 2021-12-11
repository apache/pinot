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
package org.apache.pinot.core.transport;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.common.utils.DataTable.MetadataKey;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code QueryRouter} class provides methods to route the query based on the routing table, and returns a
 * {@link AsyncQueryResponse} so that caller can handle the query response asynchronously.
 * <p>It works on {@link ServerChannels} which maintains only a single connection between the broker and each server.
 */
@ThreadSafe
public class QueryRouter {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryRouter.class);

  private final String _brokerId;
  private final BrokerMetrics _brokerMetrics;
  private final ServerChannels _serverChannels;
  private final ServerChannels _serverChannelsTls;
  private final ConcurrentHashMap<Long, AsyncQueryResponse> _asyncQueryResponseMap = new ConcurrentHashMap<>();

  /**
   * Create an unsecured query router
   *
   * @param brokerId broker id
   * @param brokerMetrics broker metrics
   */
  public QueryRouter(String brokerId, BrokerMetrics brokerMetrics) {
    _brokerId = brokerId;
    _brokerMetrics = brokerMetrics;
    _serverChannels = new ServerChannels(this, brokerMetrics);
    _serverChannelsTls = null;
  }

  /**
   * Create a query router with TLS config
   *
   * @param brokerId broker id
   * @param brokerMetrics broker metrics
   * @param tlsConfig TLS config
   */
  public QueryRouter(String brokerId, BrokerMetrics brokerMetrics, TlsConfig tlsConfig) {
    _brokerId = brokerId;
    _brokerMetrics = brokerMetrics;
    _serverChannels = new ServerChannels(this, brokerMetrics);
    _serverChannelsTls =
        Optional.ofNullable(tlsConfig).map(conf -> new ServerChannels(this, brokerMetrics, conf)).orElse(null);
  }

  public AsyncQueryResponse submitQuery(long requestId, String rawTableName,
      @Nullable BrokerRequest offlineBrokerRequest, @Nullable Map<ServerInstance, List<String>> offlineRoutingTable,
      @Nullable BrokerRequest realtimeBrokerRequest, @Nullable Map<ServerInstance, List<String>> realtimeRoutingTable,
      long timeoutMs) {
    assert offlineBrokerRequest != null || realtimeBrokerRequest != null;

    // can prefer but not require TLS until all servers guaranteed to be on TLS
    boolean preferTls = _serverChannelsTls != null;

    // Build map from server to request based on the routing table
    Map<ServerRoutingInstance, InstanceRequest> requestMap = new HashMap<>();
    if (offlineBrokerRequest != null) {
      assert offlineRoutingTable != null;
      for (Map.Entry<ServerInstance, List<String>> entry : offlineRoutingTable.entrySet()) {
        ServerRoutingInstance serverRoutingInstance =
            entry.getKey().toServerRoutingInstance(TableType.OFFLINE, preferTls);
        InstanceRequest instanceRequest = getInstanceRequest(requestId, offlineBrokerRequest, entry.getValue());
        requestMap.put(serverRoutingInstance, instanceRequest);
      }
    }
    if (realtimeBrokerRequest != null) {
      assert realtimeRoutingTable != null;
      for (Map.Entry<ServerInstance, List<String>> entry : realtimeRoutingTable.entrySet()) {
        ServerRoutingInstance serverRoutingInstance =
            entry.getKey().toServerRoutingInstance(TableType.REALTIME, preferTls);
        InstanceRequest instanceRequest = getInstanceRequest(requestId, realtimeBrokerRequest, entry.getValue());
        requestMap.put(serverRoutingInstance, instanceRequest);
      }
    }

    // Create the asynchronous query response with the request map
    AsyncQueryResponse asyncQueryResponse =
        new AsyncQueryResponse(this, requestId, requestMap.keySet(), System.currentTimeMillis(), timeoutMs);
    _asyncQueryResponseMap.put(requestId, asyncQueryResponse);
    for (Map.Entry<ServerRoutingInstance, InstanceRequest> entry : requestMap.entrySet()) {
      ServerRoutingInstance serverRoutingInstance = entry.getKey();
      ServerChannels serverChannels = serverRoutingInstance.isTlsEnabled() ? _serverChannelsTls : _serverChannels;
      try {
        serverChannels.sendRequest(rawTableName, asyncQueryResponse, serverRoutingInstance, entry.getValue());
        asyncQueryResponse.markRequestSubmitted(serverRoutingInstance);
      } catch (Exception e) {
        LOGGER.error("Caught exception while sending request {} to server: {}, marking query failed", requestId,
            serverRoutingInstance, e);
        _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.REQUEST_SEND_EXCEPTIONS, 1);
        asyncQueryResponse.setBrokerRequestSendException(e);
        asyncQueryResponse.markQueryFailed();
        break;
      }
    }

    return asyncQueryResponse;
  }

  public void shutDown() {
    _serverChannels.shutDown();
  }

  void receiveDataTable(ServerRoutingInstance serverRoutingInstance, DataTable dataTable, int responseSize,
      int deserializationTimeMs) {
    long requestId = Long.parseLong(dataTable.getMetadata().get(MetadataKey.REQUEST_ID.getName()));
    AsyncQueryResponse asyncQueryResponse = _asyncQueryResponseMap.get(requestId);

    // Query future might be null if the query is already done (maybe due to failure)
    if (asyncQueryResponse != null) {
      asyncQueryResponse.receiveDataTable(serverRoutingInstance, dataTable, responseSize, deserializationTimeMs);
    }
  }

  void markServerDown(ServerRoutingInstance serverRoutingInstance) {
    for (AsyncQueryResponse asyncQueryResponse : _asyncQueryResponseMap.values()) {
      asyncQueryResponse.markServerDown(serverRoutingInstance);
    }
  }

  void markQueryDone(long requestId) {
    _asyncQueryResponseMap.remove(requestId);
  }

  private InstanceRequest getInstanceRequest(long requestId, BrokerRequest brokerRequest, List<String> segments) {
    InstanceRequest instanceRequest = new InstanceRequest();
    instanceRequest.setRequestId(requestId);
    instanceRequest.setQuery(brokerRequest);
    PinotQuery pinotQuery = brokerRequest.getPinotQuery();
    Map<String, String> queryOptions =
        pinotQuery != null ? pinotQuery.getQueryOptions() : brokerRequest.getQueryOptions();
    if (queryOptions != null) {
      instanceRequest.setEnableTrace(Boolean.parseBoolean(queryOptions.get(CommonConstants.Broker.Request.TRACE)));
    }
    instanceRequest.setSearchSegments(segments);
    instanceRequest.setBrokerId(_brokerId);
    return instanceRequest;
  }
}
