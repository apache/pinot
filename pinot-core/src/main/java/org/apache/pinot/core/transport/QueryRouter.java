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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.config.NettyConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.request.BrokerRequestIdUtils;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
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
  private final ServerRoutingStatsManager _serverRoutingStatsManager;

  /**
   * Creates an unsecured query router.
   * @param brokerId broker id
   * @param brokerMetrics broker metrics
   * @param serverRoutingStatsManager
   */
  public QueryRouter(String brokerId, BrokerMetrics brokerMetrics,
      ServerRoutingStatsManager serverRoutingStatsManager) {
    this(brokerId, brokerMetrics, null, null, serverRoutingStatsManager);
  }

  /**
   * Creates a query router with TLS config.
   *
   * @param brokerId broker id
   * @param brokerMetrics broker metrics
   * @param nettyConfig configurations for netty library
   * @param tlsConfig TLS config
   */
  public QueryRouter(String brokerId, BrokerMetrics brokerMetrics, @Nullable NettyConfig nettyConfig,
      @Nullable TlsConfig tlsConfig, ServerRoutingStatsManager serverRoutingStatsManager) {
    _brokerId = brokerId;
    _brokerMetrics = brokerMetrics;
    _serverChannels = new ServerChannels(this, brokerMetrics, nettyConfig, null);
    _serverChannelsTls = tlsConfig != null ? new ServerChannels(this, brokerMetrics, nettyConfig, tlsConfig) : null;
    _serverRoutingStatsManager = serverRoutingStatsManager;
  }

  public AsyncQueryResponse submitQuery(long requestId, String rawTableName,
      @Nullable Map<ServerInstance, List<ServerQueryRoutingContext>> queryRoutingTable, long timeoutMs) {
    assert queryRoutingTable != null && !queryRoutingTable.isEmpty();

    // can prefer but not require TLS until all servers guaranteed to be on TLS
    boolean preferTls = _serverChannelsTls != null;

    // skip unavailable servers if the query option is set
    boolean skipUnavailableServers = false;

    // Build map from server to request based on the routing table
    Map<ServerRoutingInstance, List<InstanceRequest>> requestMap = new HashMap<>();
    for (Map.Entry<ServerInstance, List<ServerQueryRoutingContext>> entry : queryRoutingTable.entrySet()) {
      for (ServerQueryRoutingContext serverQueryRoutingContext : entry.getValue()) {
        ServerRoutingInstance serverRoutingInstance = entry.getKey().toServerRoutingInstance(preferTls);

        long requestIdWithType = serverQueryRoutingContext.getTableType().equals(TableType.OFFLINE)
            ? BrokerRequestIdUtils.getOfflineRequestId(requestId)
            : BrokerRequestIdUtils.getRealtimeRequestId(requestId);

        InstanceRequest instanceRequest =
            getInstanceRequest(requestIdWithType, serverQueryRoutingContext.getBrokerRequest(),
                serverQueryRoutingContext.getSegmentsToQuery());
        if (!skipUnavailableServers && QueryOptionsUtils.isSkipUnavailableServers(
            serverQueryRoutingContext.getBrokerRequest().getPinotQuery().getQueryOptions())) {
          // Any single query having this option set will set it for all queries. This option should not be possible
          // to differ between server instance requests pertaining to the same request ID.
          skipUnavailableServers = true;
        }

        requestMap.computeIfAbsent(serverRoutingInstance, k -> new ArrayList<>()).add(instanceRequest);
      }
    }

    // Map the request using all but the last digit. This way, servers will return request IDs where the last digit
    // (injected above) will indicate table type, but both will be able to be divided by the TABLE_TYPE_OFFSET in
    // order to properly map back to this originating request
    AsyncQueryResponse asyncQueryResponse =
        new AsyncQueryResponse(this, requestId, requestMap, System.currentTimeMillis(), timeoutMs,
            _serverRoutingStatsManager);
    _asyncQueryResponseMap.put(requestId, asyncQueryResponse);
    for (Map.Entry<ServerRoutingInstance, List<InstanceRequest>> serverRequests : requestMap.entrySet()) {
      for (InstanceRequest request : serverRequests.getValue()) {

        ServerRoutingInstance serverRoutingInstance = serverRequests.getKey();
        ServerChannels serverChannels = serverRoutingInstance.isTlsEnabled() ? _serverChannelsTls : _serverChannels;
        try {
          serverChannels.sendRequest(rawTableName, asyncQueryResponse, serverRoutingInstance, request, timeoutMs);
          asyncQueryResponse.markRequestSubmitted(serverRoutingInstance, request);
        } catch (TimeoutException e) {
          if (ServerChannels.CHANNEL_LOCK_TIMEOUT_MSG.equals(e.getMessage())) {
            _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.REQUEST_CHANNEL_LOCK_TIMEOUT_EXCEPTIONS, 1);
          }
          markQueryFailed(requestId, serverRoutingInstance, asyncQueryResponse, e);
          break;
        } catch (Exception e) {
          _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.REQUEST_SEND_EXCEPTIONS, 1);
          if (skipUnavailableServers) {
            asyncQueryResponse.skipServerResponse();
          } else {
            markQueryFailed(requestId, serverRoutingInstance, asyncQueryResponse, e);
            break;
          }
        }
      }
    }

    return asyncQueryResponse;
  }

  private void markQueryFailed(long requestId, ServerRoutingInstance serverRoutingInstance,
      AsyncQueryResponse asyncQueryResponse, Exception e) {
    LOGGER.error("Caught exception while sending request {} to server: {}, marking query failed", requestId,
        serverRoutingInstance, e);
    asyncQueryResponse.markQueryFailed(serverRoutingInstance, e);
  }

  /**
   * Connects to the given server, returns {@code true} if the server is successfully connected.
   */
  public boolean connect(ServerInstance serverInstance) {
    try {
      if (_serverChannelsTls != null) {
        _serverChannelsTls.connect(serverInstance.toServerRoutingInstance(true));
      } else {
        _serverChannels.connect(serverInstance.toServerRoutingInstance(false));
      }
      return true;
    } catch (Exception e) {
      LOGGER.debug("Failed to connect to server: {}", serverInstance, e);
      return false;
    }
  }

  public void shutDown() {
    _serverChannels.shutDown();
  }

  void receiveDataTable(ServerRoutingInstance serverRoutingInstance, DataTable dataTable, int responseSize,
      int deserializationTimeMs) {
    long requestId = Long.parseLong(dataTable.getMetadata().get(MetadataKey.REQUEST_ID.getName()));

    // TODO(egalpin): remove use of canonical request ID in later major release
    AsyncQueryResponse asyncQueryResponse =
        _asyncQueryResponseMap.get(BrokerRequestIdUtils.getCanonicalRequestId(requestId));

    // Query future might be null if the query is already done (maybe due to failure)
    if (asyncQueryResponse != null) {
      asyncQueryResponse.receiveDataTable(serverRoutingInstance, dataTable, responseSize, deserializationTimeMs);
    }
  }

  void markServerDown(ServerRoutingInstance serverRoutingInstance, Exception exception) {
    for (AsyncQueryResponse asyncQueryResponse : _asyncQueryResponseMap.values()) {
      asyncQueryResponse.markServerDown(serverRoutingInstance, exception);
    }
  }

  void markQueryDone(long requestId) {
    _asyncQueryResponseMap.remove(requestId);
  }

  private InstanceRequest getInstanceRequest(long requestId, BrokerRequest brokerRequest,
      Pair<List<String>, List<String>> segments) {
    InstanceRequest instanceRequest = new InstanceRequest();
    instanceRequest.setRequestId(requestId);
    instanceRequest.setQuery(brokerRequest);
    Map<String, String> queryOptions = brokerRequest.getPinotQuery().getQueryOptions();
    if (queryOptions != null) {
      instanceRequest.setEnableTrace(Boolean.parseBoolean(queryOptions.get(CommonConstants.Broker.Request.TRACE)));
    }
    instanceRequest.setSearchSegments(segments.getLeft());
    instanceRequest.setBrokerId(_brokerId);
    if (CollectionUtils.isNotEmpty(segments.getRight())) {
      // Don't set this field, i.e. leave it as null, if there is no optional segment at all, to be more backward
      // compatible, as there are places like in multi-stage query engine where this field is not set today when
      // creating the InstanceRequest.
      instanceRequest.setOptionalSegments(segments.getRight());
    }
    return instanceRequest;
  }
}
