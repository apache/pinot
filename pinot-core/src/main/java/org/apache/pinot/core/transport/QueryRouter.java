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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.config.NettyConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.routing.ImplicitHybridTableRouteInfo;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.config.table.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// The `QueryRouter` class provides methods to route the query based on the routing table, and returns a
/// [AsyncQueryResponse] so that caller can handle the query response asynchronously.
///
/// It works on [ServerChannels] which maintains only a single connection between the broker and each server.
@ThreadSafe
public class QueryRouter {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryRouter.class);

  private final String _brokerId;
  private final ServerChannels _serverChannels;
  private final ServerChannels _serverChannelsTls;
  private final ServerRoutingStatsManager _serverRoutingStatsManager;

  private final BrokerMetrics _brokerMetrics = BrokerMetrics.get();
  private final ConcurrentHashMap<Long, AsyncQueryResponse> _asyncQueryResponseMap = new ConcurrentHashMap<>();

  /// Creates a query router with TLS config.
  ///
  /// @param brokerId broker id
  /// @param nettyConfig configurations for netty library
  /// @param tlsConfig TLS config
  public QueryRouter(String brokerId, @Nullable NettyConfig nettyConfig, @Nullable TlsConfig tlsConfig,
      ServerRoutingStatsManager serverRoutingStatsManager, ThreadAccountant threadAccountant) {
    _brokerId = brokerId;
    _serverChannels = new ServerChannels(this, nettyConfig, null, threadAccountant);
    _serverChannelsTls = tlsConfig != null ? new ServerChannels(this, nettyConfig, tlsConfig, threadAccountant) : null;
    _serverRoutingStatsManager = serverRoutingStatsManager;
  }

  public AsyncQueryResponse submitQuery(long requestId, String rawTableName,
      @Nullable BrokerRequest offlineBrokerRequest,
      @Nullable Map<ServerInstance, SegmentsToQuery> offlineRoutingTable,
      @Nullable BrokerRequest realtimeBrokerRequest,
      @Nullable Map<ServerInstance, SegmentsToQuery> realtimeRoutingTable, long timeoutMs) {
    TableRouteInfo tableRouteInfo = new ImplicitHybridTableRouteInfo(offlineBrokerRequest, realtimeBrokerRequest,
        offlineRoutingTable, realtimeRoutingTable);

    return submitQuery(requestId, rawTableName, tableRouteInfo, timeoutMs);
  }

  public AsyncQueryResponse submitQuery(long requestId, String rawTableName, TableRouteInfo route, long timeoutMs) {
    BrokerRequest offlineBrokerRequest = route.getOfflineBrokerRequest();
    BrokerRequest realtimeBrokerRequest = route.getRealtimeBrokerRequest();

    assert offlineBrokerRequest != null || realtimeBrokerRequest != null;

    // can prefer but not require TLS until all servers guaranteed to be on TLS
    boolean preferTls = _serverChannelsTls != null;

    // skip unavailable servers if the query option is set
    boolean skipUnavailableServers = isSkipUnavailableServers(offlineBrokerRequest, realtimeBrokerRequest);

    // Build map from server to request based on the routing table
    Map<ServerRoutingInstance, InstanceRequest> requestMap = route.getRequestMap(requestId, _brokerId, preferTls);

    // Create the asynchronous query response with the request map
    AsyncQueryResponse asyncQueryResponse =
        new AsyncQueryResponse(this, requestId, requestMap.keySet(), System.currentTimeMillis(), timeoutMs,
            _serverRoutingStatsManager, skipUnavailableServers);
    _asyncQueryResponseMap.put(requestId, asyncQueryResponse);
    for (Map.Entry<ServerRoutingInstance, InstanceRequest> entry : requestMap.entrySet()) {
      ServerRoutingInstance serverRoutingInstance = entry.getKey();
      ServerChannels serverChannels = serverRoutingInstance.isTlsEnabled() ? _serverChannelsTls : _serverChannels;
      try {
        serverChannels.sendRequest(rawTableName, asyncQueryResponse, serverRoutingInstance, entry.getValue(),
            timeoutMs);
        asyncQueryResponse.markRequestSubmitted(serverRoutingInstance);
      } catch (TimeoutException e) {
        if (ServerChannels.CHANNEL_LOCK_TIMEOUT_MSG.equals(e.getMessage())) {
          _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.REQUEST_CHANNEL_LOCK_TIMEOUT_EXCEPTIONS, 1);
        }
        markQueryFailed(requestId, serverRoutingInstance, asyncQueryResponse, e);
        break;
      } catch (Exception e) {
        _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.REQUEST_SEND_EXCEPTIONS, 1);
        if (skipUnavailableServers) {
          asyncQueryResponse.skipServerResponse(serverRoutingInstance);
        } else {
          markQueryFailed(requestId, serverRoutingInstance, asyncQueryResponse, e);
          break;
        }
      }
    }

    return asyncQueryResponse;
  }

  private boolean isSkipUnavailableServers(@Nullable BrokerRequest offlineBrokerRequest,
      @Nullable BrokerRequest realtimeBrokerRequest) {
    if (offlineBrokerRequest != null && QueryOptionsUtils.isSkipUnavailableServers(
        offlineBrokerRequest.getPinotQuery().getQueryOptions())) {
      return true;
    }
    return realtimeBrokerRequest != null && QueryOptionsUtils.isSkipUnavailableServers(
        realtimeBrokerRequest.getPinotQuery().getQueryOptions());
  }

  private void markQueryFailed(long requestId, ServerRoutingInstance serverRoutingInstance,
      AsyncQueryResponse asyncQueryResponse, Exception e) {
    LOGGER.error("Caught exception while sending request {} to server: {}, marking query failed", requestId,
        serverRoutingInstance, e);
    asyncQueryResponse.markQueryFailed(serverRoutingInstance, e);
  }

  public boolean hasChannel(ServerInstance serverInstance) {
    if (_serverChannelsTls != null) {
      return _serverChannelsTls.hasChannel(
          serverInstance.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY_TLS));
    } else {
      return _serverChannels.hasChannel(
          serverInstance.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY));
    }
  }

  /// Connects the OFFLINE channel to the given server, returns `true` if it is successfully connected.
  ///
  /// Unchanged in behaviour: this is the reachability probe the failure detector already used (it
  /// opened the OFFLINE channel), now expressed as a one-line delegation. It deliberately opens a
  /// single channel rather than every channel the server may need. Callers that want the server fully
  /// connected -- startup pre-connect, for instance -- should use [#connect(ServerInstance, TableType)]
  /// for each table type instead.
  public boolean connect(ServerInstance serverInstance) {
    return connect(serverInstance, TableType.OFFLINE);
  }

  /// Connects the channel used for the given table type, returns `true` if it is successfully
  /// connected.
  ///
  /// [ServerRoutingInstance] includes the table type in its `equals`/`hashCode`, so OFFLINE and
  /// REALTIME map to **separate** channels for the same physical server. Connecting only one of them
  /// leaves the other to be established lazily by the first query that needs it -- under the per-channel
  /// lock, on the query thread.
  public boolean connect(ServerInstance serverInstance, TableType tableType) {
    try {
      if (_serverChannelsTls != null) {
        _serverChannelsTls.connect(
            serverInstance.toServerRoutingInstance(tableType, ServerInstance.RoutingType.NETTY_TLS));
      } else {
        _serverChannels.connect(
            serverInstance.toServerRoutingInstance(tableType, ServerInstance.RoutingType.NETTY));
      }
      return true;
    } catch (Exception e) {
      LOGGER.debug("Failed to connect to server: {} for table type: {}", serverInstance, tableType, e);
      return false;
    }
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

  /// Marks a server as unavailable for every in-flight query. Called when a server's channel goes inactive
  /// ([DataTableHandler]) or a request write to it fails ([ServerChannels]). Queries submitted with
  /// `skipUnavailableServers=true` degrade to partial results for this genuine unavailability; others are failed.
  void markServerUnavailable(ServerRoutingInstance serverRoutingInstance, Exception exception) {
    for (AsyncQueryResponse asyncQueryResponse : _asyncQueryResponseMap.values()) {
      if (asyncQueryResponse.markServerUnavailable(serverRoutingInstance, exception)) {
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.SERVER_MARKED_DOWN_SKIPPED, 1);
      }
    }
  }

  /// Cancels every in-flight query. Unlike
  /// [#markServerUnavailable], this always fails the queries even under `skipUnavailableServers`: all
  /// channels are being closed so there is no partial data to return.
  void cancelQuery(ServerRoutingInstance serverRoutingInstance, Exception exception) {
    for (AsyncQueryResponse asyncQueryResponse : _asyncQueryResponseMap.values()) {
      asyncQueryResponse.cancelQuery(serverRoutingInstance, exception);
    }
  }

  void markQueryDone(long requestId) {
    _asyncQueryResponseMap.remove(requestId);
  }
}
