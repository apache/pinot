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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// The `QueryRouter` class provides methods to route the query based on the routing table, and returns a
/// [AsyncQueryResponse] so that caller can handle the query response asynchronously.
///
/// It works on [ServerChannels] which maintains only a single connection between the broker and each server.
@ThreadSafe
public class QueryRouter {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryRouter.class);

  /// The query a ping carries. A server that understands pings never runs it. An older server ignores the ping flag
  /// and runs it like any other query, and naming a table no server hosts keeps that cheap: the server answers with a
  /// table-missing error, and any answer at all is what a ping is after. It has to be a real query for that to work,
  /// because a server that cannot parse a request drops it without replying. The older server still queues it behind
  /// the queries it is running, though, which is why pings stay off until every server understands them.
  private static final String PING_TABLE_NAME = "pinotBrokerPing";
  private static final BrokerRequest PING_BROKER_REQUEST = CalciteSqlCompiler.compileToBrokerRequest(
      "SELECT COUNT(*) FROM " + TableNameBuilder.OFFLINE.tableNameWithType(PING_TABLE_NAME));

  private final String _brokerId;
  private final ServerChannels _serverChannels;
  private final ServerChannels _serverChannelsTls;
  private final ServerRoutingStatsManager _serverRoutingStatsManager;

  private final BrokerMetrics _brokerMetrics = BrokerMetrics.get();
  private final ConcurrentHashMap<Long, AsyncQueryResponse> _asyncQueryResponseMap = new ConcurrentHashMap<>();
  /// Ping request ids count down from -1, so they can never collide with query request ids, which are non-negative
  /// (see `BrokerRequestIdGenerator`).
  private final AtomicLong _pingRequestIdGenerator = new AtomicLong();
  /// Never initialized, which leaves stats collection disabled: pings must not skew adaptive server selection.
  private final ServerRoutingStatsManager _noOpRoutingStatsManager =
      new ServerRoutingStatsManager(new PinotConfiguration(), _brokerMetrics);

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

  /// Returns whether the broker has a query channel to the server, connected or not. There is one channel per table
  /// type, so this checks both. A server never queried over this path -- e.g. one that only serves multi-stage queries
  /// -- has none.
  public boolean hasChannel(ServerInstance serverInstance) {
    return !channelRoutingInstances(serverInstance).isEmpty();
  }

  /// Connects to the given server, returns `true` if the server is successfully connected.
  ///
  /// This is the reachability probe the failure detector uses when pings are off, opening the OFFLINE channel with a
  /// TCP connect only. Startup pre-connect uses [#preConnect] instead.
  public boolean connect(ServerInstance serverInstance) {
    try {
      if (_serverChannelsTls != null) {
        _serverChannelsTls.connect(
            serverInstance.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY_TLS));
      } else {
        _serverChannels.connect(
            serverInstance.toServerRoutingInstance(TableType.OFFLINE, ServerInstance.RoutingType.NETTY));
      }
      return true;
    } catch (Exception e) {
      LOGGER.debug("Failed to connect to server: {}", serverInstance, e);
      return false;
    }
  }

  /// Pings the server over every channel queries to it use, and returns whether all of them answered within
  /// `timeoutMs`. Returns `false` when there is no such channel, as there is nothing to ping over.
  ///
  /// A server that is alive answers straight away even when it is busy, because it replies from its network thread
  /// instead of queueing behind queries (see `InstanceRequestHandler`). So a channel that stays silent means the server
  /// cannot answer anything -- its node is gone, its network path is blackholed, or its JVM is frozen -- or that the
  /// channel itself is half-open, left over from a server that went away. Either way queries sent over it would time
  /// out, which is why every channel has to answer, not just one.
  ///
  /// Each channel that stays silent is closed so the next attempt over it connects afresh -- see
  /// [ServerChannels#closeChannel]. Pings record no routing stats, so they never skew adaptive server selection.
  public boolean ping(ServerInstance serverInstance, long timeoutMs) {
    try {
      List<ServerRoutingInstance> routingInstances = channelRoutingInstances(serverInstance);
      if (routingInstances.isEmpty()) {
        return false;
      }
      long requestId = _pingRequestIdGenerator.decrementAndGet();
      InstanceRequest pingRequest = newPingRequest(requestId, _brokerId);
      // Like a hybrid query, the pings share one request id and one response, which waits for every channel.
      AsyncQueryResponse pingResponse =
          new AsyncQueryResponse(this, requestId, new HashSet<>(routingInstances), System.currentTimeMillis(),
              timeoutMs, _noOpRoutingStatsManager, false);
      _asyncQueryResponseMap.put(requestId, pingResponse);
      ServerChannels serverChannels = channels();
      List<ServerRoutingInstance> pingedRoutingInstances = new ArrayList<>(routingInstances.size());
      for (ServerRoutingInstance routingInstance : routingInstances) {
        pingedRoutingInstances.add(routingInstance);
        try {
          serverChannels.sendRequest(PING_TABLE_NAME, pingResponse, routingInstance, pingRequest, timeoutMs);
        } catch (Exception e) {
          // Fails the whole ping; do not spend another connect timeout on the remaining channels.
          pingResponse.markQueryFailed(routingInstance, e);
          break;
        }
      }
      // Removes the ping from _asyncQueryResponseMap on every outcome.
      // A channel the ping never reached, because an earlier send failed, stays open. That failed send already makes
      // the ping unanswered.
      Map<ServerRoutingInstance, ServerResponse> responses = pingResponse.getFinalResponses();
      boolean answered = true;
      for (ServerRoutingInstance routingInstance : pingedRoutingInstances) {
        if (responses.get(routingInstance).getDataTable() == null) {
          answered = false;
          serverChannels.closeChannel(routingInstance);
        }
      }
      return answered;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (Exception e) {
      // toServerRoutingInstance() rejects a server that has not advertised the port, which happens mid-rollout.
      LOGGER.debug("Failed to ping server: {}", serverInstance, e);
      return false;
    }
  }

  @VisibleForTesting
  static InstanceRequest newPingRequest(long requestId, String brokerId) {
    InstanceRequest pingRequest = new InstanceRequest(requestId, PING_BROKER_REQUEST);
    pingRequest.setPing(true);
    pingRequest.setSearchSegments(List.of());
    pingRequest.setBrokerId(brokerId);
    return pingRequest;
  }

  /// The routing instances of the server's query channels, connected or not: at most one per table type.
  private List<ServerRoutingInstance> channelRoutingInstances(ServerInstance serverInstance) {
    ServerChannels serverChannels = channels();
    List<ServerRoutingInstance> routingInstances = new ArrayList<>(2);
    for (TableType tableType : TableType.values()) {
      ServerRoutingInstance routingInstance = routingInstance(serverInstance, tableType);
      if (serverChannels.hasChannel(routingInstance)) {
        routingInstances.add(routingInstance);
      }
    }
    return routingInstances;
  }

  @VisibleForTesting
  ServerChannels getServerChannels() {
    return _serverChannels;
  }

  /// Queries go over TLS whenever the broker has TLS configured, so the server's channels are all of one kind.
  private ServerChannels channels() {
    return _serverChannelsTls != null ? _serverChannelsTls : _serverChannels;
  }

  private ServerRoutingInstance routingInstance(ServerInstance serverInstance, TableType tableType) {
    return serverInstance.toServerRoutingInstance(tableType,
        _serverChannelsTls != null ? ServerInstance.RoutingType.NETTY_TLS : ServerInstance.RoutingType.NETTY);
  }

  /// Opens the channel used for the given table type ahead of query traffic, including the TLS
  /// handshake, bounded by `timeoutMs`. Returns `true` if it is connected.
  ///
  /// [ServerRoutingInstance] includes the table type in its `equals`/`hashCode`, so OFFLINE and REALTIME
  /// map to **separate** channels for the same physical server; the caller decides which of them this
  /// broker actually routes to. Whatever is left out is still established lazily by the first query that
  /// needs it.
  public boolean preConnect(ServerInstance serverInstance, TableType tableType, long timeoutMs) {
    try {
      if (_serverChannelsTls != null) {
        _serverChannelsTls.preConnect(
            serverInstance.toServerRoutingInstance(tableType, ServerInstance.RoutingType.NETTY_TLS), timeoutMs);
      } else {
        _serverChannels.preConnect(
            serverInstance.toServerRoutingInstance(tableType, ServerInstance.RoutingType.NETTY), timeoutMs);
      }
      return true;
    } catch (Exception e) {
      LOGGER.debug("Failed to pre-connect to server: {} for table type: {}", serverInstance, tableType, e);
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
