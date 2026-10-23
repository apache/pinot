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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
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
import org.apache.pinot.core.routing.ImplicitHybridTableRouteInfo;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
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
  // Per channel: queries that timed out without the server answering anything since. Reset by any response from
  // the server, so a slow server that still answers some queries never reaches the threshold; a dead channel does.
  private final ConcurrentHashMap<ServerRoutingInstance, AtomicInteger> _consecutiveTimeouts =
      new ConcurrentHashMap<>();
  private final int _consecutiveTimeoutsThreshold;

  /// Creates a query router with TLS config.
  ///
  /// @param brokerId broker id
  /// @param nettyConfig configurations for netty library
  /// @param tlsConfig TLS config
  public QueryRouter(String brokerId, @Nullable NettyConfig nettyConfig, @Nullable TlsConfig tlsConfig,
      ServerRoutingStatsManager serverRoutingStatsManager, ThreadAccountant threadAccountant) {
    this(brokerId, nettyConfig, tlsConfig, serverRoutingStatsManager, threadAccountant,
        Broker.FailureDetector.DEFAULT_CONSECUTIVE_TIMEOUTS_THRESHOLD);
  }

  /// @param consecutiveTimeoutsThreshold see [Broker.FailureDetector#CONFIG_OF_CONSECUTIVE_TIMEOUTS_THRESHOLD];
  ///                                     non-positive disables timeout based failure reporting
  public QueryRouter(String brokerId, @Nullable NettyConfig nettyConfig, @Nullable TlsConfig tlsConfig,
      ServerRoutingStatsManager serverRoutingStatsManager, ThreadAccountant threadAccountant,
      int consecutiveTimeoutsThreshold) {
    _brokerId = brokerId;
    _serverChannels = new ServerChannels(this, nettyConfig, null, threadAccountant);
    _serverChannelsTls = tlsConfig != null ? new ServerChannels(this, nettyConfig, tlsConfig, threadAccountant) : null;
    _serverRoutingStatsManager = serverRoutingStatsManager;
    _consecutiveTimeoutsThreshold = consecutiveTimeoutsThreshold;
    LOGGER.info("Reporting a server unhealthy after {} consecutive unanswered query timeouts ({})",
        consecutiveTimeoutsThreshold, consecutiveTimeoutsThreshold > 0 ? "enabled" : "disabled");
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

  /// Whether this broker has sent a single-stage query to the given server over any of its channels: OFFLINE or
  /// REALTIME, TLS or plaintext. This is what decides whether the single-stage transport has an opinion on the
  /// server's health at all (see [ServerChannels#hasChannel]). It used to look at the OFFLINE channel only, so on a
  /// realtime-only cluster the failure detector's probe always answered `UNKNOWN` and the server was readmitted
  /// onto whatever channel was cached.
  public boolean hasChannel(ServerInstance serverInstance) {
    for (Pair<ServerChannels, ServerRoutingInstance> channel : getChannels(serverInstance)) {
      if (channel.getLeft().hasChannel(channel.getRight())) {
        return true;
      }
    }
    return false;
  }

  /// The failure detector's retry probe. Re-establishes every channel this broker has queried the server over:
  /// reconnects the ones that are down and replaces the ones marked suspect by [#recordServerTimeout] with a fresh
  /// connection, so that a half-open socket that still reports active is never vouched for. Returns `true` if every
  /// such channel is connected afterwards. Channels that only startup pre-connect opened are left alone, as before.
  public boolean connect(ServerInstance serverInstance) {
    try {
      for (Pair<ServerChannels, ServerRoutingInstance> channel : getChannels(serverInstance)) {
        channel.getLeft().probe(channel.getRight());
      }
      return true;
    } catch (Exception e) {
      LOGGER.debug("Failed to connect to server: {}", serverInstance, e);
      return false;
    }
  }

  /// Every channel this broker may hold to the given server, as (channel set, routing instance) pairs: one per table
  /// type, over TLS when broker-to-server TLS is configured and the server has a TLS port, and over plaintext.
  private List<Pair<ServerChannels, ServerRoutingInstance>> getChannels(ServerInstance serverInstance) {
    List<Pair<ServerChannels, ServerRoutingInstance>> channels = new ArrayList<>(4);
    for (TableType tableType : TableType.values()) {
      if (serverInstance.getPort() > 0) {
        channels.add(Pair.of(_serverChannels,
            serverInstance.toServerRoutingInstance(tableType, ServerInstance.RoutingType.NETTY)));
      }
      if (_serverChannelsTls != null && serverInstance.getNettyTlsPort() > 0) {
        channels.add(Pair.of(_serverChannelsTls,
            serverInstance.toServerRoutingInstance(tableType, ServerInstance.RoutingType.NETTY_TLS)));
      }
    }
    return channels;
  }

  /// Records that a query timed out without the server answering it. Returns `true` when this is the configured
  /// number of consecutive unanswered timeouts for the server, in which case its channel is marked suspect so the
  /// retry probe replaces it, and the caller reports the server to the failure detector. The count is reset by any
  /// response from the server ([#receiveDataTable]) and once the threshold is reached.
  boolean recordServerTimeout(ServerRoutingInstance serverRoutingInstance) {
    if (_consecutiveTimeoutsThreshold <= 0) {
      return false;
    }
    AtomicInteger consecutiveTimeouts =
        _consecutiveTimeouts.computeIfAbsent(serverRoutingInstance, k -> new AtomicInteger());
    if (consecutiveTimeouts.incrementAndGet() < _consecutiveTimeoutsThreshold) {
      return false;
    }
    consecutiveTimeouts.set(0);
    LOGGER.warn("Server: {} did not answer {} consecutive queries before they timed out, reporting it unhealthy and "
        + "marking its channel suspect", serverRoutingInstance, _consecutiveTimeoutsThreshold);
    getServerChannels(serverRoutingInstance).markSuspect(serverRoutingInstance);
    return true;
  }

  @VisibleForTesting
  int getConsecutiveTimeouts(ServerRoutingInstance serverRoutingInstance) {
    AtomicInteger consecutiveTimeouts = _consecutiveTimeouts.get(serverRoutingInstance);
    return consecutiveTimeouts != null ? consecutiveTimeouts.get() : 0;
  }

  private ServerChannels getServerChannels(ServerRoutingInstance serverRoutingInstance) {
    return serverRoutingInstance.isTlsEnabled() && _serverChannelsTls != null ? _serverChannelsTls : _serverChannels;
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

  /// Closes and forgets every cached channel to the given server: both table types, over TLS and plaintext. Returns
  /// the number of channels evicted.
  ///
  /// Called through `BrokerRequestHandler#onServerReenabled` when the routing manager re-enables a server it had
  /// seen leave the enabled set, which is how a restart shows up through Helix. See
  /// [ServerChannels#evictChannel] for why the cached channel cannot be trusted at that point.
  public int evictChannels(ServerInstance serverInstance) {
    int numEvicted = 0;
    for (TableType tableType : TableType.values()) {
      if (serverInstance.getPort() > 0 && _serverChannels.evictChannel(
          serverInstance.toServerRoutingInstance(tableType, ServerInstance.RoutingType.NETTY))) {
        numEvicted++;
      }
      if (_serverChannelsTls != null && serverInstance.getNettyTlsPort() > 0 && _serverChannelsTls.evictChannel(
          serverInstance.toServerRoutingInstance(tableType, ServerInstance.RoutingType.NETTY_TLS))) {
        numEvicted++;
      }
    }
    return numEvicted;
  }

  public void shutDown() {
    _serverChannels.shutDown();
    if (_serverChannelsTls != null) {
      _serverChannelsTls.shutDown();
    }
  }

  void receiveDataTable(ServerRoutingInstance serverRoutingInstance, DataTable dataTable, int responseSize,
      int deserializationTimeMs) {
    // Any response proves the server and its channel alive, whether or not the query it answers is still waiting
    AtomicInteger consecutiveTimeouts = _consecutiveTimeouts.get(serverRoutingInstance);
    if (consecutiveTimeouts != null) {
      consecutiveTimeouts.set(0);
    }
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
