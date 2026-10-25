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
import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
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
import org.apache.pinot.spi.utils.CommonConstants.Broker;
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
  // Told about a server that stopped answering queries on one of its channels (see ServerChannels). The request
  // handler points it at the failure detector when pings are off; with or without it, the suspect channel is replaced
  // by the next query to it, so recovery does not depend on the failure detector being configured.
  private volatile Consumer<ServerRoutingInstance> _unansweredServerReporter = serverRoutingInstance -> {
  };
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
    this(brokerId, nettyConfig, tlsConfig, serverRoutingStatsManager, threadAccountant,
        Broker.FailureDetector.DEFAULT_UNANSWERED_QUERIES_THRESHOLD,
        Broker.FailureDetector.DEFAULT_UNANSWERED_QUERY_GRACE_MS);
  }

  /// @param unansweredQueriesThreshold see [Broker.FailureDetector#CONFIG_OF_UNANSWERED_QUERIES_THRESHOLD];
  ///                                   non-positive disables the check
  /// @param unansweredQueryGraceMs see [Broker.FailureDetector#CONFIG_OF_UNANSWERED_QUERY_GRACE_MS]
  public QueryRouter(String brokerId, @Nullable NettyConfig nettyConfig, @Nullable TlsConfig tlsConfig,
      ServerRoutingStatsManager serverRoutingStatsManager, ThreadAccountant threadAccountant,
      int unansweredQueriesThreshold, long unansweredQueryGraceMs) {
    _brokerId = brokerId;
    _serverChannels = new ServerChannels(this, nettyConfig, null, threadAccountant, unansweredQueriesThreshold,
        unansweredQueryGraceMs);
    _serverChannelsTls = tlsConfig != null ? new ServerChannels(this, nettyConfig, tlsConfig, threadAccountant,
        unansweredQueriesThreshold, unansweredQueryGraceMs) : null;
    _serverRoutingStatsManager = serverRoutingStatsManager;
  }

  /// Registers the consumer told about a server that stopped answering queries on one of its channels, typically the
  /// failure detector. Called from the transport's liveness thread, never from an event loop.
  public void setUnansweredServerReporter(Consumer<ServerRoutingInstance> unansweredServerReporter) {
    _unansweredServerReporter = unansweredServerReporter;
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

  /// The reachability probe the failure detector uses when pings are off. Re-establishes every channel this broker
  /// has queried the server over, one per table type: reconnects the ones that are down and replaces the ones marked
  /// suspect (see [ServerChannels#recordUnansweredQuery]) with a fresh connection, so that a half-open socket that
  /// still reports active is never vouched for. Returns `true` if every such channel is connected afterwards, which
  /// includes the vacuous case of a server with no such channel: callers that need "reachable" must check
  /// [#hasChannel] first, as `retryUnhealthyServer` does. It used to open the OFFLINE channel only, so on a
  /// realtime-only cluster a handshake to a freshly restarted process passed the probe while the channel that carried
  /// the queries was dead. Startup pre-connect uses [#preConnect] instead.
  public boolean connect(ServerInstance serverInstance) {
    try {
      ServerChannels serverChannels = channels();
      for (ServerRoutingInstance routingInstance : channelRoutingInstances(serverInstance)) {
        serverChannels.probe(routingInstance);
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
  /// Each channel gets a ping and a response of its own. With a shared one, a channel that fails at once would end the
  /// wait for all of them, and a channel still due to answer would look silent.
  ///
  /// The connection a ping went out on is closed when that ping stays silent, so the next attempt connects afresh. It
  /// is the one closed, rather than whichever connection the channel holds by then: a query may have reconnected in
  /// the meantime, and its new connection has nothing to do with the silent one. Closing also ends whatever was still
  /// in flight on the silent connection right away (see [DataTableHandler]), rather than leaving those queries to wait
  /// out their own timeouts. Pings record no routing stats, so they never skew adaptive server selection.
  public boolean ping(ServerInstance serverInstance, long timeoutMs) {
    List<Long> requestIds = new ArrayList<>(2);
    try {
      List<ServerRoutingInstance> routingInstances = channelRoutingInstances(serverInstance);
      if (routingInstances.isEmpty()) {
        return false;
      }
      ServerChannels serverChannels = channels();
      List<SentPing> sentPings = new ArrayList<>(routingInstances.size());
      boolean answered = true;
      for (ServerRoutingInstance routingInstance : routingInstances) {
        long requestId = _pingRequestIdGenerator.decrementAndGet();
        // The timeout starts at this ping's own send, so a slow send over one channel cannot cut short the wait for
        // the next.
        AsyncQueryResponse pingResponse =
            new AsyncQueryResponse(this, requestId, Set.of(routingInstance), System.currentTimeMillis(), timeoutMs,
                _noOpRoutingStatsManager, false);
        requestIds.add(requestId);
        _asyncQueryResponseMap.put(requestId, pingResponse);
        try {
          Channel channel = serverChannels.sendRequest(PING_TABLE_NAME, pingResponse, routingInstance,
              newPingRequest(requestId, _brokerId), timeoutMs);
          sentPings.add(new SentPing(pingResponse, channel));
        } catch (Exception e) {
          // No connection carried this ping, so there is none to close. Do not spend another connect timeout on the
          // remaining channels: the ping has failed already, and the channels not reached stay as they are.
          answered = false;
          break;
        }
      }
      for (SentPing sentPing : sentPings) {
        if (sentPing.response().getFinalResponses().values().iterator().next().getDataTable() == null) {
          answered = false;
          sentPing.channel().close();
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
    } finally {
      // getFinalResponses() deregisters each response it waits for; this covers the ones it never got to.
      for (long requestId : requestIds) {
        markQueryDone(requestId);
      }
    }
  }

  /// A ping that went out, and the connection it went out on.
  private record SentPing(AsyncQueryResponse response, Channel channel) {
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

  /// Records that a query written at `submitTimeMs` to `channel` (when known) timed out unanswered; the transport
  /// judges it after a grace period (see [ServerChannels#recordUnansweredQuery]).
  void recordServerTimeout(ServerRoutingInstance serverRoutingInstance, long submitTimeMs, @Nullable Channel channel) {
    serverChannelsFor(serverRoutingInstance).recordUnansweredQuery(serverRoutingInstance, submitTimeMs, channel);
  }

  /// Called by the transport once a server has left the configured number of consecutive queries unanswered on a
  /// channel; hands the server to the registered reporter.
  void reportUnansweredServer(ServerRoutingInstance serverRoutingInstance) {
    try {
      _unansweredServerReporter.accept(serverRoutingInstance);
    } catch (Exception e) {
      LOGGER.error("Caught exception while reporting unanswered server: {}", serverRoutingInstance, e);
    }
  }

  @VisibleForTesting
  int getUnansweredQueries(ServerRoutingInstance serverRoutingInstance) {
    return serverChannelsFor(serverRoutingInstance).getUnansweredQueries(serverRoutingInstance);
  }

  private ServerChannels serverChannelsFor(ServerRoutingInstance serverRoutingInstance) {
    return serverRoutingInstance.isTlsEnabled() && _serverChannelsTls != null ? _serverChannelsTls : _serverChannels;
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

  /// Closes and forgets every cached channel to the given server: both table types, over TLS and plaintext. Returns
  /// the number of channels evicted.
  ///
  /// Called through `BrokerRequestHandler#onServerEnabled` when the routing manager enables a server, which is how a
  /// restart shows up through Helix, and through `BrokerRequestHandler#onServerRemoved` when a server leaves the
  /// cluster. See [ServerChannels#evictChannel] for why the cached channel cannot be trusted at that point.
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
    serverChannelsFor(serverRoutingInstance).recordResponse(serverRoutingInstance);
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
    markServerUnavailable(serverRoutingInstance, null, exception, true);
  }

  /// Restricts a down event to the request channel; a broker eviction is not evidence against the server.
  void markServerUnavailable(ServerRoutingInstance serverRoutingInstance, @Nullable Channel channel,
      Exception exception, boolean reportServer) {
    for (AsyncQueryResponse response : _asyncQueryResponseMap.values()) {
      if (response.markServerUnavailable(serverRoutingInstance, channel, exception, reportServer)) {
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
