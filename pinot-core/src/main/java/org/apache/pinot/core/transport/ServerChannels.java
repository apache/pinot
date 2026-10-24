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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.config.NettyConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerTimer;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.util.OsCheck;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// The `ServerChannels` class manages the channels between broker to all the connected servers.
///
/// There is only one channel between the broker and each connected server (we count OFFLINE and REALTIME as different
/// servers)
///
/// Liveness of a cached channel is not left to the kernel's retransmission timeout, which takes many minutes to
/// notice a half-open socket while `isActive()` still reports `true`:
///   - a request write that the kernel does not accept within [NettyConfig#WRITE_TIMEOUT_MS] closes the channel
///     ([RequestWriteTimeoutHandler]);
///   - on the Epoll transport, data unacknowledged for [NettyConfig#TCP_USER_TIMEOUT_MS] makes the kernel close it;
///   - a channel whose outbound queue is above [NettyConfig#WRITE_BUFFER_HIGH_WATER_MARK_BYTES] rejects requests
///     instead of queueing them in direct memory ([#sendRequest]);
///   - the routing manager evicts the cached channel of a server it re-enables after a restart ([#evictChannel]);
///   - a channel on which the server left [Broker.FailureDetector#CONFIG_OF_UNANSWERED_QUERIES_THRESHOLD]
///     consecutive queries unanswered, judged a grace period after each timeout, is marked suspect: the next query
///     to it, or the failure detector's retry probe, replaces it with a fresh connection, and the server is reported
///     ([#recordUnansweredQuery]).
@ThreadSafe
public class ServerChannels {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerChannels.class);
  public static final String CHANNEL_LOCK_TIMEOUT_MSG = "Timeout while acquiring channel lock";
  private static final long TRY_CONNECT_CHANNEL_LOCK_TIMEOUT_MS = 5_000L;
  // A sender that raced with an eviction retries on a fresh entry; one retry is all it takes because an eviction is
  // a rare, per-server event, but bound it anyway.
  private static final int MAX_SEND_ATTEMPTS = 3;

  // TSerializer currently is not thread safe, must be put into a ThreadLocal.
  private static final ThreadLocal<TSerializer> THREAD_LOCAL_T_SERIALIZER = ThreadLocal.withInitial(() -> {
    try {
      return new TSerializer(new TCompactProtocol.Factory());
    } catch (TTransportException e) {
      throw new RuntimeException("Failed to initialize Thrift Serializer", e);
    }
  });

  private final QueryRouter _queryRouter;
  private final TlsConfig _tlsConfig;
  private final EventLoopGroup _eventLoopGroup;
  private final Class<? extends SocketChannel> _channelClass;
  private final ThreadAccountant _threadAccountant;
  private final PooledByteBufAllocator _bufAllocatorWithLimits;
  private final long _writeTimeoutMs;
  private final int _tcpUserTimeoutMs;
  private final WriteBufferWaterMark _writeBufferWaterMark;
  private final int _unansweredQueriesThreshold;
  private final long _unansweredQueryGraceMs;
  // Bound on the judgments queued per channel at a time. Under a dead server at high QPS every timed-out query would
  // otherwise queue a task for the whole grace period; the first few judgments already cross the threshold, so the
  // rest add nothing but queue depth. Any value at or above the threshold keeps the behaviour.
  private final int _maxPendingJudgmentsPerChannel;
  // Judges unanswered queries once their grace period is over, and reports the server from there; null when the
  // check is disabled. Its own thread, because reporting ends in a routing update that must not run on an event loop.
  @Nullable
  private final ScheduledExecutorService _livenessExecutor;

  private final BrokerMetrics _brokerMetrics = BrokerMetrics.get();
  private final ConcurrentHashMap<ServerRoutingInstance, ServerChannel> _serverToChannelMap = new ConcurrentHashMap<>();

  /// Create a server channel with TLS config
  ///
  /// @param queryRouter query router
  /// @param tlsConfig TLS/SSL config
  public ServerChannels(QueryRouter queryRouter, @Nullable NettyConfig nettyConfig, @Nullable TlsConfig tlsConfig,
      ThreadAccountant threadAccountant) {
    this(queryRouter, nettyConfig, tlsConfig, threadAccountant,
        Broker.FailureDetector.DEFAULT_UNANSWERED_QUERIES_THRESHOLD,
        Broker.FailureDetector.DEFAULT_UNANSWERED_QUERY_GRACE_MS);
  }

  /// @param unansweredQueriesThreshold see [Broker.FailureDetector#CONFIG_OF_UNANSWERED_QUERIES_THRESHOLD];
  ///                                   non-positive disables the check
  /// @param unansweredQueryGraceMs see [Broker.FailureDetector#CONFIG_OF_UNANSWERED_QUERY_GRACE_MS]
  public ServerChannels(QueryRouter queryRouter, @Nullable NettyConfig nettyConfig, @Nullable TlsConfig tlsConfig,
      ThreadAccountant threadAccountant, int unansweredQueriesThreshold, long unansweredQueryGraceMs) {
    if (nettyConfig == null) {
      nettyConfig = new NettyConfig();
    }
    boolean enableNativeTransports = nettyConfig.isNativeTransportsEnabled();
    OsCheck.OSType operatingSystemType = OsCheck.getOperatingSystemType();
    if (enableNativeTransports
        && operatingSystemType == OsCheck.OSType.Linux
        && Epoll.isAvailable()) {
      _eventLoopGroup = new EpollEventLoopGroup();
      _channelClass = EpollSocketChannel.class;
      LOGGER.info("Using Epoll event loop");
    } else if (enableNativeTransports
        && operatingSystemType == OsCheck.OSType.MacOS
        && KQueue.isAvailable()) {
      _eventLoopGroup = new KQueueEventLoopGroup();
      _channelClass = KQueueSocketChannel.class;
      LOGGER.info("Using KQueue event loop");
    } else {
      _eventLoopGroup = new NioEventLoopGroup();
      _channelClass = NioSocketChannel.class;
      StringBuilder log = new StringBuilder("Using NIO event loop");
      if (operatingSystemType == OsCheck.OSType.Linux
          && enableNativeTransports) {
        log.append(", as Epoll is not available: ").append(Epoll.unavailabilityCause());
      } else if (operatingSystemType == OsCheck.OSType.MacOS
          && enableNativeTransports) {
        log.append(", as KQueue is not available: ").append(KQueue.unavailabilityCause());
      }
      LOGGER.info(log.toString());
    }

    _queryRouter = queryRouter;
    _tlsConfig = tlsConfig;
    _threadAccountant = threadAccountant;
    _writeTimeoutMs = nettyConfig.getWriteTimeoutMs();
    _tcpUserTimeoutMs = nettyConfig.getTcpUserTimeoutMs();
    _writeBufferWaterMark = new WriteBufferWaterMark(nettyConfig.getWriteBufferLowWaterMarkBytes(),
        nettyConfig.getWriteBufferHighWaterMarkBytes());
    _unansweredQueriesThreshold = unansweredQueriesThreshold;
    _unansweredQueryGraceMs = Math.max(0L, unansweredQueryGraceMs);
    _maxPendingJudgmentsPerChannel = Math.max(16, 2 * unansweredQueriesThreshold);
    _livenessExecutor = unansweredQueriesThreshold > 0 ? Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("server-channel-liveness-%d").setDaemon(true).build()) : null;
    LOGGER.info("Server channel liveness settings: write timeout: {}ms, TCP user timeout: {}ms ({}), write buffer "
            + "water mark: {}, unanswered queries threshold: {} ({}), unanswered query grace: {}ms", _writeTimeoutMs,
        _tcpUserTimeoutMs, isTcpUserTimeoutApplied() ? "applied" : "not applied, requires the Epoll transport",
        _writeBufferWaterMark, unansweredQueriesThreshold, _livenessExecutor != null ? "enabled" : "disabled",
        _unansweredQueryGraceMs);

    _bufAllocatorWithLimits = PooledByteBufAllocatorWithLimits.getSharedBufferAllocatorWithLimits();
    PooledByteBufAllocatorMetric metric = _bufAllocatorWithLimits.metric();
    _brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.NETTY_POOLED_USED_DIRECT_MEMORY, metric::usedDirectMemory);
    _brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.NETTY_POOLED_USED_HEAP_MEMORY, metric::usedHeapMemory);
    _brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.NETTY_POOLED_ARENAS_DIRECT, metric::numDirectArenas);
    _brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.NETTY_POOLED_ARENAS_HEAP, metric::numHeapArenas);
    _brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.NETTY_POOLED_CACHE_SIZE_SMALL, metric::smallCacheSize);
    _brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.NETTY_POOLED_CACHE_SIZE_NORMAL, metric::normalCacheSize);
    _brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.NETTY_POOLED_THREADLOCALCACHE, metric::numThreadLocalCaches);
    _brokerMetrics.setOrUpdateGlobalGauge(BrokerGauge.NETTY_POOLED_CHUNK_SIZE, metric::chunkSize);
  }

  /// Writes the request to the server's channel and returns that channel.
  public Channel sendRequest(String rawTableName, AsyncQueryResponse asyncQueryResponse,
      ServerRoutingInstance serverRoutingInstance, InstanceRequest instanceRequest, long timeoutMs)
      throws Exception {
    byte[] requestBytes = THREAD_LOCAL_T_SERIALIZER.get().serialize(instanceRequest);
    for (int attempt = 1; attempt <= MAX_SEND_ATTEMPTS; attempt++) {
      ServerChannel serverChannel = _serverToChannelMap.computeIfAbsent(serverRoutingInstance, ServerChannel::new);
      // This server is now query-carrying, whoever opened the channel. See [#hasChannel].
      serverChannel._openedByQuery = true;
      Channel channel =
          serverChannel.sendRequest(rawTableName, asyncQueryResponse, serverRoutingInstance, requestBytes, timeoutMs);
      if (channel != null) {
        return channel;
      }
      // The entry was evicted between the lookup and the send (see [#evictChannel]); it is already out of the map, so
      // the next lookup creates a fresh one.
      LOGGER.info("Channel to server: {} was evicted while sending a request, retrying on a fresh channel (attempt {})",
          serverRoutingInstance, attempt);
    }
    throw new IllegalStateException(
        "Channel to server: " + serverRoutingInstance + " kept being evicted while sending a request");
  }

  /// Closes and forgets the cached channel to the given routing instance, if there is one. Returns `true` if a
  /// channel was evicted.
  ///
  /// Called when the routing manager re-enables a server it had seen leave the enabled set, which is what a
  /// restart looks like through Helix. The cached channel then belongs to the previous process: if that process
  /// went away without a FIN (node flap, OOM kill, a network path that stopped forwarding), the socket is half-open,
  /// `isActive()` keeps reporting `true` until the kernel gives up on retransmits, and every query routed to the
  /// readmitted server would be written into it and lost. Evicting here makes the next query reconnect, to the new
  /// process at whatever address its hostname now resolves to.
  ///
  /// Queries in flight on the evicted channel fail fast through `channelInactive` rather than waiting out their
  /// timeout: the process they were sent to is gone. The entry is flagged before it is closed so that a sender that
  /// already holds it does not reconnect it after it has left the map, which would leak a channel nobody routes to
  /// (see [ServerChannel#sendRequest]).
  public boolean evictChannel(ServerRoutingInstance serverRoutingInstance) {
    ServerChannel serverChannel = _serverToChannelMap.remove(serverRoutingInstance);
    if (serverChannel == null) {
      return false;
    }
    LOGGER.info("Evicting cached channel to server: {}", serverRoutingInstance);
    _brokerMetrics.addMeteredGlobalValue(BrokerMeter.NETTY_CONNECTION_CHANNEL_EVICTIONS, 1);
    serverChannel.evict();
    return true;
  }

  /// Whether [NettyConfig#TCP_USER_TIMEOUT_MS] is set on the channels: it is a Linux socket option that Netty only
  /// exposes on the Epoll transport.
  @VisibleForTesting
  boolean isTcpUserTimeoutApplied() {
    return _tcpUserTimeoutMs > 0 && _channelClass == EpollSocketChannel.class;
  }

  /// Whether this broker has sent, or tried to send, a single-stage query to the given server.
  ///
  /// Deliberately **not** "is there an entry in the channel map". Startup pre-connect opens channels
  /// before any query, so a plain `containsKey` would flip true for every reachable server merely because
  /// pre-connect ran. `SingleConnectionBrokerRequestHandler#retryUnhealthyServer` uses this to decide
  /// whether the single-stage transport has any opinion on a server's health at all -- it reports
  /// `UNKNOWN` when it does not -- so on a cluster serving only multi-stage queries the answer has to stay
  /// `false`, exactly as it was before pre-connect existed. Otherwise a server that multi-stage can reach
  /// over gRPC but single-stage cannot reach over Netty would be voted `UNHEALTHY` and dropped from
  /// routing by a transport that never carries its queries.
  public boolean hasChannel(ServerRoutingInstance serverRoutingInstance) {
    ServerChannel serverChannel = _serverToChannelMap.get(serverRoutingInstance);
    return serverChannel != null && serverChannel._openedByQuery;
  }

  public void connect(ServerRoutingInstance serverRoutingInstance)
      throws InterruptedException, TimeoutException {
    _serverToChannelMap.computeIfAbsent(serverRoutingInstance, ServerChannel::new).connect();
  }

  /// Marks the cached channel to the given routing instance suspect: the server stopped answering queries on it
  /// while the socket still looked active. The next query to it, or the next [#probe], replaces it with a fresh
  /// connection instead of trusting `isActive()`.
  public void markSuspect(ServerRoutingInstance serverRoutingInstance) {
    ServerChannel serverChannel = _serverToChannelMap.get(serverRoutingInstance);
    if (serverChannel != null) {
      serverChannel._suspect = true;
    }
  }

  /// The failure detector's retry probe for one channel. Does nothing and returns `false` when this broker has not
  /// queried the server over this channel (see [#hasChannel]); otherwise reconnects the channel if it is down,
  /// replaces it with a fresh connection if it was marked suspect, and returns `true`, throwing if the connection
  /// cannot be established.
  public boolean probe(ServerRoutingInstance serverRoutingInstance)
      throws InterruptedException, TimeoutException {
    ServerChannel serverChannel = _serverToChannelMap.get(serverRoutingInstance);
    if (serverChannel == null || !serverChannel._openedByQuery) {
      return false;
    }
    serverChannel.connect();
    return true;
  }

  /// Records a response from the server on the given channel: it is alive, whatever it answered.
  public void recordResponse(ServerRoutingInstance serverRoutingInstance) {
    ServerChannel serverChannel = _serverToChannelMap.get(serverRoutingInstance);
    if (serverChannel != null) {
      serverChannel.recordResponse();
    }
  }

  /// Records that a query written to the given channel at `submitTimeMs` timed out unanswered. The verdict waits
  /// out the grace period: the query only counts against the channel if by then the server has still sent nothing
  /// on it since the query was written. That lets the server's own late timeout response through, so a burst of
  /// slow queries, or a broker pause that delays reading the responses, does not look like a dead channel: only a
  /// channel the server has gone silent on does. At the threshold the channel is marked suspect and the server is
  /// reported through [QueryRouter#reportUnansweredServer]. No-op when the check is disabled.
  public void recordUnansweredQuery(ServerRoutingInstance serverRoutingInstance, long submitTimeMs) {
    if (_livenessExecutor == null) {
      return;
    }
    ServerChannel serverChannel = _serverToChannelMap.get(serverRoutingInstance);
    if (serverChannel == null) {
      return;
    }
    if (serverChannel._numPendingJudgments.incrementAndGet() > _maxPendingJudgmentsPerChannel) {
      // Enough judgments queued to cross the threshold already; this query adds nothing
      serverChannel._numPendingJudgments.decrementAndGet();
      return;
    }
    try {
      _livenessExecutor.schedule(() -> {
        try {
          serverChannel.judgeUnansweredQuery(submitTimeMs);
        } catch (Exception e) {
          LOGGER.error("Caught exception while judging an unanswered query to server: {}", serverRoutingInstance, e);
        } finally {
          serverChannel._numPendingJudgments.decrementAndGet();
        }
      }, _unansweredQueryGraceMs, TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException e) {
      // Shutting down: nothing left to judge
      serverChannel._numPendingJudgments.decrementAndGet();
    }
  }

  @VisibleForTesting
  int getUnansweredQueries(ServerRoutingInstance serverRoutingInstance) {
    ServerChannel serverChannel = _serverToChannelMap.get(serverRoutingInstance);
    return serverChannel != null ? serverChannel.getUnansweredQueries() : 0;
  }

  @VisibleForTesting
  int getNumPendingJudgments(ServerRoutingInstance serverRoutingInstance) {
    ServerChannel serverChannel = _serverToChannelMap.get(serverRoutingInstance);
    return serverChannel != null ? serverChannel._numPendingJudgments.get() : 0;
  }

  /// Opens a channel ahead of query traffic, awaiting the TLS handshake so that neither the connect nor
  /// the handshake lands on the first query's critical path. Both waits are bounded by `timeoutMs`.
  ///
  /// The channel is entered into the same map the query path uses, so the first query reuses it rather
  /// than connecting again -- but it is not marked query-carrying, so [#hasChannel] is unaffected.
  public void preConnect(ServerRoutingInstance serverRoutingInstance, long timeoutMs)
      throws InterruptedException, TimeoutException {
    _serverToChannelMap.computeIfAbsent(serverRoutingInstance, ServerChannel::new).preConnect(timeoutMs);
  }

  public void shutDown() {
    if (_livenessExecutor != null) {
      _livenessExecutor.shutdownNow();
    }
    // Shut down immediately
    _eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
  }

  @VisibleForTesting
  ServerChannel getOrCreateServerChannel(ServerRoutingInstance instance) {
    return _serverToChannelMap.computeIfAbsent(instance, ServerChannel::new);
  }

  @ThreadSafe
  class ServerChannel {
    final ServerRoutingInstance _serverRoutingInstance;
    final Bootstrap _bootstrap;
    // lock to protect channel as requests must be written into channel sequentially
    final ReentrantLock _channelLock = new ReentrantLock();
    // Written under _channelLock; volatile so that evict(), which does not take the lock, closes the current channel
    volatile Channel _channel;
    // Set once a query has been sent, or attempted, through this channel; startup pre-connect leaves it
    // false. Read by hasChannel(), which the failure detector uses to decide whether the single-stage
    // transport has any opinion on this server's health. Volatile: written on a query thread, read on the
    // failure-detector retry thread.
    volatile boolean _openedByQuery;
    // Set by evict() once this entry has left the map. Checked under _channelLock before and after connecting, so a
    // sender that fetched the entry before the eviction neither writes into the evicted channel nor reconnects an
    // entry nobody can reach any more; it retries on a fresh entry instead.
    volatile boolean _evicted;
    // Set when the server stopped answering queries on this channel while the socket still looked active (see
    // judgeUnansweredQuery). connectWithoutLocking() then replaces the channel rather than trusting it.
    volatile boolean _suspect;
    // Liveness bookkeeping, guarded by _livenessLock: when the server last sent anything on this channel (the
    // connect time for a fresh channel) and how many consecutive queries it has left unanswered since. The lock is
    // taken briefly on the event loop (per response), on the query thread (when a fresh channel is connected) and on
    // the liveness thread (judgment), so that a response landing while a judgment runs is never lost: it is either
    // seen by the judgment or zeroes the count after it. See judgeUnansweredQuery.
    private final Object _livenessLock = new Object();
    private long _lastResponseTimeMs;
    private int _unansweredQueries;
    // Judgments queued for this channel, bounded by ServerChannels#_maxPendingJudgmentsPerChannel
    final AtomicInteger _numPendingJudgments = new AtomicInteger();

    ServerChannel(ServerRoutingInstance serverRoutingInstance) {
      _serverRoutingInstance = serverRoutingInstance;
      _bootstrap = new Bootstrap().remoteAddress(serverRoutingInstance.getHostname(), serverRoutingInstance.getPort())
          .option(ChannelOption.ALLOCATOR, _bufAllocatorWithLimits).group(_eventLoopGroup).channel(_channelClass)
          .option(ChannelOption.SO_KEEPALIVE, true)
          .option(ChannelOption.WRITE_BUFFER_WATER_MARK, _writeBufferWaterMark)
          .handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
              if (_tlsConfig != null) {
                // Add SSL handler first to encrypt and decrypt everything.
                ch.pipeline()
                    .addLast(ChannelHandlerFactory.SSL, ChannelHandlerFactory.getClientTlsHandler(_tlsConfig, ch));
              }
              if (_writeTimeoutMs > 0) {
                // Right behind the SSL handler (or the socket), so the timeout covers the write all the way to the
                // kernel, including the TLS encryption and any wait for the handshake.
                ch.pipeline().addLast(new RequestWriteTimeoutHandler(_serverRoutingInstance, _writeTimeoutMs));
              }

              ch.pipeline().addLast(ChannelHandlerFactory.getLengthFieldBasedFrameDecoder());
              ch.pipeline().addLast(ChannelHandlerFactory.getLengthFieldPrepender());
              ch.pipeline().addLast(
                  ChannelHandlerFactory.getDirectOOMHandler(_queryRouter, _serverRoutingInstance, _serverToChannelMap,
                      null, null));
              // NOTE: data table de-serialization happens inside this handler
              // Revisit if this becomes a bottleneck
              ch.pipeline()
                  .addLast(ChannelHandlerFactory.getDataTableHandler(_queryRouter, _threadAccountant,
                      _serverRoutingInstance));
            }
          });
      if (isTcpUserTimeoutApplied()) {
        _bootstrap.option(EpollChannelOption.TCP_USER_TIMEOUT, _tcpUserTimeoutMs);
      }
    }

    void closeChannel() {
      Channel channel = _channel;
      if (channel != null) {
        channel.close();
      }
    }

    /// See [ServerChannels#evictChannel]. Flags the entry first, then closes whatever channel it holds; no lock is
    /// taken so that the routing manager's Helix callback is never blocked behind a connect or write in progress.
    void evict() {
      _evicted = true;
      Channel channel = _channel;
      if (channel != null) {
        closeLocally(channel);
      }
    }

    /// Closes a channel the broker decided to drop (eviction after a restart, replacement of a suspect channel).
    /// The queries still in flight on it fail through `channelInactive` as on any closed channel, but the server is
    /// not reported for it: the close was the broker's decision, not evidence against the server.
    private void closeLocally(Channel channel) {
      DataTableHandler dataTableHandler = channel.pipeline().get(DataTableHandler.class);
      if (dataTableHandler != null) {
        dataTableHandler.markClosedByBroker();
      }
      channel.close();
    }

    void recordResponse() {
      synchronized (_livenessLock) {
        _lastResponseTimeMs = System.currentTimeMillis();
        _unansweredQueries = 0;
      }
    }

    int getUnansweredQueries() {
      synchronized (_livenessLock) {
        return _unansweredQueries;
      }
    }

    /// A fresh channel starts with a clean record: counted as answered from its connect time, so queries written to
    /// the channel it replaced do not count against it.
    private void resetLiveness() {
      synchronized (_livenessLock) {
        _lastResponseTimeMs = System.currentTimeMillis();
        _unansweredQueries = 0;
      }
    }

    /// Runs on the liveness thread a grace period after a query written at `submitTimeMs` timed out. See
    /// [ServerChannels#recordUnansweredQuery].
    void judgeUnansweredQuery(long submitTimeMs) {
      int unansweredQueries;
      synchronized (_livenessLock) {
        // Nothing to add once the channel is already suspect (it is reported once per episode, until replaced) or
        // no longer in use, or if the server answered something since the query was written (see resetLiveness for
        // a fresh channel). Strictly after: the connect and the first write can share a millisecond, while a real
        // answer never shares one with its request. Checked and acted on under the lock, so a response that lands
        // meanwhile either makes this judgment bail or zeroes the count right after it.
        if (_evicted || _suspect || _lastResponseTimeMs > submitTimeMs) {
          return;
        }
        unansweredQueries = ++_unansweredQueries;
        if (unansweredQueries < _unansweredQueriesThreshold) {
          return;
        }
        _unansweredQueries = 0;
        _suspect = true;
      }
      LOGGER.warn("Server: {} has left {} consecutive queries unanswered on its channel, each judged {}ms past its "
          + "timeout; marking the channel suspect and reporting the server", _serverRoutingInstance,
          unansweredQueries, _unansweredQueryGraceMs);
      _queryRouter.reportUnansweredServer(_serverRoutingInstance);
    }

    @VisibleForTesting
    void setChannel(Channel channel) {
      _channel = channel;
    }

    void setSilentShutdown() {
      Channel channel = _channel;
      if (channel != null) {
        DirectOOMHandler directOOMHandler = channel.pipeline().get(DirectOOMHandler.class);
        if (directOOMHandler != null) {
          directOOMHandler.setSilentShutDown();
        }
      }
    }

    /// Returns the channel the request was written to, or `null` if this entry was evicted before the request could
    /// be written, in which case the caller retries on a fresh entry.
    @Nullable
    Channel sendRequest(String rawTableName, AsyncQueryResponse asyncQueryResponse,
        ServerRoutingInstance serverRoutingInstance, byte[] requestBytes, long timeoutMs)
        throws InterruptedException, TimeoutException {
      if (_channelLock.tryLock(timeoutMs, TimeUnit.MILLISECONDS)) {
        try {
          if (_evicted) {
            return null;
          }
          connectWithoutLocking();
          if (_evicted) {
            // Evicted while connecting: do not leave a channel open on an entry that has left the map
            closeChannel();
            return null;
          }
          return sendRequestWithoutLocking(rawTableName, asyncQueryResponse, serverRoutingInstance, requestBytes);
        } finally {
          _channelLock.unlock();
        }
      } else {
        throw new TimeoutException(CHANNEL_LOCK_TIMEOUT_MSG);
      }
    }

    private void checkNotEvicted() {
      if (_evicted) {
        closeChannel();
        throw new IllegalStateException("Channel to server: " + _serverRoutingInstance + " has been evicted");
      }
    }

    /// Lazy query path: opens the TCP connection only. Any TLS handshake is left to proceed
    /// asynchronously so the channel lock is released as soon as the socket is up, keeping the first
    /// query's critical section short. Startup pre-connect uses [#preConnectWithoutLocking(long)]
    /// instead, which additionally pays the handshake.
    ///
    /// Also replaces a channel marked suspect: the server stopped answering on it while the socket still looked
    /// active, which is what a half-open socket looks like until the kernel gives up on it, so `isActive()` is not
    /// trusted. The suspect channel is dropped before the connect, so a failed connect does not leave it in place,
    /// and whatever is still in flight on it fails without the server being reported (nothing on it was being
    /// answered anyway).
    void connectWithoutLocking()
        throws InterruptedException {
      Channel channel = _channel;
      if (channel != null && channel.isActive() && !_suspect) {
        return;
      }
      if (channel != null && _suspect) {
        LOGGER.info("Replacing suspect channel to server: {} with a fresh connection", _serverRoutingInstance);
        _channel = null;
        closeLocally(channel);
      }
      _suspect = false;
      long startTime = System.currentTimeMillis();
      _channel = _bootstrap.connect().sync().channel();
      resetLiveness();
      recordConnectTime(System.currentTimeMillis() - startTime);
    }

    /// Like [#connectWithoutLocking()] but additionally waits out the TLS handshake, and bounds both
    /// waits by `timeoutMs`.
    ///
    /// Used only by startup pre-connect ([#preConnect(long)]), never by the lazy query path or by the
    /// failure detector's reconnect probe ([#connect()]) -- both of those keep the shorter critical
    /// section they had before this feature existed.
    ///
    /// `_channel` is assigned only after the handshake succeeds. Netty fails the handshake promise
    /// *before* it closes the channel (`SslHandler#setHandshakeFailure` calls `Promise#tryFailure` and
    /// only then `SslUtils#handleHandshakeFailure` -> `ctx.close()`, which is itself asynchronous), so a
    /// channel assigned up front would briefly still report `isActive()`, and a query written into it
    /// would fail.
    void preConnectWithoutLocking(long timeoutMs)
        throws InterruptedException, TimeoutException {
      if (_channel != null && _channel.isActive()) {
        return;
      }
      if (timeoutMs <= 0) {
        // No budget left. Must return before touching CONNECT_TIMEOUT_MILLIS: Netty only schedules its
        // connect-timeout task when the value is > 0, so passing 0 would mean "wait forever".
        throw new TimeoutException("No pre-connect budget left for server: " + _serverRoutingInstance);
      }
      long startTime = System.currentTimeMillis();
      // The shared bootstrap leaves ChannelOption.CONNECT_TIMEOUT_MILLIS at Netty's 30s default, which is
      // the whole pre-connect budget -- one server whose SYN is dropped rather than refused would consume
      // it alone and occupy a worker for the entire startup. Clone the bootstrap so the tighter bound
      // applies to pre-connect only and the query path is untouched.
      Channel channel = _bootstrap.clone()
          .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) Math.min(timeoutMs, Integer.MAX_VALUE))
          .connect().sync().channel();
      try {
        awaitTlsHandshake(channel, Math.max(0L, timeoutMs - (System.currentTimeMillis() - startTime)));
      } catch (Throwable t) {
        channel.close();
        throw t;
      }
      _channel = channel;
      resetLiveness();
      recordConnectTime(System.currentTimeMillis() - startTime);
    }

    private void recordConnectTime(long connectTimeMs) {
      _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.NETTY_CONNECTION_CONNECT_TIME_MS, connectTimeMs);
      _brokerMetrics.addTimedValue(BrokerTimer.NETTY_CONNECTION_CONNECT_LATENCY_MS, connectTimeMs,
          TimeUnit.MILLISECONDS);
    }

    /// Blocks until the TLS handshake on a freshly-connected channel completes, for at most `timeoutMs`.
    ///
    /// `bootstrap.connect().sync()` returns once the TCP connection is up; the client-mode [SslHandler]
    /// then drives the handshake asynchronously on the event loop. Awaiting its future here pays the
    /// handshake -- two round trips plus certificate validation -- on the connecting thread rather than
    /// on the first query that writes to the channel. On a plaintext channel there is no [SslHandler] in
    /// the pipeline and this is a no-op. The calling thread is never an event-loop thread, so this cannot
    /// deadlock.
    ///
    /// The wait is bounded by the caller's remaining budget rather than by [SslHandler]'s own
    /// `handshakeTimeoutMillis`, which defaults to 10s and is not set on this pipeline -- otherwise a
    /// single hung TLS peer could outlive the pre-connect budget.
    private void awaitTlsHandshake(Channel channel, long timeoutMs)
        throws InterruptedException, TimeoutException {
      SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
      if (sslHandler == null) {
        return;
      }
      Future<Channel> handshakeFuture = sslHandler.handshakeFuture();
      if (!handshakeFuture.await(timeoutMs, TimeUnit.MILLISECONDS)) {
        throw new TimeoutException("Timed out waiting for the TLS handshake to server: " + _serverRoutingInstance);
      }
      if (!handshakeFuture.isSuccess()) {
        throw new RuntimeException("Failed the TLS handshake to server: " + _serverRoutingInstance,
            handshakeFuture.cause());
      }
    }

    /// Returns the channel the request was written to.
    Channel sendRequestWithoutLocking(String rawTableName, AsyncQueryResponse asyncQueryResponse,
        ServerRoutingInstance serverRoutingInstance, byte[] requestBytes) {
      // Pin the channel: the completion listener must act on the channel it wrote to, not on whatever channel this
      // entry holds by the time the write completes, or a late failure would close a healthy reconnected channel.
      Channel channel = _channel;
      if (!channel.isWritable()) {
        // The outbound queue is above the high watermark, which only happens when the server stopped draining its
        // socket or the path stopped forwarding. Fail the query now rather than queueing yet another request buffer
        // in direct memory behind a closed send window, and mark the channel suspect so that the next query, or the
        // failure detector's probe, replaces it instead of vouching for a socket that still looks active.
        _suspect = true;
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.NETTY_CONNECTION_UNWRITABLE_REJECTIONS, 1);
        throw new IllegalStateException(String.format(
            "Channel to server: %s is not writable (%d more bytes must drain before it is), rejecting the request "
                + "instead of queueing it", serverRoutingInstance, channel.bytesBeforeWritable()));
      }
      // Tie the query to this channel before the first byte goes out, so that another channel to the same server
      // going down (a replaced one closing late, say) cannot be mistaken for this one
      asyncQueryResponse.markRequestChannel(serverRoutingInstance, channel);
      long startTimeMs = System.currentTimeMillis();
      channel.writeAndFlush(Unpooled.wrappedBuffer(requestBytes)).addListener(f -> {
        if (f.isSuccess()) {
          int requestSentLatencyMs = (int) (System.currentTimeMillis() - startTimeMs);
          _brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.NETTY_CONNECTION_SEND_REQUEST_LATENCY,
              requestSentLatencyMs, TimeUnit.MILLISECONDS);
          asyncQueryResponse.markRequestSent(serverRoutingInstance, requestSentLatencyMs);
        } else {
          LOGGER.error("Write failure to server: {} for table: {}", serverRoutingInstance, rawTableName, f.cause());
          _brokerMetrics.addMeteredGlobalValue(BrokerMeter.NETTY_CONNECTION_SEND_REQUEST_FAILURES, 1);
          if (asyncQueryResponse.markServerUnavailable(serverRoutingInstance, channel,
              new RuntimeException("Failed to send request to server: " + serverRoutingInstance, f.cause()), true)) {
            _brokerMetrics.addMeteredGlobalValue(BrokerMeter.SERVER_MARKED_DOWN_SKIPPED, 1);
          }
          channel.close();
        }
      });
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.NETTY_CONNECTION_REQUESTS_SENT, 1);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.NETTY_CONNECTION_BYTES_SENT, requestBytes.length);
      return channel;
    }

    /// Opens the TCP connection, as the failure detector's reconnect probe
    /// (`SingleConnectionBrokerRequestHandler#retryUnhealthyServer`) has always done, replacing the channel when it
    /// is suspect (see [#connectWithoutLocking()]): a probe that only looked at `isActive()` would vouch for a
    /// half-open channel and put the server straight back into routing on it, which is how a readmitted server kept
    /// losing queries. Deliberately does **not** await the TLS handshake: this runs at steady state under live
    /// traffic, and holding `_channelLock` across a handshake would make concurrent queries to a recovering server
    /// queue behind it -- the very serialization startup pre-connect exists to remove.
    void connect()
        throws InterruptedException, TimeoutException {
      if (_channelLock.tryLock(TRY_CONNECT_CHANNEL_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        try {
          checkNotEvicted();
          connectWithoutLocking();
          checkNotEvicted();
        } finally {
          _channelLock.unlock();
        }
      } else {
        throw new TimeoutException(CHANNEL_LOCK_TIMEOUT_MSG);
      }
    }

    /// Startup pre-connect: opens the connection and pays the TLS handshake, bounded by `timeoutMs`.
    void preConnect(long timeoutMs)
        throws InterruptedException, TimeoutException {
      if (_channelLock.tryLock(TRY_CONNECT_CHANNEL_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        try {
          checkNotEvicted();
          preConnectWithoutLocking(timeoutMs);
          checkNotEvicted();
        } finally {
          _channelLock.unlock();
        }
      } else {
        throw new TimeoutException(CHANNEL_LOCK_TIMEOUT_MSG);
      }
    }
  }
}
