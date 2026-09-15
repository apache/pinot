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
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// The `ServerChannels` class manages the channels between broker to all the connected servers.
///
/// There is only one channel between the broker and each connected server (we count OFFLINE and REALTIME as different
/// servers)
@ThreadSafe
public class ServerChannels {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerChannels.class);
  public static final String CHANNEL_LOCK_TIMEOUT_MSG = "Timeout while acquiring channel lock";
  private static final long TRY_CONNECT_CHANNEL_LOCK_TIMEOUT_MS = 5_000L;

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

  private final BrokerMetrics _brokerMetrics = BrokerMetrics.get();
  private final ConcurrentHashMap<ServerRoutingInstance, ServerChannel> _serverToChannelMap = new ConcurrentHashMap<>();

  /// Create a server channel with TLS config
  ///
  /// @param queryRouter query router
  /// @param tlsConfig TLS/SSL config
  public ServerChannels(QueryRouter queryRouter, @Nullable NettyConfig nettyConfig, @Nullable TlsConfig tlsConfig,
      ThreadAccountant threadAccountant) {
    boolean enableNativeTransports = nettyConfig != null && nettyConfig.isNativeTransportsEnabled();
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

  public void sendRequest(String rawTableName, AsyncQueryResponse asyncQueryResponse,
      ServerRoutingInstance serverRoutingInstance, InstanceRequest instanceRequest, long timeoutMs)
      throws Exception {
    byte[] requestBytes = THREAD_LOCAL_T_SERIALIZER.get().serialize(instanceRequest);
    ServerChannel serverChannel = _serverToChannelMap.computeIfAbsent(serverRoutingInstance, ServerChannel::new);
    // This server is now query-carrying, whoever opened the channel. See [#hasChannel].
    serverChannel._openedByQuery = true;
    serverChannel.sendRequest(rawTableName, asyncQueryResponse, serverRoutingInstance, requestBytes, timeoutMs);
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
    Channel _channel;
    // Set once a query has been sent, or attempted, through this channel; startup pre-connect leaves it
    // false. Read by hasChannel(), which the failure detector uses to decide whether the single-stage
    // transport has any opinion on this server's health. Volatile: written on a query thread, read on the
    // failure-detector retry thread.
    volatile boolean _openedByQuery;

    ServerChannel(ServerRoutingInstance serverRoutingInstance) {
      _serverRoutingInstance = serverRoutingInstance;
      _bootstrap = new Bootstrap().remoteAddress(serverRoutingInstance.getHostname(), serverRoutingInstance.getPort())
          .option(ChannelOption.ALLOCATOR, _bufAllocatorWithLimits).group(_eventLoopGroup).channel(_channelClass)
          .option(ChannelOption.SO_KEEPALIVE, true).handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
              if (_tlsConfig != null) {
                // Add SSL handler first to encrypt and decrypt everything.
                ch.pipeline()
                    .addLast(ChannelHandlerFactory.SSL, ChannelHandlerFactory.getClientTlsHandler(_tlsConfig, ch));
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
    }

    void closeChannel() {
      if (_channel != null) {
        _channel.close();
      }
    }

    @VisibleForTesting
    void setChannel(Channel channel) {
      _channel = channel;
    }

    void setSilentShutdown() {
      if (_channel != null) {
        DirectOOMHandler directOOMHandler = _channel.pipeline().get(DirectOOMHandler.class);
        if (directOOMHandler != null) {
          directOOMHandler.setSilentShutDown();
        }
      }
    }

    void sendRequest(String rawTableName, AsyncQueryResponse asyncQueryResponse,
        ServerRoutingInstance serverRoutingInstance, byte[] requestBytes, long timeoutMs)
        throws InterruptedException, TimeoutException {
      if (_channelLock.tryLock(timeoutMs, TimeUnit.MILLISECONDS)) {
        try {
          connectWithoutLocking();
          sendRequestWithoutLocking(rawTableName, asyncQueryResponse, serverRoutingInstance, requestBytes);
        } finally {
          _channelLock.unlock();
        }
      } else {
        throw new TimeoutException(CHANNEL_LOCK_TIMEOUT_MSG);
      }
    }

    /// Lazy query path: opens the TCP connection only. Any TLS handshake is left to proceed
    /// asynchronously so the channel lock is released as soon as the socket is up, keeping the first
    /// query's critical section short. Startup pre-connect uses [#preConnectWithoutLocking(long)]
    /// instead, which additionally pays the handshake.
    void connectWithoutLocking()
        throws InterruptedException {
      if (_channel == null || !_channel.isActive()) {
        long startTime = System.currentTimeMillis();
        _channel = _bootstrap.connect().sync().channel();
        recordConnectTime(System.currentTimeMillis() - startTime);
      }
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

    void sendRequestWithoutLocking(String rawTableName, AsyncQueryResponse asyncQueryResponse,
        ServerRoutingInstance serverRoutingInstance, byte[] requestBytes) {
      long startTimeMs = System.currentTimeMillis();
      _channel.writeAndFlush(Unpooled.wrappedBuffer(requestBytes)).addListener(f -> {
        if (f.isSuccess()) {
          int requestSentLatencyMs = (int) (System.currentTimeMillis() - startTimeMs);
          _brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.NETTY_CONNECTION_SEND_REQUEST_LATENCY,
              requestSentLatencyMs, TimeUnit.MILLISECONDS);
          asyncQueryResponse.markRequestSent(serverRoutingInstance, requestSentLatencyMs);
        } else {
          LOGGER.error("Write failure to server: {} for table: {}", serverRoutingInstance, rawTableName, f.cause());
          _brokerMetrics.addMeteredGlobalValue(BrokerMeter.NETTY_CONNECTION_SEND_REQUEST_FAILURES, 1);
          if (asyncQueryResponse.markServerUnavailable(serverRoutingInstance,
              new RuntimeException("Failed to send request to server: " + serverRoutingInstance, f.cause()))) {
            _brokerMetrics.addMeteredGlobalValue(BrokerMeter.SERVER_MARKED_DOWN_SKIPPED, 1);
          }
          _channel.close();
        }
      });
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.NETTY_CONNECTION_REQUESTS_SENT, 1);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.NETTY_CONNECTION_BYTES_SENT, requestBytes.length);
    }

    /// Opens the TCP connection, as the failure detector's reconnect probe
    /// (`SingleConnectionBrokerRequestHandler#retryUnhealthyServer`) has always done. Deliberately does
    /// **not** await the TLS handshake: this runs at steady state under live traffic, and holding
    /// `_channelLock` across a handshake would make concurrent queries to a recovering server queue
    /// behind it -- the very serialization startup pre-connect exists to remove.
    void connect()
        throws InterruptedException, TimeoutException {
      if (_channelLock.tryLock(TRY_CONNECT_CHANNEL_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        try {
          connectWithoutLocking();
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
          preConnectWithoutLocking(timeoutMs);
        } finally {
          _channelLock.unlock();
        }
      } else {
        throw new TimeoutException(CHANNEL_LOCK_TIMEOUT_MSG);
      }
    }
  }
}
