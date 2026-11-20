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
package org.apache.pinot.query.mailbox.channel;

import com.google.common.base.Preconditions;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.netty.shaded.io.netty.channel.WriteBufferWaterMark;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.query.grpc.GrpcKeepAliveConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// `ChannelManager` manages Grpc send/receive channels.
///
/// Grpc channels are managed centralized per Pinot component. Channels should be reused across different
/// query/job/stages.
public class ChannelManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChannelManager.class);

  /// A channel is refreshed at most once in this interval, see [#refreshChannel].
  private static final long MIN_REFRESH_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);

  /// Map from (hostname, port) to the ManagedChannel with all known channels
  private final ConcurrentHashMap<Pair<String, Integer>, ManagedChannel> _channelMap = new ConcurrentHashMap<>();
  /// When each channel was last refreshed, from [System#nanoTime()], by (hostname, port), see [#refreshChannel].
  private final ConcurrentHashMap<Pair<String, Integer>, AtomicLong> _lastRefreshTimeNs = new ConcurrentHashMap<>();
  /// The idle timeout for the channel, which cannot be disabled in gRPC.
  ///
  /// In general we want to prevent the channel from going idle, so that we don't have to re-establish the connection
  /// (including TLS negotiation) before sending any message, which increases the latency of the first query sent after
  /// a period of inactivity. In order to achieve that, we set the idle timeout to a very large value by default.
  private final Duration _idleTimeout;
  /// Transport-level liveness policy for every channel this manager hands out.
  ///
  /// The idle timeout above cannot substitute for it. A channel that is being used is never idle, so on a cluster
  /// that keeps serving queries the idle timeout never fires — and it is disabled by default anyway. Keep-alive is
  /// the only mechanism here that a peer which stopped answering without closing its socket cannot outlive.
  private final GrpcKeepAliveConfig _keepAliveConfig;
  private final int _maxInboundMessageSize;
  /// Buffer allocator configured to prefer direct (off-heap) buffers for better performance.
  /// Using a single allocator instance across all channels allows for better memory pooling and reduces fragmentation.
  private final PooledByteBufAllocator _bufAllocator;
  @Nullable
  private final SslContext _clientSslContext;
  private final WriteBufferWaterMark _writeBufferWaterMark;

  /// Constructs a `ChannelManager`.
  ///
  /// @param clientSslContext optional cached client [SslContext] to reuse across channels
  /// @param maxInboundMessageSize maximum inbound message size for gRPC channels
  /// @param idleTimeout idle timeout for gRPC channels; channels close after this period of inactivity
  /// @param writeBufferHighWaterMarkBytes Netty per-channel [WriteBufferWaterMark] high watermark. This limit is
  ///                                     per `(host, port)` peer and is shared across all streams multiplexed on
  ///                                     that channel.
  /// @param writeBufferLowWaterMarkBytes Netty per-channel [WriteBufferWaterMark] low mark. Once the channel's
  ///                                     pending write queue grows above the high watermark, the channel is marked
  ///                                     unwritable; it becomes writable again only when the queue drains below this
  ///                                     low watermark. Must satisfy `0 < low ≤ high`; validated eagerly here so
  ///                                     misconfiguration surfaces at startup rather than on the first query.
  /// @param keepAliveConfig gRPC keep-alive policy applied to every channel; see [GrpcKeepAliveConfig]
  public ChannelManager(@Nullable SslContext clientSslContext, int maxInboundMessageSize, Duration idleTimeout,
      int writeBufferHighWaterMarkBytes, int writeBufferLowWaterMarkBytes, GrpcKeepAliveConfig keepAliveConfig) {
    _clientSslContext = clientSslContext;
    _maxInboundMessageSize = maxInboundMessageSize;
    _idleTimeout = idleTimeout;
    _keepAliveConfig = keepAliveConfig;
    Preconditions.checkArgument(writeBufferLowWaterMarkBytes > 0,
        "writeBufferLowWaterMarkBytes must be positive, got: %s", writeBufferLowWaterMarkBytes);
    // The `low <= high` (and `low >= 0`) invariant is also checked by Netty's WriteBufferWaterMark constructor; by
    // constructing the watermark eagerly here we surface any violation at startup instead of on the first send to
    // a previously-unseen peer.
    _writeBufferWaterMark = new WriteBufferWaterMark(writeBufferLowWaterMarkBytes, writeBufferHighWaterMarkBytes);
    // Use direct buffers (off-heap) for better performance - matches server-side configuration
    _bufAllocator = new PooledByteBufAllocator(true);
  }

  public ManagedChannel getChannel(String hostname, int port) {
    if (_clientSslContext != null) {
      return _channelMap.computeIfAbsent(Pair.of(hostname, port),
          (k) -> {
            NettyChannelBuilder channelBuilder = NettyChannelBuilder
                .forAddress(k.getLeft(), k.getRight())
                .maxInboundMessageSize(_maxInboundMessageSize)
                .withOption(ChannelOption.ALLOCATOR, _bufAllocator)
                .withOption(ChannelOption.WRITE_BUFFER_WATER_MARK, _writeBufferWaterMark)
                .sslContext(_clientSslContext);
            return watchState(decorate(channelBuilder).build(), k.getLeft(), k.getRight());
          }
      );
    } else {
      return _channelMap.computeIfAbsent(Pair.of(hostname, port),
          (k) -> {
            NettyChannelBuilder channelBuilder = NettyChannelBuilder
                .forAddress(k.getLeft(), k.getRight())
                .maxInboundMessageSize(_maxInboundMessageSize)
                .withOption(ChannelOption.ALLOCATOR, _bufAllocator)
                .withOption(ChannelOption.WRITE_BUFFER_WATER_MARK, _writeBufferWaterMark)
                .usePlaintext();
            return watchState(decorate(channelBuilder).build(), k.getLeft(), k.getRight());
          });
    }
  }

  /// Logs at WARN when `channel` leaves `READY`, and returns it.
  ///
  /// Keep-alive makes a silent peer *detectable*; this is what makes it **legible**. Without it the only
  /// record of the failure is the absence of one: the startup lines say what was configured, and a
  /// channel that later dropped looks exactly like a channel that never had a problem. The transition is
  /// the moment worth reading in a log, because it is when a peer stopped answering — every mailbox send
  /// queued behind it has already been failing for one keep-alive interval by then.
  ///
  /// Re-arms itself, which is how one callback follows a channel for its whole life; gRPC's
  /// `notifyWhenStateChanged` is single-shot. Stops on shutdown, so a terminated channel cannot keep
  /// re-registering.
  private ManagedChannel watchState(ManagedChannel channel, String hostname, int port) {
    ConnectivityState state = channel.getState(false);
    channel.notifyWhenStateChanged(state, () -> {
      if (channel.isShutdown()) {
        return;
      }
      ConnectivityState next = channel.getState(false);
      if (state == ConnectivityState.READY && next != ConnectivityState.READY && !isRecentlyRefreshed(hostname, port)) {
        LOGGER.warn("Mailbox channel to {}:{} left READY for {}; sends to that peer will fail until it "
            + "reconnects. If keep-alive reported it, the peer stopped answering about one keep-alive "
            + "interval ago.", hostname, port, next);
      }
      watchState(channel, hostname, port);
    });
    return channel;
  }

  /// Makes the channel to the given address resolve the hostname again and open a new connection before its next
  /// stream, dropping the connection it has, and any reconnect backoff. Streams in flight finish on the old
  /// connection. Returns false if there is no channel to the address, or if it was refreshed less than a second ago.
  ///
  /// A channel resolves the hostname only when it starts, and again after a connection fails or is lost. When the
  /// server at that address goes away, gRPC resolves once, right then, and keeps that answer until the next failure.
  /// If DNS still had the old IP address at that moment (a terminating pod stays in DNS for a while), the channel
  /// later reconnects to the old address, and when another server took that address over and listens on the same
  /// port, the connection succeeds and is never questioned again. Call this when the address is known to be stale or
  /// suspect: the receiver rejected a stream as meant for someone else, or the server at the address has restarted.
  ///
  /// Moving the channel to IDLE, rather than replacing it, keeps the channel object that concurrent senders may
  /// already hold valid. The refreshes are limited to one a second, because a stale connection usually fails many
  /// streams at once, and each refresh drops the connection that the previous one opened.
  public boolean refreshChannel(String hostname, int port) {
    Pair<String, Integer> key = Pair.of(hostname, port);
    ManagedChannel channel = _channelMap.get(key);
    if (channel == null) {
      return false;
    }
    long nowNs = System.nanoTime();
    AtomicLong lastRefreshTimeNs =
        _lastRefreshTimeNs.computeIfAbsent(key, k -> new AtomicLong(nowNs - MIN_REFRESH_INTERVAL_NS));
    long lastNs = lastRefreshTimeNs.get();
    if (nowNs - lastNs < MIN_REFRESH_INTERVAL_NS || !lastRefreshTimeNs.compareAndSet(lastNs, nowNs)) {
      return false;
    }
    channel.enterIdle();
    return true;
  }

  private boolean isRecentlyRefreshed(String hostname, int port) {
    AtomicLong lastRefreshTimeNs = _lastRefreshTimeNs.get(Pair.of(hostname, port));
    return lastRefreshTimeNs != null && System.nanoTime() - lastRefreshTimeNs.get() < MIN_REFRESH_INTERVAL_NS;
  }

  /// Removes the channel to the given address and shuts it down, letting streams in flight finish. Use it for an
  /// address that is no longer used, such as the old mailbox port of a server that restarted on another port, so
  /// that channels do not pile up. Returns false if there is no channel to the address.
  public boolean retireChannel(String hostname, int port) {
    Pair<String, Integer> key = Pair.of(hostname, port);
    ManagedChannel channel = _channelMap.remove(key);
    if (channel == null) {
      return false;
    }
    _lastRefreshTimeNs.remove(key);
    channel.shutdown();
    return true;
  }

  /// Shuts down all channels, letting streams in flight finish.
  public void shutdown() {
    for (ManagedChannel channel : _channelMap.values()) {
      channel.shutdown();
    }
    _channelMap.clear();
    _lastRefreshTimeNs.clear();
  }

  private NettyChannelBuilder decorate(NettyChannelBuilder builder) {
    return _keepAliveConfig.configure(builder.idleTimeout(_idleTimeout.getSeconds(), TimeUnit.SECONDS));
  }

  /// The keep-alive policy applied to every channel this manager hands out.
  ///
  /// Public because the assertion that matters spans packages: a test of [MailboxService] has to show
  /// that the policy resolved from config reached the transport, and no assertion on parsed values can
  /// show that. Returns an immutable record, so exposing it grants no control over the manager.
  public GrpcKeepAliveConfig getKeepAliveConfig() {
    return _keepAliveConfig;
  }

  /// Bytes of direct (off-heap) memory currently pinned by the shared gRPC
  /// client allocator. Covers every channel managed by this instance and remains
  /// meaningful regardless of whether Netty is configured to prefer direct or
  /// heap buffers (e.g. `-Dio.netty.noPreferDirect=true`). Consumed by
  /// [MailboxService] to register the `MAILBOX_CLIENT_USED_DIRECT_MEMORY` gauge.
  public long usedDirectMemoryBytes() {
    return _bufAllocator.metric().usedDirectMemory();
  }

  /// Bytes of heap memory currently pinned by the shared gRPC client allocator.
  /// Consumed by [MailboxService] to register the
  /// `MAILBOX_CLIENT_USED_HEAP_MEMORY` gauge.
  public long usedHeapMemoryBytes() {
    return _bufAllocator.metric().usedHeapMemory();
  }
}
