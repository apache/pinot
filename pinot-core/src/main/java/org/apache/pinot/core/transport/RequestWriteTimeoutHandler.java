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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressiveFutureListener;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Broker-side outbound handler that closes the channel to a server when a request write makes no progress for the
/// configured timeout.
///
/// Progress is bytes the kernel accepted. A large request on a slow but healthy link keeps making progress and is
/// never timed out, however long it takes in all; a half-open socket (peer process or host gone without a FIN, or
/// the path black-holing packets) accepts nothing once its send buffer is full, while `Channel.isActive()` keeps
/// reporting `true` until the kernel gives up on retransmits many minutes later. Closing the channel here instead
/// fails the queries in flight on it through `channelInactive` (see [DataTableHandler]), releases the queued request
/// buffers, and leaves the next query to reconnect. Pending writes are failed by the close, so no exception is fired
/// through the pipeline.
///
/// Each write is forwarded with a progressive promise, which Netty's outbound buffer advances every time part of the
/// message reaches the socket; the original promise is completed from it. One timer per pending write, re-armed
/// from the last progress rather than firing per write.
///
/// Not sharable: one instance per channel, state is only touched on the channel's event loop.
public class RequestWriteTimeoutHandler extends ChannelOutboundHandlerAdapter {
  private static final Logger LOGGER = LoggerFactory.getLogger(RequestWriteTimeoutHandler.class);

  private final ServerRoutingInstance _serverRoutingInstance;
  private final long _timeoutMs;
  private final long _timeoutNanos;
  private final Set<PendingWrite> _pendingWrites = new HashSet<>();
  private boolean _closed;

  public RequestWriteTimeoutHandler(ServerRoutingInstance serverRoutingInstance, long timeoutMs) {
    _serverRoutingInstance = serverRoutingInstance;
    _timeoutMs = timeoutMs;
    _timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    if (promise.isVoid()) {
      // Nothing to complete, so nothing to watch
      ctx.write(msg, promise);
      return;
    }
    ctx.write(msg, new PendingWrite(ctx, promise)._progressivePromise);
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) {
    for (PendingWrite pendingWrite : new ArrayList<>(_pendingWrites)) {
      pendingWrite.cancelTimer();
    }
    _pendingWrites.clear();
  }

  private void writeTimedOut(ChannelHandlerContext ctx) {
    // Every stalled write has its own timer; only the first one needs to act
    if (_closed) {
      return;
    }
    _closed = true;
    Channel channel = ctx.channel();
    ChannelOutboundBuffer outboundBuffer = channel.unsafe().outboundBuffer();
    long pendingBytes = outboundBuffer != null ? outboundBuffer.totalPendingWriteBytes() : -1;
    LOGGER.error("Request write to server: {} made no progress for {}ms ({} bytes pending, writable: {}), closing the "
        + "channel as the socket is stalled or half-open", _serverRoutingInstance, _timeoutMs, pendingBytes,
        channel.isWritable());
    BrokerMetrics.get().addMeteredGlobalValue(BrokerMeter.NETTY_CONNECTION_WRITE_TIMEOUTS, 1);
    ctx.close();
  }

  /// One write in flight: the caller's promise, the progressive promise handed down the pipeline in its place, the
  /// time the kernel last accepted bytes of it, and the timer that checks on it.
  private final class PendingWrite implements ChannelProgressiveFutureListener, Runnable {
    final ChannelHandlerContext _ctx;
    final ChannelPromise _promise;
    final ChannelProgressivePromise _progressivePromise;
    long _lastProgressNanos;
    ScheduledFuture<?> _timer;

    PendingWrite(ChannelHandlerContext ctx, ChannelPromise promise) {
      _ctx = ctx;
      _promise = promise;
      _progressivePromise = ctx.newProgressivePromise();
      _lastProgressNanos = System.nanoTime();
      _pendingWrites.add(this);
      _progressivePromise.addListener(this);
      _timer = ctx.executor().schedule(this, _timeoutNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) {
      _lastProgressNanos = System.nanoTime();
    }

    @Override
    public void operationComplete(ChannelProgressiveFuture future) {
      _pendingWrites.remove(this);
      cancelTimer();
      if (future.isSuccess()) {
        _promise.trySuccess();
      } else {
        _promise.tryFailure(future.cause());
      }
    }

    /// The timer: re-arms itself from the last progress while the write is moving, closes the channel once the
    /// write has stalled for the whole timeout.
    @Override
    public void run() {
      if (_progressivePromise.isDone()) {
        return;
      }
      long idleNanos = System.nanoTime() - _lastProgressNanos;
      if (idleNanos < _timeoutNanos) {
        _timer = _ctx.executor().schedule(this, _timeoutNanos - idleNanos, TimeUnit.NANOSECONDS);
        return;
      }
      writeTimedOut(_ctx);
    }

    void cancelTimer() {
      if (_timer != null) {
        _timer.cancel(false);
        _timer = null;
      }
    }
  }
}
