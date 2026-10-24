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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressiveFutureListener;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Broker-side socket-idle monitor. While outbound bytes are pending, closes a server channel if the kernel accepts
/// no bytes for the configured timeout. Progress by the current queue head keeps later queued writes alive; enqueue,
/// cancellation and write failure do not reset the idle clock.
///
/// Install on the socket side of SSL so ciphertext progress is observable. This does not time plaintext waiting
/// inside SSL before ciphertext exists; SSL's handshake timeout and the pre-connect handshake budget remain separate.
/// Each socket write uses a progressive promise, relaying the downstream outcome to the original promise without
/// completing it early or releasing Netty-owned buffers. One connection timer runs only while writes are pending.
///
/// Not sharable: all state is confined to the channel's event loop.
public class RequestWriteTimeoutHandler extends ChannelDuplexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(RequestWriteTimeoutHandler.class);

  private final ServerRoutingInstance _serverRoutingInstance;
  private final long _timeoutMs;
  private final long _timeoutNanos;
  private final Set<PendingWrite> _pendingWrites = new HashSet<>();
  private Set<PendingWrite> _unflushedWrites = new HashSet<>();
  private long _lastProgressNanos;
  private ScheduledFuture<?> _timer;
  private boolean _closed;

  public RequestWriteTimeoutHandler(ServerRoutingInstance serverRoutingInstance, long timeoutMs) {
    _serverRoutingInstance = serverRoutingInstance;
    _timeoutMs = timeoutMs;
    _timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    // SSL supplies regular promises for ciphertext. Void writes also need monitoring; unvoid preserves their
    // exception propagation while giving the relay a real completion signal.
    PendingWrite write = new PendingWrite(ctx, promise.unvoid(), msg instanceof ByteBuf
        && ((ByteBuf) msg).isReadable());
    ctx.write(msg, write._progressivePromise);
  }

  @Override
  public void flush(ChannelHandlerContext ctx) {
    // Netty makes flushed outbound promises uncancelable. Preserve that contract for the caller's promise too,
    // while cancellation before flush still cancels the queued downstream promise so Netty releases/skips it.
    if (!_unflushedWrites.isEmpty()) {
      Set<PendingWrite> unflushedWrites = _unflushedWrites;
      // Detach this first-flush batch before callbacks can complete writes or remove the handler reentrantly.
      _unflushedWrites = new HashSet<>();
      for (PendingWrite write : unflushedWrites) {
        if (_pendingWrites.contains(write) && !write._promise.setUncancellable()) {
          write._progressivePromise.cancel(false);
        }
      }
    }
    ctx.flush();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    stopMonitoring();
    ctx.fireChannelInactive();
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) {
    stopMonitoring();
  }

  private void stopMonitoring() {
    _closed = true;
    cancelTimer();
    // The listeners still relay real downstream outcomes after removal; do not complete or release anything here.
    _pendingWrites.clear();
    _unflushedWrites.clear();
  }

  private void cancelTimer() {
    if (_timer != null) {
      _timer.cancel(false);
      _timer = null;
    }
  }

  private void scheduleTimeout(ChannelHandlerContext ctx, long delayNanos) {
    _timer = ctx.executor().schedule(() -> checkTimeout(ctx), delayNanos, TimeUnit.NANOSECONDS);
  }

  private void checkTimeout(ChannelHandlerContext ctx) {
    _timer = null;
    if (_closed || _pendingWrites.isEmpty()) {
      return;
    }
    long idleNanos = System.nanoTime() - _lastProgressNanos;
    if (idleNanos < _timeoutNanos) {
      scheduleTimeout(ctx, _timeoutNanos - idleNanos);
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

  /// Per-write outcome relay; progress updates the shared socket clock, never a separate write deadline.
  private final class PendingWrite implements ChannelProgressiveFutureListener {
    final ChannelPromise _promise;
    final ChannelProgressivePromise _progressivePromise;
    final boolean _hasBytes;
    long _progress;

    PendingWrite(ChannelHandlerContext ctx, ChannelPromise promise, boolean hasBytes) {
      _promise = promise;
      _hasBytes = hasBytes;
      _progressivePromise = ctx.newProgressivePromise();
      _pendingWrites.add(this);
      _unflushedWrites.add(this);
      if (_pendingWrites.size() == 1 && !_closed) {
        _lastProgressNanos = System.nanoTime();
        scheduleTimeout(ctx, _timeoutNanos);
      }
      _progressivePromise.addListener(this);
      _promise.addListener(future -> {
        if (future.isCancelled()) {
          // If already flushed, cancellation can fail: keep monitoring those bytes until their actual completion.
          _progressivePromise.cancel(false);
        }
      });
    }

    @Override
    public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) {
      if (progress > _progress) {
        _progress = progress;
        _lastProgressNanos = System.nanoTime();
        if (_promise instanceof ChannelProgressivePromise) {
          ((ChannelProgressivePromise) _promise).tryProgress(progress, total);
        }
      }
    }

    @Override
    public void operationComplete(ChannelProgressiveFuture future) {
      if (future.isSuccess() && _hasBytes) {
        _lastProgressNanos = System.nanoTime();
      }
      _pendingWrites.remove(this);
      _unflushedWrites.remove(this);
      if (_pendingWrites.isEmpty()) {
        cancelTimer();
      }
      if (future.isSuccess()) {
        _promise.trySuccess();
      } else if (future.isCancelled()) {
        _promise.cancel(false);
      } else {
        _promise.tryFailure(future.cause());
      }
    }
  }
}
