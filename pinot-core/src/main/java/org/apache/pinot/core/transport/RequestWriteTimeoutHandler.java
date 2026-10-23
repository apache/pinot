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
import io.netty.handler.timeout.WriteTimeoutHandler;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Broker-side outbound handler that closes the channel to a server when a request write does not complete within
/// the configured timeout.
///
/// A write completes when the kernel accepts the bytes. On a half-open socket (peer process or host gone without a
/// FIN, or the path black-holing packets) the kernel keeps retransmitting silently, the send buffer fills, and from
/// then on every request write stays pending while `Channel.isActive()` still reports `true`, until the kernel gives
/// up on retransmits many minutes later. Closing the channel here instead fails the queries in flight on it through
/// `channelInactive` (see [DataTableHandler]), releases the queued request buffers, and leaves the next query to
/// reconnect. Pending writes are failed by the close, so no exception is fired through the pipeline.
///
/// Not sharable: one instance per channel, state is only touched on the channel's event loop.
public class RequestWriteTimeoutHandler extends WriteTimeoutHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(RequestWriteTimeoutHandler.class);

  private final ServerRoutingInstance _serverRoutingInstance;
  private final long _timeoutMs;
  private boolean _closed;

  public RequestWriteTimeoutHandler(ServerRoutingInstance serverRoutingInstance, long timeoutMs) {
    super(timeoutMs, TimeUnit.MILLISECONDS);
    _serverRoutingInstance = serverRoutingInstance;
    _timeoutMs = timeoutMs;
  }

  @Override
  protected void writeTimedOut(ChannelHandlerContext ctx) {
    // Every write that was pending gets its own timeout task; only the first one needs to act.
    if (_closed) {
      return;
    }
    _closed = true;
    Channel channel = ctx.channel();
    ChannelOutboundBuffer outboundBuffer = channel.unsafe().outboundBuffer();
    long pendingBytes = outboundBuffer != null ? outboundBuffer.totalPendingWriteBytes() : -1;
    LOGGER.error("Request write to server: {} did not complete within {}ms ({} bytes pending, writable: {}), closing "
        + "the channel as the socket is stalled or half-open", _serverRoutingInstance, _timeoutMs, pendingBytes,
        channel.isWritable());
    BrokerMetrics.get().addMeteredGlobalValue(BrokerMeter.NETTY_CONNECTION_WRITE_TIMEOUTS, 1);
    ctx.close();
  }
}
