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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.spi.exception.QueryCancelledException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handling netty direct memory OOM. In this case there is a great chance that multiple channels are receiving
 * large data tables from servers concurrently. We want to close all channels to servers to proactively release
 * the direct memory, because the execution of netty threads can all block in allocating direct memory, in which case
 * no one will reach channelRead0.
 */
public class DirectOOMHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataTableHandler.class);
  private static final AtomicBoolean DIRECT_OOM_SHUTTING_DOWN = new AtomicBoolean(false);
  private final QueryRouter _queryRouter;
  private final ServerRoutingInstance _serverRoutingInstance;
  private final ConcurrentHashMap<ServerRoutingInstance, ServerChannels.ServerChannel> _serverToChannelMap;
  private volatile boolean _silentShutDown = false;

  public DirectOOMHandler(QueryRouter queryRouter, ServerRoutingInstance serverRoutingInstance,
      ConcurrentHashMap<ServerRoutingInstance, ServerChannels.ServerChannel> serverToChannelMap) {
    _queryRouter = queryRouter;
    _serverRoutingInstance = serverRoutingInstance;
    _serverToChannelMap = serverToChannelMap;
  }

  public void setSilentShutDown() {
    _silentShutDown = true;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    // if we are shutting down channels due to direct memory OOM, we short circuit the channel inactive
    if (_silentShutDown) {
      return;
    }
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    // catch direct memory oom here
    if (cause instanceof OutOfMemoryError && cause.getMessage().contains("Direct buffer")) {
      // only one thread can get here and do the shutdown
      if (DIRECT_OOM_SHUTTING_DOWN.compareAndSet(false, true)) {
        try {
          LOGGER.error("Closing ALL channels to servers, as we are running out of direct memory "
              + "while receiving response from {}", _serverRoutingInstance, cause);
          // close all channels to servers
          _serverToChannelMap.keySet().forEach(serverRoutingInstance -> {
            ServerChannels.ServerChannel removed = _serverToChannelMap.remove(serverRoutingInstance);
            removed.closeChannel();
            removed.setSilentShutdown();
          });
          _queryRouter.markServerDown(_serverRoutingInstance,
              new QueryCancelledException("Query cancelled as broker is out of direct memory"));
          BrokerMetrics.get().addMeteredGlobalValue(BrokerMeter.DIRECT_MEMORY_OOM, 1L);
        } catch (Exception e) {
          LOGGER.error("Caught exception while handling direct memory OOM", e);
        } finally {
          DIRECT_OOM_SHUTTING_DOWN.set(false);
        }
      } else {
        LOGGER.warn("Caught direct memory OOM, but another thread is already handling it", cause);
      }
    } else {
      ctx.fireExceptionCaught(cause);
    }
  }
}
