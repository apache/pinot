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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.utils.BytesUtils;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code InstanceRequestHandler} is the Netty inbound handler on Pinot Server side to handle the serialized
 * instance requests sent from Pinot Broker.
 */
public class InstanceRequestHandler extends SimpleChannelInboundHandler<ByteBuf> {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceRequestHandler.class);

  // TODO: make it configurable
  private static final int SLOW_QUERY_LATENCY_THRESHOLD_MS = 100;

  private final TDeserializer _deserializer = new TDeserializer(new TCompactProtocol.Factory());
  private final QueryScheduler _queryScheduler;
  private final ServerMetrics _serverMetrics;

  public InstanceRequestHandler(QueryScheduler queryScheduler, ServerMetrics serverMetrics) {
    _queryScheduler = queryScheduler;
    _serverMetrics = serverMetrics;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
    long queryArrivalTimeMs = System.currentTimeMillis();
    _serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES, 1);
    int requestSize = msg.readableBytes();
    _serverMetrics.addMeteredGlobalValue(ServerMeter.NETTY_CONNECTION_BYTES_RECEIVED, requestSize);
    byte[] requestBytes = new byte[requestSize];
    msg.readBytes(requestBytes);

    InstanceRequest instanceRequest = new InstanceRequest();
    try {
      _deserializer.deserialize(instanceRequest, requestBytes);
    } catch (Exception e) {
      LOGGER
          .error("Caught exception while deserializing the instance request: {}", BytesUtils.toHexString(requestBytes),
              e);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.REQUEST_DESERIALIZATION_EXCEPTIONS, 1);
      return;
    }

    ServerQueryRequest queryRequest = new ServerQueryRequest(instanceRequest, _serverMetrics, queryArrivalTimeMs);
    queryRequest.getTimerContext().startNewPhaseTimer(ServerQueryPhase.REQUEST_DESERIALIZATION, queryArrivalTimeMs)
        .stopAndRecord();

    // NOTE: executor must be provided as addCallback(future, callback) is removed from newer guava version
    Futures.addCallback(_queryScheduler.submit(queryRequest), new FutureCallback<byte[]>() {
      @Override
      public void onSuccess(@Nullable byte[] responseBytes) {
        // NOTE: response bytes can be null if data table serialization throws exception
        if (responseBytes != null) {
          long sendResponseStartTimeMs = System.currentTimeMillis();
          int queryProcessingTimeMs = (int) (sendResponseStartTimeMs - queryArrivalTimeMs);
          ctx.writeAndFlush(Unpooled.wrappedBuffer(responseBytes)).addListener(f -> {
            long sendResponseEndTimeMs = System.currentTimeMillis();
            int sendResponseLatencyMs = (int) (sendResponseEndTimeMs - sendResponseStartTimeMs);
            _serverMetrics.addMeteredGlobalValue(ServerMeter.NETTY_CONNECTION_RESPONSES_SENT, 1);
            _serverMetrics.addMeteredGlobalValue(ServerMeter.NETTY_CONNECTION_BYTES_SENT, responseBytes.length);
            _serverMetrics.addTimedValue(ServerTimer.NETTY_CONNECTION_SEND_RESPONSE_LATENCY, sendResponseLatencyMs,
                TimeUnit.MILLISECONDS);

            int totalQueryTimeMs = (int) (sendResponseEndTimeMs - queryArrivalTimeMs);
            if (totalQueryTimeMs > SLOW_QUERY_LATENCY_THRESHOLD_MS) {
              LOGGER.info(
                  "Slow query: request handler processing time: {}, send response latency: {}, total time to handle request: {}",
                  queryProcessingTimeMs, sendResponseLatencyMs, totalQueryTimeMs);
            }
          });
        }
      }

      @Override
      public void onFailure(Throwable t) {
        LOGGER.error("Caught exception while processing instance request", t);
        _serverMetrics.addMeteredGlobalValue(ServerMeter.UNCAUGHT_EXCEPTIONS, 1);
      }
    }, MoreExecutors.directExecutor());
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOGGER.error("Caught exception while fetching instance request", cause);
    _serverMetrics.addMeteredGlobalValue(ServerMeter.REQUEST_FETCH_EXCEPTIONS, 1);
  }
}
