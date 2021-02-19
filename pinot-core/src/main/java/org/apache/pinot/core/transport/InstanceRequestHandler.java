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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableImplV2;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.spi.utils.BytesUtils;
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
    final long queryArrivalTimeMs = System.currentTimeMillis();
    final int requestSize = msg.readableBytes();
    _serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES, 1);
    _serverMetrics.addMeteredGlobalValue(ServerMeter.NETTY_CONNECTION_BYTES_RECEIVED, requestSize);

    InstanceRequest instanceRequest = new InstanceRequest();
    ServerQueryRequest queryRequest;
    byte[] requestBytes = new byte[requestSize];

    try {
      // parse netty request into a ServerQueryRequest object for execution.
      msg.readBytes(requestBytes);
      _deserializer.deserialize(instanceRequest, requestBytes);
      queryRequest = new ServerQueryRequest(instanceRequest, _serverMetrics, queryArrivalTimeMs);
      queryRequest.getTimerContext().startNewPhaseTimer(ServerQueryPhase.REQUEST_DESERIALIZATION, queryArrivalTimeMs)
          .stopAndRecord();
    } catch (Exception e) {
      LOGGER.error("Caught exception while creating query request: {}", BytesUtils.toHexString(requestBytes), e);
      sendErrorResponse(ctx, instanceRequest.getRequestId(), queryArrivalTimeMs, new DataTableImplV2(), e);
      return;
    }

    // Submit query for execution and register callback for execution results.
    Futures.addCallback(_queryScheduler.submit(queryRequest), new FutureCallback<byte[]>() {
      @Override
      public void onSuccess(@Nullable byte[] responseBytes) {
        if (responseBytes != null) {
          // We have a query response (either serialized query results or serialized error message).
          sendResponse(ctx, queryArrivalTimeMs, responseBytes);
        } else {
          // response should never be null; otherwise, it will cause broker to wait until timeout.
          sendErrorResponse(ctx, queryRequest.getRequestId(), queryArrivalTimeMs, new DataTableImplV2(),
              new Exception("Null query response."));
        }
      }

      @Override
      public void onFailure(Throwable t) {
        // Query execution failed.
        LOGGER.error("Caught exception while processing instance request", t);
        sendErrorResponse(ctx, instanceRequest.getRequestId(), queryArrivalTimeMs, new DataTableImplV2(),
            new Exception(t));
      }
    }, MoreExecutors.directExecutor());
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    // This will happen only if there is an unexpected "Throwable" event in channelRead0 function
    // which has not been caught and handled yet.
    LOGGER.error("Caught exception while fetching instance request", cause);
    sendErrorResponse(ctx, 0, System.currentTimeMillis(), new DataTableImplV2(), new Exception(cause));
  }

  /**
   * Send an exception back to broker as response to the query request.
   */
  private void sendErrorResponse(ChannelHandlerContext ctx, long requestId, long queryArrivalTimeMs,
      DataTable dataTable, Exception e) {
    try {
      Map<String, String> dataTableMetadata = dataTable.getMetadata();
      dataTableMetadata.put(DataTable.REQUEST_ID_METADATA_KEY, Long.toString(requestId));

      dataTable.addException(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
      byte[] serializedDataTable = dataTable.toBytes();
      sendResponse(ctx, queryArrivalTimeMs, serializedDataTable);

      _serverMetrics.addMeteredGlobalValue(ServerMeter.QUERY_EXECUTION_EXCEPTIONS, 1);
    } catch (Exception ex2) {
      // ignore this exception since we are already dealing with a higher level exceptions.
    }
  }

  /**
   * Send a response (either query results or exception) back to broker as response to the query request.
   */
  private void sendResponse(ChannelHandlerContext ctx, long queryArrivalTimeMs, byte[] serializedDataTable) {
    long sendResponseStartTimeMs = System.currentTimeMillis();
    int queryProcessingTimeMs = (int) (sendResponseStartTimeMs - queryArrivalTimeMs);
    ctx.writeAndFlush(Unpooled.wrappedBuffer(serializedDataTable)).addListener(f -> {
      long sendResponseEndTimeMs = System.currentTimeMillis();
      int sendResponseLatencyMs = (int) (sendResponseEndTimeMs - sendResponseStartTimeMs);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.NETTY_CONNECTION_RESPONSES_SENT, 1);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.NETTY_CONNECTION_BYTES_SENT, serializedDataTable.length);
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
