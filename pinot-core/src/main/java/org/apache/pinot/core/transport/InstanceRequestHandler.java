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

  /**
   * Always return a response even when query execution throws exception; otherwise, broker
   * will keep waiting until timeout.
   */
  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
    long queryArrivalTimeMs = 0;
    InstanceRequest instanceRequest = null;
    byte[] requestBytes = null;

    try {
      // all code inside try code, so that we are able to catch all exceptions.
      final int requestSize = msg.readableBytes();

      instanceRequest = new InstanceRequest();
      ServerQueryRequest queryRequest;
      requestBytes = new byte[requestSize];

      queryArrivalTimeMs = System.currentTimeMillis();
      _serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES, 1);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.NETTY_CONNECTION_BYTES_RECEIVED, requestSize);

      // parse instance request into a query result.
      msg.readBytes(requestBytes);
      _deserializer.deserialize(instanceRequest, requestBytes);
      queryRequest = new ServerQueryRequest(instanceRequest, _serverMetrics, queryArrivalTimeMs);
      queryRequest.getTimerContext().startNewPhaseTimer(ServerQueryPhase.REQUEST_DESERIALIZATION, queryArrivalTimeMs)
          .stopAndRecord();

      // Submit query for execution and register callback for execution results.
      Futures.addCallback(_queryScheduler.submit(queryRequest),
          createCallback(ctx, queryArrivalTimeMs, instanceRequest, queryRequest), MoreExecutors.directExecutor());
    } catch (Exception e) {
      String hexString = requestBytes != null ? BytesUtils.toHexString(requestBytes) : "";
      long reqestId = instanceRequest != null ? instanceRequest.getRequestId() : 0;
      LOGGER.error("Exception while deserializing the instance request: {}", hexString, e);
      sendResponse(ctx, reqestId, queryArrivalTimeMs, new DataTableImplV2(), e);
    }
  }
  
  private FutureCallback<byte[]> createCallback(ChannelHandlerContext ctx, long queryArrivalTimeMs,
      InstanceRequest instanceRequest, ServerQueryRequest queryRequest) {
    return new FutureCallback<byte[]>() {
      @Override
      public void onSuccess(@Nullable byte[] responseBytes) {
        if (responseBytes != null) {
          // responseBytes contains either query results or exception.
          sendResponse(ctx, queryArrivalTimeMs, responseBytes);
        } else {
          // Send exception response.
          sendResponse(ctx, queryRequest.getRequestId(), queryArrivalTimeMs, new DataTableImplV2(),
              new Exception("Null query response."));
        }
      }

      @Override
      public void onFailure(Throwable t) {
        // Send exception response.
        LOGGER.error("Exception while processing instance request", t);
        sendResponse(ctx, instanceRequest.getRequestId(), queryArrivalTimeMs, new DataTableImplV2(), new Exception(t));
      }
    };
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    // Since we do not know the requestId of the original request here, there is no way for Broker to know which query
    // request this response belongs to. Hence, Broker will continue to wait for the original request until time out.
    // To prevent Broker from waiting unnecessarily, try to catch and handle all exceptions in channelRead0 method so
    // that this function is never called.
    LOGGER.error("Unhandled Exception in " + getClass().getCanonicalName(), cause);
    sendResponse(ctx, 0, System.currentTimeMillis(), new DataTableImplV2(), new Exception(cause));
  }

  /**
   * Send an exception back to broker as response to the query request.
   */
  private void sendResponse(ChannelHandlerContext ctx, long requestId, long queryArrivalTimeMs, DataTable dataTable,
      Exception e) {
    try {
      Map<String, String> dataTableMetadata = dataTable.getMetadata();
      dataTableMetadata.put(DataTable.REQUEST_ID_METADATA_KEY, Long.toString(requestId));

      dataTable.addException(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
      byte[] serializedDataTable = dataTable.toBytes();
      sendResponse(ctx, queryArrivalTimeMs, serializedDataTable);

      _serverMetrics.addMeteredGlobalValue(ServerMeter.QUERY_EXECUTION_EXCEPTIONS, 1);
    } catch (Throwable t) {
      // Ignore since we are already handling a higher level exceptions.
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
