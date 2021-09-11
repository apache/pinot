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
import org.apache.pinot.common.utils.DataTable.MetadataKey;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
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
    String tableNameWithType = null;

    try {
      // Put all code inside try block to catch all exceptions.
      int requestSize = msg.readableBytes();

      instanceRequest = new InstanceRequest();
      ServerQueryRequest queryRequest;
      requestBytes = new byte[requestSize];

      queryArrivalTimeMs = System.currentTimeMillis();
      _serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES, 1);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.NETTY_CONNECTION_BYTES_RECEIVED, requestSize);

      // Parse instance request into ServerQueryRequest.
      msg.readBytes(requestBytes);
      _deserializer.deserialize(instanceRequest, requestBytes);
      queryRequest = new ServerQueryRequest(instanceRequest, _serverMetrics, queryArrivalTimeMs);
      queryRequest.getTimerContext().startNewPhaseTimer(ServerQueryPhase.REQUEST_DESERIALIZATION, queryArrivalTimeMs)
          .stopAndRecord();
      tableNameWithType = queryRequest.getTableNameWithType();

      // Submit query for execution and register callback for execution results.
      Futures.addCallback(_queryScheduler.submit(queryRequest),
          createCallback(ctx, tableNameWithType, queryArrivalTimeMs, instanceRequest, queryRequest),
          MoreExecutors.directExecutor());
    } catch (Exception e) {
      if (e instanceof TException) {
        // Deserialization exception
        _serverMetrics.addMeteredGlobalValue(ServerMeter.REQUEST_DESERIALIZATION_EXCEPTIONS, 1);
      }

      // Send error response
      String hexString = requestBytes != null ? BytesUtils.toHexString(requestBytes) : "";
      long reqestId = instanceRequest != null ? instanceRequest.getRequestId() : 0;
      LOGGER.error("Exception while processing instance request: {}", hexString, e);
      sendErrorResponse(ctx, reqestId, tableNameWithType, queryArrivalTimeMs, DataTableBuilder.getEmptyDataTable(), e);
    }
  }

  private FutureCallback<byte[]> createCallback(ChannelHandlerContext ctx, String tableNameWithType,
      long queryArrivalTimeMs, InstanceRequest instanceRequest, ServerQueryRequest queryRequest) {
    return new FutureCallback<byte[]>() {
      @Override
      public void onSuccess(@Nullable byte[] responseBytes) {
        if (responseBytes != null) {
          // responseBytes contains either query results or exception.
          sendResponse(ctx, queryRequest.getTableNameWithType(), queryArrivalTimeMs, responseBytes);
        } else {
          // Send exception response.
          sendErrorResponse(ctx, queryRequest.getRequestId(), tableNameWithType, queryArrivalTimeMs,
              DataTableBuilder.getEmptyDataTable(), new Exception("Null query response."));
        }
      }

      @Override
      public void onFailure(Throwable t) {
        // Send exception response.
        LOGGER.error("Exception while processing instance request", t);
        sendErrorResponse(ctx, instanceRequest.getRequestId(), tableNameWithType, queryArrivalTimeMs,
            DataTableBuilder.getEmptyDataTable(), new Exception(t));
      }
    };
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    // All exceptions should be caught and handled in channelRead0 method. This is a fallback method that
    // will only be called if for some remote reason we are unable to handle exceptions in channelRead0.
    String message = "Unhandled Exception in " + getClass().getCanonicalName();
    LOGGER.error(message, cause);
    sendErrorResponse(ctx, 0, null, System.currentTimeMillis(), DataTableBuilder.getEmptyDataTable(),
        new Exception(message, cause));
  }

  /**
   * Send an exception back to broker as response to the query request.
   */
  private void sendErrorResponse(ChannelHandlerContext ctx, long requestId, String tableNameWithType,
      long queryArrivalTimeMs, DataTable dataTable, Exception e) {
    try {
      Map<String, String> dataTableMetadata = dataTable.getMetadata();
      dataTableMetadata.put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
      dataTable.addException(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
      byte[] serializedDataTable = dataTable.toBytes();
      sendResponse(ctx, tableNameWithType, queryArrivalTimeMs, serializedDataTable);
    } catch (Exception exception) {
      LOGGER.error("Exception while sending query processing error to Broker.", exception);
    } finally {
      // Log query processing exception
      LOGGER.error("Query processing error: ", e);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.QUERY_EXECUTION_EXCEPTIONS, 1);
    }
  }

  /**
   * Send a response (either query results or exception) back to broker as response to the query request.
   */
  private void sendResponse(ChannelHandlerContext ctx, String tableNameWithType, long queryArrivalTimeMs,
      byte[] serializedDataTable) {
    long sendResponseStartTimeMs = System.currentTimeMillis();
    int queryProcessingTimeMs = (int) (sendResponseStartTimeMs - queryArrivalTimeMs);
    ctx.writeAndFlush(Unpooled.wrappedBuffer(serializedDataTable)).addListener(f -> {
      long sendResponseEndTimeMs = System.currentTimeMillis();
      int sendResponseLatencyMs = (int) (sendResponseEndTimeMs - sendResponseStartTimeMs);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.NETTY_CONNECTION_RESPONSES_SENT, 1);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.NETTY_CONNECTION_BYTES_SENT, serializedDataTable.length);
      _serverMetrics.addTimedTableValue(tableNameWithType, ServerTimer.NETTY_CONNECTION_SEND_RESPONSE_LATENCY,
          sendResponseLatencyMs, TimeUnit.MILLISECONDS);

      int totalQueryTimeMs = (int) (sendResponseEndTimeMs - queryArrivalTimeMs);
      if (totalQueryTimeMs > SLOW_QUERY_LATENCY_THRESHOLD_MS) {
        LOGGER.info("Slow query: request handler processing time: {}, send response latency: {}, total time to handle "
            + "request: {}", queryProcessingTimeMs, sendResponseLatencyMs, totalQueryTimeMs);
      }
    });
  }
}
