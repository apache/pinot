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
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code InstanceRequestHandler} is the Netty inbound handler on Pinot Server side to handle the serialized
 * instance requests sent from Pinot Broker.
 */
@ChannelHandler.Sharable
public class InstanceRequestHandler extends SimpleChannelInboundHandler<ByteBuf> {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceRequestHandler.class);

  // TODO: make it configurable
  private static final int SLOW_QUERY_LATENCY_THRESHOLD_MS = 100;
  private static final int LARGE_RESPONSE_SIZE_THRESHOLD_BYTES = 100 * 1024 * 1024; // 100 MB

  private final String _instanceName;
  private final ThreadLocal<TDeserializer> _deserializer;
  private final QueryScheduler _queryScheduler;
  private final ServerMetrics _serverMetrics;
  private final AccessControl _accessControl;
  private final Map<String, Future<byte[]>> _queryFuturesById;

  public InstanceRequestHandler(String instanceName, PinotConfiguration config, QueryScheduler queryScheduler,
      ServerMetrics serverMetrics, AccessControl accessControl) {
    _instanceName = instanceName;
    _queryScheduler = queryScheduler;
    _serverMetrics = serverMetrics;
    _accessControl = accessControl;
    _deserializer = ThreadLocal.withInitial(() -> {
      try {
        return new TDeserializer(new TCompactProtocol.Factory());
      } catch (TTransportException e) {
        throw new RuntimeException("Failed to initialize Thrift Deserializer", e);
      }
    });

    if (Boolean.parseBoolean(config.getProperty(CommonConstants.Server.CONFIG_OF_ENABLE_QUERY_CANCELLATION))) {
      _queryFuturesById = new ConcurrentHashMap<>();
      LOGGER.info("Enable query cancellation");
    } else {
      _queryFuturesById = null;
    }
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
      throws Exception {
    super.userEventTriggered(ctx, evt);
    if (evt instanceof SslHandshakeCompletionEvent) {
      if (!_accessControl.isAuthorizedChannel(ctx)) {
        ctx.disconnect();
        LOGGER.error("Exception while processing instance request: Unauthorized access to pinot-server");
      }
    }
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
      _deserializer.get().deserialize(instanceRequest, requestBytes);
      queryRequest = new ServerQueryRequest(instanceRequest, _serverMetrics, queryArrivalTimeMs);
      queryRequest.getTimerContext().startNewPhaseTimer(ServerQueryPhase.REQUEST_DESERIALIZATION, queryArrivalTimeMs)
          .stopAndRecord();
      tableNameWithType = queryRequest.getTableNameWithType();
      submitQuery(queryRequest, ctx, tableNameWithType, queryArrivalTimeMs, instanceRequest);
    } catch (Exception e) {
      if (e instanceof TException) {
        // Deserialization exception
        _serverMetrics.addMeteredGlobalValue(ServerMeter.REQUEST_DESERIALIZATION_EXCEPTIONS, 1);
      }

      // Send error response
      String hexString = requestBytes != null ? BytesUtils.toHexString(requestBytes) : "";
      long requestId = instanceRequest != null ? instanceRequest.getRequestId() : 0;
      LOGGER.error("Exception while processing instance request: {}", hexString, e);
      sendErrorResponse(ctx, requestId, tableNameWithType, queryArrivalTimeMs,
          DataTableBuilderFactory.getEmptyDataTable(), e);
    }
  }

  /**
   * Submit query for execution and register callback for execution results.
   * If query cancellation is enabled, the query future is tracked as well.
   */
  @VisibleForTesting
  void submitQuery(ServerQueryRequest queryRequest, ChannelHandlerContext ctx, String tableNameWithType,
      long queryArrivalTimeMs, InstanceRequest instanceRequest) {
    ListenableFuture<byte[]> future = _queryScheduler.submit(queryRequest);
    if (_queryFuturesById != null) {
      String queryId = queryRequest.getQueryId();
      // Track the running query for cancellation.
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Keep track of running query: {}", queryId);
      }
      _queryFuturesById.put(queryId, future);
    }
    Futures.addCallback(future,
        createCallback(ctx, tableNameWithType, queryArrivalTimeMs, instanceRequest, queryRequest),
        MoreExecutors.directExecutor());
  }

  private FutureCallback<byte[]> createCallback(ChannelHandlerContext ctx, String tableNameWithType,
      long queryArrivalTimeMs, InstanceRequest instanceRequest, ServerQueryRequest queryRequest) {
    return new FutureCallback<byte[]>() {
      @Override
      public void onSuccess(@Nullable byte[] responseBytes) {
        if (_queryFuturesById != null) {
          String queryId = queryRequest.getQueryId();
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Remove track of running query: {} on success", queryId);
          }
          _queryFuturesById.remove(queryId);
        }
        if (responseBytes != null) {
          // responseBytes contains either query results or exception.
          sendResponse(ctx, queryRequest.getTableNameWithType(), queryArrivalTimeMs, responseBytes);
        } else {
          // Send exception response.
          sendErrorResponse(ctx, queryRequest.getRequestId(), tableNameWithType, queryArrivalTimeMs,
              DataTableBuilderFactory.getEmptyDataTable(), new Exception("Null query response."));
        }
      }

      @Override
      public void onFailure(Throwable t) {
        if (_queryFuturesById != null) {
          String queryId = queryRequest.getQueryId();
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Remove track of running query: {} on failure", queryId);
          }
          _queryFuturesById.remove(queryId);
        }
        // Send exception response.
        Exception e;
        if (t instanceof Exception) {
          e = (Exception) t;
          if (e instanceof CancellationException) {
            LOGGER.info("Query: {} got cancelled", queryRequest.getQueryId());
          } else {
            LOGGER.error("Exception while processing instance request", e);
          }
        } else {
          LOGGER.error("Error while processing instance request", t);
          e = new Exception(t);
        }
        sendErrorResponse(ctx, instanceRequest.getRequestId(), tableNameWithType, queryArrivalTimeMs,
            DataTableBuilderFactory.getEmptyDataTable(), e);
      }
    };
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    // All exceptions should be caught and handled in channelRead0 method. This is a fallback method that
    // will only be called if for some remote reason we are unable to handle exceptions in channelRead0.
    String message = "Unhandled Exception in " + getClass().getCanonicalName();
    LOGGER.error(message, cause);
    sendErrorResponse(ctx, 0, null, System.currentTimeMillis(), DataTableBuilderFactory.getEmptyDataTable(),
        new Exception(message, cause));
  }

  /**
   * Cancel a query as identified by the queryId. This method is non-blocking and the query may still run for a while
   * after calling this method. This method can be called multiple times.
   *
   * @param queryId a unique Id to find the query
   * @return true if a running query exists for the given queryId.
   */
  public boolean cancelQuery(String queryId) {
    Preconditions.checkState(_queryFuturesById != null, "Query cancellation is not enabled on server");
    // Keep the future as it'll be cleaned up by the thread executing the query.
    Future<byte[]> future = _queryFuturesById.get(queryId);
    if (future == null) {
      return false;
    }
    boolean done = future.isDone();
    if (!done) {
      future.cancel(true);
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Cancelled query: {} that's done: {}", queryId, done);
    }
    return true;
  }

  /**
   * @return list of ids of the queries currently running on the server.
   */
  public Set<String> getRunningQueryIds() {
    Preconditions.checkState(_queryFuturesById != null, "Query cancellation is not enabled on server");
    return new HashSet<>(_queryFuturesById.keySet());
  }

  /**
   * Send an exception back to broker as response to the query request.
   */
  private void sendErrorResponse(ChannelHandlerContext ctx, long requestId, String tableNameWithType,
      long queryArrivalTimeMs, DataTable dataTable, Exception e) {
    boolean cancelled = (e instanceof CancellationException);
    try {
      Map<String, String> dataTableMetadata = dataTable.getMetadata();
      dataTableMetadata.put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
      if (cancelled) {
        dataTable.addException(QueryException.getException(QueryException.QUERY_CANCELLATION_ERROR,
            "Query cancelled on: " + _instanceName + " " + e));
      } else {
        dataTable.addException(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR,
            "Query execution error on: " + _instanceName + " " + e));
      }
      byte[] serializedDataTable = dataTable.toBytes();
      sendResponse(ctx, tableNameWithType, queryArrivalTimeMs, serializedDataTable);
    } catch (Exception exception) {
      LOGGER.error("Exception while sending query processing error to Broker.", exception);
    } finally {
      // Log query processing exception if not due to cancellation
      if (!cancelled) {
        LOGGER.error("Query processing error: ", e);
      }
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
      if (serializedDataTable.length > LARGE_RESPONSE_SIZE_THRESHOLD_BYTES) {
        LOGGER.warn("Large query: response size in bytes: {}, table name {}",
            serializedDataTable.length, tableNameWithType);
        ServerMetrics.get().addMeteredTableValue(tableNameWithType, ServerMeter.LARGE_QUERY_RESPONSES_SENT, 1);
      }
    });
  }
}
