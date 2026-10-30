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
package org.apache.pinot.core.query.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAccumulator;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.logger.ServerQueryLogger;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.TimerContext;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Abstract class providing common scheduler functionality
/// including query runner and query worker pool
public abstract class QueryScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryScheduler.class);

  protected final PinotConfiguration _config;
  protected final String _instanceId;
  protected final QueryExecutor _queryExecutor;
  protected final ThreadAccountant _threadAccountant;
  protected final LongAccumulator _latestQueryTime;
  protected final ResourceManager _resourceManager;
  protected final ServerMetrics _serverMetrics = ServerMetrics.get();
  protected final ServerQueryLogger _queryLogger = ServerQueryLogger.getInstance();

  protected volatile boolean _isRunning = false;

  /// Constructor to initialize QueryScheduler
  /// @param queryExecutor QueryExecutor engine to use
  /// @param resourceManager for managing server thread resources
  public QueryScheduler(PinotConfiguration config, String instanceId, QueryExecutor queryExecutor,
      ThreadAccountant threadAccountant, LongAccumulator latestQueryTime, ResourceManager resourceManager) {
    _config = config;
    _instanceId = instanceId;
    _queryExecutor = queryExecutor;
    _threadAccountant = threadAccountant;
    _latestQueryTime = latestQueryTime;
    _resourceManager = resourceManager;
    // submit() and submitResponse() call each other by default. Fail here instead of with a StackOverflowError on the
    // first query.
    Preconditions.checkState(overrides("submit") || overrides("submitResponse"),
        "%s must override submitResponse(ServerQueryRequest) or the deprecated submit(ServerQueryRequest)",
        getClass().getName());
  }

  private boolean overrides(String methodName) {
    try {
      return getClass().getMethod(methodName, ServerQueryRequest.class).getDeclaringClass() != QueryScheduler.class;
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(e);
    }
  }

  public ResourceManager getResourceManager() {
    return _resourceManager;
  }

  /// Submit a query for execution. The query will be scheduled for execution as per the scheduling algorithm
  /// @param queryRequest query to schedule for execution
  /// @return Listenable future for query result representing serialized response. It is possible that the
  ///    future may return immediately or be scheduled for execution at a later time.
  /// @deprecated Use [#submitResponse(ServerQueryRequest)], which does not copy the response into one array. The
  ///    default implementation copies the result of [#submitResponse(ServerQueryRequest)]. Subclasses must override
  ///    at least one of the two methods. The server calls [#submitResponse(ServerQueryRequest)], so a subclass of a
  ///    scheduler that already overrides [#submitResponse(ServerQueryRequest)] (all the schedulers in this package)
  ///    must override that method too: an override of this method alone is not called.
  @Deprecated
  public ListenableFuture<byte[]> submit(ServerQueryRequest queryRequest) {
    return Futures.transform(submitResponse(queryRequest), QueryScheduler::toByteArray, MoreExecutors.directExecutor());
  }

  /// Submit a query for execution. The query will be scheduled for execution as per the scheduling algorithm.
  ///
  /// The serialized response can be split into several buffers, so a large response does not need one large array.
  ///
  /// The default implementation wraps the result of the deprecated [#submit(ServerQueryRequest)], so that subclasses
  /// that only override that method keep working. Subclasses must override at least one of the two methods.
  ///
  /// @param queryRequest query to schedule for execution
  /// @return Listenable future for the serialized response. It is possible that the future may return immediately or
  ///    be scheduled for execution at a later time.
  public ListenableFuture<DataBuffer> submitResponse(ServerQueryRequest queryRequest) {
    return Futures.transform(submit(queryRequest), QueryScheduler::wrap, MoreExecutors.directExecutor());
  }

  /// Query scheduler name for logging
  public abstract String name();

  /// Start query scheduler thread
  public void start() {
    _isRunning = true;
  }

  /// stop the scheduler and shutdown services
  public void stop() {
    // don't stop resourcemanager yet...we need to wait for all running queries to finish
    _isRunning = false;
  }

  /// Create a future task for the query
  /// @param queryRequest incoming query request
  /// @param executorService executor service to use for parallelizing query. This is passed to the QueryExecutor.
  ///                        This is not the executor that runs the returned future task but the one that can be
  ///                        internally used to parallelize query processing.
  /// @return Future task that can be scheduled for execution on an ExecutorService. Ideally, this future
  /// should be executed on a different executor service than `executorService` to avoid deadlock.
  protected ListenableFutureTask<DataBuffer> createResponseFutureTask(ServerQueryRequest queryRequest,
      ExecutorService executorService) {
    return ListenableFutureTask.create(() -> processQueryAndSerializeResponse(queryRequest, executorService));
  }

  /// @deprecated Use [#createResponseFutureTask(ServerQueryRequest, ExecutorService)], which does not copy the
  ///    response into one array.
  @Deprecated
  protected ListenableFutureTask<byte[]> createQueryFutureTask(ServerQueryRequest queryRequest,
      ExecutorService executorService) {
    return ListenableFutureTask.create(() -> processQueryAndSerialize(queryRequest, executorService));
  }

  /// @deprecated Use [#processQueryAndSerializeResponse(ServerQueryRequest, ExecutorService)], which does not copy
  ///    the response into one array.
  @Deprecated
  @Nullable
  protected byte[] processQueryAndSerialize(ServerQueryRequest queryRequest, ExecutorService executorService) {
    return toByteArray(processQueryAndSerializeResponse(queryRequest, executorService));
  }

  /// Process query and serialize response
  /// @param queryRequest incoming query request
  /// @param executorService Executor service to use for parallelizing query processing
  /// @return serialized query response
  @Nullable
  protected DataBuffer processQueryAndSerializeResponse(ServerQueryRequest queryRequest,
      ExecutorService executorService) {
    QueryExecutionContext executionContext = queryRequest.toExecutionContext(_instanceId);
    _latestQueryTime.accumulate(executionContext.getStartTimeMs());
    try (QueryThreadContext ignore = QueryThreadContext.open(executionContext, _threadAccountant)) {
      InstanceResponseBlock instanceResponse;
      try {
        instanceResponse =
            _queryExecutor.execute(queryRequest, QueryThreadContext.contextAwareExecutorService(executorService));
      } catch (Exception e) {
        LOGGER.error("Encountered exception while processing requestId {} from broker {}", queryRequest.getRequestId(),
            queryRequest.getBrokerId(), e);
        // For not handled exceptions
        _serverMetrics.addMeteredGlobalValue(ServerMeter.UNCAUGHT_EXCEPTIONS, 1);
        instanceResponse = new InstanceResponseBlock();
        instanceResponse.addException(QueryErrorCode.INTERNAL, e.getMessage());
      }

      long requestId = executionContext.getRequestId();
      String queryId = executionContext.getCid();
      String workloadName = executionContext.getWorkloadName();
      Map<String, String> responseMetadata = instanceResponse.getResponseMetadata();
      responseMetadata.put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
      responseMetadata.put(MetadataKey.QUERY_ID.getName(), queryId);
      responseMetadata.put(MetadataKey.WORKLOAD_NAME.getName(), workloadName);
      DataBuffer response = serializeResponse(queryRequest, instanceResponse);

      // Log the statistics
      if (_queryLogger != null) {
        _queryLogger.logQuery(queryRequest, instanceResponse, name());
      }

      // TODO: Perform this check sooner during the serialization of DataTable.
      Map<String, String> queryOptions = queryRequest.getQueryContext().getQueryOptions();
      if (response != null) {
        long responseSizeBytes = response.size();
        String tableNameWithType = queryRequest.getTableNameWithType();
        _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.QUERY_RESPONSE_SIZE, responseSizeBytes);
        Long maxResponseSizeBytes = QueryOptionsUtils.getMaxServerResponseSizeBytes(queryOptions);
        if (maxResponseSizeBytes != null && responseSizeBytes > maxResponseSizeBytes) {
          String errMsg =
              "Serialized query response size " + responseSizeBytes + " exceeds threshold " + maxResponseSizeBytes
                  + " for requestId " + requestId + " from broker " + queryRequest.getBrokerId();
          LOGGER.error(errMsg);
          _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.LARGE_QUERY_RESPONSE_SIZE_EXCEPTIONS, 1);

          instanceResponse = new InstanceResponseBlock();
          instanceResponse.addException(QueryErrorCode.QUERY_CANCELLATION, errMsg);
          instanceResponse.addMetadata(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
          instanceResponse.addMetadata(MetadataKey.QUERY_ID.getName(), queryId);
          instanceResponse.addMetadata(MetadataKey.WORKLOAD_NAME.getName(), workloadName);
          response = serializeResponse(queryRequest, instanceResponse);
        }
      }

      return response;
    }
  }

  /// Serialize the instance response for query request
  /// @param queryRequest Server query request for which response is serialized
  /// @param instanceResponse instance response to serialize
  /// @return serialized response
  @Nullable
  private DataBuffer serializeResponse(ServerQueryRequest queryRequest, InstanceResponseBlock instanceResponse) {
    TimerContext timerContext = queryRequest.getTimerContext();
    TimerContext.Timer responseSerializationTimer =
        timerContext.startNewPhaseTimer(ServerQueryPhase.RESPONSE_SERIALIZATION);
    long requestId = queryRequest.getRequestId();
    String brokerId = queryRequest.getBrokerId();

    DataBuffer response = null;
    try {
      response = instanceResponse.toDataTable().toDataBuffer();
    } catch (Exception e) {
      // First check terminate exception and use it as the response if exists. We want to return the termination reason
      // when query is explicitly terminated.
      QueryException queryException = QueryThreadContext.getTerminateException();
      // Do not log exception when query is explicitly terminated
      if (queryException == null) {
        if (e instanceof QueryException) {
          queryException = (QueryException) e;
          // TODO: Revisit if we should log exception here
          LOGGER.warn("Caught QueryException while serializing response for requestId: {}, brokerId: {}", requestId,
              brokerId, queryException);
        } else {
          _serverMetrics.addMeteredGlobalValue(ServerMeter.RESPONSE_SERIALIZATION_EXCEPTIONS, 1);
          LOGGER.error("Caught exception while serializing response for requestId: {}, brokerId: {}", requestId,
              brokerId, e);
          queryException = QueryErrorCode.INTERNAL.asException("Error serializing response", e);
        }
      }
      try {
        InstanceResponseBlock errorResponse = new InstanceResponseBlock(new ExceptionResultsBlock(queryException));
        errorResponse.addMetadata(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
        errorResponse.addMetadata(MetadataKey.QUERY_ID.getName(), queryRequest.getCid());
        String workloadName = QueryOptionsUtils.getWorkloadName(queryRequest.getQueryContext().getQueryOptions());
        errorResponse.addMetadata(MetadataKey.WORKLOAD_NAME.getName(), workloadName);
        response = errorResponse.toDataTable().toDataBuffer();
      } catch (Exception e1) {
        _serverMetrics.addMeteredGlobalValue(ServerMeter.RESPONSE_SERIALIZATION_EXCEPTIONS, 1);
        LOGGER.error("Caught exception while constructing error response for requestId: {}, brokerId: {}",
            requestId, brokerId, e1);
      }
    }

    responseSerializationTimer.stopAndRecord();
    timerContext.startNewPhaseTimer(ServerQueryPhase.TOTAL_QUERY_TIME, timerContext.getQueryArrivalTimeMs())
        .stopAndRecord();

    return response;
  }

  /// Error response future in case of internal error where query response is not available. This can happen if the
  /// query can not be executed.
  protected ListenableFuture<DataBuffer> errorResponse(ServerQueryRequest queryRequest, QueryErrorCode errorCode) {
    Map<String, String> queryOptions = queryRequest.getQueryContext().getQueryOptions();
    String workloadName = QueryOptionsUtils.getWorkloadName(queryOptions);
    InstanceResponseBlock instanceResponse = new InstanceResponseBlock();
    instanceResponse.addMetadata(MetadataKey.REQUEST_ID.getName(), Long.toString(queryRequest.getRequestId()));
    instanceResponse.addMetadata(MetadataKey.QUERY_ID.getName(), queryRequest.getCid());
    instanceResponse.addMetadata(MetadataKey.WORKLOAD_NAME.getName(), workloadName);
    instanceResponse.addException(errorCode, errorCode.getDefaultMessage());
    return Futures.immediateFuture(serializeResponse(queryRequest, instanceResponse));
  }

  protected ListenableFuture<DataBuffer> shuttingDownResponse(ServerQueryRequest queryRequest) {
    return errorResponse(queryRequest, QueryErrorCode.SERVER_SHUTTING_DOWN);
  }

  protected ListenableFuture<DataBuffer> outOfCapacityResponse(ServerQueryRequest queryRequest) {
    return errorResponse(queryRequest, QueryErrorCode.SERVER_OUT_OF_CAPACITY);
  }

  /// @deprecated Use [#errorResponse(ServerQueryRequest, QueryErrorCode)].
  @Deprecated
  protected ListenableFuture<byte[]> immediateErrorResponse(ServerQueryRequest queryRequest, QueryErrorCode errorCode) {
    return Futures.transform(errorResponse(queryRequest, errorCode), QueryScheduler::toByteArray,
        MoreExecutors.directExecutor());
  }

  /// @deprecated Use [#shuttingDownResponse(ServerQueryRequest)].
  @Deprecated
  protected ListenableFuture<byte[]> shuttingDown(ServerQueryRequest queryRequest) {
    return immediateErrorResponse(queryRequest, QueryErrorCode.SERVER_SHUTTING_DOWN);
  }

  /// @deprecated Use [#outOfCapacityResponse(ServerQueryRequest)].
  @Deprecated
  protected ListenableFuture<byte[]> outOfCapacity(ServerQueryRequest queryRequest) {
    return immediateErrorResponse(queryRequest, QueryErrorCode.SERVER_OUT_OF_CAPACITY);
  }

  /// Copies a serialized response into one array, for the deprecated methods that return `byte[]`.
  @Nullable
  private static byte[] toByteArray(@Nullable DataBuffer response) {
    if (response == null) {
      return null;
    }
    byte[] bytes = new byte[Math.toIntExact(response.size())];
    response.copyTo(0, bytes);
    return bytes;
  }

  @Nullable
  private static DataBuffer wrap(@Nullable byte[] responseBytes) {
    return responseBytes != null ? PinotByteBuffer.wrap(responseBytes) : null;
  }
}
