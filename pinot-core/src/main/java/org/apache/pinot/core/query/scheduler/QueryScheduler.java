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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAccumulator;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.logger.ServerQueryLogger;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.TimerContext;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.exception.QueryCancelledException;
import org.apache.pinot.spi.trace.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract class providing common scheduler functionality
 * including query runner and query worker pool
 */
public abstract class QueryScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryScheduler.class);

  protected final ServerMetrics _serverMetrics;
  protected final QueryExecutor _queryExecutor;
  protected final ResourceManager _resourceManager;
  protected final LongAccumulator _latestQueryTime;
  protected final ServerQueryLogger _queryLogger = ServerQueryLogger.getInstance();

  protected volatile boolean _isRunning = false;

  /**
   * Constructor to initialize QueryScheduler
   * @param queryExecutor QueryExecutor engine to use
   * @param resourceManager for managing server thread resources
   * @param serverMetrics server metrics collector
   */
  public QueryScheduler(PinotConfiguration config, QueryExecutor queryExecutor, ResourceManager resourceManager,
      ServerMetrics serverMetrics, LongAccumulator latestQueryTime) {
    Preconditions.checkNotNull(config);
    Preconditions.checkNotNull(queryExecutor);
    Preconditions.checkNotNull(resourceManager);
    Preconditions.checkNotNull(serverMetrics);
    Preconditions.checkNotNull(latestQueryTime);

    _serverMetrics = serverMetrics;
    _resourceManager = resourceManager;
    _queryExecutor = queryExecutor;
    _latestQueryTime = latestQueryTime;
  }

  /**
   * Submit a query for execution. The query will be scheduled for execution as per the scheduling algorithm
   * @param queryRequest query to schedule for execution
   * @return Listenable future for query result representing serialized response. It is possible that the
   *    future may return immediately or be scheduled for execution at a later time.
   */
  public abstract ListenableFuture<byte[]> submit(ServerQueryRequest queryRequest);

  /**
   * Query scheduler name for logging
   */
  public abstract String name();

  /**
   * Start query scheduler thread
   */
  public void start() {
    _isRunning = true;
  }

  /**
   * stop the scheduler and shutdown services
   */
  public void stop() {
    // don't stop resourcemanager yet...we need to wait for all running queries to finish
    _isRunning = false;
  }

  /**
   * Create a future task for the query
   * @param queryRequest incoming query request
   * @param executorService executor service to use for parallelizing query. This is passed to the QueryExecutor
   * @return Future task that can be scheduled for execution on an ExecutorService. Ideally, this future
   * should be executed on a different executor service than {@code e} to avoid deadlock.
   */
  protected ListenableFutureTask<byte[]> createQueryFutureTask(ServerQueryRequest queryRequest,
      ExecutorService executorService) {
    return ListenableFutureTask.create(() -> processQueryAndSerialize(queryRequest, executorService));
  }

  /**
   * Process query and serialize response
   * @param queryRequest incoming query request
   * @param executorService Executor service to use for parallelizing query processing
   * @return serialized query response
   */
  @Nullable
  protected byte[] processQueryAndSerialize(ServerQueryRequest queryRequest, ExecutorService executorService) {

    //Start instrumentation context. This must not be moved further below interspersed into the code.
    Tracing.ThreadAccountantOps.setupRunner(queryRequest.getQueryId());

    _latestQueryTime.accumulate(System.currentTimeMillis());
    InstanceResponseBlock instanceResponse;
    try {
      instanceResponse = _queryExecutor.execute(queryRequest, executorService);
    } catch (Exception e) {
      LOGGER.error("Encountered exception while processing requestId {} from broker {}", queryRequest.getRequestId(),
          queryRequest.getBrokerId(), e);
      // For not handled exceptions
      _serverMetrics.addMeteredGlobalValue(ServerMeter.UNCAUGHT_EXCEPTIONS, 1);
      instanceResponse = new InstanceResponseBlock();
      instanceResponse.addException(QueryException.getException(QueryException.INTERNAL_ERROR, e));
    }

    try {
      long requestId = queryRequest.getRequestId();
      Map<String, String> responseMetadata = instanceResponse.getResponseMetadata();
      responseMetadata.put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
      byte[] responseBytes = serializeResponse(queryRequest, instanceResponse);

      // Log the statistics
      if (_queryLogger != null) {
        _queryLogger.logQuery(queryRequest, instanceResponse, name());
      }

      // TODO: Perform this check sooner during the serialization of DataTable.
      Map<String, String> queryOptions = queryRequest.getQueryContext().getQueryOptions();
      Long maxResponseSizeBytes = QueryOptionsUtils.getMaxServerResponseSizeBytes(queryOptions);
      if (maxResponseSizeBytes != null && responseBytes != null && responseBytes.length > maxResponseSizeBytes) {
        String errMsg =
            String.format("Serialized query response size %d exceeds threshold %d for requestId %d from broker %s",
                responseBytes.length, maxResponseSizeBytes, queryRequest.getRequestId(), queryRequest.getBrokerId());
        LOGGER.error(errMsg);
        _serverMetrics.addMeteredTableValue(queryRequest.getTableNameWithType(),
            ServerMeter.LARGE_QUERY_RESPONSE_SIZE_EXCEPTIONS, 1);

        instanceResponse = new InstanceResponseBlock();
        instanceResponse.addException(QueryException.getException(QueryException.QUERY_CANCELLATION_ERROR, errMsg));
        instanceResponse.addMetadata(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));
        responseBytes = serializeResponse(queryRequest, instanceResponse);
      }

      return responseBytes;
    } finally {
      Tracing.ThreadAccountantOps.clear();
    }
  }

  /**
   * Helper function to decide whether to force the log
   *
   * TODO: come up with other criteria for forcing a log and come up with better numbers
   *
   */
  private boolean forceLog(long schedulerWaitMs, long numDocsScanned, long numSegmentsPrunedInvalid) {
    // If scheduler wait time is larger than 100ms, force the log
    if (schedulerWaitMs > 100L) {
      return true;
    }

    // If there are invalid segments, force the log
    if (numSegmentsPrunedInvalid > 0) {
      return true;
    }

    // If the number of document scanned is larger than 1 million rows, force the log
    return numDocsScanned > 1_000_000L;
  }

  /**
   * Serialize the instance response for query request
   * @param queryRequest Server query request for which response is serialized
   * @param instanceResponse instance response to serialize
   * @return serialized response bytes
   */
  @Nullable
  private byte[] serializeResponse(ServerQueryRequest queryRequest, InstanceResponseBlock instanceResponse) {
    TimerContext timerContext = queryRequest.getTimerContext();
    TimerContext.Timer responseSerializationTimer =
        timerContext.startNewPhaseTimer(ServerQueryPhase.RESPONSE_SERIALIZATION);

    byte[] responseByte = null;
    try {
      responseByte = instanceResponse.toDataTable().toBytes();
    } catch (EarlyTerminationException e) {
      Exception killedErrorMsg = Tracing.getThreadAccountant().getErrorStatus();
      String errMsg =
          "Cancelled while building data table" + (killedErrorMsg == null ? StringUtils.EMPTY : " " + killedErrorMsg);
      LOGGER.error(errMsg);
      instanceResponse = new InstanceResponseBlock(new ExceptionResultsBlock(new QueryCancelledException(errMsg, e)));
      instanceResponse.addMetadata(MetadataKey.REQUEST_ID.getName(), Long.toString(queryRequest.getRequestId()));
      return serializeResponse(queryRequest, instanceResponse);
    } catch (Exception e) {
      _serverMetrics.addMeteredGlobalValue(ServerMeter.RESPONSE_SERIALIZATION_EXCEPTIONS, 1);
      LOGGER.error("Caught exception while serializing response for requestId: {}, brokerId: {}",
          queryRequest.getRequestId(), queryRequest.getBrokerId(), e);
    }

    responseSerializationTimer.stopAndRecord();
    timerContext.startNewPhaseTimer(ServerQueryPhase.TOTAL_QUERY_TIME, timerContext.getQueryArrivalTimeMs())
        .stopAndRecord();

    return responseByte;
  }

  /**
   * Error response future in case of internal error where query response is not available. This can happen if the
   * query can not be executed.
   */
  protected ListenableFuture<byte[]> immediateErrorResponse(ServerQueryRequest queryRequest,
      ProcessingException error) {
    InstanceResponseBlock instanceResponse = new InstanceResponseBlock();
    instanceResponse.addMetadata(MetadataKey.REQUEST_ID.getName(), Long.toString(queryRequest.getRequestId()));
    instanceResponse.addException(error);
    return Futures.immediateFuture(serializeResponse(queryRequest, instanceResponse));
  }
}
