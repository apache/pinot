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
import com.google.common.util.concurrent.RateLimiter;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAccumulator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableImplV2;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.TimerContext;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract class providing common scheduler functionality
 * including query runner and query worker pool
 */
public abstract class QueryScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryScheduler.class);

  private static final String INVALID_NUM_SCANNED = "-1";
  private static final String INVALID_SEGMENTS_COUNT = "-1";
  private static final String INVALID_FRESHNESS_MS = "-1";
  private static final String QUERY_LOG_MAX_RATE_KEY = "query.log.maxRatePerSecond";
  private static final double DEFAULT_QUERY_LOG_MAX_RATE = 10_000d;

  private final RateLimiter queryLogRateLimiter;
  private final RateLimiter numDroppedLogRateLimiter;
  private final AtomicInteger numDroppedLogCounter;

  protected final ServerMetrics serverMetrics;
  protected final QueryExecutor queryExecutor;
  protected final ResourceManager resourceManager;
  protected final LongAccumulator latestQueryTime;
  protected volatile boolean isRunning = false;

  /**
   * Constructor to initialize QueryScheduler
   * @param queryExecutor QueryExecutor engine to use
   * @param resourceManager for managing server thread resources
   * @param serverMetrics server metrics collector
   */
  public QueryScheduler(@Nonnull Configuration config, @Nonnull QueryExecutor queryExecutor,
      @Nonnull ResourceManager resourceManager, @Nonnull ServerMetrics serverMetrics,
      @Nonnull LongAccumulator latestQueryTime) {
    Preconditions.checkNotNull(config);
    Preconditions.checkNotNull(queryExecutor);
    Preconditions.checkNotNull(resourceManager);
    Preconditions.checkNotNull(serverMetrics);

    this.serverMetrics = serverMetrics;
    this.resourceManager = resourceManager;
    this.queryExecutor = queryExecutor;
    this.latestQueryTime = latestQueryTime;
    this.queryLogRateLimiter = RateLimiter.create(config.getDouble(QUERY_LOG_MAX_RATE_KEY, DEFAULT_QUERY_LOG_MAX_RATE));
    this.numDroppedLogRateLimiter = RateLimiter.create(1.0d);
    this.numDroppedLogCounter = new AtomicInteger(0);

    LOGGER.info("Query log max rate: {}", queryLogRateLimiter.getRate());
  }

  /**
   * Submit a query for execution. The query will be scheduled for execution as per the scheduling algorithm
   * @param queryRequest query to schedule for execution
   * @return Listenable future for query result representing serialized response. It is possible that the
   *    future may return immediately or be scheduled for execution at a later time.
   */
  @Nonnull
  public abstract ListenableFuture<byte[]> submit(@Nonnull ServerQueryRequest queryRequest);

  /**
   * Query scheduler name for logging
   */
  public abstract String name();

  /**
   * Start query scheduler thread
   */
  public void start() {
    isRunning = true;
  }

  /**
   * stop the scheduler and shutdown services
   */
  public void stop() {
    // don't stop resourcemanager yet...we need to wait for all running queries to finish
    isRunning = false;
  }

  /**
   * Create a future task for the query
   * @param queryRequest incoming query request
   * @param executorService executor service to use for parallelizing query. This is passed to the QueryExecutor
   * @return Future task that can be scheduled for execution on an ExecutorService. Ideally, this future
   * should be executed on a different executor service than {@code e} to avoid deadlock.
   */
  protected ListenableFutureTask<byte[]> createQueryFutureTask(@Nonnull ServerQueryRequest queryRequest,
      @Nonnull ExecutorService executorService) {
    return ListenableFutureTask.create(() -> processQueryAndSerialize(queryRequest, executorService));
  }

  /**
   * Process query and serialize response
   * @param queryRequest incoming query request
   * @param executorService Executor service to use for parallelizing query processing
   * @return serialized query response
   */
  @SuppressWarnings("Duplicates")
  @Nullable
  protected byte[] processQueryAndSerialize(@Nonnull ServerQueryRequest queryRequest,
      @Nonnull ExecutorService executorService) {
    latestQueryTime.accumulate(System.currentTimeMillis());
    DataTable dataTable;
    try {
      dataTable = queryExecutor.processQuery(queryRequest, executorService);
    } catch (Exception e) {
      LOGGER.error("Encountered exception while processing requestId {} from broker {}", queryRequest.getRequestId(),
          queryRequest.getBrokerId(), e);
      // For not handled exceptions
      serverMetrics.addMeteredGlobalValue(ServerMeter.UNCAUGHT_EXCEPTIONS, 1);
      dataTable = new DataTableImplV2();
      dataTable.addException(QueryException.getException(QueryException.INTERNAL_ERROR, e));
    }
    long requestId = queryRequest.getRequestId();
    Map<String, String> dataTableMetadata = dataTable.getMetadata();
    dataTableMetadata.put(DataTable.REQUEST_ID_METADATA_KEY, Long.toString(requestId));

    byte[] responseData = serializeDataTable(queryRequest, dataTable);

    // Log the statistics
    String tableNameWithType = queryRequest.getTableNameWithType();
    long numDocsScanned =
        Long.parseLong(dataTableMetadata.getOrDefault(DataTable.NUM_DOCS_SCANNED_METADATA_KEY, INVALID_NUM_SCANNED));
    long numEntriesScannedInFilter = Long.parseLong(
        dataTableMetadata.getOrDefault(DataTable.NUM_ENTRIES_SCANNED_IN_FILTER_METADATA_KEY, INVALID_NUM_SCANNED));
    long numEntriesScannedPostFilter =
        Long.parseLong(dataTableMetadata.getOrDefault(DataTable.NUM_ENTRIES_SCANNED_POST_FILTER_METADATA_KEY, INVALID_NUM_SCANNED));
    long numSegmentsProcessed =
        Long.parseLong(dataTableMetadata.getOrDefault(DataTable.NUM_SEGMENTS_PROCESSED, INVALID_SEGMENTS_COUNT));
    long numSegmentsMatched =
        Long.parseLong(dataTableMetadata.getOrDefault(DataTable.NUM_SEGMENTS_MATCHED, INVALID_SEGMENTS_COUNT));
    long numSegmentsConsuming =
        Long.parseLong(dataTableMetadata.getOrDefault(DataTable.NUM_CONSUMING_SEGMENTS_PROCESSED, INVALID_SEGMENTS_COUNT));
    long minConsumingFreshnessMs =
        Long.parseLong(dataTableMetadata.getOrDefault(DataTable.MIN_CONSUMING_FRESHNESS_TIME_MS, INVALID_FRESHNESS_MS));

    if (numDocsScanned > 0) {
      serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.NUM_DOCS_SCANNED, numDocsScanned);
    }
    if (numEntriesScannedInFilter > 0) {
      serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.NUM_ENTRIES_SCANNED_IN_FILTER,
          numEntriesScannedInFilter);
    }
    if (numEntriesScannedPostFilter > 0) {
      serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.NUM_ENTRIES_SCANNED_POST_FILTER,
          numEntriesScannedPostFilter);
    }

    TimerContext timerContext = queryRequest.getTimerContext();
    int numSegmentsQueried = queryRequest.getSegmentsToQuery().size();
    long schedulerWaitMs = timerContext.getPhaseDurationMs(ServerQueryPhase.SCHEDULER_WAIT);

    if (queryLogRateLimiter.tryAcquire() || forceLog(schedulerWaitMs, numDocsScanned)) {
      LOGGER.info(
          "Processed requestId={},table={},segments(queried/processed/matched/consuming)={}/{}/{}/{},"
              + "schedulerWaitMs={},totalExecMs={},totalTimeMs={},minConsumingFreshnessMs={},broker={},"
              + "numDocsScanned={},scanInFilter={},scanPostFilter={},sched={}",
          requestId, tableNameWithType, numSegmentsQueried, numSegmentsProcessed, numSegmentsMatched,
          numSegmentsConsuming, schedulerWaitMs, timerContext.getPhaseDurationMs(ServerQueryPhase.QUERY_PROCESSING),
          timerContext.getPhaseDurationMs(ServerQueryPhase.TOTAL_QUERY_TIME), minConsumingFreshnessMs,
          queryRequest.getBrokerId(), numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, name());

      // Limit the dropping log message at most once per second.
      if (numDroppedLogRateLimiter.tryAcquire()) {
        // NOTE: the reported number may not be accurate since we will be missing some increments happened between
        // get() and set().
        int numDroppedLog = numDroppedLogCounter.get();
        if (numDroppedLog > 0) {
          LOGGER.info("{} logs were dropped. (log max rate per second: {})", numDroppedLog,
              queryLogRateLimiter.getRate());
          numDroppedLogCounter.set(0);
        }
      }
    } else {
      numDroppedLogCounter.incrementAndGet();
    }

    if (minConsumingFreshnessMs > -1) {
      serverMetrics.addTimedTableValue(tableNameWithType, ServerTimer.FRESHNESS_LAG_MS,
          (System.currentTimeMillis() - minConsumingFreshnessMs), TimeUnit.MILLISECONDS);
    }
    serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.NUM_SEGMENTS_QUERIED, numSegmentsQueried);
    serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.NUM_SEGMENTS_PROCESSED, numSegmentsProcessed);
    serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.NUM_SEGMENTS_MATCHED, numSegmentsMatched);

    return responseData;
  }

  /**
   * Helper function to decide whether to force the log
   *
   * TODO: come up with other criteria for forcing a log and come up with better numbers
   *
   */
  private boolean forceLog(long schedulerWaitMs, long numDocsScanned) {
    // If scheduler wait time is larger than 100ms, force the log
    if (schedulerWaitMs > 100L) {
      return true;
    }

    // If the number of document scanned is larger than 1 million rows, force the log
    if (numDocsScanned > 1_000_000L) {
      return true;
    }
    return false;
  }

  /**
   * Serialize the DataTable response for query request
   * @param queryRequest Server query request for which response is serialized
   * @param dataTable DataTable to serialize
   * @return serialized response bytes
   */
  @Nullable
  private byte[] serializeDataTable(@Nonnull ServerQueryRequest queryRequest, @Nonnull DataTable dataTable) {
    TimerContext timerContext = queryRequest.getTimerContext();
    TimerContext.Timer responseSerializationTimer =
        timerContext.startNewPhaseTimer(ServerQueryPhase.RESPONSE_SERIALIZATION);

    byte[] responseByte = null;
    try {
      responseByte = dataTable.toBytes();
    } catch (Exception e) {
      serverMetrics.addMeteredGlobalValue(ServerMeter.RESPONSE_SERIALIZATION_EXCEPTIONS, 1);
      LOGGER.error("Caught exception while serializing response for requestId: {}, brokerId: {}",
          queryRequest.getRequestId(), queryRequest.getBrokerId(), e);
    }

    responseSerializationTimer.stopAndRecord();
    timerContext.startNewPhaseTimer(ServerQueryPhase.TOTAL_QUERY_TIME, timerContext.getQueryArrivalTimeMs())
        .stopAndRecord();

    return responseByte;
  }

  /**
   * Error response future in case of internal error where query response is not available. This can happen
   * if the query can not be executed or
   * @param queryRequest
   * @param error error code to send
   * @return
   */
  protected ListenableFuture<byte[]> immediateErrorResponse(ServerQueryRequest queryRequest,
      ProcessingException error) {
    DataTable result = new DataTableImplV2();
    result.addException(error);
    return Futures.immediateFuture(serializeDataTable(queryRequest, result));
  }
}
