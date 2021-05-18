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
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.common.utils.DataTable.MetadataKey;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.TimerContext;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.spi.env.PinotConfiguration;
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
  private static final String INVALID_NUM_RESIZES = "-1";
  private static final String INVALID_RESIZE_TIME_MS = "-1";
  private static final String QUERY_LOG_MAX_RATE_KEY = "query.log.maxRatePerSecond";
  private static final double DEFAULT_QUERY_LOG_MAX_RATE = 10_000d;
  protected final ServerMetrics serverMetrics;
  protected final QueryExecutor queryExecutor;
  protected final ResourceManager resourceManager;
  protected final LongAccumulator latestQueryTime;
  private final RateLimiter queryLogRateLimiter;
  private final RateLimiter numDroppedLogRateLimiter;
  private final AtomicInteger numDroppedLogCounter;
  protected volatile boolean isRunning = false;

  /**
   * Constructor to initialize QueryScheduler
   * @param queryExecutor QueryExecutor engine to use
   * @param resourceManager for managing server thread resources
   * @param serverMetrics server metrics collector
   */
  public QueryScheduler(@Nonnull PinotConfiguration config, @Nonnull QueryExecutor queryExecutor,
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
    this.queryLogRateLimiter =
        RateLimiter.create(config.getProperty(QUERY_LOG_MAX_RATE_KEY, DEFAULT_QUERY_LOG_MAX_RATE));
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
      dataTable = DataTableBuilder.getEmptyDataTable();
      dataTable.addException(QueryException.getException(QueryException.INTERNAL_ERROR, e));
    }
    long requestId = queryRequest.getRequestId();
    Map<String, String> dataTableMetadata = dataTable.getMetadata();
    dataTableMetadata.put(MetadataKey.REQUEST_ID.getName(), Long.toString(requestId));

    byte[] responseBytes = serializeDataTable(queryRequest, dataTable);

    // Log the statistics
    String tableNameWithType = queryRequest.getTableNameWithType();
    long numDocsScanned =
        Long.parseLong(dataTableMetadata.getOrDefault(MetadataKey.NUM_DOCS_SCANNED.getName(), INVALID_NUM_SCANNED));
    long numEntriesScannedInFilter = Long.parseLong(
        dataTableMetadata.getOrDefault(MetadataKey.NUM_ENTRIES_SCANNED_IN_FILTER.getName(), INVALID_NUM_SCANNED));
    long numEntriesScannedPostFilter = Long.parseLong(
        dataTableMetadata.getOrDefault(MetadataKey.NUM_ENTRIES_SCANNED_POST_FILTER.getName(), INVALID_NUM_SCANNED));
    long numSegmentsProcessed = Long.parseLong(
        dataTableMetadata.getOrDefault(MetadataKey.NUM_SEGMENTS_PROCESSED.getName(), INVALID_SEGMENTS_COUNT));
    long numSegmentsMatched = Long.parseLong(
        dataTableMetadata.getOrDefault(MetadataKey.NUM_SEGMENTS_MATCHED.getName(), INVALID_SEGMENTS_COUNT));
    long numSegmentsConsuming = Long.parseLong(
        dataTableMetadata.getOrDefault(MetadataKey.NUM_CONSUMING_SEGMENTS_PROCESSED.getName(), INVALID_SEGMENTS_COUNT));
    long minConsumingFreshnessMs = Long.parseLong(
        dataTableMetadata.getOrDefault(MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS.getName(), INVALID_FRESHNESS_MS));
    int numResizes =
        Integer.parseInt(dataTableMetadata.getOrDefault(MetadataKey.NUM_RESIZES.getName(), INVALID_NUM_RESIZES));
    long resizeTimeMs =
        Long.parseLong(dataTableMetadata.getOrDefault(MetadataKey.RESIZE_TIME_MS.getName(), INVALID_RESIZE_TIME_MS));
    long threadCpuTimeNs =
        Long.parseLong(dataTableMetadata.getOrDefault(MetadataKey.THREAD_CPU_TIME_NS.getName(), "0"));

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
    if (numResizes > 0) {
      serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.NUM_RESIZES, numResizes);
    }
    if (resizeTimeMs > 0) {
      serverMetrics.addValueToTableGauge(tableNameWithType, ServerGauge.RESIZE_TIME_MS, resizeTimeMs);
    }
    if (threadCpuTimeNs > 0) {
      serverMetrics.addTimedTableValue(tableNameWithType, ServerTimer.EXECUTION_THREAD_CPU_TIME_NS, threadCpuTimeNs, TimeUnit.NANOSECONDS);
    }

    TimerContext timerContext = queryRequest.getTimerContext();
    int numSegmentsQueried = queryRequest.getSegmentsToQuery().size();
    long schedulerWaitMs = timerContext.getPhaseDurationMs(ServerQueryPhase.SCHEDULER_WAIT);

    // Please keep the format as name=value comma-separated with no spaces
    // Please add new entries at the end
    if (queryLogRateLimiter.tryAcquire() || forceLog(schedulerWaitMs, numDocsScanned)) {
      LOGGER.info("Processed requestId={},table={},segments(queried/processed/matched/consuming)={}/{}/{}/{},"
              + "schedulerWaitMs={},reqDeserMs={},totalExecMs={},resSerMs={},totalTimeMs={},minConsumingFreshnessMs={},broker={},"
              + "numDocsScanned={},scanInFilter={},scanPostFilter={},sched={},threadCpuTimeNs={}", requestId,
          tableNameWithType, numSegmentsQueried, numSegmentsProcessed, numSegmentsMatched, numSegmentsConsuming,
          schedulerWaitMs, timerContext.getPhaseDurationMs(ServerQueryPhase.REQUEST_DESERIALIZATION),
          timerContext.getPhaseDurationMs(ServerQueryPhase.QUERY_PROCESSING),
          timerContext.getPhaseDurationMs(ServerQueryPhase.RESPONSE_SERIALIZATION),
          timerContext.getPhaseDurationMs(ServerQueryPhase.TOTAL_QUERY_TIME), minConsumingFreshnessMs,
          queryRequest.getBrokerId(), numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, name(),
          threadCpuTimeNs);

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

    return responseBytes;
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
    return numDocsScanned > 1_000_000L;
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
    DataTable result = DataTableBuilder.getEmptyDataTable();

    Map<String, String> dataTableMetadata = result.getMetadata();
    dataTableMetadata.put(MetadataKey.REQUEST_ID.getName(), Long.toString(queryRequest.getRequestId()));

    result.addException(error);
    return Futures.immediateFuture(serializeDataTable(queryRequest, result));
  }
}
