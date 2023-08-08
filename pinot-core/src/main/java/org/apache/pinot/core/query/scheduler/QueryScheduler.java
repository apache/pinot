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
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.TimerContext;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract class providing common scheduler functionality
 * including query runner and query worker pool
 */
@SuppressWarnings("UnstableApiUsage")
public abstract class QueryScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryScheduler.class);

  private static final String INVALID_NUM_SCANNED = "-1";
  private static final String INVALID_SEGMENTS_COUNT = "-1";
  private static final String INVALID_FRESHNESS_MS = "-1";
  private static final String INVALID_NUM_RESIZES = "-1";
  private static final String INVALID_RESIZE_TIME_MS = "-1";
  private static final String QUERY_LOG_MAX_RATE_KEY = "query.log.maxRatePerSecond";
  private static final double DEFAULT_QUERY_LOG_MAX_RATE = 10_000d;
  protected final ServerMetrics _serverMetrics;
  protected final QueryExecutor _queryExecutor;
  protected final ResourceManager _resourceManager;
  protected final LongAccumulator _latestQueryTime;
  private final RateLimiter _queryLogRateLimiter;
  private final RateLimiter _numDroppedLogRateLimiter;
  private final AtomicInteger _numDroppedLogCounter;
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
    _queryLogRateLimiter = RateLimiter.create(config.getProperty(QUERY_LOG_MAX_RATE_KEY, DEFAULT_QUERY_LOG_MAX_RATE));
    _numDroppedLogRateLimiter = RateLimiter.create(1.0d);
    _numDroppedLogCounter = new AtomicInteger(0);
    LOGGER.info("Query log max rate: {}", _queryLogRateLimiter.getRate());
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
      String tableNameWithType = queryRequest.getTableNameWithType();
      long numDocsScanned =
          Long.parseLong(responseMetadata.getOrDefault(MetadataKey.NUM_DOCS_SCANNED.getName(), INVALID_NUM_SCANNED));
      long numEntriesScannedInFilter = Long.parseLong(
          responseMetadata.getOrDefault(MetadataKey.NUM_ENTRIES_SCANNED_IN_FILTER.getName(), INVALID_NUM_SCANNED));
      long numEntriesScannedPostFilter = Long.parseLong(
          responseMetadata.getOrDefault(MetadataKey.NUM_ENTRIES_SCANNED_POST_FILTER.getName(), INVALID_NUM_SCANNED));
      long numSegmentsProcessed = Long.parseLong(
          responseMetadata.getOrDefault(MetadataKey.NUM_SEGMENTS_PROCESSED.getName(), INVALID_SEGMENTS_COUNT));
      long numSegmentsMatched = Long.parseLong(
          responseMetadata.getOrDefault(MetadataKey.NUM_SEGMENTS_MATCHED.getName(), INVALID_SEGMENTS_COUNT));
      long numSegmentsPrunedInvalid = Long.parseLong(
          responseMetadata.getOrDefault(MetadataKey.NUM_SEGMENTS_PRUNED_INVALID.getName(), INVALID_SEGMENTS_COUNT));
      long numSegmentsPrunedByLimit = Long.parseLong(
          responseMetadata.getOrDefault(MetadataKey.NUM_SEGMENTS_PRUNED_BY_LIMIT.getName(), INVALID_SEGMENTS_COUNT));
      long numSegmentsPrunedByValue = Long.parseLong(
          responseMetadata.getOrDefault(MetadataKey.NUM_SEGMENTS_PRUNED_BY_VALUE.getName(), INVALID_SEGMENTS_COUNT));
      long numSegmentsConsuming = Long.parseLong(
          responseMetadata.getOrDefault(MetadataKey.NUM_CONSUMING_SEGMENTS_QUERIED.getName(), INVALID_SEGMENTS_COUNT));
      long numConsumingSegmentsProcessed = Long.parseLong(
          responseMetadata.getOrDefault(MetadataKey.NUM_CONSUMING_SEGMENTS_PROCESSED.getName(),
              INVALID_SEGMENTS_COUNT));
      long numConsumingSegmentsMatched = Long.parseLong(
          responseMetadata.getOrDefault(MetadataKey.NUM_CONSUMING_SEGMENTS_MATCHED.getName(), INVALID_SEGMENTS_COUNT));
      long minConsumingFreshnessMs = Long.parseLong(
          responseMetadata.getOrDefault(MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS.getName(), INVALID_FRESHNESS_MS));
      int numResizes =
          Integer.parseInt(responseMetadata.getOrDefault(MetadataKey.NUM_RESIZES.getName(), INVALID_NUM_RESIZES));
      long resizeTimeMs =
          Long.parseLong(responseMetadata.getOrDefault(MetadataKey.RESIZE_TIME_MS.getName(), INVALID_RESIZE_TIME_MS));
      long threadCpuTimeNs =
          Long.parseLong(responseMetadata.getOrDefault(MetadataKey.THREAD_CPU_TIME_NS.getName(), "0"));
      long systemActivitiesCpuTimeNs =
          Long.parseLong(responseMetadata.getOrDefault(MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS.getName(), "0"));
      long responseSerializationCpuTimeNs =
          Long.parseLong(responseMetadata.getOrDefault(MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName(), "0"));
      long totalCpuTimeNs = threadCpuTimeNs + systemActivitiesCpuTimeNs + responseSerializationCpuTimeNs;

      if (numDocsScanned > 0) {
        _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.NUM_DOCS_SCANNED, numDocsScanned);
      }
      if (numEntriesScannedInFilter > 0) {
        _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.NUM_ENTRIES_SCANNED_IN_FILTER,
            numEntriesScannedInFilter);
      }
      if (numEntriesScannedPostFilter > 0) {
        _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.NUM_ENTRIES_SCANNED_POST_FILTER,
            numEntriesScannedPostFilter);
      }
      if (numResizes > 0) {
        _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.NUM_RESIZES, numResizes);
      }
      if (resizeTimeMs > 0) {
        _serverMetrics.addValueToTableGauge(tableNameWithType, ServerGauge.RESIZE_TIME_MS, resizeTimeMs);
      }
      if (threadCpuTimeNs > 0) {
        _serverMetrics.addTimedTableValue(tableNameWithType, ServerTimer.EXECUTION_THREAD_CPU_TIME_NS, threadCpuTimeNs,
            TimeUnit.NANOSECONDS);
      }
      if (systemActivitiesCpuTimeNs > 0) {
        _serverMetrics.addTimedTableValue(tableNameWithType, ServerTimer.SYSTEM_ACTIVITIES_CPU_TIME_NS,
            systemActivitiesCpuTimeNs, TimeUnit.NANOSECONDS);
      }
      if (responseSerializationCpuTimeNs > 0) {
        _serverMetrics.addTimedTableValue(tableNameWithType, ServerTimer.RESPONSE_SER_CPU_TIME_NS,
            responseSerializationCpuTimeNs, TimeUnit.NANOSECONDS);
      }
      if (totalCpuTimeNs > 0) {
        _serverMetrics.addTimedTableValue(tableNameWithType, ServerTimer.TOTAL_CPU_TIME_NS, totalCpuTimeNs,
            TimeUnit.NANOSECONDS);
      }

      TimerContext timerContext = queryRequest.getTimerContext();
      int numSegmentsQueried = queryRequest.getSegmentsToQuery().size();
      long schedulerWaitMs = timerContext.getPhaseDurationMs(ServerQueryPhase.SCHEDULER_WAIT);

      // Please keep the format as name=value comma-separated with no spaces
      // Please add new entries at the end
      if (_queryLogRateLimiter.tryAcquire() || forceLog(schedulerWaitMs, numDocsScanned, numSegmentsPrunedInvalid)) {
        LOGGER.info("Processed requestId={},table={},"
                + "segments(queried/processed/matched/consumingQueried/consumingProcessed/consumingMatched/"
                + "invalid/limit/value)={}/{}/{}/{}/{}/{}/{}/{}/{},"
                + "schedulerWaitMs={},reqDeserMs={},totalExecMs={},resSerMs={},totalTimeMs={},"
                + "minConsumingFreshnessMs={},broker={},numDocsScanned={},scanInFilter={},scanPostFilter={},sched={},"
                + "threadCpuTimeNs(total/thread/sysActivity/resSer)={}/{}/{}/{}", requestId, tableNameWithType,
            numSegmentsQueried, numSegmentsProcessed, numSegmentsMatched, numSegmentsConsuming,
            numConsumingSegmentsProcessed, numConsumingSegmentsMatched, numSegmentsPrunedInvalid,
            numSegmentsPrunedByLimit, numSegmentsPrunedByValue, schedulerWaitMs,
            timerContext.getPhaseDurationMs(ServerQueryPhase.REQUEST_DESERIALIZATION),
            timerContext.getPhaseDurationMs(ServerQueryPhase.QUERY_PROCESSING),
            timerContext.getPhaseDurationMs(ServerQueryPhase.RESPONSE_SERIALIZATION),
            timerContext.getPhaseDurationMs(ServerQueryPhase.TOTAL_QUERY_TIME), minConsumingFreshnessMs,
            queryRequest.getBrokerId(), numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, name(),
            totalCpuTimeNs, threadCpuTimeNs, systemActivitiesCpuTimeNs, responseSerializationCpuTimeNs);

        // Limit the dropping log message at most once per second.
        if (_numDroppedLogRateLimiter.tryAcquire()) {
          // NOTE: the reported number may not be accurate since we will be missing some increments happened between
          // get() and set().
          int numDroppedLog = _numDroppedLogCounter.get();
          if (numDroppedLog > 0) {
            LOGGER.info("{} logs were dropped. (log max rate per second: {})", numDroppedLog,
                _queryLogRateLimiter.getRate());
            _numDroppedLogCounter.set(0);
          }
        }
      } else {
        _numDroppedLogCounter.incrementAndGet();
      }

      if (minConsumingFreshnessMs > -1 && minConsumingFreshnessMs != Long.MAX_VALUE) {
        _serverMetrics.addTimedTableValue(tableNameWithType, ServerTimer.FRESHNESS_LAG_MS,
            (System.currentTimeMillis() - minConsumingFreshnessMs), TimeUnit.MILLISECONDS);
      }
      _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.NUM_SEGMENTS_QUERIED, numSegmentsQueried);
      _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.NUM_SEGMENTS_PROCESSED, numSegmentsProcessed);
      _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.NUM_SEGMENTS_MATCHED, numSegmentsMatched);
      _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.NUM_SEGMENTS_PRUNED_INVALID,
          numSegmentsPrunedInvalid);
      _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.NUM_SEGMENTS_PRUNED_BY_LIMIT,
          numSegmentsPrunedByLimit);
      _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.NUM_SEGMENTS_PRUNED_BY_VALUE,
          numSegmentsPrunedByValue);

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
