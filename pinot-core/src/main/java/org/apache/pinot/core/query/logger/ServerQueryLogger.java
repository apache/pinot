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
package org.apache.pinot.core.query.logger;

import com.google.common.util.concurrent.RateLimiter;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("UnstableApiUsage")
public class ServerQueryLogger {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerQueryLogger.class);
  private static final AtomicReference<ServerQueryLogger> INSTANCE = new AtomicReference<>();

  private final ServerMetrics _serverMetrics;
  private final RateLimiter _queryLogRateLimiter;
  private final RateLimiter _droppedReportRateLimiter;
  private final AtomicInteger _numDroppedLogs = new AtomicInteger();

  public static void init(double queryLogMaxRate, double droppedReportMaxRate, ServerMetrics serverMetrics) {
    if (INSTANCE.compareAndSet(null, new ServerQueryLogger(queryLogMaxRate, droppedReportMaxRate, serverMetrics))) {
      LOGGER.info("Initialized ServerQueryLogger with query log max rate: {}, dropped report max rate: {}",
          queryLogMaxRate, droppedReportMaxRate);
    } else {
      LOGGER.error("ServerQueryLogger is already initialized, not initializing it again");
    }
  }

  @Nullable
  public static ServerQueryLogger getInstance() {
    // NOTE: In some tests, ServerQueryLogger might not be initialized. Returns null when it is not initialized.
    return INSTANCE.get();
  }

  private ServerQueryLogger(double queryLogMaxRate, double droppedReportMaxRate, ServerMetrics serverMetrics) {
    _serverMetrics = serverMetrics;
    _queryLogRateLimiter = RateLimiter.create(queryLogMaxRate);
    _droppedReportRateLimiter = RateLimiter.create(droppedReportMaxRate);
  }

  public void logQuery(ServerQueryRequest request, InstanceResponseBlock response, String schedulerType) {
    String tableNameWithType = request.getTableNameWithType();
    Map<String, String> responseMetadata = response.getResponseMetadata();

    long numDocsScanned = getLongValue(responseMetadata, MetadataKey.NUM_DOCS_SCANNED.getName(), -1);
    addToTableMeter(tableNameWithType, ServerMeter.NUM_DOCS_SCANNED, numDocsScanned);

    long numEntriesScannedInFilter =
        getLongValue(responseMetadata, MetadataKey.NUM_ENTRIES_SCANNED_IN_FILTER.getName(), -1);
    addToTableMeter(tableNameWithType, ServerMeter.NUM_ENTRIES_SCANNED_IN_FILTER, numEntriesScannedInFilter);

    long numEntriesScannedPostFilter =
        getLongValue(responseMetadata, MetadataKey.NUM_ENTRIES_SCANNED_POST_FILTER.getName(), -1);
    addToTableMeter(tableNameWithType, ServerMeter.NUM_ENTRIES_SCANNED_POST_FILTER, numEntriesScannedPostFilter);

    int numSegmentsQueried = request.getSegmentsToQuery().size();
    addToTableMeter(tableNameWithType, ServerMeter.NUM_SEGMENTS_QUERIED, numSegmentsQueried);

    long numSegmentsProcessed = getLongValue(responseMetadata, MetadataKey.NUM_SEGMENTS_PROCESSED.getName(), -1);
    addToTableMeter(tableNameWithType, ServerMeter.NUM_SEGMENTS_PROCESSED, numSegmentsProcessed);

    long numSegmentsMatched = getLongValue(responseMetadata, MetadataKey.NUM_SEGMENTS_MATCHED.getName(), -1);
    addToTableMeter(tableNameWithType, ServerMeter.NUM_SEGMENTS_MATCHED, numSegmentsMatched);

    long numSegmentsPrunedInvalid =
        getLongValue(responseMetadata, MetadataKey.NUM_SEGMENTS_PRUNED_INVALID.getName(), -1);
    addToTableMeter(tableNameWithType, ServerMeter.NUM_SEGMENTS_PRUNED_INVALID, numSegmentsPrunedInvalid);

    long numSegmentsPrunedByLimit =
        getLongValue(responseMetadata, MetadataKey.NUM_SEGMENTS_PRUNED_BY_LIMIT.getName(), -1);
    addToTableMeter(tableNameWithType, ServerMeter.NUM_SEGMENTS_PRUNED_BY_LIMIT, numSegmentsPrunedByLimit);

    long numSegmentsPrunedByValue =
        getLongValue(responseMetadata, MetadataKey.NUM_SEGMENTS_PRUNED_BY_VALUE.getName(), -1);
    addToTableMeter(tableNameWithType, ServerMeter.NUM_SEGMENTS_PRUNED_BY_VALUE, numSegmentsPrunedByValue);

    long numConsumingSegmentsQueried =
        getLongValue(responseMetadata, MetadataKey.NUM_CONSUMING_SEGMENTS_QUERIED.getName(), -1);
    long numConsumingSegmentsProcessed =
        getLongValue(responseMetadata, MetadataKey.NUM_CONSUMING_SEGMENTS_PROCESSED.getName(), -1);
    long numConsumingSegmentsMatched =
        getLongValue(responseMetadata, MetadataKey.NUM_CONSUMING_SEGMENTS_MATCHED.getName(), -1);

    long minConsumingFreshnessMs =
        getLongValue(responseMetadata, MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS.getName(), -1);
    if (minConsumingFreshnessMs > 0 && minConsumingFreshnessMs != Long.MAX_VALUE) {
      _serverMetrics.addTimedTableValue(tableNameWithType, ServerTimer.FRESHNESS_LAG_MS,
          (System.currentTimeMillis() - minConsumingFreshnessMs), TimeUnit.MILLISECONDS);
    }

    long numResizes = getLongValue(responseMetadata, MetadataKey.NUM_RESIZES.getName(), -1);
    addToTableMeter(tableNameWithType, ServerMeter.NUM_RESIZES, numResizes);

    long resizeTimeMs = getLongValue(responseMetadata, MetadataKey.RESIZE_TIME_MS.getName(), -1);
    addToTableMeter(tableNameWithType, ServerMeter.RESIZE_TIME_MS, resizeTimeMs);

    long threadCpuTimeNs = getLongValue(responseMetadata, MetadataKey.THREAD_CPU_TIME_NS.getName(), 0);
    if (threadCpuTimeNs > 0) {
      _serverMetrics.addTimedTableValue(tableNameWithType, ServerTimer.EXECUTION_THREAD_CPU_TIME_NS, threadCpuTimeNs,
          TimeUnit.NANOSECONDS);
      _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.TOTAL_THREAD_CPU_TIME_MILLIS,
          TimeUnit.MILLISECONDS.convert(threadCpuTimeNs, TimeUnit.NANOSECONDS));
    }

    long systemActivitiesCpuTimeNs =
        getLongValue(responseMetadata, MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS.getName(), 0);
    if (systemActivitiesCpuTimeNs > 0) {
      _serverMetrics.addTimedTableValue(tableNameWithType, ServerTimer.SYSTEM_ACTIVITIES_CPU_TIME_NS,
          systemActivitiesCpuTimeNs, TimeUnit.NANOSECONDS);
    }

    long responseSerializationCpuTimeNs =
        getLongValue(responseMetadata, MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName(), 0);
    if (responseSerializationCpuTimeNs > 0) {
      _serverMetrics.addTimedTableValue(tableNameWithType, ServerTimer.RESPONSE_SER_CPU_TIME_NS,
          responseSerializationCpuTimeNs, TimeUnit.NANOSECONDS);
    }

    long totalCpuTimeNs = threadCpuTimeNs + systemActivitiesCpuTimeNs + responseSerializationCpuTimeNs;
    if (totalCpuTimeNs > 0) {
      _serverMetrics.addTimedTableValue(tableNameWithType, ServerTimer.TOTAL_CPU_TIME_NS, totalCpuTimeNs,
          TimeUnit.NANOSECONDS);
    }

    TimerContext timerContext = request.getTimerContext();
    long schedulerWaitMs = timerContext.getPhaseDurationMs(ServerQueryPhase.SCHEDULER_WAIT);

    // Please keep the format as name=value comma-separated with no spaces
    // Please add new entries at the end
    if (_queryLogRateLimiter.tryAcquire() || forceLog(schedulerWaitMs, numDocsScanned, numSegmentsPrunedInvalid)) {
      LOGGER.info("Processed requestId={},table={},"
              + "segments(queried/processed/matched/consumingQueried/consumingProcessed/consumingMatched/"
              + "invalid/limit/value)={}/{}/{}/{}/{}/{}/{}/{}/{},"
              + "schedulerWaitMs={},reqDeserMs={},totalExecMs={},resSerMs={},totalTimeMs={},"
              + "minConsumingFreshnessMs={},broker={},numDocsScanned={},scanInFilter={},scanPostFilter={},sched={},"
              + "threadCpuTimeNs(total/thread/sysActivity/resSer)={}/{}/{}/{}", request.getRequestId(),
          tableNameWithType,
          numSegmentsQueried, numSegmentsProcessed, numSegmentsMatched, numConsumingSegmentsQueried,
          numConsumingSegmentsProcessed, numConsumingSegmentsMatched, numSegmentsPrunedInvalid,
          numSegmentsPrunedByLimit, numSegmentsPrunedByValue, schedulerWaitMs,
          timerContext.getPhaseDurationMs(ServerQueryPhase.REQUEST_DESERIALIZATION),
          timerContext.getPhaseDurationMs(ServerQueryPhase.QUERY_PROCESSING),
          timerContext.getPhaseDurationMs(ServerQueryPhase.RESPONSE_SERIALIZATION),
          timerContext.getPhaseDurationMs(ServerQueryPhase.TOTAL_QUERY_TIME), minConsumingFreshnessMs,
          request.getBrokerId(), numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, schedulerType,
          totalCpuTimeNs, threadCpuTimeNs, systemActivitiesCpuTimeNs, responseSerializationCpuTimeNs);

      // Limit the dropping log message at most once per second.
      if (_droppedReportRateLimiter.tryAcquire()) {
        // NOTE: the reported number may not be accurate since we will be missing some increments happened between
        // get() and set().
        int numDroppedLogs = _numDroppedLogs.get();
        if (numDroppedLogs > 0) {
          LOGGER.info("{} logs were dropped. (log max rate per second: {})", numDroppedLogs,
              _queryLogRateLimiter.getRate());
          _numDroppedLogs.set(0);
        }
      }
    } else {
      _numDroppedLogs.getAndIncrement();
    }
  }

  private static long getLongValue(Map<String, String> metadata, String key, long defaultValue) {
    String value = metadata.get(key);
    return value != null ? Long.parseLong(value) : defaultValue;
  }

  private void addToTableMeter(String tableNameWithType, ServerMeter meter, long value) {
    if (value > 0) {
      _serverMetrics.addMeteredTableValue(tableNameWithType, meter, value);
    }
  }

  /**
   * Returns {@code true} when the query should be logged even if the query log rate is reached.
   *
   * TODO: come up with other criteria for forcing a log and come up with better numbers.
   */
  private static boolean forceLog(long schedulerWaitMs, long numDocsScanned, long numSegmentsPrunedInvalid) {
    // If scheduler wait time is larger than 100ms, force the log.
    if (schedulerWaitMs > 100) {
      return true;
    }

    // If the number of document scanned is larger than 1 million rows, force the log.
    if (numDocsScanned > 1_000_000) {
      return true;
    }

    // If there are invalid segments, force the log.
    if (numSegmentsPrunedInvalid > 0) {
      return true;
    }

    return false;
  }
}
