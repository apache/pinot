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
package org.apache.pinot.core.query.reduce;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerTimer;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.common.utils.DataTable.MetadataKey;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is the base reduce service.
 */
@ThreadSafe
public abstract class BaseReduceService {

  // Set the reducer priority higher than NORM but lower than MAX, because if a query is complete
  // we want to deserialize and return response as soon. This is the same as server side 'pqr' threads.
  protected static final int QUERY_RUNNER_THREAD_PRIORITY = 7;
  // brw -> Shorthand for broker reduce worker threads.
  protected static final String REDUCE_THREAD_NAME_FORMAT = "brw-%d";

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseReduceService.class);

  protected final ExecutorService _reduceExecutorService;
  protected final int _maxReduceThreadsPerQuery;
  protected final int _groupByTrimThreshold;

  public BaseReduceService(PinotConfiguration config) {
    _maxReduceThreadsPerQuery = config.getProperty(CommonConstants.Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY,
        CommonConstants.Broker.DEFAULT_MAX_REDUCE_THREADS_PER_QUERY);
    _groupByTrimThreshold = config.getProperty(CommonConstants.Broker.CONFIG_OF_BROKER_GROUPBY_TRIM_THRESHOLD,
        CommonConstants.Broker.DEFAULT_BROKER_GROUPBY_TRIM_THRESHOLD);

    int numThreadsInExecutorService = Runtime.getRuntime().availableProcessors();
    LOGGER.info("Initializing BrokerReduceService with {} threads, and {} max reduce threads.",
        numThreadsInExecutorService, _maxReduceThreadsPerQuery);

    ThreadFactory reduceThreadFactory =
        new ThreadFactoryBuilder().setDaemon(false).setPriority(QUERY_RUNNER_THREAD_PRIORITY)
            .setNameFormat(REDUCE_THREAD_NAME_FORMAT).build();

    // ExecutorService is initialized with numThreads same as availableProcessors.
    _reduceExecutorService = Executors.newFixedThreadPool(numThreadsInExecutorService, reduceThreadFactory);
  }

  protected static void updateAlias(QueryContext queryContext, BrokerResponseNative brokerResponseNative) {
    ResultTable resultTable = brokerResponseNative.getResultTable();
    if (resultTable == null) {
      return;
    }
    List<String> aliasList = queryContext.getAliasList();
    if (aliasList.isEmpty()) {
      return;
    }

    String[] columnNames = resultTable.getDataSchema().getColumnNames();
    List<ExpressionContext> selectExpressions = getSelectExpressions(queryContext.getSelectExpressions());
    int numSelectExpressions = selectExpressions.size();
    // For query like `SELECT *`, we skip alias update.
    if (columnNames.length != numSelectExpressions) {
      return;
    }
    for (int i = 0; i < numSelectExpressions; i++) {
      String alias = aliasList.get(i);
      if (alias != null) {
        columnNames[i] = alias;
      }
    }
  }

  protected static List<ExpressionContext> getSelectExpressions(List<ExpressionContext> selectExpressions) {
    // NOTE: For DISTINCT queries, need to extract the arguments as the SELECT expressions
    if (selectExpressions.size() == 1 && selectExpressions.get(0).getType() == ExpressionContext.Type.FUNCTION
        && selectExpressions.get(0).getFunction().getFunctionName().equals("distinct")) {
      return selectExpressions.get(0).getFunction().getArguments();
    }
    return selectExpressions;
  }

  protected void shutDown() {
    _reduceExecutorService.shutdownNow();
  }

  protected static class ExecutionStatsAggregator {
    private final List<QueryProcessingException> _processingExceptions = new ArrayList<>();
    private final Map<String, String> _traceInfo = new HashMap<>();
    private final boolean _enableTrace;

    private long _numDocsScanned = 0L;
    private long _numEntriesScannedInFilter = 0L;
    private long _numEntriesScannedPostFilter = 0L;
    private long _numSegmentsQueried = 0L;
    private long _numSegmentsProcessed = 0L;
    private long _numSegmentsMatched = 0L;
    private long _numConsumingSegmentsProcessed = 0L;
    private long _minConsumingFreshnessTimeMs = Long.MAX_VALUE;
    private long _numTotalDocs = 0L;
    private long _offlineThreadCpuTimeNs = 0L;
    private long _realtimeThreadCpuTimeNs = 0L;
    private long _offlineSystemActivitiesCpuTimeNs = 0L;
    private long _realtimeSystemActivitiesCpuTimeNs = 0L;
    private long _offlineResponseSerializationCpuTimeNs = 0L;
    private long _realtimeResponseSerializationCpuTimeNs = 0L;
    private long _offlineTotalCpuTimeNs = 0L;
    private long _realtimeTotalCpuTimeNs = 0L;
    private boolean _numGroupsLimitReached = false;

    protected ExecutionStatsAggregator(boolean enableTrace) {
      _enableTrace = enableTrace;
    }

    protected synchronized void aggregate(ServerRoutingInstance routingInstance, DataTable dataTable) {
      Map<String, String> metadata = dataTable.getMetadata();
      // Reduce on trace info.
      if (_enableTrace) {
        _traceInfo.put(routingInstance.getHostname(), metadata.get(MetadataKey.TRACE_INFO.getName()));
      }

      // Reduce on exceptions.
      Map<Integer, String> exceptions = dataTable.getExceptions();
      for (int key : exceptions.keySet()) {
        _processingExceptions.add(new QueryProcessingException(key, exceptions.get(key)));
      }

      // Reduce on execution statistics.
      String numDocsScannedString = metadata.get(MetadataKey.NUM_DOCS_SCANNED.getName());
      if (numDocsScannedString != null) {
        _numDocsScanned += Long.parseLong(numDocsScannedString);
      }
      String numEntriesScannedInFilterString = metadata.get(MetadataKey.NUM_ENTRIES_SCANNED_IN_FILTER.getName());
      if (numEntriesScannedInFilterString != null) {
        _numEntriesScannedInFilter += Long.parseLong(numEntriesScannedInFilterString);
      }
      String numEntriesScannedPostFilterString = metadata.get(MetadataKey.NUM_ENTRIES_SCANNED_POST_FILTER.getName());
      if (numEntriesScannedPostFilterString != null) {
        _numEntriesScannedPostFilter += Long.parseLong(numEntriesScannedPostFilterString);
      }
      String numSegmentsQueriedString = metadata.get(MetadataKey.NUM_SEGMENTS_QUERIED.getName());
      if (numSegmentsQueriedString != null) {
        _numSegmentsQueried += Long.parseLong(numSegmentsQueriedString);
      }

      String numSegmentsProcessedString = metadata.get(MetadataKey.NUM_SEGMENTS_PROCESSED.getName());
      if (numSegmentsProcessedString != null) {
        _numSegmentsProcessed += Long.parseLong(numSegmentsProcessedString);
      }
      String numSegmentsMatchedString = metadata.get(MetadataKey.NUM_SEGMENTS_MATCHED.getName());
      if (numSegmentsMatchedString != null) {
        _numSegmentsMatched += Long.parseLong(numSegmentsMatchedString);
      }

      String numConsumingString = metadata.get(MetadataKey.NUM_CONSUMING_SEGMENTS_PROCESSED.getName());
      if (numConsumingString != null) {
        _numConsumingSegmentsProcessed += Long.parseLong(numConsumingString);
      }

      String minConsumingFreshnessTimeMsString = metadata.get(MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS.getName());
      if (minConsumingFreshnessTimeMsString != null) {
        _minConsumingFreshnessTimeMs =
            Math.min(Long.parseLong(minConsumingFreshnessTimeMsString), _minConsumingFreshnessTimeMs);
      }

      String threadCpuTimeNsString = metadata.get(MetadataKey.THREAD_CPU_TIME_NS.getName());
      if (threadCpuTimeNsString != null) {
        if (routingInstance.getTableType() == TableType.OFFLINE) {
          _offlineThreadCpuTimeNs += Long.parseLong(threadCpuTimeNsString);
        } else {
          _realtimeThreadCpuTimeNs += Long.parseLong(threadCpuTimeNsString);
        }
      }

      String systemActivitiesCpuTimeNsString = metadata.get(MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS.getName());
      if (systemActivitiesCpuTimeNsString != null) {
        if (routingInstance.getTableType() == TableType.OFFLINE) {
          _offlineSystemActivitiesCpuTimeNs += Long.parseLong(systemActivitiesCpuTimeNsString);
        } else {
          _realtimeSystemActivitiesCpuTimeNs += Long.parseLong(systemActivitiesCpuTimeNsString);
        }
      }

      String responseSerializationCpuTimeNsString = metadata.get(MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName());
      if (responseSerializationCpuTimeNsString != null) {
        if (routingInstance.getTableType() == TableType.OFFLINE) {
          _offlineResponseSerializationCpuTimeNs += Long.parseLong(responseSerializationCpuTimeNsString);
        } else {
          _realtimeResponseSerializationCpuTimeNs += Long.parseLong(responseSerializationCpuTimeNsString);
        }
      }
      _offlineTotalCpuTimeNs =
          _offlineThreadCpuTimeNs + _offlineSystemActivitiesCpuTimeNs + _offlineResponseSerializationCpuTimeNs;
      _realtimeTotalCpuTimeNs =
          _realtimeThreadCpuTimeNs + _realtimeSystemActivitiesCpuTimeNs + _realtimeResponseSerializationCpuTimeNs;

      String numTotalDocsString = metadata.get(MetadataKey.TOTAL_DOCS.getName());
      if (numTotalDocsString != null) {
        _numTotalDocs += Long.parseLong(numTotalDocsString);
      }
      _numGroupsLimitReached |= Boolean.parseBoolean(metadata.get(MetadataKey.NUM_GROUPS_LIMIT_REACHED.getName()));
    }

    protected void setStats(String rawTableName, BrokerResponseNative brokerResponseNative,
        BrokerMetrics brokerMetrics) {
      // set exception
      List<QueryProcessingException> processingExceptions = brokerResponseNative.getProcessingExceptions();
      processingExceptions.addAll(_processingExceptions);

      // add all trace.
      if (_enableTrace) {
        brokerResponseNative.getTraceInfo().putAll(_traceInfo);
      }

      // Set execution statistics.
      brokerResponseNative.setNumDocsScanned(_numDocsScanned);
      brokerResponseNative.setNumEntriesScannedInFilter(_numEntriesScannedInFilter);
      brokerResponseNative.setNumEntriesScannedPostFilter(_numEntriesScannedPostFilter);
      brokerResponseNative.setNumSegmentsQueried(_numSegmentsQueried);
      brokerResponseNative.setNumSegmentsProcessed(_numSegmentsProcessed);
      brokerResponseNative.setNumSegmentsMatched(_numSegmentsMatched);
      brokerResponseNative.setTotalDocs(_numTotalDocs);
      brokerResponseNative.setNumGroupsLimitReached(_numGroupsLimitReached);
      brokerResponseNative.setOfflineThreadCpuTimeNs(_offlineThreadCpuTimeNs);
      brokerResponseNative.setRealtimeThreadCpuTimeNs(_realtimeThreadCpuTimeNs);
      brokerResponseNative.setOfflineSystemActivitiesCpuTimeNs(_offlineSystemActivitiesCpuTimeNs);
      brokerResponseNative.setRealtimeSystemActivitiesCpuTimeNs(_realtimeSystemActivitiesCpuTimeNs);
      brokerResponseNative.setOfflineResponseSerializationCpuTimeNs(_offlineResponseSerializationCpuTimeNs);
      brokerResponseNative.setRealtimeResponseSerializationCpuTimeNs(_realtimeResponseSerializationCpuTimeNs);
      brokerResponseNative.setOfflineTotalCpuTimeNs(_offlineTotalCpuTimeNs);
      brokerResponseNative.setRealtimeTotalCpuTimeNs(_realtimeTotalCpuTimeNs);
      if (_numConsumingSegmentsProcessed > 0) {
        brokerResponseNative.setNumConsumingSegmentsQueried(_numConsumingSegmentsProcessed);
        brokerResponseNative.setMinConsumingFreshnessTimeMs(_minConsumingFreshnessTimeMs);
      }

      // Update broker metrics.
      if (brokerMetrics != null) {
        brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.DOCUMENTS_SCANNED, _numDocsScanned);
        brokerMetrics
            .addMeteredTableValue(rawTableName, BrokerMeter.ENTRIES_SCANNED_IN_FILTER, _numEntriesScannedInFilter);
        brokerMetrics
            .addMeteredTableValue(rawTableName, BrokerMeter.ENTRIES_SCANNED_POST_FILTER, _numEntriesScannedPostFilter);
        brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.OFFLINE_THREAD_CPU_TIME_NS, _offlineThreadCpuTimeNs,
            TimeUnit.NANOSECONDS);
        brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.REALTIME_THREAD_CPU_TIME_NS,
            _realtimeThreadCpuTimeNs,
            TimeUnit.NANOSECONDS);
        brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.OFFLINE_SYSTEM_ACTIVITIES_CPU_TIME_NS,
            _offlineSystemActivitiesCpuTimeNs, TimeUnit.NANOSECONDS);
        brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.REALTIME_SYSTEM_ACTIVITIES_CPU_TIME_NS,
            _realtimeSystemActivitiesCpuTimeNs, TimeUnit.NANOSECONDS);
        brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.OFFLINE_RESPONSE_SER_CPU_TIME_NS,
            _offlineResponseSerializationCpuTimeNs, TimeUnit.NANOSECONDS);
        brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.REALTIME_RESPONSE_SER_CPU_TIME_NS,
            _realtimeResponseSerializationCpuTimeNs, TimeUnit.NANOSECONDS);
        brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.OFFLINE_TOTAL_CPU_TIME_NS, _offlineTotalCpuTimeNs,
            TimeUnit.NANOSECONDS);
        brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.REALTIME_TOTAL_CPU_TIME_NS, _realtimeTotalCpuTimeNs,
            TimeUnit.NANOSECONDS);

        if (_numConsumingSegmentsProcessed > 0 && _minConsumingFreshnessTimeMs > 0) {
          brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.FRESHNESS_LAG_MS,
              System.currentTimeMillis() - _minConsumingFreshnessTimeMs, TimeUnit.MILLISECONDS);
        }
      }
    }
  }
}
