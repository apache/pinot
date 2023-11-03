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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerTimer;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.BrokerResponseStats;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class ExecutionStatsAggregator {
  private final List<QueryProcessingException> _processingExceptions = new ArrayList<>();
  private final Map<String, Map<String, String>> _operatorStats = new HashMap<>();
  private final Set<String> _tableNames = new HashSet<>();
  private final Map<String, String> _traceInfo = new HashMap<>();
  private final boolean _enableTrace;

  private long _numDocsScanned = 0L;
  private long _numEntriesScannedInFilter = 0L;
  private long _numEntriesScannedPostFilter = 0L;
  private long _numSegmentsQueried = 0L;
  private long _numSegmentsProcessed = 0L;
  private long _numSegmentsMatched = 0L;
  private long _numConsumingSegmentsQueried = 0L;
  private long _numConsumingSegmentsProcessed = 0L;
  private long _numConsumingSegmentsMatched = 0L;
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
  private long _numSegmentsPrunedByServer = 0L;
  private long _numSegmentsPrunedInvalid = 0L;
  private long _numSegmentsPrunedByLimit = 0L;
  private long _numSegmentsPrunedByValue = 0L;
  private long _explainPlanNumEmptyFilterSegments = 0L;
  private long _explainPlanNumMatchAllFilterSegments = 0L;
  private boolean _numGroupsLimitReached = false;
  private boolean _maxRowsInJoinLimitReached = false;
  private int _numBlocks = 0;
  private int _numRows = 0;
  private long _stageExecutionTimeMs = 0;
  private long _stageExecStartTimeMs = -1;
  private long _stageExecEndTimeMs = -1;
  private int _stageExecutionUnit = 0;

  public ExecutionStatsAggregator(boolean enableTrace) {
    _enableTrace = enableTrace;
  }

  public void aggregate(ServerRoutingInstance routingInstance, DataTable dataTable) {
    aggregate(routingInstance, dataTable.getMetadata(), dataTable.getExceptions());
  }

  public synchronized void aggregate(@Nullable ServerRoutingInstance routingInstance, Map<String, String> metadata,
      Map<Integer, String> exceptions) {

    String tableNamesStr = metadata.get(DataTable.MetadataKey.TABLE.getName());
    String tableName = null;

    if (tableNamesStr != null) {
      List<String> tableNames = Arrays.stream(tableNamesStr.split("::")).collect(Collectors.toList());
      _tableNames.addAll(tableNames);

      //TODO: Decide a strategy to split stageLevel stats across tables for brokerMetrics
      // assigning everything to the first table only for now
      tableName = tableNames.get(0);
    }

    TableType tableType = null;
    String instanceName = null;
    if (routingInstance != null) {
      tableType = routingInstance.getTableType();
      instanceName = routingInstance.getShortName();;
    } else if (tableName != null) {
      tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
      instanceName = tableName;
    } else {
      tableType = null;
      instanceName = null;
    }

    // Reduce on trace info.
    if (_enableTrace && metadata.containsKey(DataTable.MetadataKey.TRACE_INFO.getName()) && instanceName != null) {
      _traceInfo.put(instanceName, metadata.get(DataTable.MetadataKey.TRACE_INFO.getName()));
    }

    String operatorId = metadata.get(DataTable.MetadataKey.OPERATOR_ID.getName());
    if (operatorId != null) {
      if (_enableTrace) {
        _operatorStats.put(operatorId, metadata);
      } else {
        _operatorStats.put(operatorId, new HashMap<>());
      }
    }

    // Reduce on exceptions.
    for (int key : exceptions.keySet()) {
      _processingExceptions.add(new QueryProcessingException(key, exceptions.get(key)));
    }

    // Reduce on execution statistics.
    String numDocsScannedString = metadata.get(DataTable.MetadataKey.NUM_DOCS_SCANNED.getName());
    if (numDocsScannedString != null) {
      _numDocsScanned += Long.parseLong(numDocsScannedString);
    }
    String numEntriesScannedInFilterString =
        metadata.get(DataTable.MetadataKey.NUM_ENTRIES_SCANNED_IN_FILTER.getName());
    if (numEntriesScannedInFilterString != null) {
      _numEntriesScannedInFilter += Long.parseLong(numEntriesScannedInFilterString);
    }
    String numEntriesScannedPostFilterString =
        metadata.get(DataTable.MetadataKey.NUM_ENTRIES_SCANNED_POST_FILTER.getName());
    if (numEntriesScannedPostFilterString != null) {
      _numEntriesScannedPostFilter += Long.parseLong(numEntriesScannedPostFilterString);
    }
    String numSegmentsQueriedString = metadata.get(DataTable.MetadataKey.NUM_SEGMENTS_QUERIED.getName());
    if (numSegmentsQueriedString != null) {
      _numSegmentsQueried += Long.parseLong(numSegmentsQueriedString);
    }

    String numSegmentsProcessedString = metadata.get(DataTable.MetadataKey.NUM_SEGMENTS_PROCESSED.getName());
    if (numSegmentsProcessedString != null) {
      _numSegmentsProcessed += Long.parseLong(numSegmentsProcessedString);
    }
    String numSegmentsMatchedString = metadata.get(DataTable.MetadataKey.NUM_SEGMENTS_MATCHED.getName());
    if (numSegmentsMatchedString != null) {
      _numSegmentsMatched += Long.parseLong(numSegmentsMatchedString);
    }

    String numConsumingSegmentsQueriedString =
        metadata.get(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_QUERIED.getName());
    if (numConsumingSegmentsQueriedString != null) {
      _numConsumingSegmentsQueried += Long.parseLong(numConsumingSegmentsQueriedString);
    }

    String numConsumingSegmentsProcessed =
        metadata.get(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_PROCESSED.getName());
    if (numConsumingSegmentsProcessed != null) {
      _numConsumingSegmentsProcessed += Long.parseLong(numConsumingSegmentsProcessed);
    }

    String numConsumingSegmentsMatched = metadata.get(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_MATCHED.getName());
    if (numConsumingSegmentsMatched != null) {
      _numConsumingSegmentsMatched += Long.parseLong(numConsumingSegmentsMatched);
    }

    String minConsumingFreshnessTimeMsString =
        metadata.get(DataTable.MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS.getName());
    if (minConsumingFreshnessTimeMsString != null) {
      _minConsumingFreshnessTimeMs =
          Math.min(Long.parseLong(minConsumingFreshnessTimeMsString), _minConsumingFreshnessTimeMs);
    }

    String threadCpuTimeNsString = metadata.get(DataTable.MetadataKey.THREAD_CPU_TIME_NS.getName());
    if (tableType != null && threadCpuTimeNsString != null) {
      if (tableType == TableType.OFFLINE) {
        _offlineThreadCpuTimeNs += Long.parseLong(threadCpuTimeNsString);
      } else {
        _realtimeThreadCpuTimeNs += Long.parseLong(threadCpuTimeNsString);
      }
    }

    String systemActivitiesCpuTimeNsString =
        metadata.get(DataTable.MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS.getName());
    if (tableType != null && systemActivitiesCpuTimeNsString != null) {
      if (tableType == TableType.OFFLINE) {
        _offlineSystemActivitiesCpuTimeNs += Long.parseLong(systemActivitiesCpuTimeNsString);
      } else {
        _realtimeSystemActivitiesCpuTimeNs += Long.parseLong(systemActivitiesCpuTimeNsString);
      }
    }

    String responseSerializationCpuTimeNsString =
        metadata.get(DataTable.MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName());
    if (tableType != null && responseSerializationCpuTimeNsString != null) {
      if (tableType == TableType.OFFLINE) {
        _offlineResponseSerializationCpuTimeNs += Long.parseLong(responseSerializationCpuTimeNsString);
      } else {
        _realtimeResponseSerializationCpuTimeNs += Long.parseLong(responseSerializationCpuTimeNsString);
      }
    }
    _offlineTotalCpuTimeNs =
        _offlineThreadCpuTimeNs + _offlineSystemActivitiesCpuTimeNs + _offlineResponseSerializationCpuTimeNs;
    _realtimeTotalCpuTimeNs =
        _realtimeThreadCpuTimeNs + _realtimeSystemActivitiesCpuTimeNs + _realtimeResponseSerializationCpuTimeNs;

    withNotNullLongMetadata(metadata, DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_SERVER,
        l -> _numSegmentsPrunedByServer += l);
    withNotNullLongMetadata(metadata, DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_INVALID,
        l -> _numSegmentsPrunedInvalid += l);
    withNotNullLongMetadata(metadata, DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_LIMIT,
        l -> _numSegmentsPrunedByLimit += l);
    withNotNullLongMetadata(metadata, DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_VALUE,
        l -> _numSegmentsPrunedByValue += l);

    String explainPlanNumEmptyFilterSegments =
        metadata.get(DataTable.MetadataKey.EXPLAIN_PLAN_NUM_EMPTY_FILTER_SEGMENTS.getName());
    if (explainPlanNumEmptyFilterSegments != null) {
      _explainPlanNumEmptyFilterSegments += Long.parseLong(explainPlanNumEmptyFilterSegments);
    }

    String explainPlanNumMatchAllFilterSegments =
        metadata.get(DataTable.MetadataKey.EXPLAIN_PLAN_NUM_MATCH_ALL_FILTER_SEGMENTS.getName());
    if (explainPlanNumMatchAllFilterSegments != null) {
      _explainPlanNumMatchAllFilterSegments += Long.parseLong(explainPlanNumMatchAllFilterSegments);
    }

    String numTotalDocsString = metadata.get(DataTable.MetadataKey.TOTAL_DOCS.getName());
    if (numTotalDocsString != null) {
      _numTotalDocs += Long.parseLong(numTotalDocsString);
    }
    _numGroupsLimitReached |=
        Boolean.parseBoolean(metadata.get(DataTable.MetadataKey.NUM_GROUPS_LIMIT_REACHED.getName()));

    _maxRowsInJoinLimitReached |=
        Boolean.parseBoolean(metadata.get(DataTable.MetadataKey.MAX_ROWS_IN_JOIN_LIMIT_REACHED.getName()));

    String numBlocksString = metadata.get(DataTable.MetadataKey.NUM_BLOCKS.getName());
    if (numBlocksString != null) {
      _numBlocks += Long.parseLong(numBlocksString);
    }

    String numRowsString = metadata.get(DataTable.MetadataKey.NUM_ROWS.getName());
    if (numBlocksString != null) {
      _numRows += Long.parseLong(numRowsString);
    }

    String operatorExecutionTimeString = metadata.get(DataTable.MetadataKey.OPERATOR_EXECUTION_TIME_MS.getName());
    if (operatorExecutionTimeString != null) {
      _stageExecutionTimeMs += Long.parseLong(operatorExecutionTimeString);
      _stageExecutionUnit += 1;
    }

    String operatorExecStartTimeString = metadata.get(DataTable.MetadataKey.OPERATOR_EXEC_START_TIME_MS.getName());
    if (operatorExecStartTimeString != null) {
      long operatorExecStartTime = Long.parseLong(operatorExecStartTimeString);
      _stageExecStartTimeMs = _stageExecStartTimeMs == -1 ? operatorExecStartTime
          : Math.min(operatorExecStartTime, _stageExecStartTimeMs);
    }

    String operatorExecEndTimeString = metadata.get(DataTable.MetadataKey.OPERATOR_EXEC_END_TIME_MS.getName());
    if (operatorExecEndTimeString != null) {
      long operatorExecEndTime = Long.parseLong(operatorExecEndTimeString);
      _stageExecEndTimeMs = _stageExecEndTimeMs == -1 ? operatorExecEndTime
          : Math.max(operatorExecEndTime, _stageExecEndTimeMs);
    }
  }

  public void setStats(BrokerResponseNative brokerResponseNative) {
    setStats(null, brokerResponseNative, null);
  }

  public void setStats(@Nullable String rawTableName, BrokerResponseNative brokerResponseNative,
      @Nullable BrokerMetrics brokerMetrics) {
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
    brokerResponseNative.setMaxRowsInJoinLimitReached(_maxRowsInJoinLimitReached);
    brokerResponseNative.setOfflineThreadCpuTimeNs(_offlineThreadCpuTimeNs);
    brokerResponseNative.setRealtimeThreadCpuTimeNs(_realtimeThreadCpuTimeNs);
    brokerResponseNative.setOfflineSystemActivitiesCpuTimeNs(_offlineSystemActivitiesCpuTimeNs);
    brokerResponseNative.setRealtimeSystemActivitiesCpuTimeNs(_realtimeSystemActivitiesCpuTimeNs);
    brokerResponseNative.setOfflineResponseSerializationCpuTimeNs(_offlineResponseSerializationCpuTimeNs);
    brokerResponseNative.setRealtimeResponseSerializationCpuTimeNs(_realtimeResponseSerializationCpuTimeNs);
    brokerResponseNative.setOfflineTotalCpuTimeNs(_offlineTotalCpuTimeNs);
    brokerResponseNative.setRealtimeTotalCpuTimeNs(_realtimeTotalCpuTimeNs);
    brokerResponseNative.setNumSegmentsPrunedByServer(_numSegmentsPrunedByServer);
    brokerResponseNative.setNumSegmentsPrunedInvalid(_numSegmentsPrunedInvalid);
    brokerResponseNative.setNumSegmentsPrunedByLimit(_numSegmentsPrunedByLimit);
    brokerResponseNative.setNumSegmentsPrunedByValue(_numSegmentsPrunedByValue);
    brokerResponseNative.setExplainPlanNumEmptyFilterSegments(_explainPlanNumEmptyFilterSegments);
    brokerResponseNative.setExplainPlanNumMatchAllFilterSegments(_explainPlanNumMatchAllFilterSegments);
    if (_numConsumingSegmentsQueried > 0) {
      brokerResponseNative.setNumConsumingSegmentsQueried(_numConsumingSegmentsQueried);
    }
    if (_minConsumingFreshnessTimeMs != Long.MAX_VALUE) {
      brokerResponseNative.setMinConsumingFreshnessTimeMs(_minConsumingFreshnessTimeMs);
    }
    brokerResponseNative.setNumConsumingSegmentsProcessed(_numConsumingSegmentsProcessed);
    brokerResponseNative.setNumConsumingSegmentsMatched(_numConsumingSegmentsMatched);

    // Update broker metrics.
    if (brokerMetrics != null && rawTableName != null) {
      brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.DOCUMENTS_SCANNED, _numDocsScanned);
      brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.ENTRIES_SCANNED_IN_FILTER,
          _numEntriesScannedInFilter);
      brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.ENTRIES_SCANNED_POST_FILTER,
          _numEntriesScannedPostFilter);
      brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.OFFLINE_THREAD_CPU_TIME_NS, _offlineThreadCpuTimeNs,
          TimeUnit.NANOSECONDS);
      brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.REALTIME_THREAD_CPU_TIME_NS, _realtimeThreadCpuTimeNs,
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

      if (_minConsumingFreshnessTimeMs != Long.MAX_VALUE) {
        brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.FRESHNESS_LAG_MS,
            System.currentTimeMillis() - _minConsumingFreshnessTimeMs, TimeUnit.MILLISECONDS);
      }
    }
  }

  public void setStageLevelStats(@Nullable String rawTableName, BrokerResponseStats brokerResponseStats,
      @Nullable BrokerMetrics brokerMetrics) {
    if (_enableTrace) {
      setStats(rawTableName, brokerResponseStats, brokerMetrics);
      brokerResponseStats.setOperatorStats(_operatorStats);
    }

    brokerResponseStats.setNumBlocks(_numBlocks);
    brokerResponseStats.setNumRows(_numRows);
    brokerResponseStats.setMaxRowsInJoinLimitReached(_maxRowsInJoinLimitReached);
    brokerResponseStats.setNumGroupsLimitReached(_numGroupsLimitReached);
    brokerResponseStats.setStageExecutionTimeMs(_stageExecutionTimeMs);
    brokerResponseStats.setStageExecutionUnit(_stageExecutionUnit);
    brokerResponseStats.setTableNames(new ArrayList<>(_tableNames));
    if (_stageExecStartTimeMs >= 0 && _stageExecEndTimeMs >= 0) {
      brokerResponseStats.setStageExecWallTimeMs(_stageExecEndTimeMs - _stageExecStartTimeMs);
    }
  }

  private void withNotNullLongMetadata(Map<String, String> metadata, DataTable.MetadataKey key, LongConsumer consumer) {
    String strValue = metadata.get(key.getName());
    if (strValue != null) {
      consumer.accept(Long.parseLong(strValue));
    }
  }
}
