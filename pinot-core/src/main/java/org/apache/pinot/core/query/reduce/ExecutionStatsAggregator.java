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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerTimer;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableType;


public class ExecutionStatsAggregator {
  private final List<QueryProcessingException> _processingExceptions = new ArrayList<>();
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

  public ExecutionStatsAggregator(boolean enableTrace) {
    _enableTrace = enableTrace;
  }

  public void aggregate(ServerRoutingInstance routingInstance, DataTable dataTable) {
    TableType tableType = routingInstance.getTableType();
    String instanceName = routingInstance.getShortName();
    Map<String, String> metadata = dataTable.getMetadata();

    // Reduce on trace info.
    if (_enableTrace && metadata.containsKey(DataTable.MetadataKey.TRACE_INFO.getName())) {
      _traceInfo.put(instanceName, metadata.get(DataTable.MetadataKey.TRACE_INFO.getName()));
    }

    // Reduce on exceptions.
    Map<Integer, String> exceptions = dataTable.getExceptions();
    for (Map.Entry<Integer, String> entry : exceptions.entrySet()) {
      _processingExceptions.add(new QueryProcessingException(entry.getKey(), entry.getValue()));
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
  }

  public void setStats(String rawTableName, BrokerResponseNative brokerResponseNative, BrokerMetrics brokerMetrics) {
    // set exception
    List<QueryProcessingException> processingExceptions = brokerResponseNative.getExceptions();
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
    brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.DOCUMENTS_SCANNED, _numDocsScanned);
    brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.ENTRIES_SCANNED_IN_FILTER, _numEntriesScannedInFilter);
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

  private void withNotNullLongMetadata(Map<String, String> metadata, DataTable.MetadataKey key, LongConsumer consumer) {
    String strValue = metadata.get(key.getName());
    if (strValue != null) {
      consumer.accept(Long.parseLong(strValue));
    }
  }
}
