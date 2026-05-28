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

import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;


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
  private long _offlineThreadMemAllocatedBytes = 0L;
  private long _realtimeThreadMemAllocatedBytes = 0L;
  private long _offlineResponseSerMemAllocatedBytes = 0L;
  private long _realtimeResponseSerMemAllocatedBytes = 0L;
  private long _offlineTotalMemAllocatedBytes = 0L;
  private long _realtimeTotalMemAllocatedBytes = 0L;
  private long _numSegmentsPrunedByServer = 0L;
  private long _numSegmentsPrunedInvalid = 0L;
  private long _numSegmentsPrunedByLimit = 0L;
  private long _numSegmentsPrunedByValue = 0L;
  private long _explainPlanNumEmptyFilterSegments = 0L;
  private long _explainPlanNumMatchAllFilterSegments = 0L;
  private boolean _groupsTrimmed = false;
  private boolean _numGroupsLimitReached = false;
  private boolean _numGroupsWarningLimitReached = false;
  private boolean _maxRowsInDistinctReached = false;
  private boolean _maxRowsWithoutChangeInDistinctReached = false;
  private boolean _maxExecutionTimeInDistinctReached = false;

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

    // Stats for memory allocated.
    String threadMemAllocatedBytesStr = metadata.get(DataTable.MetadataKey.THREAD_MEM_ALLOCATED_BYTES.getName());
    if (tableType != null && threadMemAllocatedBytesStr != null) {
      if (tableType == TableType.OFFLINE) {
        _offlineThreadMemAllocatedBytes += Long.parseLong(threadMemAllocatedBytesStr);
      } else {
        _realtimeThreadMemAllocatedBytes += Long.parseLong(threadMemAllocatedBytesStr);
      }
    }
    String responseSerMemAlocatedBytesStr =
        metadata.get(DataTable.MetadataKey.RESPONSE_SER_MEM_ALLOCATED_BYTES.getName());
    if (tableType != null && responseSerMemAlocatedBytesStr != null) {
      if (tableType == TableType.OFFLINE) {
        _offlineResponseSerMemAllocatedBytes += Long.parseLong(responseSerMemAlocatedBytesStr);
      } else {
        _realtimeResponseSerMemAllocatedBytes += Long.parseLong(responseSerMemAlocatedBytesStr);
      }
    }
    _offlineTotalMemAllocatedBytes =
        _offlineThreadMemAllocatedBytes + _offlineResponseSerMemAllocatedBytes;
    _realtimeTotalMemAllocatedBytes =
        _realtimeThreadMemAllocatedBytes + _realtimeResponseSerMemAllocatedBytes;

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

    _groupsTrimmed |= Boolean.parseBoolean(metadata.get(DataTable.MetadataKey.GROUPS_TRIMMED.getName()));
    _numGroupsLimitReached |=
        Boolean.parseBoolean(metadata.get(DataTable.MetadataKey.NUM_GROUPS_LIMIT_REACHED.getName()));
    _numGroupsWarningLimitReached |=
        Boolean.parseBoolean(metadata.get(DataTable.MetadataKey.NUM_GROUPS_WARNING_LIMIT_REACHED.getName()));
    String distinctEarlyTermination =
        metadata.get(DataTable.MetadataKey.EARLY_TERMINATION_REASON.getName());
    if (distinctEarlyTermination != null) {
      try {
        BaseResultsBlock.EarlyTerminationReason reason =
            BaseResultsBlock.EarlyTerminationReason.valueOf(distinctEarlyTermination);
        switch (reason) {
          case DISTINCT_MAX_ROWS:
            _maxRowsInDistinctReached = true;
            break;
          case DISTINCT_MAX_ROWS_WITHOUT_CHANGE:
            _maxRowsWithoutChangeInDistinctReached = true;
            break;
          case DISTINCT_MAX_EXECUTION_TIME:
            _maxExecutionTimeInDistinctReached = true;
            break;
          default:
            break;
        }
      } catch (IllegalArgumentException e) {
        // Ignore unknown reason.
      }
    }
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
    brokerResponseNative.setGroupsTrimmed(_groupsTrimmed);
    brokerResponseNative.setNumGroupsLimitReached(_numGroupsLimitReached);
    brokerResponseNative.setNumGroupsWarningLimitReached(_numGroupsWarningLimitReached);
    brokerResponseNative.setMaxRowsInDistinctReached(_maxRowsInDistinctReached);
    brokerResponseNative.setMaxRowsWithoutChangeInDistinctReached(_maxRowsWithoutChangeInDistinctReached);
    brokerResponseNative.setMaxExecutionTimeInDistinctReached(_maxExecutionTimeInDistinctReached);
    brokerResponseNative.setOfflineThreadCpuTimeNs(_offlineThreadCpuTimeNs);
    brokerResponseNative.setRealtimeThreadCpuTimeNs(_realtimeThreadCpuTimeNs);
    brokerResponseNative.setOfflineSystemActivitiesCpuTimeNs(_offlineSystemActivitiesCpuTimeNs);
    brokerResponseNative.setRealtimeSystemActivitiesCpuTimeNs(_realtimeSystemActivitiesCpuTimeNs);
    brokerResponseNative.setOfflineResponseSerializationCpuTimeNs(_offlineResponseSerializationCpuTimeNs);
    brokerResponseNative.setRealtimeResponseSerializationCpuTimeNs(_realtimeResponseSerializationCpuTimeNs);
    brokerResponseNative.setOfflineThreadMemAllocatedBytes(_offlineThreadMemAllocatedBytes);
    brokerResponseNative.setRealtimeThreadMemAllocatedBytes(_realtimeThreadMemAllocatedBytes);
    brokerResponseNative.setOfflineResponseSerMemAllocatedBytes(_offlineResponseSerMemAllocatedBytes);
    brokerResponseNative.setRealtimeResponseSerMemAllocatedBytes(_realtimeResponseSerMemAllocatedBytes);
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

  /**
   * Writes the accumulated execution stats onto the given DataTable's metadata (and exception map),
   * so a merged-only DataTable can be re-injected into the regular reduce path with the same
   * downstream totals as a direct reduce of the original inputs would have produced.
   *
   * <p>Unlike {@link #setStats(String, BrokerResponseNative, BrokerMetrics)}, this method does NOT
   * bump broker meters or timers. The merge-only path is expected to run off the request-serving
   * path; meter increments fire when the result is eventually re-reduced.
   *
   * <p>Limitations of the round-trip via DataTable metadata:
   * <ul>
   *   <li>CPU and memory stats round-trip as a single combined value per key
   *       ({@link DataTable.MetadataKey#THREAD_CPU_TIME_NS}, etc.) because the wire format has no
   *       per-tableType keys. In the standard reduce path the aggregator attributes each server's
   *       value to offline vs realtime based on {@code routingInstance.getTableType()} and surfaces
   *       them as separate fields on {@link BrokerResponseNative}; on a re-reduce of the merged
   *       DataTable the whole combined value lands in one bucket — whichever tableType the caller
   *       assigned to the synthetic server response. So the per-tableType split visible on
   *       BrokerResponse is lost across the round-trip, even though the total is preserved.
   *   <li>Per-server exceptions are written via {@link DataTable#addException(int, String)} which
   *       backs a {@code Map<Integer, String>} keyed by error code; if two inputs reported the
   *       same error code the merged DataTable carries last-write-wins for the message.
   *   <li>Per-server trace info is JSON-encoded into a single
   *       {@link DataTable.MetadataKey#TRACE_INFO} entry; the downstream aggregator reads it back
   *       as one trace blob attributed to the synthetic server.
   * </ul>
   */
  public void setStatsOnMergedDataTable(DataTable dataTable) {
    Map<String, String> metadata = dataTable.getMetadata();

    // Additive long stats: mirror setStats()'s pattern of unconditional writes. Accumulators are
    // initialized to 0, so a 0 here is indistinguishable to downstream from "absent" — the
    // downstream aggregator's Long.parseLong("0") + null-check both produce 0.
    putLong(metadata, DataTable.MetadataKey.NUM_DOCS_SCANNED, _numDocsScanned);
    putLong(metadata, DataTable.MetadataKey.NUM_ENTRIES_SCANNED_IN_FILTER, _numEntriesScannedInFilter);
    putLong(metadata, DataTable.MetadataKey.NUM_ENTRIES_SCANNED_POST_FILTER, _numEntriesScannedPostFilter);
    putLong(metadata, DataTable.MetadataKey.NUM_SEGMENTS_QUERIED, _numSegmentsQueried);
    putLong(metadata, DataTable.MetadataKey.NUM_SEGMENTS_PROCESSED, _numSegmentsProcessed);
    putLong(metadata, DataTable.MetadataKey.NUM_SEGMENTS_MATCHED, _numSegmentsMatched);
    putLong(metadata, DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_QUERIED, _numConsumingSegmentsQueried);
    putLong(metadata, DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_PROCESSED, _numConsumingSegmentsProcessed);
    putLong(metadata, DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_MATCHED, _numConsumingSegmentsMatched);
    putLong(metadata, DataTable.MetadataKey.TOTAL_DOCS, _numTotalDocs);
    putLong(metadata, DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_SERVER, _numSegmentsPrunedByServer);
    putLong(metadata, DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_INVALID, _numSegmentsPrunedInvalid);
    putLong(metadata, DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_LIMIT, _numSegmentsPrunedByLimit);
    putLong(metadata, DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_VALUE, _numSegmentsPrunedByValue);
    putLong(metadata, DataTable.MetadataKey.EXPLAIN_PLAN_NUM_EMPTY_FILTER_SEGMENTS,
        _explainPlanNumEmptyFilterSegments);
    putLong(metadata, DataTable.MetadataKey.EXPLAIN_PLAN_NUM_MATCH_ALL_FILTER_SEGMENTS,
        _explainPlanNumMatchAllFilterSegments);
    // Collapse offline+realtime decomposition back to the combined wire-format keys.
    putLong(metadata, DataTable.MetadataKey.THREAD_CPU_TIME_NS,
        _offlineThreadCpuTimeNs + _realtimeThreadCpuTimeNs);
    putLong(metadata, DataTable.MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS,
        _offlineSystemActivitiesCpuTimeNs + _realtimeSystemActivitiesCpuTimeNs);
    putLong(metadata, DataTable.MetadataKey.RESPONSE_SER_CPU_TIME_NS,
        _offlineResponseSerializationCpuTimeNs + _realtimeResponseSerializationCpuTimeNs);
    putLong(metadata, DataTable.MetadataKey.THREAD_MEM_ALLOCATED_BYTES,
        _offlineThreadMemAllocatedBytes + _realtimeThreadMemAllocatedBytes);
    putLong(metadata, DataTable.MetadataKey.RESPONSE_SER_MEM_ALLOCATED_BYTES,
        _offlineResponseSerMemAllocatedBytes + _realtimeResponseSerMemAllocatedBytes);

    // MIN_CONSUMING_FRESHNESS_TIME_MS: sentinel-guarded. Long.MAX_VALUE means "no input had a real
    // freshness reading"; writing the sentinel would mislead downstream observability.
    if (_minConsumingFreshnessTimeMs != Long.MAX_VALUE) {
      metadata.put(DataTable.MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS.getName(),
          Long.toString(_minConsumingFreshnessTimeMs));
    }

    // Boolean flags: OR-reduced; only write the key when true (a "false" entry is noise and the
    // existing reduce path treats absent as false).
    if (_groupsTrimmed) {
      metadata.put(DataTable.MetadataKey.GROUPS_TRIMMED.getName(), "true");
    }
    if (_numGroupsLimitReached) {
      metadata.put(DataTable.MetadataKey.NUM_GROUPS_LIMIT_REACHED.getName(), "true");
    }
    if (_numGroupsWarningLimitReached) {
      metadata.put(DataTable.MetadataKey.NUM_GROUPS_WARNING_LIMIT_REACHED.getName(), "true");
    }

    // Exceptions: copy each accumulated exception onto the DataTable. Last-write-wins on error-code
    // collision (wire format is Map<Integer, String>).
    for (QueryProcessingException qpe : _processingExceptions) {
      dataTable.addException(qpe.getErrorCode(), qpe.getMessage());
    }

    // Trace: JSON-encode the per-server map into a single TRACE_INFO metadata entry. On downstream
    // readback the aggregator reads it as one string under the synthetic server's name.
    if (_enableTrace && !_traceInfo.isEmpty()) {
      try {
        metadata.put(DataTable.MetadataKey.TRACE_INFO.getName(), JsonUtils.objectToString(_traceInfo));
      } catch (JsonProcessingException e) {
        throw new IllegalStateException("Failed to serialize trace info for merged DataTable", e);
      }
    }
  }

  private static void putLong(Map<String, String> metadata, DataTable.MetadataKey key, long value) {
    metadata.put(key.getName(), Long.toString(value));
  }
}
