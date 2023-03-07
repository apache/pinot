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
package org.apache.pinot.common.response.broker;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * This class implements pinot-broker's response format for any given query.
 * All fields either primitive data types, or native objects (as opposed to JSONObjects).
 *
 * Supports serialization via JSON.
 */
@JsonPropertyOrder({
    "resultTable", "exceptions", "numServersQueried", "numServersResponded", "numSegmentsQueried",
    "numSegmentsProcessed", "numSegmentsMatched", "numConsumingSegmentsQueried", "numConsumingSegmentsProcessed",
    "numConsumingSegmentsMatched", "numDocsScanned", "numEntriesScannedInFilter", "numEntriesScannedPostFilter",
    "numGroupsLimitReached", "totalDocs", "timeUsedMs", "offlineThreadCpuTimeNs", "realtimeThreadCpuTimeNs",
    "offlineSystemActivitiesCpuTimeNs", "realtimeSystemActivitiesCpuTimeNs", "offlineResponseSerializationCpuTimeNs",
    "realtimeResponseSerializationCpuTimeNs", "offlineTotalCpuTimeNs", "realtimeTotalCpuTimeNs", "segmentStatistics",
    "traceInfo"
})
public class BrokerResponseNative implements BrokerResponse {
  public static final BrokerResponseNative EMPTY_RESULT = BrokerResponseNative.empty();
  public static final BrokerResponseNative NO_TABLE_RESULT =
      new BrokerResponseNative(QueryException.BROKER_RESOURCE_MISSING_ERROR);
  public static final BrokerResponseNative TABLE_DOES_NOT_EXIST =
      new BrokerResponseNative(QueryException.TABLE_DOES_NOT_EXIST_ERROR);
  public static final BrokerResponseNative BROKER_ONLY_EXPLAIN_PLAN_OUTPUT = getBrokerResponseExplainPlanOutput();

  private long _numSegmentsPrunedByBroker = 0L;
  private int _numServersQueried = 0;
  private int _numServersResponded = 0;
  private int _numRowsResultSet = 0;
  private String _tableName = null;
  private TableType _tableType = null;
  private ResultTable _resultTable;
  private Map<String, String> _traceInfo = new HashMap<>();
  private List<QueryProcessingException> _processingExceptions = new ArrayList<>();
  private List<String> _segmentStatistics = new ArrayList<>();
  protected Map<DataTable.MetadataKey, Object> _aggregatedStats = new HashMap<>();

  public BrokerResponseNative() {
  }

  public BrokerResponseNative(ProcessingException exception) {
    _processingExceptions.add(new QueryProcessingException(exception.getErrorCode(), exception.getMessage()));
  }

  public BrokerResponseNative(List<ProcessingException> exceptions) {
    for (ProcessingException exception : exceptions) {
      _processingExceptions.add(new QueryProcessingException(exception.getErrorCode(), exception.getMessage()));
    }
  }

  /** Generate EXPLAIN PLAN output when queries are evaluated by Broker without going to the Server. */
  private static BrokerResponseNative getBrokerResponseExplainPlanOutput() {
    BrokerResponseNative brokerResponse = BrokerResponseNative.empty();
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{"BROKER_EVALUATE", 0, -1});
    brokerResponse.setResultTable(new ResultTable(DataSchema.EXPLAIN_RESULT_SCHEMA, rows));
    return brokerResponse;
  }

  /**
   * Get a new empty {@link BrokerResponseNative}.
   */
  public static BrokerResponseNative empty() {
    return new BrokerResponseNative();
  }

  public static BrokerResponseNative fromJsonString(String jsonString)
      throws IOException {
    return JsonUtils.stringToObject(jsonString, BrokerResponseNative.class);
  }

  public void setAggregatedStats(Map<DataTable.MetadataKey, Object> aggregatedStats) {
    _aggregatedStats = aggregatedStats;
    _tableName = (String) _aggregatedStats.get(DataTable.MetadataKey.TABLE);
    if (_tableName != null && !_tableName.isEmpty()) {
      _tableType = TableNameBuilder.getTableTypeFromTableName(_tableName);
    }
  }

  @JsonProperty("offlineSystemActivitiesCpuTimeNs")
  @Override
  public long getOfflineSystemActivitiesCpuTimeNs() {
    if (_tableType == TableType.OFFLINE) {
      return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS, 0L);
    } else {
      return 0L;
    }
  }

  @JsonProperty("offlineSystemActivitiesCpuTimeNs")
  @Override
  public void setOfflineSystemActivitiesCpuTimeNs(long offlineSystemActivitiesCpuTimeNs) {
    if (_tableType == TableType.OFFLINE) {
      _aggregatedStats.put(DataTable.MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS, offlineSystemActivitiesCpuTimeNs);
    }
  }

  @JsonProperty("realtimeSystemActivitiesCpuTimeNs")
  @Override
  public long getRealtimeSystemActivitiesCpuTimeNs() {
    if (_tableType == TableType.REALTIME) {
      return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS, 0L);
    } else {
      return 0L;
    }
  }

  @JsonProperty("realtimeSystemActivitiesCpuTimeNs")
  @Override
  public void setRealtimeSystemActivitiesCpuTimeNs(long realtimeSystemActivitiesCpuTimeNs) {
    if (_tableType == TableType.REALTIME) {
      _aggregatedStats.put(DataTable.MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS, realtimeSystemActivitiesCpuTimeNs);
    }
  }

  @JsonProperty("offlineThreadCpuTimeNs")
  @Override
  public long getOfflineThreadCpuTimeNs() {
    if (_tableType == TableType.OFFLINE) {
      return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.THREAD_CPU_TIME_NS, 0L);
    } else {
      return 0L;
    }
  }

  @JsonProperty("offlineThreadCpuTimeNs")
  @Override
  public void setOfflineThreadCpuTimeNs(long timeUsedMs) {
    if (_tableType == TableType.OFFLINE) {
      _aggregatedStats.put(DataTable.MetadataKey.THREAD_CPU_TIME_NS, timeUsedMs);
    }
  }

  @JsonProperty("realtimeThreadCpuTimeNs")
  @Override
  public long getRealtimeThreadCpuTimeNs() {
    if (_tableType == TableType.REALTIME) {
      return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.THREAD_CPU_TIME_NS, 0L);
    } else {
      return 0L;
    }
  }

  @JsonProperty("realtimeThreadCpuTimeNs")
  @Override
  public void setRealtimeThreadCpuTimeNs(long timeUsedMs) {
    if (_tableType == TableType.REALTIME) {
      _aggregatedStats.put(DataTable.MetadataKey.THREAD_CPU_TIME_NS, timeUsedMs);
    }
  }

  @JsonProperty("offlineResponseSerializationCpuTimeNs")
  @Override
  public long getOfflineResponseSerializationCpuTimeNs() {
    if (_tableType == TableType.REALTIME) {
      return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.RESPONSE_SER_CPU_TIME_NS, 0L);
    } else {
      return 0L;
    }
  }

  @JsonProperty("offlineResponseSerializationCpuTimeNs")
  @Override
  public void setOfflineResponseSerializationCpuTimeNs(long offlineResponseSerializationCpuTimeNs) {
    if (_tableType == TableType.OFFLINE) {
      _aggregatedStats.put(DataTable.MetadataKey.RESPONSE_SER_CPU_TIME_NS, offlineResponseSerializationCpuTimeNs);
    }
  }

  @JsonProperty("realtimeResponseSerializationCpuTimeNs")
  @Override
  public long getRealtimeResponseSerializationCpuTimeNs() {
    if (_tableType == TableType.REALTIME) {
      return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.RESPONSE_SER_CPU_TIME_NS, 0L);
    } else {
      return 0L;
    }
  }

  @JsonProperty("realtimeResponseSerializationCpuTimeNs")
  @Override
  public void setRealtimeResponseSerializationCpuTimeNs(long realtimeResponseSerializationCpuTimeNs) {
    if (_tableType == TableType.REALTIME) {
      _aggregatedStats.put(DataTable.MetadataKey.RESPONSE_SER_CPU_TIME_NS, realtimeResponseSerializationCpuTimeNs);
    }
  }

  @JsonProperty("offlineTotalCpuTimeNs")
  @Override
  public long getOfflineTotalCpuTimeNs() {
    return getOfflineSystemActivitiesCpuTimeNs() + getOfflineThreadCpuTimeNs()
        + getOfflineResponseSerializationCpuTimeNs();
  }

  @JsonProperty("offlineTotalCpuTimeNs")
  @Override
  public void setOfflineTotalCpuTimeNs(long offlineTotalCpuTimeNs) {
    if (_tableType == TableType.OFFLINE) {
      _aggregatedStats.put(DataTable.MetadataKey.THREAD_CPU_TIME_NS, offlineTotalCpuTimeNs);
    }
  }

  @JsonProperty("realtimeTotalCpuTimeNs")
  @Override
  public long getRealtimeTotalCpuTimeNs() {
    return getRealtimeSystemActivitiesCpuTimeNs() + getRealtimeThreadCpuTimeNs()
        + getRealtimeResponseSerializationCpuTimeNs();
  }

  @JsonProperty("realtimeTotalCpuTimeNs")
  @Override
  public void setRealtimeTotalCpuTimeNs(long realtimeTotalCpuTimeNs) {
    if (_tableType == TableType.REALTIME) {
      _aggregatedStats.put(DataTable.MetadataKey.THREAD_CPU_TIME_NS, realtimeTotalCpuTimeNs);
    }
  }

  @JsonProperty("numSegmentsPrunedByBroker")
  @Override
  public long getNumSegmentsPrunedByBroker() {
    return _numSegmentsPrunedByBroker;
  }

  @JsonProperty("numSegmentsPrunedByBroker")
  @Override
  public void setNumSegmentsPrunedByBroker(long numSegmentsPrunedByBroker) {
    _numSegmentsPrunedByBroker = numSegmentsPrunedByBroker;
  }

  @JsonProperty("numSegmentsPrunedByServer")
  @Override
  public long getNumSegmentsPrunedByServer() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_SERVER, 0L);
  }

  @JsonProperty("numSegmentsPrunedByServer")
  @Override
  public void setNumSegmentsPrunedByServer(long numSegmentsPrunedByServer) {
    _aggregatedStats.put(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_SERVER, numSegmentsPrunedByServer);
  }

  @JsonProperty("numSegmentsPrunedInvalid")
  @Override
  public long getNumSegmentsPrunedInvalid() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_INVALID, 0L);
  }

  @JsonProperty("numSegmentsPrunedInvalid")
  @Override
  public void setNumSegmentsPrunedInvalid(long numSegmentsPrunedInvalid) {
    _aggregatedStats.put(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_INVALID, numSegmentsPrunedInvalid);
  }

  @JsonProperty("numSegmentsPrunedByLimit")
  @Override
  public long getNumSegmentsPrunedByLimit() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_LIMIT, 0L);
  }

  @JsonProperty("numSegmentsPrunedByLimit")
  @Override
  public void setNumSegmentsPrunedByLimit(long numSegmentsPrunedByLimit) {
    _aggregatedStats.put(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_LIMIT, numSegmentsPrunedByLimit);
  }

  @JsonProperty("numSegmentsPrunedByValue")
  @Override
  public long getNumSegmentsPrunedByValue() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_VALUE, 0L);
  }

  @JsonProperty("numSegmentsPrunedByValue")
  @Override
  public void setNumSegmentsPrunedByValue(long numSegmentsPrunedByValue) {
    _aggregatedStats.put(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_VALUE, numSegmentsPrunedByValue);
  }

  @JsonProperty("explainPlanNumEmptyFilterSegments")
  @Override
  public long getExplainPlanNumEmptyFilterSegments() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.EXPLAIN_PLAN_NUM_EMPTY_FILTER_SEGMENTS, 0L);
  }

  @JsonProperty("explainPlanNumEmptyFilterSegments")
  @Override
  public void setExplainPlanNumEmptyFilterSegments(long explainPlanNumEmptyFilterSegments) {
    _aggregatedStats.put(DataTable.MetadataKey.EXPLAIN_PLAN_NUM_EMPTY_FILTER_SEGMENTS,
        explainPlanNumEmptyFilterSegments);
  }

  @JsonProperty("explainPlanNumMatchAllFilterSegments")
  @Override
  public long getExplainPlanNumMatchAllFilterSegments() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.EXPLAIN_PLAN_NUM_MATCH_ALL_FILTER_SEGMENTS, 0L);
  }

  @JsonProperty("explainPlanNumMatchAllFilterSegments")
  @Override
  public void setExplainPlanNumMatchAllFilterSegments(long explainPlanNumMatchAllFilterSegments) {
    _aggregatedStats.put(DataTable.MetadataKey.EXPLAIN_PLAN_NUM_MATCH_ALL_FILTER_SEGMENTS,
        explainPlanNumMatchAllFilterSegments);
  }

  @JsonProperty("resultTable")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Override
  public ResultTable getResultTable() {
    return _resultTable;
  }

  @JsonProperty("resultTable")
  @Override
  public void setResultTable(ResultTable resultTable) {
    _resultTable = resultTable;
    _numRowsResultSet = resultTable.getRows().size();
  }

  @JsonProperty("exceptions")
  public List<QueryProcessingException> getProcessingExceptions() {
    return _processingExceptions;
  }

  @JsonProperty("exceptions")
  public void setProcessingExceptions(List<QueryProcessingException> processingExceptions) {
    _processingExceptions = processingExceptions;
  }

  @JsonProperty("numServersQueried")
  @Override
  public int getNumServersQueried() {
    return _numServersQueried;
  }

  @JsonProperty("numServersQueried")
  @Override
  public void setNumServersQueried(int numServersQueried) {
    _numServersQueried = numServersQueried;
  }

  @JsonProperty("numServersResponded")
  @Override
  public int getNumServersResponded() {
    return _numServersResponded;
  }

  @JsonProperty("numServersResponded")
  @Override
  public void setNumServersResponded(int numServersResponded) {
    _numServersResponded = numServersResponded;
  }

  @JsonProperty("numDocsScanned")
  public long getNumDocsScanned() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.NUM_DOCS_SCANNED, 0L);
  }

  @JsonProperty("numDocsScanned")
  public void setNumDocsScanned(long numDocsScanned) {
    _aggregatedStats.put(DataTable.MetadataKey.NUM_DOCS_SCANNED, numDocsScanned);
  }

  @JsonProperty("numEntriesScannedInFilter")
  @Override
  public long getNumEntriesScannedInFilter() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.NUM_ENTRIES_SCANNED_IN_FILTER, 0L);
  }

  @JsonProperty("numEntriesScannedInFilter")
  public void setNumEntriesScannedInFilter(long numEntriesScannedInFilter) {
    _aggregatedStats.put(DataTable.MetadataKey.NUM_ENTRIES_SCANNED_IN_FILTER, numEntriesScannedInFilter);
  }

  @JsonProperty("numEntriesScannedPostFilter")
  @Override
  public long getNumEntriesScannedPostFilter() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.NUM_ENTRIES_SCANNED_POST_FILTER, 0L);
  }

  @JsonProperty("numEntriesScannedPostFilter")
  public void setNumEntriesScannedPostFilter(long numEntriesScannedPostFilter) {
    _aggregatedStats.put(DataTable.MetadataKey.NUM_ENTRIES_SCANNED_POST_FILTER, numEntriesScannedPostFilter);
  }

  @JsonProperty("numSegmentsQueried")
  @Override
  public long getNumSegmentsQueried() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.NUM_SEGMENTS_QUERIED, 0L);
  }

  @JsonProperty("numSegmentsQueried")
  public void setNumSegmentsQueried(long numSegmentsQueried) {
    _aggregatedStats.put(DataTable.MetadataKey.NUM_SEGMENTS_QUERIED, numSegmentsQueried);
  }

  @JsonProperty("numSegmentsProcessed")
  @Override
  public long getNumSegmentsProcessed() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.NUM_SEGMENTS_PROCESSED, 0L);
  }

  @JsonProperty("numSegmentsProcessed")
  public void setNumSegmentsProcessed(long numSegmentsProcessed) {
    _aggregatedStats.put(DataTable.MetadataKey.NUM_SEGMENTS_PROCESSED, numSegmentsProcessed);
  }

  @JsonProperty("numSegmentsMatched")
  @Override
  public long getNumSegmentsMatched() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.NUM_SEGMENTS_MATCHED, 0L);
  }

  @JsonProperty("numSegmentsMatched")
  public void setNumSegmentsMatched(long numSegmentsMatched) {
    _aggregatedStats.put(DataTable.MetadataKey.NUM_SEGMENTS_MATCHED, numSegmentsMatched);
  }

  @JsonProperty("numConsumingSegmentsQueried")
  @Override
  public long getNumConsumingSegmentsQueried() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_QUERIED, 0L);
  }

  @JsonProperty("numConsumingSegmentsQueried")
  public void setNumConsumingSegmentsQueried(long numConsumingSegmentsQueried) {
    _aggregatedStats.put(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_QUERIED, numConsumingSegmentsQueried);
  }

  @JsonProperty("numConsumingSegmentsProcessed")
  @Override
  public long getNumConsumingSegmentsProcessed() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_PROCESSED, 0L);
  }

  @JsonProperty("numConsumingSegmentsProcessed")
  public void setNumConsumingSegmentsProcessed(long numConsumingSegmentsProcessed) {
    _aggregatedStats.put(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_PROCESSED, numConsumingSegmentsProcessed);
  }

  @JsonProperty("numConsumingSegmentsMatched")
  @Override
  public long getNumConsumingSegmentsMatched() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_MATCHED, 0L);
  }

  @JsonProperty("numConsumingSegmentsMatched")
  public void setNumConsumingSegmentsMatched(long numConsumingSegmentsMatched) {
    _aggregatedStats.put(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_MATCHED, numConsumingSegmentsMatched);
  }

  @JsonProperty("minConsumingFreshnessTimeMs")
  @Override
  public long getMinConsumingFreshnessTimeMs() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS, 0L);
  }

  @JsonProperty("minConsumingFreshnessTimeMs")
  public void setMinConsumingFreshnessTimeMs(long minConsumingFreshnessTimeMs) {
    _aggregatedStats.put(DataTable.MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS, minConsumingFreshnessTimeMs);
  }

  @JsonProperty("totalDocs")
  @Override
  public long getTotalDocs() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.TOTAL_DOCS, 0L);
  }

  @JsonProperty("totalDocs")
  public void setTotalDocs(long totalDocs) {
    _aggregatedStats.put(DataTable.MetadataKey.TOTAL_DOCS, totalDocs);
  }

  @JsonProperty("numGroupsLimitReached")
  @Override
  public boolean isNumGroupsLimitReached() {
    return Boolean.parseBoolean(
        (String) _aggregatedStats.getOrDefault(DataTable.MetadataKey.NUM_GROUPS_LIMIT_REACHED, "false"));
  }

  @JsonProperty("numGroupsLimitReached")
  public void setNumGroupsLimitReached(boolean numGroupsLimitReached) {
    _aggregatedStats.put(DataTable.MetadataKey.NUM_GROUPS_LIMIT_REACHED, String.valueOf(numGroupsLimitReached));
  }

  @JsonProperty("timeUsedMs")
  public long getTimeUsedMs() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.TIME_USED_MS, 0L);
  }

  @JsonProperty("timeUsedMs")
  @Override
  public void setTimeUsedMs(long timeUsedMs) {
    _aggregatedStats.put(DataTable.MetadataKey.TIME_USED_MS, timeUsedMs);
  }

  @JsonProperty("numRowsResultSet")
  @Override
  public int getNumRowsResultSet() {
    return _numRowsResultSet;
  }

  @JsonProperty("numRowsResultSet")
  @Override
  public void setNumRowsResultSet(int numRowsResultSet) {
    _numRowsResultSet = numRowsResultSet;
  }

  @JsonProperty("segmentStatistics")
  public List<String> getSegmentStatistics() {
    return _segmentStatistics;
  }

  @JsonProperty("segmentStatistics")
  public void setSegmentStatistics(List<String> segmentStatistics) {
    _segmentStatistics = segmentStatistics;
  }

  @JsonProperty("traceInfo")
  public Map<String, String> getTraceInfo() {
    return _traceInfo;
  }

  @JsonProperty("traceInfo")
  public void setTraceInfo(Map<String, String> traceInfo) {
    _traceInfo = traceInfo;
  }

  @Override
  public String toJsonString()
      throws IOException {
    return JsonUtils.objectToString(this);
  }

  @JsonIgnore
  @Override
  public void setExceptions(List<ProcessingException> exceptions) {
    for (ProcessingException exception : exceptions) {
      _processingExceptions.add(new QueryProcessingException(exception.getErrorCode(), exception.getMessage()));
    }
  }

  public void addToExceptions(QueryProcessingException processingException) {
    _processingExceptions.add(processingException);
  }

  @JsonIgnore
  @Override
  public int getExceptionsSize() {
    return _processingExceptions.size();
  }
}
