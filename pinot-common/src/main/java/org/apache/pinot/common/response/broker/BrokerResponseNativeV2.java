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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.ProcessingException;
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
    "resultTable", "requestId", "stageStats", "brokerId", "exceptions", "numServersQueried", "numServersResponded",
    "numSegmentsQueried", "numSegmentsProcessed", "numSegmentsMatched", "numConsumingSegmentsQueried",
    "numConsumingSegmentsProcessed", "numConsumingSegmentsMatched", "numDocsScanned", "numEntriesScannedInFilter",
    "numEntriesScannedPostFilter", "numGroupsLimitReached", "maxRowsInJoinReached", "totalDocs", "timeUsedMs",
    "offlineThreadCpuTimeNs", "realtimeThreadCpuTimeNs", "offlineSystemActivitiesCpuTimeNs",
    "realtimeSystemActivitiesCpuTimeNs", "offlineResponseSerializationCpuTimeNs",
    "realtimeResponseSerializationCpuTimeNs", "offlineTotalCpuTimeNs", "realtimeTotalCpuTimeNs", "brokerReduceTimeMs",
    "segmentStatistics", "traceInfo", "partialResult"
})
public class BrokerResponseNativeV2 implements BrokerResponse {

  private final List<JsonNode> _stageIdStats = new ArrayList<>();
  /**
   * The max number of rows seen at runtime.
   * <p>
   * In single-stage this doesn't make sense given it is the max number of rows read from the table. But in multi-stage
   * virtual rows can be generated. For example, in a join query, the number of rows can be more than the number of rows
   * in the table.
   */
  private long _maxRows = 0;
  private final StatMap<DataTable.MetadataKey> _serverStats = new StatMap<>(DataTable.MetadataKey.class);
  private List<QueryProcessingException> _processingExceptions;
  private ResultTable _resultTable;
  private boolean _maxRowsInJoinReached;
  private int _numServersResponded;
  private int _numServersQueried;
  private long _offlineThreadCpuTimeNs;
  private long _realtimeThreadCpuTimeNs;
  private long _offlineSystemActivitiesCpuTimeNs;
  private long _realtimeSystemActivitiesCpuTimeNs;
  private long _offlineResponseSerializationCpuTimeNs;
  private long _realtimeResponseSerializationCpuTimeNs;
  private long _numSegmentsPrunedByBroker;
  private String _requestId;
  private String _brokerId;
  private long _brokerReduceTimeMs;

  public BrokerResponseNativeV2() {
    _processingExceptions = new ArrayList<>();
  }

  public BrokerResponseNativeV2(ProcessingException exception) {
    this(Collections.singletonList(exception));
  }

  public BrokerResponseNativeV2(List<ProcessingException> exceptions) {
    _processingExceptions = new ArrayList<>(exceptions.size());
    for (ProcessingException exception : exceptions) {
      _processingExceptions.add(new QueryProcessingException(exception.getErrorCode(), exception.getMessage()));
    }
  }

  /**
   * Get a new empty {@link BrokerResponseNativeV2}.
   */
  public static BrokerResponseNativeV2 empty() {
    return new BrokerResponseNativeV2();
  }

  public void addStageStats(JsonNode stageStats) {
    ObjectNode node = JsonUtils.newObjectNode();
    node.put("stage", _stageIdStats.size());
    node.set("stats", stageStats);
    _stageIdStats.add(node);
  }

  @JsonProperty
  public List<JsonNode> getStageStats() {
    return _stageIdStats;
  }

  @JsonProperty
  public long getMaxRows() {
    return _maxRows;
  }

  public void mergeMaxRows(long maxRows) {
    _maxRows = Math.max(_maxRows, maxRows);
  }

  @Override
  public long getTimeUsedMs() {
    return _serverStats.getLong(DataTable.MetadataKey.TIME_USED_MS);
  }

  @Override
  public void setTimeUsedMs(long timeUsedMs) {
    _serverStats.merge(DataTable.MetadataKey.TIME_USED_MS, timeUsedMs);
  }

  @Override
  public long getNumDocsScanned() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_DOCS_SCANNED);
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_ENTRIES_SCANNED_IN_FILTER);
  }

  @Override
  public long getNumEntriesScannedPostFilter() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_ENTRIES_SCANNED_POST_FILTER);
  }

  @Override
  public long getNumSegmentsQueried() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_SEGMENTS_QUERIED);
  }

  @Override
  public long getNumSegmentsProcessed() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_SEGMENTS_PROCESSED);
  }

  @Override
  public long getNumSegmentsMatched() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_SEGMENTS_MATCHED);
  }

  @Override
  public long getNumConsumingSegmentsQueried() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_QUERIED);
  }

  @Override
  public long getNumConsumingSegmentsProcessed() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_PROCESSED);
  }

  @Override
  public long getNumConsumingSegmentsMatched() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_MATCHED);
  }

  @Override
  public long getMinConsumingFreshnessTimeMs() {
    return _serverStats.getLong(DataTable.MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS);
  }

  @Override
  public long getTotalDocs() {
    return _serverStats.getLong(DataTable.MetadataKey.TOTAL_DOCS);
  }

  @Override
  public boolean isNumGroupsLimitReached() {
    return _serverStats.getBoolean(DataTable.MetadataKey.NUM_GROUPS_LIMIT_REACHED);
  }

  public void mergeNumGroupsLimitReached(boolean numGroupsLimitReached) {
    _serverStats.merge(DataTable.MetadataKey.NUM_GROUPS_LIMIT_REACHED, numGroupsLimitReached);
  }

  @Override
  public long getNumSegmentsPrunedByServer() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_SERVER);
  }

  @Override
  public long getNumSegmentsPrunedInvalid() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_INVALID);
  }

  @Override
  public long getNumSegmentsPrunedByLimit() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_LIMIT);
  }

  @Override
  public long getNumSegmentsPrunedByValue() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_VALUE);
  }

  @Override
  public long getExplainPlanNumEmptyFilterSegments() {
    return _serverStats.getLong(DataTable.MetadataKey.EXPLAIN_PLAN_NUM_EMPTY_FILTER_SEGMENTS);
  }

  @Override
  public long getExplainPlanNumMatchAllFilterSegments() {
    return _serverStats.getLong(DataTable.MetadataKey.EXPLAIN_PLAN_NUM_MATCH_ALL_FILTER_SEGMENTS);
  }

  @Override
  public long getOfflineTotalCpuTimeNs() {
    return getOfflineThreadCpuTimeNs() + getOfflineSystemActivitiesCpuTimeNs()
        + getOfflineResponseSerializationCpuTimeNs();
  }

  @Override
  public long getRealtimeTotalCpuTimeNs() {
    return getRealtimeThreadCpuTimeNs() + getRealtimeSystemActivitiesCpuTimeNs()
        + getRealtimeResponseSerializationCpuTimeNs();
  }

  @Override
  public void setExceptions(List<ProcessingException> exceptions) {
    for (ProcessingException exception : exceptions) {
      _processingExceptions.add(new QueryProcessingException(exception.getErrorCode(), exception.getMessage()));
    }
  }

  public void addToExceptions(QueryProcessingException processingException) {
    _processingExceptions.add(processingException);
  }

  @Override
  public int getNumServersQueried() {
    return _numServersQueried;
  }

  @Override
  public void setNumServersQueried(int numServersQueried) {
    _numServersQueried = numServersQueried;
  }

  @Override
  public int getNumServersResponded() {
    return _numServersResponded;
  }

  @Override
  public void setNumServersResponded(int numServersResponded) {
    _numServersResponded = numServersResponded;
  }

  @JsonProperty("maxRowsInJoinReached")
  public boolean isMaxRowsInJoinReached() {
    return _maxRowsInJoinReached;
  }

  @JsonProperty("maxRowsInJoinReached")
  public void mergeMaxRowsInJoinReached(boolean maxRowsInJoinReached) {
    _maxRowsInJoinReached |= maxRowsInJoinReached;
  }

  @Override
  public int getExceptionsSize() {
    return _processingExceptions.size();
  }

  @Override
  public void setResultTable(@Nullable ResultTable resultTable) {
    _resultTable = resultTable;
  }

  @Nullable
  @Override
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ResultTable getResultTable() {
    return _resultTable;
  }

  @JsonProperty("exceptions")
  @Override
  public List<QueryProcessingException> getProcessingExceptions() {
    return List.of();
  }

  @Override
  public int getNumRowsResultSet() {
    return 0;
  }

  @Override
  public long getOfflineThreadCpuTimeNs() {
    return _offlineThreadCpuTimeNs;
  }

  @Override
  public long getRealtimeThreadCpuTimeNs() {
    return _realtimeThreadCpuTimeNs;
  }

  @Override
  public long getOfflineSystemActivitiesCpuTimeNs() {
    return _offlineSystemActivitiesCpuTimeNs;
  }

  @Override
  public long getRealtimeSystemActivitiesCpuTimeNs() {
    return _realtimeSystemActivitiesCpuTimeNs;
  }

  @Override
  public long getOfflineResponseSerializationCpuTimeNs() {
    return _offlineResponseSerializationCpuTimeNs;
  }

  @Override
  public long getRealtimeResponseSerializationCpuTimeNs() {
    return _realtimeResponseSerializationCpuTimeNs;
  }

  @Override
  public long getNumSegmentsPrunedByBroker() {
    return _numSegmentsPrunedByBroker;
  }

  @Override
  public void setNumSegmentsPrunedByBroker(long numSegmentsPrunedByBroker) {
    _numSegmentsPrunedByBroker = numSegmentsPrunedByBroker;
  }

  @Override
  public String getRequestId() {
    return _requestId;
  }

  @Override
  public void setRequestId(String requestId) {
    _requestId = requestId;
  }

  @Override
  public String getBrokerId() {
    return _brokerId;
  }

  @Override
  public void setBrokerId(String requestId) {
    _brokerId = requestId;
  }

  @Override
  public long getBrokerReduceTimeMs() {
    return _brokerReduceTimeMs;
  }

  @Override
  public void setBrokerReduceTimeMs(long brokerReduceTimeMs) {
    _brokerReduceTimeMs = brokerReduceTimeMs;
  }

  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  @Override
  public boolean isPartialResult() {
    return isNumGroupsLimitReached() || getExceptionsSize() > 0 || isMaxRowsInJoinReached();
  }

  public void addServerStats(StatMap<DataTable.MetadataKey> serverStats) {
    // Set execution statistics.
    _serverStats.merge(serverStats);

    long threadCpuTimeNs = serverStats.getLong(DataTable.MetadataKey.THREAD_CPU_TIME_NS);
    long systemActivitiesCpuTimeNs = serverStats.getLong(DataTable.MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS);
    long responseSerializationCpuTimeNs = serverStats.getLong(DataTable.MetadataKey.RESPONSE_SER_CPU_TIME_NS);

    String tableName = serverStats.getString(DataTable.MetadataKey.TABLE);
    if (tableName != null) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);

      if (tableType == TableType.OFFLINE) {
        _offlineThreadCpuTimeNs += threadCpuTimeNs;
        _offlineSystemActivitiesCpuTimeNs += systemActivitiesCpuTimeNs;
        _offlineResponseSerializationCpuTimeNs += responseSerializationCpuTimeNs;
      } else {
        _realtimeThreadCpuTimeNs += threadCpuTimeNs;
        _realtimeSystemActivitiesCpuTimeNs += systemActivitiesCpuTimeNs;
        _realtimeResponseSerializationCpuTimeNs += responseSerializationCpuTimeNs;
      }
    }
  }
}
