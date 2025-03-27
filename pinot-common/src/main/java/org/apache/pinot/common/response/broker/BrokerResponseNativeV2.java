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
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.ProcessingException;


/**
 * Broker response for multi-stage engine.
 * TODO: Currently this class cannot be used to deserialize the JSON response.
 */
@JsonPropertyOrder({
    "resultTable", "numRowsResultSet", "partialResult", "exceptions", "numGroupsLimitReached",
    "numGroupsWarningLimitReached", "maxRowsInJoinReached", "maxRowsInWindowReached", "timeUsedMs", "stageStats",
    "maxRowsInOperator", "requestId", "clientRequestId", "brokerId", "numDocsScanned", "totalDocs",
    "numEntriesScannedInFilter", "numEntriesScannedPostFilter", "numServersQueried", "numServersResponded",
    "numSegmentsQueried", "numSegmentsProcessed", "numSegmentsMatched", "numConsumingSegmentsQueried",
    "numConsumingSegmentsProcessed", "numConsumingSegmentsMatched", "minConsumingFreshnessTimeMs",
    "numSegmentsPrunedByBroker", "numSegmentsPrunedByServer", "numSegmentsPrunedInvalid", "numSegmentsPrunedByLimit",
    "numSegmentsPrunedByValue", "brokerReduceTimeMs", "offlineThreadCpuTimeNs", "realtimeThreadCpuTimeNs",
    "offlineSystemActivitiesCpuTimeNs", "realtimeSystemActivitiesCpuTimeNs", "offlineResponseSerializationCpuTimeNs",
    "realtimeResponseSerializationCpuTimeNs", "offlineTotalCpuTimeNs", "realtimeTotalCpuTimeNs",
    "explainPlanNumEmptyFilterSegments", "explainPlanNumMatchAllFilterSegments", "traceInfo", "tablesQueried"
})
public class BrokerResponseNativeV2 implements BrokerResponse {
  private final StatMap<StatKey> _brokerStats = new StatMap<>(StatKey.class);
  private final List<QueryProcessingException> _exceptions = new ArrayList<>();

  private ResultTable _resultTable;
  private int _numRowsResultSet;
  private boolean _maxRowsInJoinReached;
  private boolean _maxRowsInWindowReached;
  private long _timeUsedMs;
  /**
   * Statistics for each stage of the query execution.
   */
  private ObjectNode _stageStats;
  /**
   * The max number of rows seen at runtime.
   * <p>
   * In single-stage this doesn't make sense given it is the max number of rows read from the table. But in multi-stage
   * virtual rows can be generated. For example, in a join query, the number of rows can be more than the number of rows
   * in the table.
   */
  private long _maxRowsInOperator;
  private String _requestId;
  private String _clientRequestId;
  private String _brokerId;
  private int _numServersQueried;
  private int _numServersResponded;
  private long _brokerReduceTimeMs;
  private Set<String> _tablesQueried = Set.of();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @Override
  public ResultTable getResultTable() {
    return _resultTable;
  }

  @Override
  public void setResultTable(@Nullable ResultTable resultTable) {
    _resultTable = resultTable;
    // NOTE: Update _numRowsResultSet when setting non-null result table. We might set null result table when user wants
    //       to hide the result but only show the stats, in which case we should not update _numRowsResultSet.
    if (resultTable != null) {
      _numRowsResultSet = resultTable.getRows().size();
    }
  }

  @Override
  public int getNumRowsResultSet() {
    return _numRowsResultSet;
  }

  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  @Override
  public boolean isPartialResult() {
    return getExceptionsSize() > 0 || isNumGroupsLimitReached() || isMaxRowsInJoinReached();
  }

  @Override
  public List<QueryProcessingException> getExceptions() {
    return _exceptions;
  }

  public void addException(QueryProcessingException exception) {
    _exceptions.add(exception);
  }

  public void addException(ProcessingException exception) {
    addException(new QueryProcessingException(exception.getErrorCode(), exception.getMessage()));
  }

  @Override
  public boolean isNumGroupsLimitReached() {
    return _brokerStats.getBoolean(StatKey.NUM_GROUPS_LIMIT_REACHED);
  }

  public void mergeNumGroupsLimitReached(boolean numGroupsLimitReached) {
    _brokerStats.merge(StatKey.NUM_GROUPS_LIMIT_REACHED, numGroupsLimitReached);
  }

  @Override
  public boolean isNumGroupsWarningLimitReached() {
    return _brokerStats.getBoolean(StatKey.NUM_GROUPS_WARNING_LIMIT_REACHED);
  }

  public void mergeNumGroupsWarningLimitReached(boolean numGroupsWarningLimitReached) {
    _brokerStats.merge(StatKey.NUM_GROUPS_WARNING_LIMIT_REACHED, numGroupsWarningLimitReached);
  }

  @Override
  public boolean isMaxRowsInJoinReached() {
    return _maxRowsInJoinReached;
  }

  public void mergeMaxRowsInJoinReached(boolean maxRowsInJoinReached) {
    _maxRowsInJoinReached |= maxRowsInJoinReached;
  }

  @Override
  public boolean isMaxRowsInWindowReached() {
    return _maxRowsInWindowReached;
  }

  public void mergeMaxRowsInWindowReached(boolean maxRowsInWindowReached) {
    _maxRowsInWindowReached |= maxRowsInWindowReached;
  }

  /**
   * Returns the stage statistics.
   */
  public ObjectNode getStageStats() {
    return _stageStats;
  }

  public void setStageStats(ObjectNode stageStats) {
    _stageStats = stageStats;
  }

  /**
   * Returns the maximum number of rows seen by a single operator in the query processing chain.
   */
  public long getMaxRowsInOperator() {
    return _maxRowsInOperator;
  }

  public void mergeMaxRowsInOperator(long maxRows) {
    _maxRowsInOperator = Math.max(_maxRowsInOperator, maxRows);
  }

  @Override
  public long getTimeUsedMs() {
    return _timeUsedMs;
  }

  public void setTimeUsedMs(long timeUsedMs) {
    _timeUsedMs = timeUsedMs;
  }

  @Override
  public String getRequestId() {
    return _requestId;
  }

  @Override
  public String getClientRequestId() {
    return _clientRequestId;
  }

  @Override
  public void setClientRequestId(String clientRequestId) {
    _clientRequestId = clientRequestId;
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
  public void setBrokerId(String brokerId) {
    _brokerId = brokerId;
  }

  @Override
  public long getNumDocsScanned() {
    return _brokerStats.getLong(StatKey.NUM_DOCS_SCANNED);
  }

  @Override
  public long getTotalDocs() {
    return _brokerStats.getLong(StatKey.TOTAL_DOCS);
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    return _brokerStats.getLong(StatKey.NUM_ENTRIES_SCANNED_IN_FILTER);
  }

  @Override
  public long getNumEntriesScannedPostFilter() {
    return _brokerStats.getLong(StatKey.NUM_ENTRIES_SCANNED_POST_FILTER);
  }

  @Override
  public int getNumServersQueried() {
    return _numServersQueried;
  }

  public void setNumServersQueried(int numServersQueried) {
    _numServersQueried = numServersQueried;
  }

  @Override
  public int getNumServersResponded() {
    return _numServersResponded;
  }

  public void setNumServersResponded(int numServersResponded) {
    _numServersResponded = numServersResponded;
  }

  @Override
  public long getNumSegmentsQueried() {
    return _brokerStats.getLong(StatKey.NUM_SEGMENTS_QUERIED);
  }

  @Override
  public long getNumSegmentsProcessed() {
    return _brokerStats.getLong(StatKey.NUM_SEGMENTS_PROCESSED);
  }

  @Override
  public long getNumSegmentsMatched() {
    return _brokerStats.getLong(StatKey.NUM_SEGMENTS_MATCHED);
  }

  @Override
  public long getNumConsumingSegmentsQueried() {
    return _brokerStats.getLong(StatKey.NUM_CONSUMING_SEGMENTS_QUERIED);
  }

  @Override
  public long getNumConsumingSegmentsProcessed() {
    return _brokerStats.getLong(StatKey.NUM_CONSUMING_SEGMENTS_PROCESSED);
  }

  @Override
  public long getNumConsumingSegmentsMatched() {
    return _brokerStats.getLong(StatKey.NUM_CONSUMING_SEGMENTS_MATCHED);
  }

  @Override
  public long getMinConsumingFreshnessTimeMs() {
    return _brokerStats.getLong(StatKey.MIN_CONSUMING_FRESHNESS_TIME_MS);
  }

  @Override
  public long getNumSegmentsPrunedByBroker() {
    return 0;
  }

  @Override
  public long getNumSegmentsPrunedByServer() {
    return _brokerStats.getLong(StatKey.NUM_SEGMENTS_PRUNED_BY_SERVER);
  }

  @Override
  public long getNumSegmentsPrunedInvalid() {
    return _brokerStats.getLong(StatKey.NUM_SEGMENTS_PRUNED_INVALID);
  }

  @Override
  public long getNumSegmentsPrunedByLimit() {
    return _brokerStats.getLong(StatKey.NUM_SEGMENTS_PRUNED_BY_LIMIT);
  }

  @Override
  public long getNumSegmentsPrunedByValue() {
    return _brokerStats.getLong(StatKey.NUM_SEGMENTS_PRUNED_BY_VALUE);
  }

  @Override
  public long getBrokerReduceTimeMs() {
    return _brokerReduceTimeMs;
  }

  public void setBrokerReduceTimeMs(long brokerReduceTimeMs) {
    _brokerReduceTimeMs = brokerReduceTimeMs;
  }

  @Override
  public long getOfflineThreadCpuTimeNs() {
    return 0;
  }

  @Override
  public long getRealtimeThreadCpuTimeNs() {
    return 0;
  }

  @Override
  public long getOfflineSystemActivitiesCpuTimeNs() {
    return 0;
  }

  @Override
  public long getRealtimeSystemActivitiesCpuTimeNs() {
    return 0;
  }

  @Override
  public long getOfflineResponseSerializationCpuTimeNs() {
    return 0;
  }

  @Override
  public long getRealtimeResponseSerializationCpuTimeNs() {
    return 0;
  }

  @Override
  public long getExplainPlanNumEmptyFilterSegments() {
    return 0;
  }

  @Override
  public long getExplainPlanNumMatchAllFilterSegments() {
    return 0;
  }

  @Override
  public Map<String, String> getTraceInfo() {
    return Map.of();
  }

  public void addBrokerStats(StatMap<StatKey> brokerStats) {
    _brokerStats.merge(brokerStats);
  }

  // NOTE: The following keys should match the keys in the leaf-stage operator.
  public enum StatKey implements StatMap.Key {
    NUM_DOCS_SCANNED(StatMap.Type.LONG),
    TOTAL_DOCS(StatMap.Type.LONG),
    NUM_ENTRIES_SCANNED_IN_FILTER(StatMap.Type.LONG),
    NUM_ENTRIES_SCANNED_POST_FILTER(StatMap.Type.LONG),
    NUM_SEGMENTS_QUERIED(StatMap.Type.INT),
    NUM_SEGMENTS_PROCESSED(StatMap.Type.INT),
    NUM_SEGMENTS_MATCHED(StatMap.Type.INT),
    NUM_CONSUMING_SEGMENTS_QUERIED(StatMap.Type.INT),
    NUM_CONSUMING_SEGMENTS_PROCESSED(StatMap.Type.INT),
    NUM_CONSUMING_SEGMENTS_MATCHED(StatMap.Type.INT),
    MIN_CONSUMING_FRESHNESS_TIME_MS(StatMap.Type.LONG) {
      @Override
      public long merge(long value1, long value2) {
        return StatMap.Key.minPositive(value1, value2);
      }
    },
    NUM_SEGMENTS_PRUNED_BY_SERVER(StatMap.Type.INT),
    NUM_SEGMENTS_PRUNED_INVALID(StatMap.Type.INT),
    NUM_SEGMENTS_PRUNED_BY_LIMIT(StatMap.Type.INT),
    NUM_SEGMENTS_PRUNED_BY_VALUE(StatMap.Type.INT),
    NUM_GROUPS_LIMIT_REACHED(StatMap.Type.BOOLEAN),
    NUM_GROUPS_WARNING_LIMIT_REACHED(StatMap.Type.BOOLEAN);

    private final StatMap.Type _type;

    StatKey(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }

  @Override
  public void setTablesQueried(@NotNull Set<String> tablesQueried) {
    _tablesQueried = tablesQueried;
  }

  @Override
  @NotNull
  public Set<String> getTablesQueried() {
    return _tablesQueried;
  }
}
