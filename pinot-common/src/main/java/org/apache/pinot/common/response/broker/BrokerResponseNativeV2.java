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
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.ProcessingException;


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

  private ObjectNode _stageStats = null;
  /**
   * The max number of rows seen at runtime.
   * <p>
   * In single-stage this doesn't make sense given it is the max number of rows read from the table. But in multi-stage
   * virtual rows can be generated. For example, in a join query, the number of rows can be more than the number of rows
   * in the table.
   */
  private long _maxRowsInOperator = 0;
  private final StatMap<StatKey> _brokerStats = new StatMap<>(StatKey.class);
  private final List<QueryProcessingException> _processingExceptions;
  private ResultTable _resultTable;
  private String _requestId;
  private String _brokerId;

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

  public void setStageStats(ObjectNode stageStats) {
    _stageStats = stageStats;
  }

  @JsonProperty
  public ObjectNode getStageStats() {
    return _stageStats;
  }

  @JsonProperty
  public long getMaxRowsInOperator() {
    return _maxRowsInOperator;
  }

  public void mergeMaxRowsInOperator(long maxRows) {
    _maxRowsInOperator = Math.max(_maxRowsInOperator, maxRows);
  }

  @Override
  public long getTimeUsedMs() {
    return _brokerStats.getLong(StatKey.TIME_USED_MS);
  }

  @Override
  public void setTimeUsedMs(long timeUsedMs) {
    _brokerStats.merge(StatKey.TIME_USED_MS, timeUsedMs);
  }

  @Override
  public long getNumDocsScanned() {
    return _brokerStats.getLong(StatKey.NUM_DOCS_SCANNED);
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
  public long getTotalDocs() {
    return _brokerStats.getLong(StatKey.TOTAL_DOCS);
  }

  @Override
  public boolean isNumGroupsLimitReached() {
    return _brokerStats.getBoolean(StatKey.NUM_GROUPS_LIMIT_REACHED);
  }

  public void mergeNumGroupsLimitReached(boolean numGroupsLimitReached) {
    _brokerStats.merge(StatKey.NUM_GROUPS_LIMIT_REACHED, numGroupsLimitReached);
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
  public long getExplainPlanNumEmptyFilterSegments() {
    return _brokerStats.getLong(StatKey.EXPLAIN_PLAN_NUM_EMPTY_FILTER_SEGMENTS);
  }

  @Override
  public long getExplainPlanNumMatchAllFilterSegments() {
    return _brokerStats.getLong(StatKey.EXPLAIN_PLAN_NUM_MATCH_ALL_FILTER_SEGMENTS);
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
    return _brokerStats.getInt(StatKey.NUM_SERVERS_QUERIED);
  }

  @Override
  public void setNumServersQueried(int numServersQueried) {
    _brokerStats.merge(StatKey.NUM_SERVERS_QUERIED, numServersQueried);
  }

  @Override
  public int getNumServersResponded() {
    return _brokerStats.getInt(StatKey.NUM_SERVERS_RESPONDED);
  }

  @Override
  public void setNumServersResponded(int numServersResponded) {
    _brokerStats.merge(StatKey.NUM_SERVERS_RESPONDED, numServersResponded);
  }

  @JsonProperty("maxRowsInJoinReached")
  public boolean isMaxRowsInJoinReached() {
    return _brokerStats.getBoolean(StatKey.MAX_ROWS_IN_JOIN_REACHED);
  }

  @JsonProperty("maxRowsInJoinReached")
  public void mergeMaxRowsInJoinReached(boolean maxRowsInJoinReached) {
    _brokerStats.merge(StatKey.MAX_ROWS_IN_JOIN_REACHED, maxRowsInJoinReached);
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

  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  @Override
  public int getNumRowsResultSet() {
    return BrokerResponse.super.getNumRowsResultSet();
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
  public long getNumSegmentsPrunedByBroker() {
    return _brokerStats.getInt(StatKey.NUM_SEGMENTS_PRUNED_BY_BROKER);
  }

  @Override
  public void setNumSegmentsPrunedByBroker(long numSegmentsPrunedByBroker) {
    _brokerStats.merge(StatKey.NUM_SEGMENTS_PRUNED_BY_BROKER, (int) numSegmentsPrunedByBroker);
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
    return _brokerStats.getLong(StatKey.BROKER_REDUCE_TIME_MS);
  }

  @Override
  public void setBrokerReduceTimeMs(long brokerReduceTimeMs) {
    _brokerStats.merge(StatKey.BROKER_REDUCE_TIME_MS, brokerReduceTimeMs);
  }

  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  @Override
  public boolean isPartialResult() {
    return isNumGroupsLimitReached() || getExceptionsSize() > 0 || isMaxRowsInJoinReached();
  }

  public void addServerStats(StatMap<StatKey> serverStats) {
    // Set execution statistics.
    _brokerStats.merge(serverStats);
  }

  public enum StatKey implements StatMap.Key {
    TIME_USED_MS(StatMap.Type.LONG),
    NUM_DOCS_SCANNED(StatMap.Type.LONG),
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
    TOTAL_DOCS(StatMap.Type.LONG),
    NUM_GROUPS_LIMIT_REACHED(StatMap.Type.BOOLEAN),
    NUM_SEGMENTS_PRUNED_BY_SERVER(StatMap.Type.INT),
    NUM_SEGMENTS_PRUNED_INVALID(StatMap.Type.INT),
    NUM_SEGMENTS_PRUNED_BY_LIMIT(StatMap.Type.INT),
    NUM_SEGMENTS_PRUNED_BY_VALUE(StatMap.Type.INT),
    NUM_SERGMENTS_PRUNED_BY_BROKER(StatMap.Type.INT),
    EXPLAIN_PLAN_NUM_EMPTY_FILTER_SEGMENTS(StatMap.Type.INT),
    EXPLAIN_PLAN_NUM_MATCH_ALL_FILTER_SEGMENTS(StatMap.Type.INT),
    THREAD_CPU_TIME_NS(StatMap.Type.LONG),
    SYSTEM_ACTIVITIES_CPU_TIME_NS(StatMap.Type.LONG),
    RESPONSE_SER_CPU_TIME_NS(StatMap.Type.LONG),
    NUM_SEGMENTS_PRUNED_BY_BROKER(StatMap.Type.INT),
    MAX_ROWS_IN_JOIN_REACHED(StatMap.Type.BOOLEAN),
    NUM_SERVERS_RESPONDED(StatMap.Type.INT),
    NUM_SERVERS_QUERIED(StatMap.Type.INT),
    BROKER_REDUCE_TIME_MS(StatMap.Type.LONG);

    private final StatMap.Type _type;

    StatKey(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }
}
