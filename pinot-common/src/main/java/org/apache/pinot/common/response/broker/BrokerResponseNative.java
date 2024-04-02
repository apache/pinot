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
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements pinot-broker's response format for any given query.
 * All fields either primitive data types, or native objects (as opposed to JSONObjects).
 * <p>
 * Supports serialization via JSON.
 */
@JsonPropertyOrder({
    "resultTable", "requestId", "brokerId", "exceptions", "numServersQueried", "numServersResponded",
    "numSegmentsQueried", "numSegmentsProcessed", "numSegmentsMatched", "numConsumingSegmentsQueried",
    "numConsumingSegmentsProcessed", "numConsumingSegmentsMatched", "numDocsScanned", "numEntriesScannedInFilter",
    "numEntriesScannedPostFilter", "numGroupsLimitReached", "maxRowsInJoinReached", "totalDocs", "timeUsedMs",
    "offlineThreadCpuTimeNs", "realtimeThreadCpuTimeNs", "offlineSystemActivitiesCpuTimeNs",
    "realtimeSystemActivitiesCpuTimeNs", "offlineResponseSerializationCpuTimeNs",
    "realtimeResponseSerializationCpuTimeNs", "offlineTotalCpuTimeNs", "realtimeTotalCpuTimeNs", "brokerReduceTimeMs",
    "segmentStatistics", "traceInfo", "partialResult"
})
public class BrokerResponseNative implements BrokerResponse {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerResponseNative.class);
  public static final BrokerResponseNative EMPTY_RESULT = BrokerResponseNative.empty();
  public static final BrokerResponseNative NO_TABLE_RESULT =
      new BrokerResponseNative(QueryException.BROKER_RESOURCE_MISSING_ERROR);
  public static final BrokerResponseNative TABLE_DOES_NOT_EXIST =
      new BrokerResponseNative(QueryException.TABLE_DOES_NOT_EXIST_ERROR);
  public static final BrokerResponseNative BROKER_ONLY_EXPLAIN_PLAN_OUTPUT = getBrokerResponseExplainPlanOutput();

  private String _requestId;
  private String _brokerId;
  private int _numServersQueried = 0;
  private int _numServersResponded = 0;
  private long _brokerReduceTimeMs = 0L;
  /**
   * Whether the max rows in join has been reached.
   * <p>
   * This max can be set by using PinotHintOptions.JOIN_HINT_OPTIONS and defaults to
   * HashJoiner.MAX_ROWS_IN_JOIN_DEFAULT.
   */
  private boolean _maxRowsInJoinReached = false;
  private boolean _partialResult = false;
  private long _offlineThreadCpuTimeNs = 0L;
  private long _realtimeThreadCpuTimeNs = 0L;
  private long _offlineSystemActivitiesCpuTimeNs = 0L;
  private long _realtimeSystemActivitiesCpuTimeNs = 0L;
  private long _offlineResponseSerializationCpuTimeNs = 0L;
  private long _realtimeResponseSerializationCpuTimeNs = 0L;
  private long _numSegmentsPrunedByBroker = 0L;
  private int _numRowsResultSet = 0;
  private ResultTable _resultTable;
  private Map<String, String> _traceInfo = new HashMap<>();
  private Map<String, JsonNode> _traceInfo2 = new HashMap<>();
  private List<QueryProcessingException> _processingExceptions = new ArrayList<>();
  private List<String> _segmentStatistics = new ArrayList<>();
  private final StatMap<DataTable.MetadataKey> _serverStats = new StatMap<>(DataTable.MetadataKey.class);

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

  @JsonProperty("offlineSystemActivitiesCpuTimeNs")
  @Override
  public long getOfflineSystemActivitiesCpuTimeNs() {
    return _offlineSystemActivitiesCpuTimeNs;
  }

  @JsonProperty("offlineSystemActivitiesCpuTimeNs")
  @Override
  public void setOfflineSystemActivitiesCpuTimeNs(long offlineSystemActivitiesCpuTimeNs) {
    _offlineSystemActivitiesCpuTimeNs = offlineSystemActivitiesCpuTimeNs;
  }

  @JsonProperty("realtimeSystemActivitiesCpuTimeNs")
  @Override
  public long getRealtimeSystemActivitiesCpuTimeNs() {
    return _realtimeSystemActivitiesCpuTimeNs;
  }

  @JsonProperty("realtimeSystemActivitiesCpuTimeNs")
  @Override
  public void setRealtimeSystemActivitiesCpuTimeNs(long realtimeSystemActivitiesCpuTimeNs) {
    _realtimeSystemActivitiesCpuTimeNs = realtimeSystemActivitiesCpuTimeNs;
  }

  @JsonProperty("offlineThreadCpuTimeNs")
  @Override
  public long getOfflineThreadCpuTimeNs() {
    return _offlineThreadCpuTimeNs;
  }

  @JsonProperty("offlineThreadCpuTimeNs")
  @Override
  public void setOfflineThreadCpuTimeNs(long timeUsedMs) {
    _offlineThreadCpuTimeNs = timeUsedMs;
  }

  @JsonProperty("realtimeThreadCpuTimeNs")
  @Override
  public long getRealtimeThreadCpuTimeNs() {
    return _realtimeThreadCpuTimeNs;
  }

  @JsonProperty("realtimeThreadCpuTimeNs")
  @Override
  public void setRealtimeThreadCpuTimeNs(long timeUsedMs) {
    _realtimeThreadCpuTimeNs = timeUsedMs;
  }

  @JsonProperty("offlineResponseSerializationCpuTimeNs")
  @Override
  public long getOfflineResponseSerializationCpuTimeNs() {
    return _offlineResponseSerializationCpuTimeNs;
  }

  @JsonProperty("offlineResponseSerializationCpuTimeNs")
  @Override
  public void setOfflineResponseSerializationCpuTimeNs(long offlineResponseSerializationCpuTimeNs) {
    _offlineResponseSerializationCpuTimeNs = offlineResponseSerializationCpuTimeNs;
  }

  @JsonProperty("realtimeResponseSerializationCpuTimeNs")
  @Override
  public long getRealtimeResponseSerializationCpuTimeNs() {
    return _realtimeResponseSerializationCpuTimeNs;
  }

  @JsonProperty("realtimeResponseSerializationCpuTimeNs")
  @Override
  public void setRealtimeResponseSerializationCpuTimeNs(long realtimeResponseSerializationCpuTimeNs) {
    _realtimeResponseSerializationCpuTimeNs = realtimeResponseSerializationCpuTimeNs;
  }

  @JsonProperty("offlineTotalCpuTimeNs")
  @Override
  public long getOfflineTotalCpuTimeNs() {
    return getOfflineThreadCpuTimeNs() + getOfflineSystemActivitiesCpuTimeNs()
        + getOfflineResponseSerializationCpuTimeNs();
  }

  @JsonProperty("realtimeTotalCpuTimeNs")
  @Override
  public long getRealtimeTotalCpuTimeNs() {
    return getRealtimeThreadCpuTimeNs() + getRealtimeSystemActivitiesCpuTimeNs()
        + getRealtimeResponseSerializationCpuTimeNs();
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
    return _serverStats.getLong(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_SERVER);
  }

  @JsonProperty("numSegmentsPrunedByServer")
  @Override
  public void setNumSegmentsPrunedByServer(long numSegmentsPrunedByServer) {
    _serverStats.merge(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_SERVER, (int) numSegmentsPrunedByServer);
  }

  @JsonProperty("numSegmentsPrunedInvalid")
  @Override
  public long getNumSegmentsPrunedInvalid() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_INVALID);
  }

  @JsonProperty("numSegmentsPrunedInvalid")
  @Override
  public void setNumSegmentsPrunedInvalid(long numSegmentsPrunedInvalid) {
    _serverStats.merge(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_INVALID, (int) numSegmentsPrunedInvalid);
  }

  @JsonProperty("numSegmentsPrunedByLimit")
  @Override
  public long getNumSegmentsPrunedByLimit() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_LIMIT);
  }

  @JsonProperty("numSegmentsPrunedByLimit")
  @Override
  public void setNumSegmentsPrunedByLimit(long numSegmentsPrunedByLimit) {
    _serverStats.merge(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_LIMIT, (int) numSegmentsPrunedByLimit);
  }

  @JsonProperty("numSegmentsPrunedByValue")
  @Override
  public long getNumSegmentsPrunedByValue() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_VALUE);
  }

  @JsonProperty("numSegmentsPrunedByValue")
  @Override
  public void setNumSegmentsPrunedByValue(long numSegmentsPrunedByValue) {
    _serverStats.merge(DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_VALUE, (int) numSegmentsPrunedByValue);
  }

  @JsonProperty("explainPlanNumEmptyFilterSegments")
  @Override
  public long getExplainPlanNumEmptyFilterSegments() {
    return _serverStats.getLong(DataTable.MetadataKey.EXPLAIN_PLAN_NUM_EMPTY_FILTER_SEGMENTS);
  }

  @JsonProperty("explainPlanNumEmptyFilterSegments")
  @Override
  public void setExplainPlanNumEmptyFilterSegments(long explainPlanNumEmptyFilterSegments) {
    _serverStats.merge(DataTable.MetadataKey.EXPLAIN_PLAN_NUM_EMPTY_FILTER_SEGMENTS,
        (int) explainPlanNumEmptyFilterSegments);
  }

  @JsonProperty("explainPlanNumMatchAllFilterSegments")
  @Override
  public long getExplainPlanNumMatchAllFilterSegments() {
    return _serverStats.getLong(DataTable.MetadataKey.EXPLAIN_PLAN_NUM_MATCH_ALL_FILTER_SEGMENTS);
  }

  @JsonProperty("explainPlanNumMatchAllFilterSegments")
  @Override
  public void setExplainPlanNumMatchAllFilterSegments(long explainPlanNumMatchAllFilterSegments) {
    _serverStats.merge(DataTable.MetadataKey.EXPLAIN_PLAN_NUM_MATCH_ALL_FILTER_SEGMENTS,
        (int) explainPlanNumMatchAllFilterSegments);
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
    // If query level parameter is set to not return the results, then resultTable will be null.
    if (resultTable != null) {
      _numRowsResultSet = resultTable.getRows().size();
    }
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
    return _serverStats.getLong(DataTable.MetadataKey.NUM_DOCS_SCANNED);
  }

  @JsonProperty("numDocsScanned")
  public void setNumDocsScanned(long numDocsScanned) {
    _serverStats.merge(DataTable.MetadataKey.NUM_DOCS_SCANNED, numDocsScanned);
  }

  @JsonProperty("numEntriesScannedInFilter")
  @Override
  public long getNumEntriesScannedInFilter() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_ENTRIES_SCANNED_IN_FILTER);
  }

  @JsonProperty("numEntriesScannedInFilter")
  public void setNumEntriesScannedInFilter(long numEntriesScannedInFilter) {
    _serverStats.merge(DataTable.MetadataKey.NUM_ENTRIES_SCANNED_IN_FILTER, numEntriesScannedInFilter);
  }

  @JsonProperty("numEntriesScannedPostFilter")
  @Override
  public long getNumEntriesScannedPostFilter() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_ENTRIES_SCANNED_POST_FILTER);
  }

  @JsonProperty("numEntriesScannedPostFilter")
  public void setNumEntriesScannedPostFilter(long numEntriesScannedPostFilter) {
    _serverStats.merge(DataTable.MetadataKey.NUM_ENTRIES_SCANNED_POST_FILTER, numEntriesScannedPostFilter);
  }

  @JsonProperty("numSegmentsQueried")
  @Override
  public long getNumSegmentsQueried() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_SEGMENTS_QUERIED);
  }

  @JsonProperty("numSegmentsQueried")
  public void setNumSegmentsQueried(long numSegmentsQueried) {
    _serverStats.merge(DataTable.MetadataKey.NUM_SEGMENTS_QUERIED, (int) numSegmentsQueried);
  }

  @JsonProperty("numSegmentsProcessed")
  @Override
  public long getNumSegmentsProcessed() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_SEGMENTS_PROCESSED);
  }

  @JsonProperty("numSegmentsProcessed")
  public void setNumSegmentsProcessed(long numSegmentsProcessed) {
    _serverStats.merge(DataTable.MetadataKey.NUM_SEGMENTS_PROCESSED, (int) numSegmentsProcessed);
  }

  @JsonProperty("numSegmentsMatched")
  @Override
  public long getNumSegmentsMatched() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_SEGMENTS_MATCHED);
  }

  @JsonProperty("numSegmentsMatched")
  public void setNumSegmentsMatched(long numSegmentsMatched) {
    _serverStats.merge(DataTable.MetadataKey.NUM_SEGMENTS_MATCHED, (int) numSegmentsMatched);
  }

  @JsonProperty("numConsumingSegmentsQueried")
  @Override
  public long getNumConsumingSegmentsQueried() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_QUERIED);
  }

  @JsonProperty("numConsumingSegmentsQueried")
  public void setNumConsumingSegmentsQueried(long numConsumingSegmentsQueried) {
    _serverStats.merge(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_QUERIED, (int) numConsumingSegmentsQueried);
  }

  @JsonProperty("numConsumingSegmentsProcessed")
  @Override
  public long getNumConsumingSegmentsProcessed() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_PROCESSED);
  }

  @JsonProperty("numConsumingSegmentsProcessed")
  public void setNumConsumingSegmentsProcessed(long numConsumingSegmentsProcessed) {
    _serverStats.merge(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_PROCESSED, (int) numConsumingSegmentsProcessed);
  }

  @JsonProperty("numConsumingSegmentsMatched")
  @Override
  public long getNumConsumingSegmentsMatched() {
    return _serverStats.getLong(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_MATCHED);
  }

  @JsonProperty("numConsumingSegmentsMatched")
  public void setNumConsumingSegmentsMatched(long numConsumingSegmentsMatched) {
    _serverStats.merge(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_MATCHED, (int) numConsumingSegmentsMatched);
  }

  @JsonProperty("minConsumingFreshnessTimeMs")
  @Override
  public long getMinConsumingFreshnessTimeMs() {
    return _serverStats.getLong(DataTable.MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS);
  }

  @JsonProperty("minConsumingFreshnessTimeMs")
  public void setMinConsumingFreshnessTimeMs(long minConsumingFreshnessTimeMs) {
    _serverStats.merge(DataTable.MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS, minConsumingFreshnessTimeMs);
  }

  @JsonProperty("totalDocs")
  @Override
  public long getTotalDocs() {
    return _serverStats.getLong(DataTable.MetadataKey.TOTAL_DOCS);
  }

  @JsonProperty("totalDocs")
  public void setTotalDocs(long totalDocs) {
    _serverStats.merge(DataTable.MetadataKey.TOTAL_DOCS, totalDocs);
  }

  @JsonProperty("numGroupsLimitReached")
  @Override
  public boolean isNumGroupsLimitReached() {
    return _serverStats.getBoolean(DataTable.MetadataKey.NUM_GROUPS_LIMIT_REACHED);
  }

  @JsonProperty("numGroupsLimitReached")
  public void setNumGroupsLimitReached(boolean numGroupsLimitReached) {
    _serverStats.merge(DataTable.MetadataKey.NUM_GROUPS_LIMIT_REACHED, numGroupsLimitReached);
  }

  public void mergeNumGroupsLimitReached(boolean numGroupsLimitReached) {
    _serverStats.merge(DataTable.MetadataKey.NUM_GROUPS_LIMIT_REACHED, numGroupsLimitReached);
  }

  @JsonProperty("maxRowsInJoinReached")
  @Override
  public boolean isMaxRowsInJoinReached() {
    return _maxRowsInJoinReached;
  }

  public void setMaxRowsInJoinReached(boolean maxRowsInJoinReached) {
    _maxRowsInJoinReached = maxRowsInJoinReached;
  }

  public void mergeMaxRowsInJoinReached(boolean maxRowsInJoinReached) {
    _maxRowsInJoinReached |= maxRowsInJoinReached;
  }

  @JsonProperty("partialResult")
  public boolean isPartialResult() {
    return _partialResult;
  }

  @JsonProperty("partialResult")
  public void setPartialResult(boolean partialResult) {
    _partialResult = partialResult;
  }

  @JsonProperty("timeUsedMs")
  public long getTimeUsedMs() {
    return _serverStats.getLong(DataTable.MetadataKey.TIME_USED_MS);
  }

  @JsonProperty("timeUsedMs")
  @Override
  public void setTimeUsedMs(long timeUsedMs) {
    _serverStats.merge(DataTable.MetadataKey.TIME_USED_MS, timeUsedMs);
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
    _traceInfo2.clear();
    for (Map.Entry<String, String> entry : _traceInfo.entrySet()) {
      try {
        _traceInfo2.put(entry.getKey(), JsonUtils.stringToJsonNode(entry.getValue()));
      } catch (IOException e) {
          LOGGER.debug("Caught exception while converting entry " + entry.getKey() + " from traceInfo to "
              + "traceInfo2", e);
      }
    }
  }

  @JsonProperty("traceInfo2")
  public Map<String, JsonNode> getTraceInfo2() {
    return _traceInfo2;
  }

  @JsonProperty("traceInfo2")
  public void setTraceInfo2(Map<String, JsonNode> traceInfo) {
    _traceInfo2 = traceInfo;
    _traceInfo.clear();
    for (Map.Entry<String, JsonNode> entry : _traceInfo2.entrySet()) {
      _traceInfo.put(entry.getKey(), entry.getValue().toString());
    }
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

  @JsonProperty("requestId")
  @Override
  public String getRequestId() {
    return _requestId;
  }

  @JsonProperty("requestId")
  @Override
  public void setRequestId(String requestId) {
    _requestId = requestId;
  }

  @JsonProperty("brokerId")
  @Override
  public String getBrokerId() {
    return _brokerId;
  }

  @JsonProperty("brokerId")
  @Override
  public void setBrokerId(String requestId) {
    _brokerId = requestId;
  }

  @JsonProperty("brokerReduceTimeMs")
  @Override
  public long getBrokerReduceTimeMs() {
    return _brokerReduceTimeMs;
  }

  @JsonProperty("brokerReduceTimeMs")
  @Override
  public void setBrokerReduceTimeMs(long brokerReduceTimeMs) {
    _brokerReduceTimeMs = brokerReduceTimeMs;
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
