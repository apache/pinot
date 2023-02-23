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
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * This class implements pinot-broker's response format for any given query.
 * All fields either primitive data types, or native objects (as opposed to JSONObjects).
 *
 * Supports serialization via JSON.
 */
@JsonPropertyOrder({
    "resultTable", "stageIdStats"
})
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class BrokerResponseNativeV2 implements BrokerResponse {
  public static final BrokerResponseNativeV2 EMPTY_RESULT = BrokerResponseNativeV2.empty();
  public static final BrokerResponseNativeV2 NO_TABLE_RESULT =
      new BrokerResponseNativeV2(QueryException.BROKER_RESOURCE_MISSING_ERROR);
  public static final BrokerResponseNativeV2 TABLE_DOES_NOT_EXIST =
      new BrokerResponseNativeV2(QueryException.TABLE_DOES_NOT_EXIST_ERROR);
  public static final BrokerResponseNativeV2 BROKER_ONLY_EXPLAIN_PLAN_OUTPUT = getBrokerResponseExplainPlanOutput();

  private ResultTable _resultTable;
  private List<String> _segmentStatistics = new ArrayList<>();
  private Map<Integer, BrokerResponseStats> _stageIdStats = new HashMap<>();

  public BrokerResponseNativeV2() {
  }

  public BrokerResponseNativeV2(ProcessingException exception) {
//    _processingExceptions.add(new QueryProcessingException(exception.getErrorCode(), exception.getMessage()));
  }

  public BrokerResponseNativeV2(List<ProcessingException> exceptions) {
//    for (ProcessingException exception : exceptions) {
//      _processingExceptions.add(new QueryProcessingException(exception.getErrorCode(), exception.getMessage()));
//    }
  }

  /** Generate EXPLAIN PLAN output when queries are evaluated by Broker without going to the Server. */
  private static BrokerResponseNativeV2 getBrokerResponseExplainPlanOutput() {
    BrokerResponseNativeV2 brokerResponse = BrokerResponseNativeV2.empty();
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{"BROKER_EVALUATE", 0, -1});
    brokerResponse.setResultTable(new ResultTable(DataSchema.EXPLAIN_RESULT_SCHEMA, rows));
    return brokerResponse;
  }

  /**
   * Get a new empty {@link BrokerResponseNativeV2}.
   */
  public static BrokerResponseNativeV2 empty() {
    return new BrokerResponseNativeV2();
  }

  public static BrokerResponseNativeV2 fromJsonString(String jsonString)
      throws IOException {
    return JsonUtils.stringToObject(jsonString, BrokerResponseNativeV2.class);
  }

  public void addStageStat(Integer stageId, BrokerResponseStats brokerResponseStats) {
    if (!brokerResponseStats.getOperatorIds().isEmpty()) {
      _stageIdStats.put(stageId, brokerResponseStats);
    }
  }

  @JsonProperty("stageIdStats")
  public Map<Integer, BrokerResponseStats> getStageIdStats() {
    return _stageIdStats;
  }

  @JsonProperty("offlineSystemActivitiesCpuTimeNs")
  @Override
  public long getOfflineSystemActivitiesCpuTimeNs() {
    return 0;
  }

  @JsonProperty("offlineSystemActivitiesCpuTimeNs")
  @Override
  public void setOfflineSystemActivitiesCpuTimeNs(long offlineSystemActivitiesCpuTimeNs) {
//    _offlineSystemActivitiesCpuTimeNs = offlineSystemActivitiesCpuTimeNs;
  }

  @JsonProperty("realtimeSystemActivitiesCpuTimeNs")
  @Override
  public long getRealtimeSystemActivitiesCpuTimeNs() {
    return 0;
  }

  @JsonProperty("realtimeSystemActivitiesCpuTimeNs")
  @Override
  public void setRealtimeSystemActivitiesCpuTimeNs(long realtimeSystemActivitiesCpuTimeNs) {
//    _realtimeSystemActivitiesCpuTimeNs = realtimeSystemActivitiesCpuTimeNs;
  }

  @JsonProperty("offlineThreadCpuTimeNs")
  @Override
  public long getOfflineThreadCpuTimeNs() {
    return 0;
  }

  @JsonProperty("offlineThreadCpuTimeNs")
  @Override
  public void setOfflineThreadCpuTimeNs(long timeUsedMs) {
//    _offlineThreadCpuTimeNs = timeUsedMs;
  }

  @JsonProperty("realtimeThreadCpuTimeNs")
  @Override
  public long getRealtimeThreadCpuTimeNs() {
    return 0;
  }

  @JsonProperty("realtimeThreadCpuTimeNs")
  @Override
  public void setRealtimeThreadCpuTimeNs(long timeUsedMs) {
//    _realtimeThreadCpuTimeNs = timeUsedMs;
  }

  @JsonProperty("offlineResponseSerializationCpuTimeNs")
  @Override
  public long getOfflineResponseSerializationCpuTimeNs() {
    return 0;
  }

  @JsonProperty("offlineResponseSerializationCpuTimeNs")
  @Override
  public void setOfflineResponseSerializationCpuTimeNs(long offlineResponseSerializationCpuTimeNs) {
//    _offlineResponseSerializationCpuTimeNs = offlineResponseSerializationCpuTimeNs;
  }

  @JsonProperty("realtimeResponseSerializationCpuTimeNs")
  @Override
  public long getRealtimeResponseSerializationCpuTimeNs() {
    return 0;
  }

  @JsonProperty("realtimeResponseSerializationCpuTimeNs")
  @Override
  public void setRealtimeResponseSerializationCpuTimeNs(long realtimeResponseSerializationCpuTimeNs) {
//    _realtimeResponseSerializationCpuTimeNs = realtimeResponseSerializationCpuTimeNs;
  }

  @JsonProperty("offlineTotalCpuTimeNs")
  @Override
  public long getOfflineTotalCpuTimeNs() {
    return 0;
  }

  @JsonProperty("offlineTotalCpuTimeNs")
  @Override
  public void setOfflineTotalCpuTimeNs(long offlineTotalCpuTimeNs) {
//    _offlineTotalCpuTimeNs = offlineTotalCpuTimeNs;
  }

  @JsonProperty("realtimeTotalCpuTimeNs")
  @Override
  public long getRealtimeTotalCpuTimeNs() {
    return 0;
  }

  @JsonProperty("realtimeTotalCpuTimeNs")
  @Override
  public void setRealtimeTotalCpuTimeNs(long realtimeTotalCpuTimeNs) {
//    _realtimeTotalCpuTimeNs = realtimeTotalCpuTimeNs;
  }

  @JsonProperty("numSegmentsPrunedByBroker")
  @Override
  public long getNumSegmentsPrunedByBroker() {
    return 0;
  }

  @JsonProperty("numSegmentsPrunedByBroker")
  @Override
  public void setNumSegmentsPrunedByBroker(long numSegmentsPrunedByBroker) {
//    _numSegmentsPrunedByBroker = numSegmentsPrunedByBroker;
  }

  @JsonProperty("numSegmentsPrunedByServer")
  @Override
  public long getNumSegmentsPrunedByServer() {
    return 0;
  }

  @JsonProperty("numSegmentsPrunedByServer")
  @Override
  public void setNumSegmentsPrunedByServer(long numSegmentsPrunedByServer) {
//    _numSegmentsPrunedByServer = numSegmentsPrunedByServer;
  }

  @JsonProperty("numSegmentsPrunedInvalid")
  @Override
  public long getNumSegmentsPrunedInvalid() {
    return 0;
  }

  @JsonProperty("numSegmentsPrunedInvalid")
  @Override
  public void setNumSegmentsPrunedInvalid(long numSegmentsPrunedInvalid) {
//    _numSegmentsPrunedInvalid = numSegmentsPrunedInvalid;
  }

  @JsonProperty("numSegmentsPrunedByLimit")
  @Override
  public long getNumSegmentsPrunedByLimit() {
    return 0;
  }

  @JsonProperty("numSegmentsPrunedByLimit")
  @Override
  public void setNumSegmentsPrunedByLimit(long numSegmentsPrunedByLimit) {
//    _numSegmentsPrunedByLimit = numSegmentsPrunedByLimit;
  }

  @JsonProperty("numSegmentsPrunedByValue")
  @Override
  public long getNumSegmentsPrunedByValue() {
    return 0;
  }

  @JsonProperty("numSegmentsPrunedByValue")
  @Override
  public void setNumSegmentsPrunedByValue(long numSegmentsPrunedByValue) {
//    _numSegmentsPrunedByValue = numSegmentsPrunedByValue;
  }

  @JsonProperty("explainPlanNumEmptyFilterSegments")
  @Override
  public long getExplainPlanNumEmptyFilterSegments() {
    return 0;
  }

  @JsonProperty("explainPlanNumEmptyFilterSegments")
  @Override
  public void setExplainPlanNumEmptyFilterSegments(long explainPlanNumEmptyFilterSegments) {
//    _explainPlanNumEmptyFilterSegments = explainPlanNumEmptyFilterSegments;
  }

  @JsonProperty("explainPlanNumMatchAllFilterSegments")
  @Override
  public long getExplainPlanNumMatchAllFilterSegments() {
    return 0;
  }

  @JsonProperty("explainPlanNumMatchAllFilterSegments")
  @Override
  public void setExplainPlanNumMatchAllFilterSegments(long explainPlanNumMatchAllFilterSegments) {
//    _explainPlanNumMatchAllFilterSegments = explainPlanNumMatchAllFilterSegments;
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
//    _numRowsResultSet = resultTable.getRows().size();
  }

  @JsonProperty("exceptions")
  public List<QueryProcessingException> getProcessingExceptions() {
    return new ArrayList<>();
  }

  @JsonProperty("exceptions")
  public void setProcessingExceptions(List<QueryProcessingException> processingExceptions) {
//    _processingExceptions = processingExceptions;
  }

  @JsonProperty("numServersQueried")
  @Override
  public int getNumServersQueried() {
    return 0;
  }

  @JsonProperty("numServersQueried")
  @Override
  public void setNumServersQueried(int numServersQueried) {
//    _numServersQueried = numServersQueried;
  }

  @JsonProperty("numServersResponded")
  @Override
  public int getNumServersResponded() {
    return 0;
  }

  @JsonProperty("numServersResponded")
  @Override
  public void setNumServersResponded(int numServersResponded) {
//    _numServersResponded = numServersResponded;
  }

  @JsonProperty("numDocsScanned")
  public long getNumDocsScanned() {
    return 0;
  }

  @JsonProperty("numDocsScanned")
  public void setNumDocsScanned(long numDocsScanned) {
//    _numDocsScanned = numDocsScanned;
  }

  @JsonProperty("numEntriesScannedInFilter")
  @Override
  public long getNumEntriesScannedInFilter() {
    return 0;
  }

  @JsonProperty("numEntriesScannedInFilter")
  public void setNumEntriesScannedInFilter(long numEntriesScannedInFilter) {
//    _numEntriesScannedInFilter = numEntriesScannedInFilter;
  }

  @JsonProperty("numEntriesScannedPostFilter")
  @Override
  public long getNumEntriesScannedPostFilter() {
    return 0;
  }

  @JsonProperty("numEntriesScannedPostFilter")
  public void setNumEntriesScannedPostFilter(long numEntriesScannedPostFilter) {
//    _numEntriesScannedPostFilter = numEntriesScannedPostFilter;
  }

  @JsonProperty("numSegmentsQueried")
  @Override
  public long getNumSegmentsQueried() {
    return 0;
  }

  @JsonProperty("numSegmentsQueried")
  public void setNumSegmentsQueried(long numSegmentsQueried) {
//    _numSegmentsQueried = numSegmentsQueried;
  }

  @JsonProperty("numSegmentsProcessed")
  @Override
  public long getNumSegmentsProcessed() {
    return 0;
  }

  @JsonProperty("numSegmentsProcessed")
  public void setNumSegmentsProcessed(long numSegmentsProcessed) {
//    _numSegmentsProcessed = numSegmentsProcessed;
  }

  @JsonProperty("numSegmentsMatched")
  @Override
  public long getNumSegmentsMatched() {
    return 0;
  }

  @JsonProperty("numSegmentsMatched")
  public void setNumSegmentsMatched(long numSegmentsMatched) {
//    _numSegmentsMatched = numSegmentsMatched;
  }

  @JsonProperty("numConsumingSegmentsQueried")
  @Override
  public long getNumConsumingSegmentsQueried() {
    return 0;
  }

  @JsonProperty("numConsumingSegmentsQueried")
  public void setNumConsumingSegmentsQueried(long numConsumingSegmentsQueried) {
//    _numConsumingSegmentsQueried = numConsumingSegmentsQueried;
  }

  @JsonProperty("numConsumingSegmentsProcessed")
  @Override
  public long getNumConsumingSegmentsProcessed() {
    return 0;
  }
  @JsonProperty("numConsumingSegmentsProcessed")
  public void setNumConsumingSegmentsProcessed(long numConsumingSegmentsProcessed) {
//    _numConsumingSegmentsProcessed = numConsumingSegmentsProcessed;
  }

  @JsonProperty("numConsumingSegmentsMatched")
  @Override
  public long getNumConsumingSegmentsMatched() {
    return 0;
  }

  @JsonProperty("numConsumingSegmentsMatched")
  public void setNumConsumingSegmentsMatched(long numConsumingSegmentsMatched) {
//    _numConsumingSegmentsMatched = numConsumingSegmentsMatched;
  }

  @JsonProperty("minConsumingFreshnessTimeMs")
  @Override
  public long getMinConsumingFreshnessTimeMs() {
    return 0;
  }

  @JsonProperty("minConsumingFreshnessTimeMs")
  public void setMinConsumingFreshnessTimeMs(long minConsumingFreshnessTimeMs) {
//    _minConsumingFreshnessTimeMs = minConsumingFreshnessTimeMs;
  }

  @JsonProperty("totalDocs")
  @Override
  public long getTotalDocs() {
    return 0;
  }

  @JsonProperty("totalDocs")
  public void setTotalDocs(long totalDocs) {
//    _totalDocs = totalDocs;
  }

  @JsonProperty("numGroupsLimitReached")
  @Override
  public boolean isNumGroupsLimitReached() {
    return false;
  }

  @JsonProperty("numGroupsLimitReached")
  public void setNumGroupsLimitReached(boolean numGroupsLimitReached) {
//    _numGroupsLimitReached = numGroupsLimitReached;
  }

  @JsonProperty("timeUsedMs")
  public long getTimeUsedMs() {
    return 0;
  }

  @JsonProperty("timeUsedMs")
  @Override
  public void setTimeUsedMs(long timeUsedMs) {
//    _timeUsedMs = timeUsedMs;
  }

  @JsonProperty("numRowsResultSet")
  @Override
  public int getNumRowsResultSet() {
    return 0;
  }

  @JsonProperty("numRowsResultSet")
  @Override
  public void setNumRowsResultSet(int numRowsResultSet) {
//    _numRowsResultSet = numRowsResultSet;
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
    return new HashMap<>();
  }

  @JsonProperty("traceInfo")
  public void setTraceInfo(Map<String, String> traceInfo) {
//    _traceInfo = traceInfo;
  }



  @Override
  public String toJsonString()
      throws IOException {
    return JsonUtils.objectToString(this);
  }

  @JsonIgnore
  @Override
  public void setExceptions(List<ProcessingException> exceptions) {
//    for (ProcessingException exception : exceptions) {
//      _processingExceptions.add(new QueryProcessingException(exception.getErrorCode(), exception.getMessage()));
//    }
  }

  public void addToExceptions(QueryProcessingException processingException) {
//    _processingExceptions.add(processingException);
  }

  @JsonIgnore
  @Override
  public int getExceptionsSize() {
    return 0;
  }
}
