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
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.spi.utils.JsonUtils;


@JsonPropertyOrder({
    "exceptions", "numBlocks", "numRows", "stageExecutionTimeMs", "numServersQueried", "numServersResponded",
    "numSegmentsQueried", "numSegmentsProcessed", "numSegmentsMatched", "numConsumingSegmentsQueried",
    "numConsumingSegmentsProcessed", "numConsumingSegmentsMatched", "numDocsScanned", "numEntriesScannedInFilter",
    "numEntriesScannedPostFilter", "numGroupsLimitReached", "totalDocs", "timeUsedMs", "offlineThreadCpuTimeNs",
    "realtimeThreadCpuTimeNs", "offlineSystemActivitiesCpuTimeNs", "realtimeSystemActivitiesCpuTimeNs",
    "offlineResponseSerializationCpuTimeNs", "realtimeResponseSerializationCpuTimeNs", "offlineTotalCpuTimeNs",
    "realtimeTotalCpuTimeNs", "traceInfo", "operatorIds", "tableNames"
})
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class BrokerResponseStats {
  private int _numServersQueried = 0;
  private int _numServersResponded = 0;
  private long _numDocsScanned = 0L;
  private long _numEntriesScannedInFilter = 0L;
  private long _numEntriesScannedPostFilter = 0L;
  private long _numSegmentsQueried = 0L;
  private long _numSegmentsProcessed = 0L;
  private long _numSegmentsMatched = 0L;
  private long _numConsumingSegmentsQueried = 0L;
  private long _numConsumingSegmentsProcessed = 0L;
  private long _numConsumingSegmentsMatched = 0L;
  // the timestamp indicating the freshness of the data queried in consuming segments.
  // This can be ingestion timestamp if provided by the stream, or the last index time
  private long _minConsumingFreshnessTimeMs = 0L;

  private long _totalDocs = 0L;
  private boolean _numGroupsLimitReached = false;
  private long _timeUsedMs = 0L;
  private long _offlineThreadCpuTimeNs = 0L;
  private long _realtimeThreadCpuTimeNs = 0L;
  private long _offlineSystemActivitiesCpuTimeNs = 0L;
  private long _realtimeSystemActivitiesCpuTimeNs = 0L;
  private long _offlineResponseSerializationCpuTimeNs = 0L;
  private long _realtimeResponseSerializationCpuTimeNs = 0L;
  private long _offlineTotalCpuTimeNs = 0L;
  private long _realtimeTotalCpuTimeNs = 0L;
  private long _numSegmentsPrunedByBroker = 0L;
  private long _numSegmentsPrunedByServer = 0L;
  private long _numSegmentsPrunedInvalid = 0L;
  private long _numSegmentsPrunedByLimit = 0L;
  private long _numSegmentsPrunedByValue = 0L;
  private long _explainPlanNumEmptyFilterSegments = 0L;
  private long _explainPlanNumMatchAllFilterSegments = 0L;
  private int _numRowsResultSet = 0;

  // V2 Engine Stats
  private int _numBlocks = 0;
  private int _numRows = 0;
  private long _stageExecutionTimeMs = 0;
  private List<String> _operatorIds = new ArrayList<>();
  private List<String> _tableNames = new ArrayList<>();

  private Map<String, String> _traceInfo = new HashMap<>();
  private List<QueryProcessingException> _processingExceptions = new ArrayList<>();

  @JsonProperty("offlineSystemActivitiesCpuTimeNs")

  public long getOfflineSystemActivitiesCpuTimeNs() {
    return _offlineSystemActivitiesCpuTimeNs;
  }

  @JsonProperty("offlineSystemActivitiesCpuTimeNs")

  public void setOfflineSystemActivitiesCpuTimeNs(long offlineSystemActivitiesCpuTimeNs) {
    _offlineSystemActivitiesCpuTimeNs = offlineSystemActivitiesCpuTimeNs;
  }

  @JsonProperty("realtimeSystemActivitiesCpuTimeNs")

  public long getRealtimeSystemActivitiesCpuTimeNs() {
    return _realtimeSystemActivitiesCpuTimeNs;
  }

  @JsonProperty("realtimeSystemActivitiesCpuTimeNs")

  public void setRealtimeSystemActivitiesCpuTimeNs(long realtimeSystemActivitiesCpuTimeNs) {
    _realtimeSystemActivitiesCpuTimeNs = realtimeSystemActivitiesCpuTimeNs;
  }

  @JsonProperty("offlineThreadCpuTimeNs")

  public long getOfflineThreadCpuTimeNs() {
    return _offlineThreadCpuTimeNs;
  }

  @JsonProperty("offlineThreadCpuTimeNs")

  public void setOfflineThreadCpuTimeNs(long timeUsedMs) {
    _offlineThreadCpuTimeNs = timeUsedMs;
  }

  @JsonProperty("realtimeThreadCpuTimeNs")

  public long getRealtimeThreadCpuTimeNs() {
    return _realtimeThreadCpuTimeNs;
  }

  @JsonProperty("realtimeThreadCpuTimeNs")

  public void setRealtimeThreadCpuTimeNs(long timeUsedMs) {
    _realtimeThreadCpuTimeNs = timeUsedMs;
  }

  @JsonProperty("offlineResponseSerializationCpuTimeNs")

  public long getOfflineResponseSerializationCpuTimeNs() {
    return _offlineResponseSerializationCpuTimeNs;
  }

  @JsonProperty("offlineResponseSerializationCpuTimeNs")

  public void setOfflineResponseSerializationCpuTimeNs(long offlineResponseSerializationCpuTimeNs) {
    _offlineResponseSerializationCpuTimeNs = offlineResponseSerializationCpuTimeNs;
  }

  @JsonProperty("realtimeResponseSerializationCpuTimeNs")

  public long getRealtimeResponseSerializationCpuTimeNs() {
    return _realtimeResponseSerializationCpuTimeNs;
  }

  @JsonProperty("realtimeResponseSerializationCpuTimeNs")

  public void setRealtimeResponseSerializationCpuTimeNs(long realtimeResponseSerializationCpuTimeNs) {
    _realtimeResponseSerializationCpuTimeNs = realtimeResponseSerializationCpuTimeNs;
  }

  @JsonProperty("offlineTotalCpuTimeNs")

  public long getOfflineTotalCpuTimeNs() {
    return _offlineTotalCpuTimeNs;
  }

  @JsonProperty("offlineTotalCpuTimeNs")

  public void setOfflineTotalCpuTimeNs(long offlineTotalCpuTimeNs) {
    _offlineTotalCpuTimeNs = offlineTotalCpuTimeNs;
  }

  @JsonProperty("realtimeTotalCpuTimeNs")

  public long getRealtimeTotalCpuTimeNs() {
    return _realtimeTotalCpuTimeNs;
  }

  @JsonProperty("realtimeTotalCpuTimeNs")

  public void setRealtimeTotalCpuTimeNs(long realtimeTotalCpuTimeNs) {
    _realtimeTotalCpuTimeNs = realtimeTotalCpuTimeNs;
  }

  @JsonProperty("numSegmentsPrunedByBroker")

  public long getNumSegmentsPrunedByBroker() {
    return _numSegmentsPrunedByBroker;
  }

  @JsonProperty("numSegmentsPrunedByBroker")

  public void setNumSegmentsPrunedByBroker(long numSegmentsPrunedByBroker) {
    _numSegmentsPrunedByBroker = numSegmentsPrunedByBroker;
  }

  @JsonProperty("numSegmentsPrunedByServer")

  public long getNumSegmentsPrunedByServer() {
    return _numSegmentsPrunedByServer;
  }

  @JsonProperty("numSegmentsPrunedByServer")

  public void setNumSegmentsPrunedByServer(long numSegmentsPrunedByServer) {
    _numSegmentsPrunedByServer = numSegmentsPrunedByServer;
  }

  @JsonProperty("numSegmentsPrunedInvalid")

  public long getNumSegmentsPrunedInvalid() {
    return _numSegmentsPrunedInvalid;
  }

  @JsonProperty("numSegmentsPrunedInvalid")

  public void setNumSegmentsPrunedInvalid(long numSegmentsPrunedInvalid) {
    _numSegmentsPrunedInvalid = numSegmentsPrunedInvalid;
  }

  @JsonProperty("numSegmentsPrunedByLimit")

  public long getNumSegmentsPrunedByLimit() {
    return _numSegmentsPrunedByLimit;
  }

  @JsonProperty("numSegmentsPrunedByLimit")

  public void setNumSegmentsPrunedByLimit(long numSegmentsPrunedByLimit) {
    _numSegmentsPrunedByLimit = numSegmentsPrunedByLimit;
  }

  @JsonProperty("numSegmentsPrunedByValue")

  public long getNumSegmentsPrunedByValue() {
    return _numSegmentsPrunedByValue;
  }

  @JsonProperty("numSegmentsPrunedByValue")

  public void setNumSegmentsPrunedByValue(long numSegmentsPrunedByValue) {
    _numSegmentsPrunedByValue = numSegmentsPrunedByValue;
  }

  @JsonProperty("explainPlanNumEmptyFilterSegments")

  public long getExplainPlanNumEmptyFilterSegments() {
    return _explainPlanNumEmptyFilterSegments;
  }

  @JsonProperty("explainPlanNumEmptyFilterSegments")

  public void setExplainPlanNumEmptyFilterSegments(long explainPlanNumEmptyFilterSegments) {
    _explainPlanNumEmptyFilterSegments = explainPlanNumEmptyFilterSegments;
  }

  @JsonProperty("explainPlanNumMatchAllFilterSegments")

  public long getExplainPlanNumMatchAllFilterSegments() {
    return _explainPlanNumMatchAllFilterSegments;
  }

  @JsonProperty("explainPlanNumMatchAllFilterSegments")

  public void setExplainPlanNumMatchAllFilterSegments(long explainPlanNumMatchAllFilterSegments) {
    _explainPlanNumMatchAllFilterSegments = explainPlanNumMatchAllFilterSegments;
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

  public int getNumServersQueried() {
    return _numServersQueried;
  }

  @JsonProperty("numServersQueried")

  public void setNumServersQueried(int numServersQueried) {
    _numServersQueried = numServersQueried;
  }

  @JsonProperty("numServersResponded")

  public int getNumServersResponded() {
    return _numServersResponded;
  }

  @JsonProperty("numServersResponded")

  public void setNumServersResponded(int numServersResponded) {
    _numServersResponded = numServersResponded;
  }

  @JsonProperty("numDocsScanned")
  public long getNumDocsScanned() {
    return _numDocsScanned;
  }

  @JsonProperty("numDocsScanned")
  public void setNumDocsScanned(long numDocsScanned) {
    _numDocsScanned = numDocsScanned;
  }

  @JsonProperty("numEntriesScannedInFilter")

  public long getNumEntriesScannedInFilter() {
    return _numEntriesScannedInFilter;
  }

  @JsonProperty("numEntriesScannedInFilter")
  public void setNumEntriesScannedInFilter(long numEntriesScannedInFilter) {
    _numEntriesScannedInFilter = numEntriesScannedInFilter;
  }

  @JsonProperty("numEntriesScannedPostFilter")

  public long getNumEntriesScannedPostFilter() {
    return _numEntriesScannedPostFilter;
  }

  @JsonProperty("numEntriesScannedPostFilter")
  public void setNumEntriesScannedPostFilter(long numEntriesScannedPostFilter) {
    _numEntriesScannedPostFilter = numEntriesScannedPostFilter;
  }

  @JsonProperty("numSegmentsQueried")

  public long getNumSegmentsQueried() {
    return _numSegmentsQueried;
  }

  @JsonProperty("numSegmentsQueried")
  public void setNumSegmentsQueried(long numSegmentsQueried) {
    _numSegmentsQueried = numSegmentsQueried;
  }

  @JsonProperty("numSegmentsProcessed")

  public long getNumSegmentsProcessed() {
    return _numSegmentsProcessed;
  }

  @JsonProperty("numSegmentsProcessed")
  public void setNumSegmentsProcessed(long numSegmentsProcessed) {
    _numSegmentsProcessed = numSegmentsProcessed;
  }

  @JsonProperty("numSegmentsMatched")

  public long getNumSegmentsMatched() {
    return _numSegmentsMatched;
  }

  @JsonProperty("numSegmentsMatched")
  public void setNumSegmentsMatched(long numSegmentsMatched) {
    _numSegmentsMatched = numSegmentsMatched;
  }

  @JsonProperty("numConsumingSegmentsQueried")

  public long getNumConsumingSegmentsQueried() {
    return _numConsumingSegmentsQueried;
  }

  @JsonProperty("numConsumingSegmentsQueried")
  public void setNumConsumingSegmentsQueried(long numConsumingSegmentsQueried) {
    _numConsumingSegmentsQueried = numConsumingSegmentsQueried;
  }

  @JsonProperty("numConsumingSegmentsProcessed")

  public long getNumConsumingSegmentsProcessed() {
    return _numConsumingSegmentsProcessed;
  }

  @JsonProperty("numConsumingSegmentsProcessed")
  public void setNumConsumingSegmentsProcessed(long numConsumingSegmentsProcessed) {
    _numConsumingSegmentsProcessed = numConsumingSegmentsProcessed;
  }

  @JsonProperty("numConsumingSegmentsMatched")

  public long getNumConsumingSegmentsMatched() {
    return _numConsumingSegmentsMatched;
  }

  @JsonProperty("numConsumingSegmentsMatched")
  public void setNumConsumingSegmentsMatched(long numConsumingSegmentsMatched) {
    _numConsumingSegmentsMatched = numConsumingSegmentsMatched;
  }

  @JsonProperty("minConsumingFreshnessTimeMs")

  public long getMinConsumingFreshnessTimeMs() {
    return _minConsumingFreshnessTimeMs;
  }

  @JsonProperty("minConsumingFreshnessTimeMs")
  public void setMinConsumingFreshnessTimeMs(long minConsumingFreshnessTimeMs) {
    _minConsumingFreshnessTimeMs = minConsumingFreshnessTimeMs;
  }

  @JsonProperty("totalDocs")

  public long getTotalDocs() {
    return _totalDocs;
  }

  @JsonProperty("totalDocs")
  public void setTotalDocs(long totalDocs) {
    _totalDocs = totalDocs;
  }

  @JsonProperty("numGroupsLimitReached")

  public boolean isNumGroupsLimitReached() {
    return _numGroupsLimitReached;
  }

  @JsonProperty("numGroupsLimitReached")
  public void setNumGroupsLimitReached(boolean numGroupsLimitReached) {
    _numGroupsLimitReached = numGroupsLimitReached;
  }

  @JsonProperty("timeUsedMs")
  public long getTimeUsedMs() {
    return _timeUsedMs;
  }

  @JsonProperty("timeUsedMs")

  public void setTimeUsedMs(long timeUsedMs) {
    _timeUsedMs = timeUsedMs;
  }

  @JsonProperty("numRowsResultSet")

  public int getNumRowsResultSet() {
    return _numRowsResultSet;
  }

  @JsonProperty("numRowsResultSet")

  public void setNumRowsResultSet(int numRowsResultSet) {
    _numRowsResultSet = numRowsResultSet;
  }

  @JsonProperty("numBlocks")
  public int getNumBlocks() {
    return _numBlocks;
  }

  @JsonProperty("numBlocks")
  public void setNumBlocks(int numBlocks) {
    _numBlocks = numBlocks;
  }

  @JsonProperty("numRows")
  public int getNumRows() {
    return _numRows;
  }

  @JsonProperty("numRows")
  public void setNumRows(int numRows) {
    _numRows = numRows;
  }

  @JsonProperty("stageExecutionTimeMs")
  public long getStageExecutionTimeMs() {
    return _stageExecutionTimeMs;
  }

  @JsonProperty("stageExecutionTimeMs")
  public void setStageExecutionTimeMs(long stageExecutionTimeMs) {
    _stageExecutionTimeMs = stageExecutionTimeMs;
  }

  @JsonProperty("traceInfo")
  public Map<String, String> getTraceInfo() {
    return _traceInfo;
  }

  @JsonProperty("traceInfo")
  public void setTraceInfo(Map<String, String> traceInfo) {
    _traceInfo = traceInfo;
  }

  public String toJsonString()
      throws IOException {
    return JsonUtils.objectToString(this);
  }

  @JsonIgnore
  public void setExceptions(List<ProcessingException> exceptions) {
    for (ProcessingException exception : exceptions) {
      _processingExceptions.add(new QueryProcessingException(exception.getErrorCode(), exception.getMessage()));
    }
  }

  public void addToExceptions(QueryProcessingException processingException) {
    _processingExceptions.add(processingException);
  }

  @JsonIgnore
  public int getExceptionsSize() {
    return _processingExceptions.size();
  }

  @JsonProperty("operatorIds")
  public List<String> getOperatorIds() {
    return _operatorIds;
  }

  @JsonProperty("operatorIds")
  public void setOperatorIds(List<String> operatorIds) {
    _operatorIds = operatorIds;
  }

  @JsonProperty("tableNames")
  public List<String> getTableNames() {
    return _tableNames;
  }

  @JsonProperty("tableNames")
  public void setTableNames(List<String> tableNames) {
    _tableNames = tableNames;
  }
}
