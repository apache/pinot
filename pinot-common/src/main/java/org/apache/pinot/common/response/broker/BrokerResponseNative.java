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
    "resultTable", "requestId", "exceptions", "numServersQueried", "numServersResponded", "numSegmentsQueried",
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

  private String _requestId;
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
  private ResultTable _resultTable;
  private Map<String, String> _traceInfo = new HashMap<>();
  private List<QueryProcessingException> _processingExceptions = new ArrayList<>();
  private List<String> _segmentStatistics = new ArrayList<>();

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
    return _offlineTotalCpuTimeNs;
  }

  @JsonProperty("offlineTotalCpuTimeNs")
  @Override
  public void setOfflineTotalCpuTimeNs(long offlineTotalCpuTimeNs) {
    _offlineTotalCpuTimeNs = offlineTotalCpuTimeNs;
  }

  @JsonProperty("realtimeTotalCpuTimeNs")
  @Override
  public long getRealtimeTotalCpuTimeNs() {
    return _realtimeTotalCpuTimeNs;
  }

  @JsonProperty("realtimeTotalCpuTimeNs")
  @Override
  public void setRealtimeTotalCpuTimeNs(long realtimeTotalCpuTimeNs) {
    _realtimeTotalCpuTimeNs = realtimeTotalCpuTimeNs;
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
    return _numSegmentsPrunedByServer;
  }

  @JsonProperty("numSegmentsPrunedByServer")
  @Override
  public void setNumSegmentsPrunedByServer(long numSegmentsPrunedByServer) {
    _numSegmentsPrunedByServer = numSegmentsPrunedByServer;
  }

  @JsonProperty("numSegmentsPrunedInvalid")
  @Override
  public long getNumSegmentsPrunedInvalid() {
    return _numSegmentsPrunedInvalid;
  }

  @JsonProperty("numSegmentsPrunedInvalid")
  @Override
  public void setNumSegmentsPrunedInvalid(long numSegmentsPrunedInvalid) {
    _numSegmentsPrunedInvalid = numSegmentsPrunedInvalid;
  }

  @JsonProperty("numSegmentsPrunedByLimit")
  @Override
  public long getNumSegmentsPrunedByLimit() {
    return _numSegmentsPrunedByLimit;
  }

  @JsonProperty("numSegmentsPrunedByLimit")
  @Override
  public void setNumSegmentsPrunedByLimit(long numSegmentsPrunedByLimit) {
    _numSegmentsPrunedByLimit = numSegmentsPrunedByLimit;
  }

  @JsonProperty("numSegmentsPrunedByValue")
  @Override
  public long getNumSegmentsPrunedByValue() {
    return _numSegmentsPrunedByValue;
  }

  @JsonProperty("numSegmentsPrunedByValue")
  @Override
  public void setNumSegmentsPrunedByValue(long numSegmentsPrunedByValue) {
    _numSegmentsPrunedByValue = numSegmentsPrunedByValue;
  }

  @JsonProperty("explainPlanNumEmptyFilterSegments")
  @Override
  public long getExplainPlanNumEmptyFilterSegments() {
    return _explainPlanNumEmptyFilterSegments;
  }

  @JsonProperty("explainPlanNumEmptyFilterSegments")
  @Override
  public void setExplainPlanNumEmptyFilterSegments(long explainPlanNumEmptyFilterSegments) {
    _explainPlanNumEmptyFilterSegments = explainPlanNumEmptyFilterSegments;
  }

  @JsonProperty("explainPlanNumMatchAllFilterSegments")
  @Override
  public long getExplainPlanNumMatchAllFilterSegments() {
    return _explainPlanNumMatchAllFilterSegments;
  }

  @JsonProperty("explainPlanNumMatchAllFilterSegments")
  @Override
  public void setExplainPlanNumMatchAllFilterSegments(long explainPlanNumMatchAllFilterSegments) {
    _explainPlanNumMatchAllFilterSegments = explainPlanNumMatchAllFilterSegments;
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
    return _numDocsScanned;
  }

  @JsonProperty("numDocsScanned")
  public void setNumDocsScanned(long numDocsScanned) {
    _numDocsScanned = numDocsScanned;
  }

  @JsonProperty("numEntriesScannedInFilter")
  @Override
  public long getNumEntriesScannedInFilter() {
    return _numEntriesScannedInFilter;
  }

  @JsonProperty("numEntriesScannedInFilter")
  public void setNumEntriesScannedInFilter(long numEntriesScannedInFilter) {
    _numEntriesScannedInFilter = numEntriesScannedInFilter;
  }

  @JsonProperty("numEntriesScannedPostFilter")
  @Override
  public long getNumEntriesScannedPostFilter() {
    return _numEntriesScannedPostFilter;
  }

  @JsonProperty("numEntriesScannedPostFilter")
  public void setNumEntriesScannedPostFilter(long numEntriesScannedPostFilter) {
    _numEntriesScannedPostFilter = numEntriesScannedPostFilter;
  }

  @JsonProperty("numSegmentsQueried")
  @Override
  public long getNumSegmentsQueried() {
    return _numSegmentsQueried;
  }

  @JsonProperty("numSegmentsQueried")
  public void setNumSegmentsQueried(long numSegmentsQueried) {
    _numSegmentsQueried = numSegmentsQueried;
  }

  @JsonProperty("numSegmentsProcessed")
  @Override
  public long getNumSegmentsProcessed() {
    return _numSegmentsProcessed;
  }

  @JsonProperty("numSegmentsProcessed")
  public void setNumSegmentsProcessed(long numSegmentsProcessed) {
    _numSegmentsProcessed = numSegmentsProcessed;
  }

  @JsonProperty("numSegmentsMatched")
  @Override
  public long getNumSegmentsMatched() {
    return _numSegmentsMatched;
  }

  @JsonProperty("numSegmentsMatched")
  public void setNumSegmentsMatched(long numSegmentsMatched) {
    _numSegmentsMatched = numSegmentsMatched;
  }

  @JsonProperty("numConsumingSegmentsQueried")
  @Override
  public long getNumConsumingSegmentsQueried() {
    return _numConsumingSegmentsQueried;
  }

  @JsonProperty("numConsumingSegmentsQueried")
  public void setNumConsumingSegmentsQueried(long numConsumingSegmentsQueried) {
    _numConsumingSegmentsQueried = numConsumingSegmentsQueried;
  }

  @JsonProperty("numConsumingSegmentsProcessed")
  @Override
  public long getNumConsumingSegmentsProcessed() {
    return _numConsumingSegmentsProcessed;
  }
  @JsonProperty("numConsumingSegmentsProcessed")
  public void setNumConsumingSegmentsProcessed(long numConsumingSegmentsProcessed) {
    _numConsumingSegmentsProcessed = numConsumingSegmentsProcessed;
  }

  @JsonProperty("numConsumingSegmentsMatched")
  @Override
  public long getNumConsumingSegmentsMatched() {
    return _numConsumingSegmentsMatched;
  }

  @JsonProperty("numConsumingSegmentsMatched")
  public void setNumConsumingSegmentsMatched(long numConsumingSegmentsMatched) {
    _numConsumingSegmentsMatched = numConsumingSegmentsMatched;
  }

  @JsonProperty("minConsumingFreshnessTimeMs")
  @Override
  public long getMinConsumingFreshnessTimeMs() {
    return _minConsumingFreshnessTimeMs;
  }

  @JsonProperty("minConsumingFreshnessTimeMs")
  public void setMinConsumingFreshnessTimeMs(long minConsumingFreshnessTimeMs) {
    _minConsumingFreshnessTimeMs = minConsumingFreshnessTimeMs;
  }

  @JsonProperty("totalDocs")
  @Override
  public long getTotalDocs() {
    return _totalDocs;
  }

  @JsonProperty("totalDocs")
  public void setTotalDocs(long totalDocs) {
    _totalDocs = totalDocs;
  }

  @JsonProperty("numGroupsLimitReached")
  @Override
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
  @Override
  public void setTimeUsedMs(long timeUsedMs) {
    _timeUsedMs = timeUsedMs;
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


  @JsonProperty("requestId")
  @Override
  public String getRequestId() {
    return _requestId;
  }

  @Override
  public void setRequestId(String requestId) {
    _requestId = requestId;
  }
}
