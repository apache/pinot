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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Broker response for single-stage engine.
 * This class can be used to serialize/deserialize the broker response.
 */
@JsonPropertyOrder({
    "resultTable", "numRowsResultSet", "partialResult", "exceptions", "numGroupsLimitReached", "timeUsedMs",
    "requestId", "brokerId", "numDocsScanned", "totalDocs", "numEntriesScannedInFilter", "numEntriesScannedPostFilter",
    "numServersQueried", "numServersResponded", "numSegmentsQueried", "numSegmentsProcessed", "numSegmentsMatched",
    "numConsumingSegmentsQueried", "numConsumingSegmentsProcessed", "numConsumingSegmentsMatched",
    "minConsumingFreshnessTimeMs", "numSegmentsPrunedByBroker", "numSegmentsPrunedByServer",
    "numSegmentsPrunedInvalid", "numSegmentsPrunedByLimit", "numSegmentsPrunedByValue", "brokerReduceTimeMs",
    "offlineThreadCpuTimeNs", "realtimeThreadCpuTimeNs", "offlineSystemActivitiesCpuTimeNs",
    "realtimeSystemActivitiesCpuTimeNs", "offlineResponseSerializationCpuTimeNs",
    "realtimeResponseSerializationCpuTimeNs", "offlineTotalCpuTimeNs", "realtimeTotalCpuTimeNs",
    "explainPlanNumEmptyFilterSegments", "explainPlanNumMatchAllFilterSegments", "traceInfo", "tablesQueried"
})
@JsonIgnoreProperties(ignoreUnknown = true)
public class BrokerResponseNative implements BrokerResponse {
  public static final BrokerResponseNative EMPTY_RESULT = BrokerResponseNative.empty();
  public static final BrokerResponseNative NO_TABLE_RESULT =
      new BrokerResponseNative(QueryException.BROKER_RESOURCE_MISSING_ERROR);
  public static final BrokerResponseNative TABLE_DOES_NOT_EXIST =
      new BrokerResponseNative(QueryException.TABLE_DOES_NOT_EXIST_ERROR);
  public static final BrokerResponseNative TABLE_IS_DISABLED =
      new BrokerResponseNative(QueryException.TABLE_IS_DISABLED_ERROR);
  public static final BrokerResponseNative BROKER_ONLY_EXPLAIN_PLAN_OUTPUT = getBrokerResponseExplainPlanOutput();

  private ResultTableRows _resultTableRows;
  private int _numRowsResultSet = 0;
  private List<QueryProcessingException> _exceptions = new ArrayList<>();
  private boolean _numGroupsLimitReached = false;
  private long _timeUsedMs = 0L;
  private String _requestId;
  private String _brokerId;
  private long _numDocsScanned = 0L;
  private long _totalDocs = 0L;
  private long _numEntriesScannedInFilter = 0L;
  private long _numEntriesScannedPostFilter = 0L;
  private int _numServersQueried = 0;
  private int _numServersResponded = 0;
  private long _numSegmentsQueried = 0L;
  private long _numSegmentsProcessed = 0L;
  private long _numSegmentsMatched = 0L;
  private long _numConsumingSegmentsQueried = 0L;
  private long _numConsumingSegmentsProcessed = 0L;
  private long _numConsumingSegmentsMatched = 0L;
  private long _minConsumingFreshnessTimeMs = 0L;
  private long _numSegmentsPrunedByBroker = 0L;
  private long _numSegmentsPrunedByServer = 0L;
  private long _numSegmentsPrunedInvalid = 0L;
  private long _numSegmentsPrunedByLimit = 0L;
  private long _numSegmentsPrunedByValue = 0L;
  private long _brokerReduceTimeMs = 0L;
  private long _offlineThreadCpuTimeNs = 0L;
  private long _realtimeThreadCpuTimeNs = 0L;
  private long _offlineSystemActivitiesCpuTimeNs = 0L;
  private long _realtimeSystemActivitiesCpuTimeNs = 0L;
  private long _offlineResponseSerializationCpuTimeNs = 0L;
  private long _realtimeResponseSerializationCpuTimeNs = 0L;
  private long _explainPlanNumEmptyFilterSegments = 0L;
  private long _explainPlanNumMatchAllFilterSegments = 0L;
  private Map<String, String> _traceInfo = new HashMap<>();
  private Set<String> _tablesQueried = Set.of();

  public BrokerResponseNative() {
  }

  public BrokerResponseNative(ProcessingException exception) {
    _exceptions.add(new QueryProcessingException(exception.getErrorCode(), exception.getMessage()));
  }

  public BrokerResponseNative(List<ProcessingException> exceptions) {
    for (ProcessingException exception : exceptions) {
      _exceptions.add(new QueryProcessingException(exception.getErrorCode(), exception.getMessage()));
    }
  }

  /** Generate EXPLAIN PLAN output when queries are evaluated by Broker without going to the Server. */
  private static BrokerResponseNative getBrokerResponseExplainPlanOutput() {
    BrokerResponseNative brokerResponse = BrokerResponseNative.empty();
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{"BROKER_EVALUATE", 0, -1});
    brokerResponse.setResultTable(new ResultTableRows(DataSchema.EXPLAIN_RESULT_SCHEMA, rows));
    return brokerResponse;
  }

  /**
   * Get a new empty {@link BrokerResponseNative}.
   */
  public static BrokerResponseNative empty() {
    return new BrokerResponseNative();
  }

  @VisibleForTesting
  public static BrokerResponseNative fromJsonString(String jsonString)
      throws IOException {
    return JsonUtils.stringToObject(jsonString, BrokerResponseNative.class);
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @Override
  public ResultTableRows getResultTable() {
    return _resultTableRows;
  }

  @Override
  public void setResultTable(@Nullable ResultTableRows resultTableRows) {
    _resultTableRows = resultTableRows;
    // NOTE: Update _numRowsResultSet when setting non-null result table. We might set null result table when user wants
    //       to hide the result but only show the stats, in which case we should not update _numRowsResultSet.
    if (resultTableRows != null) {
      _numRowsResultSet = resultTableRows.getRows().size();
    }
  }

  @Override
  public int getNumRowsResultSet() {
    return _numRowsResultSet;
  }

  public void setNumRowsResultSet(int numRowsResultSet) {
    _numRowsResultSet = numRowsResultSet;
  }

  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  @Override
  public boolean isPartialResult() {
    return getExceptionsSize() > 0 || isNumGroupsLimitReached();
  }

  @Override
  public List<QueryProcessingException> getExceptions() {
    return _exceptions;
  }

  public void setExceptions(List<QueryProcessingException> exceptions) {
    _exceptions = exceptions;
  }

  public void addException(QueryProcessingException exception) {
    _exceptions.add(exception);
  }

  public void addException(ProcessingException exception) {
    addException(new QueryProcessingException(exception.getErrorCode(), exception.getMessage()));
  }

  @Override
  public boolean isNumGroupsLimitReached() {
    return _numGroupsLimitReached;
  }

  public void setNumGroupsLimitReached(boolean numGroupsLimitReached) {
    _numGroupsLimitReached = numGroupsLimitReached;
  }

  @JsonIgnore
  @Override
  public boolean isMaxRowsInJoinReached() {
    return false;
  }

  @JsonIgnore
  @Override
  public boolean isMaxRowsInWindowReached() {
    return false;
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
    return _numDocsScanned;
  }

  public void setNumDocsScanned(long numDocsScanned) {
    _numDocsScanned = numDocsScanned;
  }

  @Override
  public long getTotalDocs() {
    return _totalDocs;
  }

  public void setTotalDocs(long totalDocs) {
    _totalDocs = totalDocs;
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    return _numEntriesScannedInFilter;
  }

  public void setNumEntriesScannedInFilter(long numEntriesScannedInFilter) {
    _numEntriesScannedInFilter = numEntriesScannedInFilter;
  }

  @Override
  public long getNumEntriesScannedPostFilter() {
    return _numEntriesScannedPostFilter;
  }

  public void setNumEntriesScannedPostFilter(long numEntriesScannedPostFilter) {
    _numEntriesScannedPostFilter = numEntriesScannedPostFilter;
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
    return _numSegmentsQueried;
  }

  public void setNumSegmentsQueried(long numSegmentsQueried) {
    _numSegmentsQueried = numSegmentsQueried;
  }

  @Override
  public long getNumSegmentsProcessed() {
    return _numSegmentsProcessed;
  }

  public void setNumSegmentsProcessed(long numSegmentsProcessed) {
    _numSegmentsProcessed = numSegmentsProcessed;
  }

  @Override
  public long getNumSegmentsMatched() {
    return _numSegmentsMatched;
  }

  public void setNumSegmentsMatched(long numSegmentsMatched) {
    _numSegmentsMatched = numSegmentsMatched;
  }

  @Override
  public long getNumConsumingSegmentsQueried() {
    return _numConsumingSegmentsQueried;
  }

  public void setNumConsumingSegmentsQueried(long numConsumingSegmentsQueried) {
    _numConsumingSegmentsQueried = numConsumingSegmentsQueried;
  }

  @Override
  public long getNumConsumingSegmentsProcessed() {
    return _numConsumingSegmentsProcessed;
  }

  public void setNumConsumingSegmentsProcessed(long numConsumingSegmentsProcessed) {
    _numConsumingSegmentsProcessed = numConsumingSegmentsProcessed;
  }

  @Override
  public long getNumConsumingSegmentsMatched() {
    return _numConsumingSegmentsMatched;
  }

  public void setNumConsumingSegmentsMatched(long numConsumingSegmentsMatched) {
    _numConsumingSegmentsMatched = numConsumingSegmentsMatched;
  }

  @Override
  public long getMinConsumingFreshnessTimeMs() {
    return _minConsumingFreshnessTimeMs;
  }

  public void setMinConsumingFreshnessTimeMs(long minConsumingFreshnessTimeMs) {
    _minConsumingFreshnessTimeMs = minConsumingFreshnessTimeMs;
  }

  @Override
  public long getNumSegmentsPrunedByBroker() {
    return _numSegmentsPrunedByBroker;
  }

  public void setNumSegmentsPrunedByBroker(long numSegmentsPrunedByBroker) {
    _numSegmentsPrunedByBroker = numSegmentsPrunedByBroker;
  }

  @Override
  public long getNumSegmentsPrunedByServer() {
    return _numSegmentsPrunedByServer;
  }

  public void setNumSegmentsPrunedByServer(long numSegmentsPrunedByServer) {
    _numSegmentsPrunedByServer = numSegmentsPrunedByServer;
  }

  @Override
  public long getNumSegmentsPrunedInvalid() {
    return _numSegmentsPrunedInvalid;
  }

  public void setNumSegmentsPrunedInvalid(long numSegmentsPrunedInvalid) {
    _numSegmentsPrunedInvalid = numSegmentsPrunedInvalid;
  }

  @Override
  public long getNumSegmentsPrunedByLimit() {
    return _numSegmentsPrunedByLimit;
  }

  public void setNumSegmentsPrunedByLimit(long numSegmentsPrunedByLimit) {
    _numSegmentsPrunedByLimit = numSegmentsPrunedByLimit;
  }

  @Override
  public long getNumSegmentsPrunedByValue() {
    return _numSegmentsPrunedByValue;
  }

  public void setNumSegmentsPrunedByValue(long numSegmentsPrunedByValue) {
    _numSegmentsPrunedByValue = numSegmentsPrunedByValue;
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
    return _offlineThreadCpuTimeNs;
  }

  public void setOfflineThreadCpuTimeNs(long offlineThreadCpuTimeNs) {
    _offlineThreadCpuTimeNs = offlineThreadCpuTimeNs;
  }

  @Override
  public long getOfflineSystemActivitiesCpuTimeNs() {
    return _offlineSystemActivitiesCpuTimeNs;
  }

  public void setOfflineSystemActivitiesCpuTimeNs(long offlineSystemActivitiesCpuTimeNs) {
    _offlineSystemActivitiesCpuTimeNs = offlineSystemActivitiesCpuTimeNs;
  }

  @Override
  public long getOfflineResponseSerializationCpuTimeNs() {
    return _offlineResponseSerializationCpuTimeNs;
  }

  public void setOfflineResponseSerializationCpuTimeNs(long offlineResponseSerializationCpuTimeNs) {
    _offlineResponseSerializationCpuTimeNs = offlineResponseSerializationCpuTimeNs;
  }

  @Override
  public long getRealtimeThreadCpuTimeNs() {
    return _realtimeThreadCpuTimeNs;
  }

  public void setRealtimeThreadCpuTimeNs(long timeUsedMs) {
    _realtimeThreadCpuTimeNs = timeUsedMs;
  }

  @Override
  public long getRealtimeSystemActivitiesCpuTimeNs() {
    return _realtimeSystemActivitiesCpuTimeNs;
  }

  public void setRealtimeSystemActivitiesCpuTimeNs(long realtimeSystemActivitiesCpuTimeNs) {
    _realtimeSystemActivitiesCpuTimeNs = realtimeSystemActivitiesCpuTimeNs;
  }

  @Override
  public long getRealtimeResponseSerializationCpuTimeNs() {
    return _realtimeResponseSerializationCpuTimeNs;
  }

  public void setRealtimeResponseSerializationCpuTimeNs(long realtimeResponseSerializationCpuTimeNs) {
    _realtimeResponseSerializationCpuTimeNs = realtimeResponseSerializationCpuTimeNs;
  }

  @Override
  public long getExplainPlanNumEmptyFilterSegments() {
    return _explainPlanNumEmptyFilterSegments;
  }

  public void setExplainPlanNumEmptyFilterSegments(long explainPlanNumEmptyFilterSegments) {
    _explainPlanNumEmptyFilterSegments = explainPlanNumEmptyFilterSegments;
  }

  @Override
  public long getExplainPlanNumMatchAllFilterSegments() {
    return _explainPlanNumMatchAllFilterSegments;
  }

  public void setExplainPlanNumMatchAllFilterSegments(long explainPlanNumMatchAllFilterSegments) {
    _explainPlanNumMatchAllFilterSegments = explainPlanNumMatchAllFilterSegments;
  }

  @Override
  public Map<String, String> getTraceInfo() {
    return _traceInfo;
  }

  public void setTraceInfo(Map<String, String> traceInfo) {
    _traceInfo = traceInfo;
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
