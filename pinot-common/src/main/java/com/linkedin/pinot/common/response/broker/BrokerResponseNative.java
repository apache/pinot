/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.response.broker;

import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.ProcessingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.type.TypeReference;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * This class implements pinot-broker's response format for any given query.
 * All fields either primitive data types, or native objects (as opposed to JSONObjects).
 *
 * Supports serialization via JSON.
 */
@JsonPropertyOrder({"selectionResults", "aggregationResults", "exceptions", "numServersQueried", "numServersResponded",
    "numDocsScanned", "numEntriesScannedInFilter", "numEntriesScannedPostFilter", "totalDocs", "numGroupsLimitReached",
    "timeUsedMs", "segmentStatistics", "traceInfo"})
public class BrokerResponseNative implements BrokerResponse {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final BrokerResponseNative EMPTY_RESULT = BrokerResponseNative.empty();
  public static final BrokerResponseNative NO_TABLE_RESULT =
      new BrokerResponseNative(QueryException.BROKER_RESOURCE_MISSING_ERROR);

  private int _numServersQueried = 0;
  private int _numServersResponded = 0;
  private long _numDocsScanned = 0L;
  private long _numEntriesScannedInFilter = 0L;
  private long _numEntriesScannedPostFilter = 0L;
  private long _totalDocs = 0L;
  private boolean _numGroupsLimitReached = false;
  private long _timeUsedMs = 0L;

  private SelectionResults _selectionResults;
  private List<AggregationResult> _aggregationResults;

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

  /**
   * Get a new empty {@link BrokerResponseNative}.
   */
  public static BrokerResponseNative empty() {
    return new BrokerResponseNative();
  }

  @JsonProperty("selectionResults")
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public SelectionResults getSelectionResults() {
    return _selectionResults;
  }

  @JsonProperty("selectionResults")
  public void setSelectionResults(SelectionResults selectionResults) {
    _selectionResults = selectionResults;
  }

  @JsonProperty("aggregationResults")
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public List<AggregationResult> getAggregationResults() {
    return _aggregationResults;
  }

  @JsonProperty("aggregationResults")
  public void setAggregationResults(List<AggregationResult> aggregationResults) {
    _aggregationResults = aggregationResults;
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
  public String toJsonString() throws IOException {
    return OBJECT_MAPPER.writeValueAsString(this);
  }

  @Override
  public JSONObject toJson() throws IOException, JSONException {
    return new JSONObject(toJsonString());
  }

  public static BrokerResponseNative fromJsonString(String jsonString) throws IOException {
    return OBJECT_MAPPER.readValue(jsonString, new TypeReference<BrokerResponseNative>() {
    });
  }

  public static BrokerResponseNative fromJsonObject(JSONObject jsonObject) throws IOException {
    return fromJsonString(jsonObject.toString());
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
