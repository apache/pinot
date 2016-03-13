/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.common.response.BrokerResponseFactory;
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
@JsonPropertyOrder({"selectionResults", "traceInfo", "numDocsScanned", "aggregationResults", "timeUsedMs", "segmentStatistics", "exceptions", "totalDocs"})
public class BrokerResponseNative implements BrokerResponse {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private long _numDocsScanned = 0;
  private long _totalDocs = 0;
  private long _timeUsedMs = 0;

  private SelectionResults _selectionResults;
  private List<AggregationResult> _aggregationResults;

  private Map<String, String> _traceInfo;
  private List<QueryProcessingException> _processingExceptions;
  private List<String> _segmentStatistics;

  public static final BrokerResponseNative EMPTY_RESULT;
  public static final BrokerResponseNative NO_TABLE_RESULT;

  static {
    EMPTY_RESULT = new BrokerResponseNative();
    EMPTY_RESULT.setTimeUsedMs(0);

    NO_TABLE_RESULT = new BrokerResponseNative();
    NO_TABLE_RESULT.setTimeUsedMs(0);

    List<ProcessingException> processingExceptions = new ArrayList<ProcessingException>();
    ProcessingException exception = QueryException.BROKER_RESOURCE_MISSING_ERROR.deepCopy();
    exception.setMessage("No table hit!");

    processingExceptions.add(exception);
    NO_TABLE_RESULT.setExceptions(processingExceptions);
  }

  public BrokerResponseNative() {
    _traceInfo = new HashMap<String, String>();
    _processingExceptions = new ArrayList<QueryProcessingException>();
    _segmentStatistics = new ArrayList<String>();
  }

  @JsonProperty("numDocsScanned")
  public long getNumDocsScanned() {
    return _numDocsScanned;
  }

  @JsonProperty("numDocsScanned")
  public void setNumDocsScanned(long numDocsScanned) {
    _numDocsScanned = numDocsScanned;
  }

  @JsonProperty("totalDocs")
  public long getTotalDocs() {
    return _totalDocs;
  }

  @JsonProperty("totalDocs")
  public void setTotalDocs(long totalDocs) {
    _totalDocs = totalDocs;
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

  @JsonProperty("selectionResults")
  public void setSelectionResults(SelectionResults selectionResults) {
    _selectionResults = selectionResults;
  }

  @JsonProperty("aggregationResults")
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public void setAggregationResults(List<AggregationResult> aggregationResults) {
    _aggregationResults = aggregationResults;
  }

  @JsonProperty("selectionResults")
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public SelectionResults getSelectionResults() {
    return _selectionResults;
  }

  @JsonProperty("aggregationResults")
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public List<AggregationResult> getAggregationResults() {
    return _aggregationResults;
  }

  @JsonProperty("traceInfo")
  public Map<String, String> getTraceInfo() {
    return _traceInfo;
  }

  @JsonProperty("traceInfo")
  public void setTraceInfo(Map<String, String> traceInfo) {
    _traceInfo = traceInfo;
  }

  @JsonProperty("exceptions")
  public List<QueryProcessingException> getProcessingExceptions() {
    return _processingExceptions;
  }

  @JsonProperty("exception")
  public void setProcessingExceptions(List<QueryProcessingException> processingExceptions) {
    _processingExceptions = processingExceptions;
  }

  @JsonProperty("segmentStatistics")
  public List<String> getSegmentStatistics() {
    return _segmentStatistics;
  }

  @JsonProperty("segmentStatistics")
  public void setSegmentStatistics(List<String> segmentStatistics) {
    _segmentStatistics = segmentStatistics;
  }

  public String toJsonString()
      throws IOException {
    return OBJECT_MAPPER.writeValueAsString(this);
  }

  @Override
  public JSONObject toJson()
      throws IOException, JSONException {
    return new JSONObject(toJsonString());
  }

  public BrokerResponseNative fromJsonString(String jsonString)
      throws IOException {
    return OBJECT_MAPPER.readValue(jsonString, new TypeReference<BrokerResponseNative>() {
    });
  }

  public BrokerResponseNative fromJsonObject(JSONObject jsonObject)
      throws IOException {
    return fromJsonString(jsonObject.toString());
  }

  @JsonIgnore
  @Override
  public void setExceptions(List<ProcessingException> exceptions) {
    for (com.linkedin.pinot.common.response.ProcessingException exception : exceptions) {
      QueryProcessingException processingException = new QueryProcessingException();
      processingException.setErrorCode(exception.getErrorCode());
      processingException.setMessage(exception.getMessage());
      _processingExceptions.add(processingException);
    }
  }

  @JsonIgnore
  @Override
  public BrokerResponseFactory.ResponseType getResponseType() {
    return BrokerResponseFactory.ResponseType.BROKER_RESPONSE_TYPE_NATIVE;
  }

  @JsonIgnore
  @Override
  public int getExceptionsSize() {
    return _processingExceptions.size();
  }
}
