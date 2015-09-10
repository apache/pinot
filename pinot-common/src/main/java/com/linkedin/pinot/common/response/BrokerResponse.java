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
package com.linkedin.pinot.common.response;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.exception.QueryException;


/**
 * BrokerResponse
 *
 */
public class BrokerResponse {
  private long _totalDocs = 0;
  private long _numDocsScanned = 0;
  private long _timeUsedMs = 0;
  private List<JSONObject> _aggregationResults;
  private List<ResponseStatistics> _segmentStatistics;
  private List<ProcessingException> _exceptions;
  private Map<String, String> _traceInfo;
  private JSONObject _selectionResults;
  public static BrokerResponse EMPTY_RESULT;
  public static BrokerResponse NO_TABLE_RESULT;

  static {
    EMPTY_RESULT = new BrokerResponse();
    EMPTY_RESULT.setTimeUsedMs(0);

    NO_TABLE_RESULT = new BrokerResponse();
    NO_TABLE_RESULT.setTimeUsedMs(0);
    List<ProcessingException> processingExceptions = new ArrayList<ProcessingException>();
    ProcessingException exception = QueryException.BROKER_RESOURCE_MISSING_ERROR.deepCopy();
    exception.setMessage("No table hit!");
    processingExceptions.add(exception);
    NO_TABLE_RESULT.setExceptions(processingExceptions);
  }

  public BrokerResponse() {
    _aggregationResults = new ArrayList<JSONObject>();
    _segmentStatistics = new ArrayList<ResponseStatistics>();
    _exceptions = new ArrayList<ProcessingException>();
    _traceInfo = new LinkedHashMap<String, String>();
  }

  public long getTotalDocs() {
    return _totalDocs;
  }

  public void setTotalDocs(long totalDocs) {
    _totalDocs = totalDocs;
  }

  public long getNumDocsScanned() {
    return _numDocsScanned;
  }

  public void setNumDocsScanned(long numDocsScanned) {
    _numDocsScanned = numDocsScanned;
  }

  public long getTimeUsedMs() {
    return _timeUsedMs;
  }

  public void setTimeUsedMs(long timeUsedMs) {
    _timeUsedMs = timeUsedMs;
  }

  public int getAggregationResultsSize() {
    return (_aggregationResults == null) ? 0 : _aggregationResults.size();
  }

  public java.util.Iterator<JSONObject> getAggregationResultsIterator() {
    return (_aggregationResults == null) ? null : _aggregationResults.iterator();
  }

  public void addToAggregationResults(JSONObject elem) {
    if (_aggregationResults == null) {
      _aggregationResults = new ArrayList<JSONObject>();
    }
    _aggregationResults.add(elem);
  }

  public List<JSONObject> getAggregationResults() {
    return _aggregationResults;
  }

  public void setAggregationResults(List<JSONObject> aggregationResults) {
    _aggregationResults = aggregationResults;
  }

  public JSONObject getSelectionResults() {
    return _selectionResults;
  }

  public void setSelectionResults(JSONObject selectionResults) {
    _selectionResults = selectionResults;
  }

  public int getSegmentStatisticsSize() {
    return (_segmentStatistics == null) ? 0 : _segmentStatistics.size();
  }

  public java.util.Iterator<ResponseStatistics> getSegmentStatisticsIterator() {
    return (_segmentStatistics == null) ? null : _segmentStatistics.iterator();
  }

  public void addToSegmentStatistics(ResponseStatistics elem) {
    if (_segmentStatistics == null) {
      _segmentStatistics = new ArrayList<ResponseStatistics>();
    }
    _segmentStatistics.add(elem);
  }

  public List<ResponseStatistics> getSegmentStatistics() {
    return _segmentStatistics;
  }

  public void setSegmentStatistics(List<ResponseStatistics> segmentStatistics) {
    _segmentStatistics = segmentStatistics;
  }

  public int getExceptionsSize() {
    return (_exceptions == null) ? 0 : _exceptions.size();
  }

  public java.util.Iterator<ProcessingException> getExceptionsIterator() {
    return (_exceptions == null) ? null : _exceptions.iterator();
  }

  public void addToExceptions(ProcessingException elem) {
    if (_exceptions == null) {
      _exceptions = new ArrayList<ProcessingException>();
    }
    _exceptions.add(elem);
  }

  public List<ProcessingException> getExceptions() {
    return _exceptions;
  }

  public void setExceptions(List<ProcessingException> exceptions) {
    _exceptions = exceptions;
  }

  public int getTraceInfoSize() {
    return (_traceInfo == null) ? 0 : _traceInfo.size();
  }

  public void putToTraceInfo(String key, String val) {
    if (_traceInfo == null) {
      _traceInfo = new LinkedHashMap<String, String>();
    }
    _traceInfo.put(key, val);
  }

  public Map<String, String> getTraceInfo() {
    return _traceInfo;
  }

  public void setTraceInfo(Map<String, String> traceInfo) {
    _traceInfo = traceInfo;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("BrokerResponse(");

    sb.append("totalDocs:");
    sb.append(_totalDocs);

    sb.append(", ");
    sb.append("numDocsScanned:");
    sb.append(_numDocsScanned);

    sb.append(", ");
    sb.append("timeUsedMs:");
    sb.append(_timeUsedMs);
    sb.append(", ");
    sb.append("aggregationResults:");
    if (_aggregationResults == null) {
      sb.append("null");
    } else {
      sb.append(_aggregationResults);
    }
    sb.append(", ");
    sb.append("selectionResults:");
    if (_selectionResults == null) {
      sb.append("null");
    } else {
      sb.append(_selectionResults);
    }
    sb.append(", ");
    sb.append("segmentStatistics:");
    if (_segmentStatistics == null) {
      sb.append("null");
    } else {
      sb.append(_segmentStatistics);
    }
    sb.append(", ");
    sb.append("exceptions:");
    if (_exceptions == null) {
      sb.append("null");
    } else {
      sb.append(_exceptions);
    }
    sb.append(", ");
    sb.append("traceInfo:");
    if (_traceInfo == null) {
      sb.append("null");
    } else {
      sb.append(_traceInfo);
    }
    sb.append(")");
    return sb.toString();
  }

  public JSONObject toJson() throws JSONException {
    JSONObject retJsonObject = new JSONObject();
    retJsonObject.put("totalDocs", _totalDocs);
    retJsonObject.put("timeUsedMs", _timeUsedMs);
    retJsonObject.put("numDocsScanned", _numDocsScanned);
    retJsonObject.put("aggregationResults", new JSONArray(_aggregationResults));
    retJsonObject.put("selectionResults", _selectionResults);
    retJsonObject.put("segmentStatistics", new JSONArray(_segmentStatistics));
    retJsonObject.put("exceptions", new JSONArray(_exceptions));
    JSONObject traceInfo = new JSONObject();
    for (String key : _traceInfo.keySet()) {
      traceInfo.put(key,  new JSONArray(_traceInfo.get(key)));
    }
    retJsonObject.put("traceInfo", traceInfo);
    return retJsonObject;
  }

  public static BrokerResponse fromJson(JSONObject retJsonObject) throws JSONException {
    BrokerResponse brokerResponse = new BrokerResponse();
    brokerResponse.setTotalDocs(retJsonObject.getLong("totalDocs"));
    brokerResponse.setTimeUsedMs(retJsonObject.getLong("timeUsedMs"));
    brokerResponse.setNumDocsScanned(retJsonObject.getLong("numDocsScanned"));
    if (retJsonObject.has("aggregationResults")) {
      JSONArray aggregationResults = retJsonObject.getJSONArray("aggregationResults");
      if (aggregationResults != null && aggregationResults.length() > 0) {
        List<JSONObject> aggregationResultJSONObjects = new ArrayList<JSONObject>();
        for (int i = 0; i < aggregationResults.length(); ++i) {
          aggregationResultJSONObjects.add(aggregationResults.getJSONObject(i));
        }
        brokerResponse.setAggregationResults(aggregationResultJSONObjects);
      }
    }
    if (retJsonObject.has("selectionResults")) {
      JSONObject selectionResults = retJsonObject.getJSONObject("selectionResults");
      brokerResponse.setSelectionResults(selectionResults);
    }

    if (retJsonObject.has("segmentStatistics")) {
      JSONArray segmentStatistics = retJsonObject.getJSONArray("segmentStatistics");
      if (segmentStatistics != null && segmentStatistics.length() > 0) {
        List<ResponseStatistics> segmentStatisticObjects = new ArrayList<ResponseStatistics>();
        for (int i = 0; i < segmentStatistics.length(); ++i) {
          segmentStatisticObjects.add((ResponseStatistics) segmentStatistics.get(i));
        }
        brokerResponse.setSegmentStatistics(segmentStatisticObjects);
      }
    }

    if (retJsonObject.has("exceptions")) {
      JSONArray exceptions = retJsonObject.getJSONArray("exceptions");
      if (exceptions != null && exceptions.length() > 0) {
        List<ProcessingException> exceptionsList = new ArrayList<ProcessingException>();
        for (int i = 0; i < exceptions.length(); ++i) {
          ProcessingException processingException = new ProcessingException();
          String exceptionString = exceptions.getString(i);
          int errorCode = Integer.parseInt(exceptionString.split("errorCode:")[1].split(", message:")[0]);
          String errorMsg = exceptionString.split(", message:")[1];
          errorMsg = errorMsg.substring(0, errorMsg.length() - 1);
          processingException.setErrorCode(errorCode);
          processingException.setMessage(errorMsg);
          exceptionsList.add(processingException);
        }
        brokerResponse.setExceptions(exceptionsList);
      }
    }

    if (retJsonObject.has("traceInfo")) {
      JSONObject traceInfoObject = retJsonObject.getJSONObject("traceInfo");
      Map<String, String> traceInfoMap = new LinkedHashMap<String, String>();

      Iterator iterator = traceInfoObject.keys();
      while (iterator.hasNext()) {
        String key = (String) iterator.next();
        String value = traceInfoObject.getString(key);
        traceInfoMap.put(key, value);
      }
      brokerResponse.setTraceInfo(traceInfoMap);
    }
    return brokerResponse;
  }

  public static BrokerResponse getNullBrokerResponse() {
    return NO_TABLE_RESULT;
  }

  public static BrokerResponse getEmptyBrokerResponse() {
    return EMPTY_RESULT;
  }
}
