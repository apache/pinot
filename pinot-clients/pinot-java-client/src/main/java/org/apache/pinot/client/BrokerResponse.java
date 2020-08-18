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
package org.apache.pinot.client;

import com.fasterxml.jackson.databind.JsonNode;


/**
 * Reimplementation of BrokerResponse from pinot-common, so that pinot-api does not depend on pinot-common.
 */
public class BrokerResponse {
  private JsonNode _aggregationResults;
  private JsonNode _selectionResults;
  private JsonNode _resultTable;
  private JsonNode _exceptions;
  private ResponseStats _responseStats;

  private BrokerResponse() {
  }

  private BrokerResponse(JsonNode brokerResponse) {
    _aggregationResults = brokerResponse.get("aggregationResults");
    _exceptions = brokerResponse.get("exceptions");
    _selectionResults = brokerResponse.get("selectionResults");
    _resultTable = brokerResponse.get("resultTable");
    _responseStats = ResponseStats.fromJson(brokerResponse);
  }

  public static class ResponseStats {
    private final int _numServersQueried;
    private final int _numServersResponded;
    private final long _numDocsScanned;
    private final long _numEntriesScannedInFilter;
    private final long _numEntriesScannedPostFilter;
    private final long _numSegmentsQueried;
    private final long _numSegmentsProcessed;
    private final long _numSegmentsMatched;
    private final long _numConsumingSegmentsQueried;
    private final long _minConsumingFreshnessTimeMs;
    private final long _totalDocs;
    private final boolean _numGroupsLimitReached;
    private final long _timeUsedMs;

    private ResponseStats(JsonNode brokerResponse) {
      _numServersQueried = brokerResponse.has("numServersQueried") ?
          brokerResponse.get("numServersQueried").asInt() : -1;
      _numServersResponded = brokerResponse.has("numServersResponded") ?
          brokerResponse.get("numServersResponded").asInt() : -1;
      _numDocsScanned = brokerResponse.has("numDocsScanned") ?
          brokerResponse.get("numDocsScanned").asLong() : -1L;
      _numEntriesScannedInFilter = brokerResponse.has("numEntriesScannedInFilter") ?
          brokerResponse.get("numEntriesScannedInFilter").asLong() : -1L;
      _numEntriesScannedPostFilter = brokerResponse.has("numEntriesScannedPostFilter") ?
          brokerResponse.get("numEntriesScannedPostFilter").asLong() : -1L;
      _numSegmentsQueried = brokerResponse.has("numSegmentsQueried") ?
          brokerResponse.get("numSegmentsQueried").asLong() : -1L;
      _numSegmentsProcessed = brokerResponse.has("numSegmentsProcessed") ?
          brokerResponse.get("numSegmentsProcessed").asLong() : -1L;
      _numSegmentsMatched = brokerResponse.has("numSegmentsMatched") ?
          brokerResponse.get("numSegmentsMatched").asLong() : -1L;
      _numConsumingSegmentsQueried = brokerResponse.has("numConsumingSegmentsQueried") ?
          brokerResponse.get("numConsumingSegmentsQueried").asLong() : -1L;
      _minConsumingFreshnessTimeMs = brokerResponse.has("minConsumingFreshnessTimeMs") ?
          brokerResponse.get("minConsumingFreshnessTimeMs").asLong() : -1L;
      _totalDocs = brokerResponse.has("totalDocs") ?
          brokerResponse.get("totalDocs").asLong() : -1L;
      _numGroupsLimitReached = brokerResponse.has("numGroupsLimitReached")
          && brokerResponse.get("numGroupsLimitReached").asBoolean();
      _timeUsedMs = brokerResponse.has("timeUsedMs") ?
          brokerResponse.get("timeUsedMs").asLong() : -1L;
    }

    static ResponseStats fromJson(JsonNode json) {
      return new ResponseStats(json);
    }

    public int getNumServersQueried() {
      return _numServersQueried;
    }

    public int getNumServersResponded() {
      return _numServersResponded;
    }

    public long getNumDocsScanned() {
      return _numDocsScanned;
    }

    public long getNumEntriesScannedInFilter() {
      return _numEntriesScannedInFilter;
    }

    public long getNumEntriesScannedPostFilter() {
      return _numEntriesScannedPostFilter;
    }

    public long getNumSegmentsQueried() {
      return _numSegmentsQueried;
    }

    public long getNumSegmentsProcessed() {
      return _numSegmentsProcessed;
    }

    public long getNumSegmentsMatched() {
      return _numSegmentsMatched;
    }

    public long getNumConsumingSegmentsQueried() {
      return _numConsumingSegmentsQueried;
    }

    public long getMinConsumingFreshnessTimeMs() {
      return _minConsumingFreshnessTimeMs;
    }

    public long getTotalDocs() {
      return _totalDocs;
    }

    public boolean isNumGroupsLimitReached() {
      return _numGroupsLimitReached;
    }

    public long getTimeUsedMs() {
      return _timeUsedMs;
    }

    @Override
    public String toString() {
      return "{numServersQueried: " + _numServersQueried +
          ", numServersResponded: " + _numServersResponded +
          ", numDocsScanned: " + _numDocsScanned +
          ", numEntriesScannedInFilter: " + _numEntriesScannedInFilter +
          ", numEntriesScannedPostFilter: " + _numEntriesScannedPostFilter +
          ", numSegmentsQueried: " + _numSegmentsQueried +
          ", numSegmentsProcessed: " + _numSegmentsProcessed +
          ", numSegmentsMatched: " + _numSegmentsMatched +
          ", numConsumingSegmentsQueried: " + _numConsumingSegmentsQueried +
          ", minConsumingFreshnessTimeMs: " + _minConsumingFreshnessTimeMs +
          "ms, totalDocs: " + _totalDocs +
          ", numGroupsLimitReached: " + _numGroupsLimitReached +
          ", timeUsedMs: " + _timeUsedMs +
          "ms}";
    }
  }

  boolean hasExceptions() {
    return _exceptions != null && _exceptions.size() != 0;
  }

  JsonNode getExceptions() {
    return _exceptions;
  }

  JsonNode getAggregationResults() {
    return _aggregationResults;
  }

  JsonNode getSelectionResults() {
    return _selectionResults;
  }

  JsonNode getResultTable() {
    return _resultTable;
  }

  int getAggregationResultsSize() {
    if (_aggregationResults == null) {
      return 0;
    } else {
      return _aggregationResults.size();
    }
  }

  public ResponseStats getResponseStats() {
    return _responseStats;
  }

  static BrokerResponse fromJson(JsonNode json) {
    return new BrokerResponse(json);
  }

  static BrokerResponse empty() {
    return new BrokerResponse();
  }
}
