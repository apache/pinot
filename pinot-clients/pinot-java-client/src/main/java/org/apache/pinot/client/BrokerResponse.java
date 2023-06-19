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
  private String _requestId;
  private JsonNode _aggregationResults;
  private JsonNode _selectionResults;
  private JsonNode _resultTable;
  private JsonNode _exceptions;
  private ExecutionStats _executionStats;

  private BrokerResponse() {
  }

  private BrokerResponse(JsonNode brokerResponse) {
    _requestId = brokerResponse.get("requestId") != null ? brokerResponse.get("requestId").asText() : "-1";
    _aggregationResults = brokerResponse.get("aggregationResults");
    _exceptions = brokerResponse.get("exceptions");
    _selectionResults = brokerResponse.get("selectionResults");
    _resultTable = brokerResponse.get("resultTable");
    _executionStats = ExecutionStats.fromJson(brokerResponse);
  }

  boolean hasExceptions() {
    return _exceptions != null && !_exceptions.isEmpty();
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

  ExecutionStats getExecutionStats() {
    return _executionStats;
  }

  static BrokerResponse fromJson(JsonNode json) {
    return new BrokerResponse(json);
  }

  static BrokerResponse empty() {
    return new BrokerResponse();
  }

  public String getRequestId() {
    return _requestId;
  }
}
