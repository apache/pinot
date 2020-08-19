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
class BrokerResponse {
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

  ResponseStats getResponseStats() {
    return _responseStats;
  }

  static BrokerResponse fromJson(JsonNode json) {
    return new BrokerResponse(json);
  }

  static BrokerResponse empty() {
    return new BrokerResponse();
  }
}
