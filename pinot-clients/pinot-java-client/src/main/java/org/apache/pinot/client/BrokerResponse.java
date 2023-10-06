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
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;


/**
 * Reimplementation of BrokerResponse from pinot-common, so that pinot-api does not depend on pinot-common.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public class BrokerResponse {
  private String _requestId;
  private String _brokerId;
  private JsonNode _aggregationResults;
  private JsonNode _selectionResults;
  private JsonNode _resultTable;
  private JsonNode _exceptions;
  private ExecutionStats _executionStats;

  private BrokerResponse(JsonNode brokerResponse) {
    _requestId = brokerResponse.get("requestId") != null ? brokerResponse.get("requestId").asText() : "unknown";
    _brokerId = brokerResponse.get("brokerId") != null ? brokerResponse.get("brokerId").asText() : "unknown";
    _aggregationResults = brokerResponse.get("aggregationResults");
    _exceptions = brokerResponse.get("exceptions");
    _selectionResults = brokerResponse.get("selectionResults");
    _resultTable = brokerResponse.get("resultTable");
    _executionStats = ExecutionStats.fromJson(brokerResponse);
  }

  public boolean hasExceptions() {
    return _exceptions != null && !_exceptions.isEmpty();
  }

  public int getAggregationResultsSize() {
    if (_aggregationResults == null) {
      return 0;
    } else {
      return _aggregationResults.size();
    }
  }

  public static BrokerResponse fromJson(JsonNode json) {
    return new BrokerResponse(json);
  }

  public static BrokerResponse empty() {
    return new BrokerResponse();
  }
}
