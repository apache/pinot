/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.client;

import java.util.List;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Reimplementation of BrokerResponse from pinot-common, so that pinot-api does not depend on pinot-common.
 */
class BrokerResponse {
  private JSONArray _aggregationResults;
  private JSONObject _selectionResults;
  private JSONArray _exceptions;

  private BrokerResponse() {
  }

  private BrokerResponse(JSONObject brokerResponse) {
    try {
      if (brokerResponse.has("aggregationResults")) {
        _aggregationResults = brokerResponse.getJSONArray("aggregationResults");
      }
      if (brokerResponse.has("exceptions")) {
        _exceptions = brokerResponse.getJSONArray("exceptions");
      }
      if (brokerResponse.has("selectionResults")) {
        _selectionResults = brokerResponse.getJSONObject("selectionResults");
      }
    } catch (JSONException e) {
      throw new PinotClientException(e);
    }
  }

  boolean hasExceptions() {
    return _exceptions != null && _exceptions.length() != 0;
  }

  JSONArray getExceptions() {
    return _exceptions;
  }

  JSONArray getAggregationResults() {
    return _aggregationResults;
  }

  JSONObject getSelectionResults() {
    return _selectionResults;
  }

  int getAggregationResultsSize() {
    if (_aggregationResults == null) {
      return 0;
    } else {
      return _aggregationResults.length();
    }
  }

  static BrokerResponse fromJson(JSONObject json) {
    return new BrokerResponse(json);
  }

  static BrokerResponse empty() {
    return new BrokerResponse();
  }
}
