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
