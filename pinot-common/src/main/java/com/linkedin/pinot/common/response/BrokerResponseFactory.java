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

import com.linkedin.pinot.common.response.broker.BrokerResponseNative;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Factory class to build a concrete BrokerResponse object for the given type.
 */
public class BrokerResponseFactory {

  /**
   * Enum for broker response types.
   */
  public enum ResponseType {
    BROKER_RESPONSE_TYPE_JSON,
    BROKER_RESPONSE_TYPE_NATIVE
  }

  private static final String BROKER_RESPONSE_TYPE_KEY = "responseType";
  public static final String ILLEGAL_RESPONSE_TYPE =
      "Could not create brokerResponse object for specified illegal type.";

  /**
   * Returns a new BrokerResponse object of the specified concrete type.
   * Throws a IllegalArgumentException if an invalid response type is passed in as argument.
   *
   * @param brokerResponseType
   * @return
   */
  public static BrokerResponse get(ResponseType brokerResponseType) {
    if (brokerResponseType.equals(ResponseType.BROKER_RESPONSE_TYPE_JSON)) {
      return new BrokerResponseJSON();
    } else if (brokerResponseType.equals(ResponseType.BROKER_RESPONSE_TYPE_NATIVE)) {
      return new BrokerResponseNative();
    } else {
      throw new IllegalArgumentException(ILLEGAL_RESPONSE_TYPE);
    }
  }

  /**
   * Given a brokerRequest JSONObject, return the requested enum ResponseType
   * - If request does not have responseType property, return the default value of ResponseType.BROKER_RESPONSE_JSON
   * - If requested responseType property does not match enum values, throws IllegalArgumentException.
   * - If JSONObject cannot be parsed, throws JSONException.
   *
   * @param brokerRequest
   * @return
   * @throws JSONException
   */
  public static ResponseType getResponseType(JSONObject brokerRequest)
      throws JSONException {
    if (!brokerRequest.has((BROKER_RESPONSE_TYPE_KEY))) {
      return ResponseType.BROKER_RESPONSE_TYPE_JSON;
    }

    return ResponseType.valueOf(brokerRequest.get(BROKER_RESPONSE_TYPE_KEY).toString());
  }

  /**
   * Given a responseType string, return the appropriate enum ResponseType.
   * If the input string is null, return the default BROKER_RESPONSE_TYPE_JSON.
   * If input string cannot be mapped to enum field, throws an IllegalArgumentException
   *
   * @param responseType
   * @return
   */
  public static ResponseType getResponseType(String responseType) {
    if (responseType == null || responseType.equalsIgnoreCase(ResponseType.BROKER_RESPONSE_TYPE_JSON.name())) {
      return ResponseType.BROKER_RESPONSE_TYPE_JSON;
    } else if (responseType.equalsIgnoreCase(ResponseType.BROKER_RESPONSE_TYPE_NATIVE.name())) {
      return ResponseType.BROKER_RESPONSE_TYPE_NATIVE;
    } else {
      throw new IllegalArgumentException("Illegal response type string " + responseType);
    }
  }

  /**
   * Returns the static BrokerResponse of specified type, for case when table in query is not found.
   *
   * @param brokerResponseType
   * @return
   */
  public static BrokerResponse getNoTableHitBrokerResponse(ResponseType brokerResponseType) {
    if (brokerResponseType.equals(ResponseType.BROKER_RESPONSE_TYPE_NATIVE)) {
      return BrokerResponseNative.NO_TABLE_RESULT;
    } else if (brokerResponseType.equals(ResponseType.BROKER_RESPONSE_TYPE_JSON)) {
      return BrokerResponseJSON.NO_TABLE_RESULT;
    } else {
      throw new RuntimeException(ILLEGAL_RESPONSE_TYPE);
    }
  }

  /**
   * Returns the static empty BrokerResponse of specified type.
   * @param brokerResponseType
   * @return
   */
  public static BrokerResponse getEmptyBrokerResponse(ResponseType brokerResponseType) {
    if (brokerResponseType.equals(ResponseType.BROKER_RESPONSE_TYPE_NATIVE)) {
      return BrokerResponseNative.EMPTY_RESULT;
    } else if (brokerResponseType.equals(ResponseType.BROKER_RESPONSE_TYPE_JSON)) {
      return BrokerResponseJSON.EMPTY_RESULT;
    } else {
      throw new RuntimeException(ILLEGAL_RESPONSE_TYPE);
    }
  }
}
