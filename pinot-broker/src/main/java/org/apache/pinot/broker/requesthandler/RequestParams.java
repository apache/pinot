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
package org.apache.pinot.broker.requesthandler;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Splitter;
import java.util.Map;

import static org.apache.pinot.common.utils.CommonConstants.Broker.Request.*;


public class RequestParams {

  private String _query;
  private Map<String, String> _debugOptions = null;
  private boolean _trace;
  private boolean _validateQuery;

  public RequestParams(JsonNode request) {
    _query = request.get(PQL).asText();
    _validateQuery = request.has(VALIDATE_QUERY) && request.get(VALIDATE_QUERY).asBoolean();
    _trace = request.has(TRACE) && request.get(TRACE).asBoolean();
    if (request.has(DEBUG_OPTIONS)) {
      _debugOptions = Splitter.on(';')
          .omitEmptyStrings()
          .trimResults()
          .withKeyValueSeparator('=')
          .split(request.get(DEBUG_OPTIONS).asText());
    }
  }

  public String getQuery() {
    return _query;
  }

  public Map<String, String> getDebugOptions() {
    return _debugOptions;
  }

  public boolean isTrace() {
    return _trace;
  }

  public boolean isValidateQuery() {
    return _validateQuery;
  }
}
