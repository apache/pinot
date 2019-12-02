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
package org.apache.pinot.core.util;

import java.util.Map;
import org.apache.pinot.common.utils.CommonConstants.Broker.Request;


/**
 * Wrapper class to read query options
 */
public class QueryOptions {

  private String _groupByMode = Request.PQL;
  private String _responseFormat = Request.PQL;
  private boolean _preserveType = false;

  public QueryOptions(Map<String, String> queryOptions) {
    if (queryOptions != null) {
      _groupByMode = queryOptions.getOrDefault(Request.QueryOptionKey.GROUP_BY_MODE, Request.PQL);
      _responseFormat = queryOptions.getOrDefault(Request.QueryOptionKey.RESPONSE_FORMAT, Request.PQL);

      String preserveTypeString = queryOptions.getOrDefault(Request.QueryOptionKey.PRESERVE_TYPE, "false");
      _preserveType = Boolean.valueOf(preserveTypeString);
    }
  }

  public boolean isGroupByModeSQL() {
    return _groupByMode.equalsIgnoreCase(Request.SQL);
  }

  public boolean isResponseFormatSQL() {
      return _responseFormat.equalsIgnoreCase(Request.SQL);
  }

  public boolean isPreserveType() {
    return _preserveType;
  }
}
