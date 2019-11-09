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

import static org.apache.pinot.common.utils.CommonConstants.Broker.Request.QueryOptionKey;


/**
 * Helper methods for reading query options
 */
public final class QueryOptionsUtils {

  private QueryOptionsUtils() {

  }

  public static boolean isGroupByMode(String groupByMode, Map<String, String> queryOptions) {
    if (queryOptions != null) {
      String groupByModeValue = queryOptions.get(QueryOptionKey.GROUP_BY_MODE);
      return groupByModeValue != null && groupByModeValue.equalsIgnoreCase(groupByMode);
    }
    return false;
  }

  public static boolean isResponseFormat(String responseFormat, Map<String, String> queryOptions) {
    if (queryOptions != null) {
      String responseFormatValue = queryOptions.get(QueryOptionKey.RESPONSE_FORMAT);
      return responseFormatValue != null && responseFormatValue.equalsIgnoreCase(responseFormat);
    }
    return false;
  }

  public static boolean isPreserveType(Map<String, String> queryOptions) {
    String preserveTypeString =
        (queryOptions == null) ? "false" : queryOptions.getOrDefault(QueryOptionKey.PRESERVE_TYPE, "false");
    return Boolean.valueOf(preserveTypeString);
  }
}
