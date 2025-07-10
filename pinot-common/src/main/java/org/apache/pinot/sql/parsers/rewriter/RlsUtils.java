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
package org.apache.pinot.sql.parsers.rewriter;

import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.spi.utils.CommonConstants;


public class RlsUtils {

  private RlsUtils() {
  }

  public static String buildRlsFilterKey(String tableName) {
    return String.format("%s-%s", CommonConstants.RLS_FILTERS, tableName);
  }

  public static Map<String, String> extractRlsFilters(Map<String, String> requestMetadata) {
    return requestMetadata.entrySet().stream()
        .filter(e -> e.getKey().startsWith(CommonConstants.RLS_FILTERS))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public static String getRlsFilterForTable(Map<String, String> queryOptions, String tableName) {
    return queryOptions.get(buildRlsFilterKey(tableName));
  }
}
