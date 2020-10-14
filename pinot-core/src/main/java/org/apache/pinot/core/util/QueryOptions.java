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

import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.CommonConstants.Broker.Request;


/**
 * Wrapper class to read query options
 */
public class QueryOptions {
  private final Long _timeoutMs;
  private final boolean _groupByModeSQL;
  private final boolean _responseFormatSQL;
  private final boolean _preserveType;
  private final boolean _skipUpsert;

  public QueryOptions(@Nullable Map<String, String> queryOptions) {
    if (queryOptions != null) {
      _timeoutMs = getTimeoutMs(queryOptions);
      _groupByModeSQL = Request.SQL.equalsIgnoreCase(queryOptions.get(Request.QueryOptionKey.GROUP_BY_MODE));
      _responseFormatSQL = Request.SQL.equalsIgnoreCase(queryOptions.get(Request.QueryOptionKey.RESPONSE_FORMAT));
      _preserveType = Boolean.parseBoolean(queryOptions.get(Request.QueryOptionKey.PRESERVE_TYPE));
      _skipUpsert = Boolean.parseBoolean(queryOptions.get(Request.QueryOptionKey.SKIP_UPSERT));
    } else {
      _timeoutMs = null;
      _groupByModeSQL = false;
      _responseFormatSQL = false;
      _preserveType = false;
      _skipUpsert = false;
    }
  }

  @Nullable
  public Long getTimeoutMs() {
    return _timeoutMs;
  }

  public boolean isGroupByModeSQL() {
    return _groupByModeSQL;
  }

  public boolean isResponseFormatSQL() {
    return _responseFormatSQL;
  }

  public boolean isPreserveType() {
    return _preserveType;
  }

  public boolean isUpsertSkipped() {
    return _skipUpsert;
  }

  @Nullable
  public static Long getTimeoutMs(Map<String, String> queryOptions) {
    String timeoutMsString = queryOptions.get(Request.QueryOptionKey.TIMEOUT_MS);
    if (timeoutMsString != null) {
      long timeoutMs = Long.parseLong(timeoutMsString);
      Preconditions.checkState(timeoutMs > 0, "Query timeout must be positive, got: %s", timeoutMs);
      return timeoutMs;
    } else {
      return null;
    }
  }
}
