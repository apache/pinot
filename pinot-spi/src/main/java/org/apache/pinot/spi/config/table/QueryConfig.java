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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.pinot.spi.config.BaseJsonConfig;

import javax.annotation.Nullable;


/**
 * The {@code QueryConfig} class contains the table-level query execution related configurations.
 */
public class QueryConfig extends BaseJsonConfig {
  // The timeout for the entire query execution in milliseconds. This value will be gathered on the broker side, and
  // passed to the servers within the broker request.
  // If the broker times out, it will stop waiting for more server responses and return the reduced server responses
  // gathered so far, and numServersResponded should be smaller than numServersQueried.
  // If the server times out, it will directly interrupt the query execution. The server response does not matter much
  // because by the time the server times out, the broker should already timed out and returned the response.
  private final Long _timeoutMs;
  private final Integer _maxThreadsPerQuery;

  @JsonCreator
  public QueryConfig(@JsonProperty("timeoutMs") @Nullable Long timeoutMs,
                     @JsonProperty("maxThreadsPerQuery") @Nullable Integer maxThreadsPerQuery) {
    Preconditions.checkArgument(timeoutMs == null || timeoutMs > 0, "Invalid 'timeoutMs': %s", timeoutMs);
    Preconditions.checkArgument(maxThreadsPerQuery == null || maxThreadsPerQuery < 1, "Invalid 'maxThreadsPerQuery': %s",
        maxThreadsPerQuery);
    _timeoutMs = timeoutMs;
    _maxThreadsPerQuery = maxThreadsPerQuery;
  }

  @Nullable
  public Long getTimeoutMs() {
    return _timeoutMs;
  }

  @Nullable
  public Integer getMaxThreadsPerQuery() {
    return _maxThreadsPerQuery;
  }

}
