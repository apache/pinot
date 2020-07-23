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

  // By default, Pinot serves queries to Hybrid tables by applying a time filter based on data availability.
  // For example, when the most recent offline segment is for 1/2/2000, and a query has no time filter, then the broker
  // queries the offline table for * - 1/2/2000 (exclusive), and the realtime table with 1/2/2000 (inclusive) - *.
  // This config tells the broker to immediately start serving offline segments once available; or in other words, to
  // query the offline table with the most recent time value inclusively.
  private final Boolean _serveOfflineSegmentsImmediately;

  @JsonCreator
  public QueryConfig(@JsonProperty("timeoutMs") @Nullable Long timeoutMs,
      @JsonProperty("serveOfflineSegmentsImmediately") @Nullable Boolean serveOfflineSegmentsImmediately) {
    Preconditions.checkArgument(timeoutMs == null || timeoutMs > 0, "Invalid 'timeoutMs': %s", timeoutMs);
    _timeoutMs = timeoutMs;
    _serveOfflineSegmentsImmediately = serveOfflineSegmentsImmediately;
  }

  @Nullable
  public Long getTimeoutMs() {
    return _timeoutMs;
  }

  @Nullable
  public Boolean getServeOfflineSegmentsImmediately() {
    return _serveOfflineSegmentsImmediately;
  }
}
