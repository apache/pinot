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
package org.apache.pinot.client;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Simple POJO to hold query execution statistics for a request. These stats come in every
 * query that's executed and can be used for debugging Pinot slow queries.
 *
 * <p>Please note that objects of this class will hold a reference to the given JsonNode object
 * and that will only be released when the object is GC'ed.</p>
 */
class ExecutionStats {
  private final JsonNode brokerResponse;

  ExecutionStats(JsonNode brokerResponse) {
    this.brokerResponse = brokerResponse;
  }

  static ExecutionStats fromJson(JsonNode json) {
    return new ExecutionStats(json);
  }

  public int getNumServersQueried() {
    // Lazily load the field from the JsonNode to avoid reading the stats when not needed.
    return brokerResponse.has("numServersQueried") ?
        brokerResponse.get("numServersQueried").asInt() : -1;
  }

  public int getNumServersResponded() {
    return brokerResponse.has("numServersResponded") ?
        brokerResponse.get("numServersResponded").asInt() : -1;
  }

  public long getNumDocsScanned() {
    return brokerResponse.has("numDocsScanned") ?
        brokerResponse.get("numDocsScanned").asLong() : -1L;
  }

  public long getNumEntriesScannedInFilter() {
    return brokerResponse.has("numEntriesScannedInFilter") ?
        brokerResponse.get("numEntriesScannedInFilter").asLong() : -1L;
  }

  public long getNumEntriesScannedPostFilter() {
    return brokerResponse.has("numEntriesScannedPostFilter") ?
        brokerResponse.get("numEntriesScannedPostFilter").asLong() : -1L;
  }

  public long getNumSegmentsQueried() {
    return brokerResponse.has("numSegmentsQueried") ?
        brokerResponse.get("numSegmentsQueried").asLong() : -1L;
  }

  public long getNumSegmentsProcessed() {
    return brokerResponse.has("numSegmentsProcessed") ?
        brokerResponse.get("numSegmentsProcessed").asLong() : -1L;
  }

  public long getNumSegmentsMatched() {
    return brokerResponse.has("numSegmentsMatched") ?
        brokerResponse.get("numSegmentsMatched").asLong() : -1L;
  }

  public long getNumConsumingSegmentsQueried() {
    return brokerResponse.has("numConsumingSegmentsQueried") ?
        brokerResponse.get("numConsumingSegmentsQueried").asLong() : -1L;
  }

  public long getMinConsumingFreshnessTimeMs() {
    return brokerResponse.has("minConsumingFreshnessTimeMs") ?
        brokerResponse.get("minConsumingFreshnessTimeMs").asLong() : -1L;
  }

  public long getTotalDocs() {
    return brokerResponse.has("totalDocs") ?
        brokerResponse.get("totalDocs").asLong() : -1L;
  }

  public boolean isNumGroupsLimitReached() {
    return brokerResponse.has("numGroupsLimitReached")
        && brokerResponse.get("numGroupsLimitReached").asBoolean();
  }

  public long getTimeUsedMs() {
    return brokerResponse.has("timeUsedMs") ?
        brokerResponse.get("timeUsedMs").asLong() : -1L;
  }

  @Override
  public String toString() {
    return "{numServersQueried: " + getNumServersQueried() +
        ", numServersResponded: " + getNumServersResponded() +
        ", numDocsScanned: " + getNumDocsScanned() +
        ", numEntriesScannedInFilter: " + getNumEntriesScannedInFilter() +
        ", numEntriesScannedPostFilter: " + getNumEntriesScannedPostFilter() +
        ", numSegmentsQueried: " + getNumSegmentsQueried() +
        ", numSegmentsProcessed: " + getNumSegmentsProcessed() +
        ", numSegmentsMatched: " + getNumSegmentsMatched() +
        ", numConsumingSegmentsQueried: " + getNumConsumingSegmentsQueried() +
        ", minConsumingFreshnessTimeMs: " + getMinConsumingFreshnessTimeMs() +
        "ms, totalDocs: " + getTotalDocs() +
        ", numGroupsLimitReached: " + isNumGroupsLimitReached() +
        ", timeUsedMs: " + getTimeUsedMs() +
        "ms}";
  }
}
