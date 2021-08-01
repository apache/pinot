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
import java.util.HashMap;
import java.util.Map;

/**
 * Simple POJO to hold query execution statistics for a request. These stats come in every
 * query that's executed and can be used for debugging Pinot slow queries.
 *
 * <p>Please note that objects of this class will hold a reference to the given JsonNode object
 * and that will only be released when the object is GC'ed.</p>
 */
public class ExecutionStats {

  private static final String NUM_SERVERS_QUERIED = "numServersQueried";
  private static final String NUM_SERVERS_RESPONDED = "numServersResponded";
  private static final String NUM_DOCS_SCANNED = "numDocsScanned";
  private static final String NUM_ENTRIES_SCANNED_IN_FILTER = "numEntriesScannedInFilter";
  private static final String NUM_ENTRIES_SCANNED_POST_FILTER = "numEntriesScannedPostFilter";
  private static final String NUM_SEGMENTS_QUERIED = "numSegmentsQueried";
  private static final String NUM_SEGMENTS_PROCESSED = "numSegmentsProcessed";
  private static final String NUM_SEGMENTS_MATCHED = "numSegmentsMatched";
  private static final String NUM_CONSUMING_SEGMENTS_QUERIED = "numConsumingSegmentsQueried";
  private static final String MIN_CONSUMING_FRESHNESS_TIME_MS = "minConsumingFreshnessTimeMs";
  private static final String TOTAL_DOCS = "totalDocs";
  private static final String NUM_GROUPS_LIMIT_REACHED = "numGroupsLimitReached";
  private static final String TIME_USED_MS = "timeUsedMs";

  private final JsonNode brokerResponse;

  ExecutionStats(JsonNode brokerResponse) {
    this.brokerResponse = brokerResponse;
  }

  public static ExecutionStats fromJson(JsonNode json) {
    return new ExecutionStats(json);
  }

  public int getNumServersQueried() {
    // Lazily load the field from the JsonNode to avoid reading the stats when not needed.
    return brokerResponse.has(NUM_SERVERS_QUERIED) ?
        brokerResponse.get(NUM_SERVERS_QUERIED).asInt() : -1;
  }

  public int getNumServersResponded() {
    return brokerResponse.has(NUM_SERVERS_RESPONDED) ?
        brokerResponse.get(NUM_SERVERS_RESPONDED).asInt() : -1;
  }

  public long getNumDocsScanned() {
    return brokerResponse.has(NUM_DOCS_SCANNED) ?
        brokerResponse.get(NUM_DOCS_SCANNED).asLong() : -1L;
  }

  public long getNumEntriesScannedInFilter() {
    return brokerResponse.has(NUM_ENTRIES_SCANNED_IN_FILTER) ?
        brokerResponse.get(NUM_ENTRIES_SCANNED_IN_FILTER).asLong() : -1L;
  }

  public long getNumEntriesScannedPostFilter() {
    return brokerResponse.has(NUM_ENTRIES_SCANNED_POST_FILTER) ?
        brokerResponse.get(NUM_ENTRIES_SCANNED_POST_FILTER).asLong() : -1L;
  }

  public long getNumSegmentsQueried() {
    return brokerResponse.has(NUM_SEGMENTS_QUERIED) ?
        brokerResponse.get(NUM_SEGMENTS_QUERIED).asLong() : -1L;
  }

  public long getNumSegmentsProcessed() {
    return brokerResponse.has(NUM_SEGMENTS_PROCESSED) ?
        brokerResponse.get(NUM_SEGMENTS_PROCESSED).asLong() : -1L;
  }

  public long getNumSegmentsMatched() {
    return brokerResponse.has(NUM_SEGMENTS_MATCHED) ?
        brokerResponse.get(NUM_SEGMENTS_MATCHED).asLong() : -1L;
  }

  public long getNumConsumingSegmentsQueried() {
    return brokerResponse.has(NUM_CONSUMING_SEGMENTS_QUERIED) ?
        brokerResponse.get(NUM_CONSUMING_SEGMENTS_QUERIED).asLong() : -1L;
  }

  public long getMinConsumingFreshnessTimeMs() {
    return brokerResponse.has(MIN_CONSUMING_FRESHNESS_TIME_MS) ?
        brokerResponse.get(MIN_CONSUMING_FRESHNESS_TIME_MS).asLong() : -1L;
  }

  public long getTotalDocs() {
    return brokerResponse.has(TOTAL_DOCS) ?
        brokerResponse.get(TOTAL_DOCS).asLong() : -1L;
  }

  public boolean isNumGroupsLimitReached() {
    return brokerResponse.has(NUM_GROUPS_LIMIT_REACHED)
        && brokerResponse.get(NUM_GROUPS_LIMIT_REACHED).asBoolean();
  }

  public long getTimeUsedMs() {
    return brokerResponse.has(TIME_USED_MS) ?
        brokerResponse.get(TIME_USED_MS).asLong() : -1L;
  }

  @Override
  public String toString() {
    Map<String, Object> map = new HashMap<>();
    map.put(NUM_SERVERS_QUERIED, getNumServersQueried());
    map.put(NUM_SERVERS_RESPONDED, getNumServersResponded());
    map.put(NUM_DOCS_SCANNED, getNumDocsScanned());
    map.put(NUM_ENTRIES_SCANNED_IN_FILTER, getNumEntriesScannedInFilter());
    map.put(NUM_ENTRIES_SCANNED_POST_FILTER, getNumEntriesScannedPostFilter());
    map.put(NUM_SEGMENTS_QUERIED, getNumSegmentsQueried());
    map.put(NUM_SEGMENTS_PROCESSED, getNumSegmentsProcessed());
    map.put(NUM_SEGMENTS_MATCHED, getNumSegmentsMatched());
    map.put(NUM_CONSUMING_SEGMENTS_QUERIED, getNumConsumingSegmentsQueried());
    map.put(MIN_CONSUMING_FRESHNESS_TIME_MS, getMinConsumingFreshnessTimeMs() + "ms");
    map.put(TOTAL_DOCS, getTotalDocs());
    map.put(NUM_GROUPS_LIMIT_REACHED, isNumGroupsLimitReached());
    map.put(TIME_USED_MS, getTimeUsedMs() + "ms");
    return map.toString();
  }
}
