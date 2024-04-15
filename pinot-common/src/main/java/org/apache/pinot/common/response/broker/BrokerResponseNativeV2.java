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
package org.apache.pinot.common.response.broker;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * This class implements pinot-broker's response format for any given query.
 * All fields either primitive data types, or native objects (as opposed to JSONObjects).
 *
 * Supports serialization via JSON.
 */
@JsonPropertyOrder({
    "resultTable", "requestId", "stageStats", "brokerId", "exceptions", "numServersQueried", "numServersResponded",
    "numSegmentsQueried", "numSegmentsProcessed", "numSegmentsMatched", "numConsumingSegmentsQueried",
    "numConsumingSegmentsProcessed", "numConsumingSegmentsMatched", "numDocsScanned", "numEntriesScannedInFilter",
    "numEntriesScannedPostFilter", "numGroupsLimitReached", "maxRowsInJoinReached", "totalDocs", "timeUsedMs",
    "offlineThreadCpuTimeNs", "realtimeThreadCpuTimeNs", "offlineSystemActivitiesCpuTimeNs",
    "realtimeSystemActivitiesCpuTimeNs", "offlineResponseSerializationCpuTimeNs",
    "realtimeResponseSerializationCpuTimeNs", "offlineTotalCpuTimeNs", "realtimeTotalCpuTimeNs", "brokerReduceTimeMs",
    "segmentStatistics", "traceInfo", "partialResult"
})
public class BrokerResponseNativeV2 extends BrokerResponseNative {

  private final List<JsonNode> _stageIdStats = new ArrayList<>();
  /**
   * The max number of rows seen at runtime.
   * <p>
   * In single-stage this doesn't make sense given it is the max number of rows read from the table. But in multi-stage
   * virtual rows can be generated. For example, in a join query, the number of rows can be more than the number of rows
   * in the table.
   */
  private long _maxRows = 0;

  public BrokerResponseNativeV2() {
  }

  public BrokerResponseNativeV2(ProcessingException exception) {
    super(exception);
  }

  public BrokerResponseNativeV2(List<ProcessingException> exceptions) {
    super(exceptions);
  }

  /**
   * Get a new empty {@link BrokerResponseNativeV2}.
   */
  public static BrokerResponseNativeV2 empty() {
    return new BrokerResponseNativeV2();
  }

  public void addStageStats(JsonNode stageStats) {
    ObjectNode node = JsonUtils.newObjectNode();
    node.put("stage", _stageIdStats.size());
    node.set("stats", stageStats);
    _stageIdStats.add(node);
  }

  @JsonProperty("stageStats")
  public List<JsonNode> getStageIdStats() {
    return _stageIdStats;
  }

  @JsonProperty("maxRows")
  public long getMaxRows() {
    return _maxRows;
  }

  public void mergeMaxRows(long maxRows) {
    _maxRows = Math.max(_maxRows, maxRows);
  }
}
