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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * This class implements pinot-broker's response format for any given query.
 * All fields either primitive data types, or native objects (as opposed to JSONObjects).
 *
 * Supports serialization via JSON.
 */
@JsonPropertyOrder({
    "resultTable", "requestId", "stageStats", "exceptions", "numServersQueried", "numServersResponded",
    "numSegmentsQueried", "numSegmentsProcessed", "numSegmentsMatched", "numConsumingSegmentsQueried",
    "numConsumingSegmentsProcessed", "numConsumingSegmentsMatched", "numDocsScanned", "numEntriesScannedInFilter",
    "numEntriesScannedPostFilter", "numGroupsLimitReached", "totalDocs", "timeUsedMs", "offlineThreadCpuTimeNs",
    "realtimeThreadCpuTimeNs", "offlineSystemActivitiesCpuTimeNs", "realtimeSystemActivitiesCpuTimeNs",
    "offlineResponseSerializationCpuTimeNs", "realtimeResponseSerializationCpuTimeNs", "offlineTotalCpuTimeNs",
    "realtimeTotalCpuTimeNs", "segmentStatistics", "traceInfo"
})
public class BrokerResponseNativeV2 extends BrokerResponseNative {

  private final Map<Integer, BrokerResponseStats> _stageIdStats = new HashMap<>();

  public BrokerResponseNativeV2() {
  }

  public BrokerResponseNativeV2(ProcessingException exception) {
    super(exception);
  }

  public BrokerResponseNativeV2(List<ProcessingException> exceptions) {
    super(exceptions);
  }

  /** Generate EXPLAIN PLAN output when queries are evaluated by Broker without going to the Server. */
  private static BrokerResponseNativeV2 getBrokerResponseExplainPlanOutput() {
    BrokerResponseNativeV2 brokerResponse = BrokerResponseNativeV2.empty();
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{"BROKER_EVALUATE", 0, -1});
    brokerResponse.setResultTable(new ResultTable(DataSchema.EXPLAIN_RESULT_SCHEMA, rows));
    return brokerResponse;
  }

  /**
   * Get a new empty {@link BrokerResponseNativeV2}.
   */
  public static BrokerResponseNativeV2 empty() {
    return new BrokerResponseNativeV2();
  }

  public static BrokerResponseNativeV2 fromJsonString(String jsonString)
      throws IOException {
    return JsonUtils.stringToObject(jsonString, BrokerResponseNativeV2.class);
  }

  public void addStageStat(Integer stageId, BrokerResponseStats brokerResponseStats) {
    // StageExecutionWallTime will always be there, other stats are optional such as OperatorStats
    if (brokerResponseStats.getStageExecWallTimeMs() != -1) {
      _stageIdStats.put(stageId, brokerResponseStats);
    }
  }

  @JsonProperty("stageStats")
  public Map<Integer, BrokerResponseStats> getStageIdStats() {
    return _stageIdStats;
  }
}
