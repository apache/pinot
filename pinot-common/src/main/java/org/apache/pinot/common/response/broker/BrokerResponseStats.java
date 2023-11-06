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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;


// TODO: Decouple the execution stats aggregator logic and make it into a util that can aggregate 2 values with the
//  same metadataKey
// TODO: Replace member fields with a simple map of <MetadataKey, Object>
// TODO: Add a subStat field, stage level subStats will contain each operator stats
@JsonPropertyOrder({
    "brokerId", "requestId", "exceptions", "numBlocks", "numRows", "stageExecutionTimeMs", "stageExecutionUnit",
    "stageExecWallTimeMs", "stageExecEndTimeMs", "numServersQueried", "numServersResponded", "numSegmentsQueried",
    "numSegmentsProcessed", "numSegmentsMatched", "numConsumingSegmentsQueried", "numConsumingSegmentsProcessed",
    "numConsumingSegmentsMatched", "numDocsScanned", "numEntriesScannedInFilter", "numEntriesScannedPostFilter",
    "numGroupsLimitReached", "maxRowsInJoinReached", "totalDocs", "timeUsedMs", "offlineThreadCpuTimeNs",
    "realtimeThreadCpuTimeNs", "offlineSystemActivitiesCpuTimeNs", "realtimeSystemActivitiesCpuTimeNs",
    "offlineResponseSerializationCpuTimeNs", "realtimeResponseSerializationCpuTimeNs", "offlineTotalCpuTimeNs",
    "realtimeTotalCpuTimeNs", "brokerReduceTimeMs", "traceInfo", "operatorStats", "tableNames"
})
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class BrokerResponseStats extends BrokerResponseNative {

  private int _numBlocks = 0;
  private int _numRows = 0;
  private long _stageExecutionTimeMs = 0;
  private int _stageExecutionUnit = 0;
  private long _stageExecWallTimeMs = -1;
  private Map<String, Map<String, String>> _operatorStats = new HashMap<>();
  private List<String> _tableNames = new ArrayList<>();

  @Override
  public ResultTable getResultTable() {
    return null;
  }

  @JsonProperty("numBlocks")
  public int getNumBlocks() {
    return _numBlocks;
  }

  @JsonProperty("numBlocks")
  public void setNumBlocks(int numBlocks) {
    _numBlocks = numBlocks;
  }

  @JsonProperty("numRows")
  public int getNumRows() {
    return _numRows;
  }

  @JsonProperty("numRows")
  public void setNumRows(int numRows) {
    _numRows = numRows;
  }

  @JsonProperty("stageExecutionTimeMs")
  public long getStageExecutionTimeMs() {
    return _stageExecutionTimeMs;
  }

  @JsonProperty("stageExecutionTimeMs")
  public void setStageExecutionTimeMs(long stageExecutionTimeMs) {
    _stageExecutionTimeMs = stageExecutionTimeMs;
  }

  @JsonProperty("stageExecWallTimeMs")
  public long getStageExecWallTimeMs() {
    return _stageExecWallTimeMs;
  }

  @JsonProperty("stageExecWallTimeMs")
  public void setStageExecWallTimeMs(long stageExecWallTimeMs) {
    _stageExecWallTimeMs = stageExecWallTimeMs;
  }

  @JsonProperty("stageExecutionUnit")
  public long getStageExecutionUnit() {
    return _stageExecutionUnit;
  }

  @JsonProperty("stageExecutionUnit")
  public void setStageExecutionUnit(int stageExecutionUnit) {
    _stageExecutionUnit = stageExecutionUnit;
  }

  public String toJsonString()
      throws IOException {
    return JsonUtils.objectToString(this);
  }

  @JsonProperty("operatorStats")
  public Map<String, Map<String, String>> getOperatorStats() {
    return _operatorStats;
  }

  @JsonProperty("operatorStats")
  public void setOperatorStats(Map<String, Map<String, String>> operatorStats) {
    _operatorStats = operatorStats;
  }

  @JsonProperty("tableNames")
  public List<String> getTableNames() {
    return _tableNames;
  }

  @JsonProperty("tableNames")
  public void setTableNames(List<String> tableNames) {
    _tableNames = tableNames;
  }
}
