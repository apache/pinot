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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.spi.utils.JsonUtils;


// TODO: Decouple the execution stats aggregator logic and make it into a util that can aggregate 2 values with the
//  same metadataKey
// TODO: Replace member fields with a simple map of <MetadataKey, Object>
// TODO: Add a subStat field, stage level subStats will contain each operator stats
@JsonPropertyOrder({"exceptions", "numBlocks", "numRows", "stageExecutionTimeMs", "numServersQueried",
    "numServersResponded", "numSegmentsQueried", "numSegmentsProcessed", "numSegmentsMatched",
    "numConsumingSegmentsQueried", "numConsumingSegmentsProcessed", "numConsumingSegmentsMatched",
    "numDocsScanned", "numEntriesScannedInFilter", "numEntriesScannedPostFilter", "numGroupsLimitReached",
    "totalDocs", "timeUsedMs", "offlineThreadCpuTimeNs", "realtimeThreadCpuTimeNs",
    "offlineSystemActivitiesCpuTimeNs", "realtimeSystemActivitiesCpuTimeNs", "offlineResponseSerializationCpuTimeNs",
    "realtimeResponseSerializationCpuTimeNs", "offlineTotalCpuTimeNs", "realtimeTotalCpuTimeNs",
    "traceInfo", "operatorStats", "tableNames"})
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class BrokerResponseStats extends BrokerResponseNative {

  private Map<String, Map<String, String>> _operatorStats = new HashMap<>();
  private List<String> _tableNames = new ArrayList<>();

  @Override
  public ResultTable getResultTable() {
    return null;
  }

  @JsonProperty("numBlocks")
  public long getNumBlocks() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.NUM_BLOCKS, 0L);
  }

  @JsonProperty("numBlocks")
  public void setNumBlocks(long numBlocks) {
    _aggregatedStats.put(DataTable.MetadataKey.NUM_BLOCKS, numBlocks);
  }

  @JsonProperty("numRows")
  public long getNumRows() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.NUM_ROWS, 0L);
  }

  @JsonProperty("numRows")
  public void setNumRows(long numRows) {
    _aggregatedStats.put(DataTable.MetadataKey.NUM_BLOCKS, numRows);
  }

  @JsonProperty("stageExecutionTimeMs")
  public long getStageExecutionTimeMs() {
    return (Long) _aggregatedStats.getOrDefault(DataTable.MetadataKey.OPERATOR_EXECUTION_TIME_MS, 0L);
  }

  @JsonProperty("stageExecutionTimeMs")
  public void setStageExecutionTimeMs(long stageExecutionTimeMs) {
    _aggregatedStats.put(DataTable.MetadataKey.OPERATOR_EXECUTION_TIME_MS, stageExecutionTimeMs);
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
    String tableNames = (String) _aggregatedStats.get(DataTable.MetadataKey.TABLE);
    if (tableNames == null || tableNames.isEmpty()) {
      return new ArrayList<>();
    } else {
      return Arrays.stream(tableNames.split(DataTable.MetadataKey.MULTI_VALUE_STRING_SEPARATOR)).distinct().collect(
          Collectors.toList());
    }
  }

  @JsonProperty("tableNames")
  public void setTableNames(List<String> tableNames) {
    _tableNames = tableNames;
  }
}
