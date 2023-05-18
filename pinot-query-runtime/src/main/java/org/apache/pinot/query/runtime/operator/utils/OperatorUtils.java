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
package org.apache.pinot.query.runtime.operator.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.query.planner.DispatchablePlanFragment;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.operator.OperatorStats;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OperatorUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(OperatorUtils.class);
  private static final Map<String, String> OPERATOR_TOKEN_MAPPING = new HashMap<>();

  static {
    OPERATOR_TOKEN_MAPPING.put("=", "equals");
    OPERATOR_TOKEN_MAPPING.put(">", "greaterThan");
    OPERATOR_TOKEN_MAPPING.put("<", "lessThan");
    OPERATOR_TOKEN_MAPPING.put("<=", "lessThanOrEqual");
    OPERATOR_TOKEN_MAPPING.put(">=", "greaterThanOrEqual");
    OPERATOR_TOKEN_MAPPING.put("<>", "notEquals");
    OPERATOR_TOKEN_MAPPING.put("!=", "notEquals");
    OPERATOR_TOKEN_MAPPING.put("+", "plus");
    OPERATOR_TOKEN_MAPPING.put("-", "minus");
    OPERATOR_TOKEN_MAPPING.put("*", "times");
    OPERATOR_TOKEN_MAPPING.put("/", "divide");
    OPERATOR_TOKEN_MAPPING.put("||", "concat");
  }

  private OperatorUtils() {
    // do not instantiate.
  }

  /**
   * Canonicalize function name since Logical plan uses Parser.jj extracted tokens.
   * @param functionName input Function name
   * @return Canonicalize form of the input function name
   */
  public static String canonicalizeFunctionName(String functionName) {
    functionName = StringUtils.remove(functionName, " ");
    functionName = OPERATOR_TOKEN_MAPPING.getOrDefault(functionName, functionName);
    return functionName;
  }

  public static void recordTableName(OperatorStats operatorStats, DispatchablePlanFragment dispatchablePlanFragment) {
    String tableName = dispatchablePlanFragment.getTableName();
    if (tableName != null) {
      operatorStats.recordSingleStat(DataTable.MetadataKey.TABLE.getName(), tableName);
    }
  }

  public static String operatorStatsToJson(OperatorStats operatorStats) {
    try {
      Map<String, Object> jsonOut = new HashMap<>();
      jsonOut.put("requestId", operatorStats.getRequestId());
      jsonOut.put("stageId", operatorStats.getStageId());
      jsonOut.put("serverAddress", operatorStats.getServerAddress().toString());
      jsonOut.put("executionStats", operatorStats.getExecutionStats());
      return JsonUtils.objectToString(jsonOut);
    } catch (Exception e) {
      LOGGER.warn("Error occurred while serializing operatorStats: {}", operatorStats, e);
    }
    return null;
  }

  public static OperatorStats operatorStatsFromJson(String json) {
    try {
      JsonNode operatorStatsNode = JsonUtils.stringToJsonNode(json);
      long requestId = operatorStatsNode.get("requestId").asLong();
      int stageId = operatorStatsNode.get("stageId").asInt();
      String serverAddressStr = operatorStatsNode.get("serverAddress").asText();
      VirtualServerAddress serverAddress = VirtualServerAddress.parse(serverAddressStr);

      OperatorStats operatorStats =
          new OperatorStats(requestId, stageId, serverAddress);
      operatorStats.recordExecutionStats(
          JsonUtils.jsonNodeToObject(operatorStatsNode.get("executionStats"), new TypeReference<Map<String, String>>() {
          }));

      return operatorStats;
    } catch (Exception e) {
      LOGGER.warn("Error occurred while deserializing operatorStats: {}", json, e);
    }
    return null;
  }

  public static Map<String, OperatorStats> getOperatorStatsFromMetadata(MetadataBlock metadataBlock) {
    Map<String, OperatorStats> operatorStatsMap = new HashMap<>();
    for (Map.Entry<String, String> entry : metadataBlock.getStats().entrySet()) {
      try {
        operatorStatsMap.put(entry.getKey(), operatorStatsFromJson(entry.getValue()));
      } catch (Exception e) {
        LOGGER.warn("Error occurred while fetching operator stats from metadata", e);
      }
    }
    return operatorStatsMap;
  }

  public static Map<String, String> getMetadataFromOperatorStats(Map<String, OperatorStats> operatorStatsMap) {
    Map<String, String> metadataStats = new HashMap<>();
    for (Map.Entry<String, OperatorStats> entry : operatorStatsMap.entrySet()) {
      try {
        metadataStats.put(entry.getKey(), operatorStatsToJson(entry.getValue()));
      } catch (Exception e) {
        LOGGER.warn("Error occurred while fetching metadata from operator stats", e);
      }
    }
    return metadataStats;
  }
}
