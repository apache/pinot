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
package org.apache.pinot.common.utils.config;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.JoinOverFlowMode;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.WindowOverFlowMode;


/**
 * Utils to parse query options.
 */
public class QueryOptionsUtils {

  private QueryOptionsUtils() {
  }

  private static final Map<String, String> CONFIG_RESOLVER;
  private static final RuntimeException CLASS_LOAD_ERROR;

  static {
    // this is a bit hacky, but lots of the code depends directly on usage of
    // Map<String, String> (JSON serialization/GRPC code) so we cannot just
    // refactor all code to use a case-insensitive abstraction like PinotConfiguration
    // without a lot of work - additionally, the config constants are string constants
    // instead of enums so there's no good way to iterate over them, but they are
    // public API so we cannot just change them to be an enum
    Map<String, String> configResolver = new HashMap<>();
    Throwable classLoadError = null;

    try {
      for (Field declaredField : QueryOptionKey.class.getDeclaredFields()) {
        if (declaredField.getType().equals(String.class)) {
          int mods = declaredField.getModifiers();
          if (Modifier.isStatic(mods) && Modifier.isFinal(mods)) {
            String config = (String) declaredField.get(null);
            configResolver.put(config.toLowerCase(), config);
          }
        }
      }
    } catch (IllegalAccessException e) {
      // prefer rethrowing this during runtime instead of a ClassNotFoundException
      configResolver = null;
      classLoadError = e;
    }

    CONFIG_RESOLVER = configResolver == null ? null : ImmutableMap.copyOf(configResolver);
    CLASS_LOAD_ERROR = classLoadError == null ? null
        : new RuntimeException("Failure to build case insensitive mapping.", classLoadError);
  }

  public static Map<String, String> resolveCaseInsensitiveOptions(Map<String, String> queryOptions) {
    if (CLASS_LOAD_ERROR != null) {
      throw CLASS_LOAD_ERROR;
    }

    Map<String, String> resolved = new HashMap<>();
    for (Map.Entry<String, String> configEntry : queryOptions.entrySet()) {
      String config = CONFIG_RESOLVER.get(configEntry.getKey().toLowerCase());
      if (config != null) {
        resolved.put(config, configEntry.getValue());
      } else {
        resolved.put(configEntry.getKey(), configEntry.getValue());
      }
    }

    return resolved;
  }

  @Nullable
  public static Long getTimeoutMs(Map<String, String> queryOptions) {
    String timeoutMsString = queryOptions.get(QueryOptionKey.TIMEOUT_MS);
    return checkedParseLongPositive(QueryOptionKey.TIMEOUT_MS, timeoutMsString);
  }

  @Nullable
  public static Long getMaxServerResponseSizeBytes(Map<String, String> queryOptions) {
    String responseSize = queryOptions.get(QueryOptionKey.MAX_SERVER_RESPONSE_SIZE_BYTES);
    return checkedParseLongPositive(QueryOptionKey.MAX_SERVER_RESPONSE_SIZE_BYTES, responseSize);
  }

  @Nullable
  public static Long getMaxQueryResponseSizeBytes(Map<String, String> queryOptions) {
    String responseSize = queryOptions.get(QueryOptionKey.MAX_QUERY_RESPONSE_SIZE_BYTES);
    return checkedParseLongPositive(QueryOptionKey.MAX_QUERY_RESPONSE_SIZE_BYTES, responseSize);
  }

  public static boolean isAndScanReorderingEnabled(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.AND_SCAN_REORDERING));
  }

  public static boolean isSkipUpsert(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.SKIP_UPSERT));
  }

  public static boolean isSkipUpsertView(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.SKIP_UPSERT_VIEW));
  }

  public static long getUpsertViewFreshnessMs(Map<String, String> queryOptions) {
    String freshnessMsString = queryOptions.get(QueryOptionKey.UPSERT_VIEW_FRESHNESS_MS);
    return freshnessMsString != null ? Long.parseLong(freshnessMsString) : -1; //can blow up with NFE
  }

  public static boolean isScanStarTreeNodes(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.SCAN_STAR_TREE_NODES));
  }

  public static boolean isSkipStarTree(Map<String, String> queryOptions) {
    return "false".equalsIgnoreCase(queryOptions.get(QueryOptionKey.USE_STAR_TREE));
  }

  public static boolean isSkipScanFilterReorder(Map<String, String> queryOptions) {
    return "false".equalsIgnoreCase(queryOptions.get(QueryOptionKey.USE_SCAN_REORDER_OPTIMIZATION));
  }

  @Nullable
  public static Map<String, Set<FieldConfig.IndexType>> getSkipIndexes(Map<String, String> queryOptions) {
    // Example config:  skipIndexes='col1=inverted,range&col2=inverted'
    String skipIndexesStr = queryOptions.get(QueryOptionKey.SKIP_INDEXES);
    if (skipIndexesStr == null) {
      return null;
    }

    String[] perColumnIndexSkip = StringUtils.split(skipIndexesStr, '&');
    Map<String, Set<FieldConfig.IndexType>> skipIndexes = new HashMap<>();
    for (String columnConf : perColumnIndexSkip) {
      String[] conf = StringUtils.split(columnConf, '=');
      if (conf.length != 2) {
        throw new RuntimeException("Invalid format for " + QueryOptionKey.SKIP_INDEXES
            + ". Example of valid format: SET skipIndexes='col1=inverted,range&col2=inverted'");
      }
      String columnName = conf[0];
      String[] indexTypes = StringUtils.split(conf[1], ',');

      for (String indexType : indexTypes) {
        skipIndexes.computeIfAbsent(columnName, k -> new HashSet<>())
            .add(FieldConfig.IndexType.valueOf(indexType.toUpperCase()));
      }
    }

    return skipIndexes;
  }

  @Nullable
  public static Boolean isUseFixedReplica(Map<String, String> queryOptions) {
    String useFixedReplica = queryOptions.get(CommonConstants.Broker.Request.QueryOptionKey.USE_FIXED_REPLICA);
    return useFixedReplica != null ? Boolean.parseBoolean(useFixedReplica) : null;
  }

  @Nullable
  public static Integer getNumReplicaGroupsToQuery(Map<String, String> queryOptions) {
    String numReplicaGroupsToQuery = queryOptions.get(QueryOptionKey.NUM_REPLICA_GROUPS_TO_QUERY);
    return checkedParseIntPositive(QueryOptionKey.NUM_REPLICA_GROUPS_TO_QUERY, numReplicaGroupsToQuery);
  }

  public static boolean isExplainPlanVerbose(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.EXPLAIN_PLAN_VERBOSE));
  }

  public static boolean isUseMultistageEngine(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.USE_MULTISTAGE_ENGINE));
  }

  public static boolean isGetCursor(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.GET_CURSOR));
  }

  public static Integer getCursorNumRows(Map<String, String> queryOptions) {
    String cursorNumRows = queryOptions.get(QueryOptionKey.CURSOR_NUM_ROWS);
    return checkedParseIntPositive(QueryOptionKey.CURSOR_NUM_ROWS, cursorNumRows);
  }

  public static Optional<Boolean> isExplainAskingServers(Map<String, String> queryOptions) {
    String value = queryOptions.get(QueryOptionKey.EXPLAIN_ASKING_SERVERS);
    if (value == null) {
      return Optional.empty();
    }
    return Optional.of(Boolean.parseBoolean(value));
  }

  @Nullable
  public static Integer getMaxExecutionThreads(Map<String, String> queryOptions) {
    String maxExecutionThreadsString = queryOptions.get(QueryOptionKey.MAX_EXECUTION_THREADS);
    return checkedParseIntPositive(QueryOptionKey.MAX_EXECUTION_THREADS, maxExecutionThreadsString);
  }

  @Nullable
  public static Integer getMinSegmentGroupTrimSize(Map<String, String> queryOptions) {
    String minSegmentGroupTrimSizeString = queryOptions.get(QueryOptionKey.MIN_SEGMENT_GROUP_TRIM_SIZE);
    // NOTE: Non-positive value means turning off the segment level trim
    return uncheckedParseInt(QueryOptionKey.MIN_SEGMENT_GROUP_TRIM_SIZE, minSegmentGroupTrimSizeString);
  }

  @Nullable
  public static Integer getMinServerGroupTrimSize(Map<String, String> queryOptions) {
    String minServerGroupTrimSizeString = queryOptions.get(QueryOptionKey.MIN_SERVER_GROUP_TRIM_SIZE);
    // NOTE: Non-positive value means turning off the segment level trim
    return uncheckedParseInt(QueryOptionKey.MIN_SERVER_GROUP_TRIM_SIZE, minServerGroupTrimSizeString);
  }

  @Nullable
  public static Integer getMinBrokerGroupTrimSize(Map<String, String> queryOptions) {
    String minBrokerGroupTrimSizeString = queryOptions.get(QueryOptionKey.MIN_BROKER_GROUP_TRIM_SIZE);
    // NOTE: Non-positive value means turning off the broker level trim
    return uncheckedParseInt(QueryOptionKey.MIN_BROKER_GROUP_TRIM_SIZE, minBrokerGroupTrimSizeString);
  }

  @Nullable
  public static Integer getMSEMinGroupTrimSize(Map<String, String> queryOptions) {
    String mseMinGroupTrimSizeString = queryOptions.get(QueryOptionKey.MSE_MIN_GROUP_TRIM_SIZE);
    // NOTE: Non-positive value means turning off the intermediate stage trim
    return uncheckedParseInt(QueryOptionKey.MSE_MIN_GROUP_TRIM_SIZE, mseMinGroupTrimSizeString);
  }

  @Nullable
  public static Integer getGroupTrimThreshold(Map<String, String> queryOptions) {
    String groupByTrimThreshold = queryOptions.get(QueryOptionKey.GROUP_TRIM_THRESHOLD);
    // NOTE: Non-positive value means turning off the on-the-fly trim before all groups are added
    return uncheckedParseInt(QueryOptionKey.GROUP_TRIM_THRESHOLD, groupByTrimThreshold);
  }

  @Nullable
  public static Integer getNumThreadsExtractFinalResult(Map<String, String> queryOptions) {
    String numThreadsExtractFinalResultString = queryOptions.get(QueryOptionKey.NUM_THREADS_EXTRACT_FINAL_RESULT);
    return checkedParseInt(QueryOptionKey.NUM_THREADS_EXTRACT_FINAL_RESULT, numThreadsExtractFinalResultString, 1);
  }

  @Nullable
  public static Integer getChunkSizeExtractFinalResult(Map<String, String> queryOptions) {
    String chunkSizeExtractFinalResultString =
        queryOptions.get(QueryOptionKey.CHUNK_SIZE_EXTRACT_FINAL_RESULT);
    return checkedParseInt(QueryOptionKey.CHUNK_SIZE_EXTRACT_FINAL_RESULT, chunkSizeExtractFinalResultString, 1);
  }

  public static boolean isNullHandlingEnabled(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.ENABLE_NULL_HANDLING));
  }

  public static boolean isServerReturnFinalResult(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.SERVER_RETURN_FINAL_RESULT));
  }

  public static boolean isServerReturnFinalResultKeyUnpartitioned(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.SERVER_RETURN_FINAL_RESULT_KEY_UNPARTITIONED));
  }

  public static boolean isFilteredAggregationsSkipEmptyGroups(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.FILTERED_AGGREGATIONS_SKIP_EMPTY_GROUPS));
  }

  @Nullable
  public static String getOrderByAlgorithm(Map<String, String> queryOptions) {
    return queryOptions.get(QueryOptionKey.ORDER_BY_ALGORITHM);
  }

  @Nullable
  public static Integer getMultiStageLeafLimit(Map<String, String> queryOptions) {
    String maxLeafLimitStr = queryOptions.get(QueryOptionKey.MULTI_STAGE_LEAF_LIMIT);
    return checkedParseIntNonNegative(QueryOptionKey.MULTI_STAGE_LEAF_LIMIT, maxLeafLimitStr);
  }

  public static boolean getErrorOnNumGroupsLimit(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.ERROR_ON_NUM_GROUPS_LIMIT));
  }

  @Nullable
  public static Integer getNumGroupsLimit(Map<String, String> queryOptions) {
    String maxNumGroupLimit = queryOptions.get(QueryOptionKey.NUM_GROUPS_LIMIT);
    return checkedParseIntPositive(QueryOptionKey.NUM_GROUPS_LIMIT, maxNumGroupLimit);
  }

  @Nullable
  public static Integer getMaxInitialResultHolderCapacity(Map<String, String> queryOptions) {
    String maxInitialResultHolderCapacity = queryOptions.get(QueryOptionKey.MAX_INITIAL_RESULT_HOLDER_CAPACITY);
    return checkedParseIntPositive(QueryOptionKey.MAX_INITIAL_RESULT_HOLDER_CAPACITY, maxInitialResultHolderCapacity);
  }

  public static boolean optimizeMaxInitialResultHolderCapacityEnabled(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.OPTIMIZE_MAX_INITIAL_RESULT_HOLDER_CAPACITY));
  }

  @Nullable
  public static Integer getMSEMaxInitialResultHolderCapacity(Map<String, String> queryOptions) {
    String maxInitialCapacity = queryOptions.get(QueryOptionKey.MSE_MAX_INITIAL_RESULT_HOLDER_CAPACITY);
    return checkedParseIntPositive(QueryOptionKey.MSE_MAX_INITIAL_RESULT_HOLDER_CAPACITY, maxInitialCapacity);
  }

  @Nullable
  public static Integer getMinInitialIndexedTableCapacity(Map<String, String> queryOptions) {
    String minInitialIndexedTableCapacity = queryOptions.get(QueryOptionKey.MIN_INITIAL_INDEXED_TABLE_CAPACITY);
    return checkedParseIntPositive(QueryOptionKey.MIN_INITIAL_INDEXED_TABLE_CAPACITY, minInitialIndexedTableCapacity);
  }

  public static boolean shouldDropResults(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(CommonConstants.Broker.Request.QueryOptionKey.DROP_RESULTS));
  }

  @Nullable
  public static Integer getMaxStreamingPendingBlocks(Map<String, String> queryOptions) {
    String maxStreamingPendingBlocks = queryOptions.get(QueryOptionKey.MAX_STREAMING_PENDING_BLOCKS);
    return checkedParseIntPositive(QueryOptionKey.MAX_STREAMING_PENDING_BLOCKS, maxStreamingPendingBlocks);
  }

  @Nullable
  public static Integer getMaxRowsInJoin(Map<String, String> queryOptions) {
    String maxRowsInJoin = queryOptions.get(QueryOptionKey.MAX_ROWS_IN_JOIN);
    return checkedParseIntPositive(QueryOptionKey.MAX_ROWS_IN_JOIN, maxRowsInJoin);
  }

  @Nullable
  public static JoinOverFlowMode getJoinOverflowMode(Map<String, String> queryOptions) {
    String joinOverflowModeStr = queryOptions.get(QueryOptionKey.JOIN_OVERFLOW_MODE);
    return joinOverflowModeStr != null ? JoinOverFlowMode.valueOf(joinOverflowModeStr) : null;
  }

  @Nullable
  public static Integer getMaxRowsInWindow(Map<String, String> queryOptions) {
    String maxRowsInWindow = queryOptions.get(QueryOptionKey.MAX_ROWS_IN_WINDOW);
    return checkedParseIntPositive(QueryOptionKey.MAX_ROWS_IN_WINDOW, maxRowsInWindow);
  }

  @Nullable
  public static WindowOverFlowMode getWindowOverflowMode(Map<String, String> queryOptions) {
    String windowOverflowModeStr = queryOptions.get(QueryOptionKey.WINDOW_OVERFLOW_MODE);
    return windowOverflowModeStr != null ? WindowOverFlowMode.valueOf(windowOverflowModeStr) : null;
  }

  public static boolean isSkipUnavailableServers(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.SKIP_UNAVAILABLE_SERVERS));
  }

  public static boolean isSecondaryWorkload(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.IS_SECONDARY_WORKLOAD));
  }

  public static Boolean isUseMSEToFillEmptySchema(Map<String, String> queryOptions, boolean defaultValue) {
    String useMSEToFillEmptySchema = queryOptions.get(QueryOptionKey.USE_MSE_TO_FILL_EMPTY_RESPONSE_SCHEMA);
    return useMSEToFillEmptySchema != null ? Boolean.parseBoolean(useMSEToFillEmptySchema) : defaultValue;
  }

  @Nullable
  private static Integer uncheckedParseInt(String optionName, @Nullable String optionValue) {
    if (optionValue == null) {
      return null;
    }
    try {
      return Integer.parseInt(optionValue);
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException(String.format("%s must be an integer, got: %s", optionName, optionValue));
    }
  }

  @Nullable
  private static Integer checkedParseIntPositive(String optionName, @Nullable String optionValue) {
    return checkedParseInt(optionName, optionValue, 1);
  }

  @Nullable
  private static Integer checkedParseIntNonNegative(String optionName, @Nullable String optionValue) {
    return checkedParseInt(optionName, optionValue, 0);
  }

  @Nullable
  private static Integer checkedParseInt(String optionName, @Nullable String optionValue, int minValue) {
    if (optionValue == null) {
      return null;
    }
    int value;
    try {
      value = Integer.parseInt(optionValue);
    } catch (NumberFormatException nfe) {
      throw intParseException(optionName, optionValue, minValue);
    }
    if (value < minValue) {
      throw intParseException(optionName, optionValue, minValue);
    }
    return value;
  }

  private static IllegalArgumentException intParseException(String optionName, String optionValue, int minValue) {
    return new IllegalArgumentException(
        String.format("%s must be a number between %d and 2^31-1, got: %s", optionName, minValue, optionValue));
  }

  @Nullable
  private static Long checkedParseLongPositive(String optionName, @Nullable String optionValue) {
    return checkedParseLong(optionName, optionValue, 1);
  }

  @Nullable
  private static Long checkedParseLong(String optionName, @Nullable String optionValue, long minValue) {
    if (optionValue == null) {
      return null;
    }
    long value;
    try {
      value = Long.parseLong(optionValue);
    } catch (NumberFormatException nfe) {
      throw longParseException(optionName, optionValue, minValue);
    }
    if (value < minValue) {
      throw longParseException(optionName, optionValue, minValue);
    }
    return value;
  }

  private static IllegalArgumentException longParseException(String optionName, @Nullable String optionValue,
      long minValue) {
    return new IllegalArgumentException(
        String.format("%s must be a number between %d and 2^63-1, got: %s", optionName, minValue, optionValue));
  }
}
