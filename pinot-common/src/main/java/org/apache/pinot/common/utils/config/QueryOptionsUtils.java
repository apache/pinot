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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.Prop;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
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

  public static final Prop<Long> TIMEOUT_MS_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.TIMEOUT_MS, QueryOptionsUtils::parsePositiveLong, null);
  public static final Prop<String> TABLE_SAMPLER_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.TABLE_SAMPLER, QueryOptionsUtils::parseString, null);
  public static final Prop<Long> EXTRA_PASSIVE_TIMEOUT_MS_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.EXTRA_PASSIVE_TIMEOUT_MS, QueryOptionsUtils::parseNonNegativeLong, null);
  public static final Prop<Long> MAX_SERVER_RESPONSE_SIZE_BYTES_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.MAX_SERVER_RESPONSE_SIZE_BYTES, QueryOptionsUtils::parsePositiveLong, null);
  public static final Prop<Long> MAX_QUERY_RESPONSE_SIZE_BYTES_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.MAX_QUERY_RESPONSE_SIZE_BYTES, QueryOptionsUtils::parsePositiveLong, null);
  public static final Prop<Long> UPSERT_VIEW_FRESHNESS_MS_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.UPSERT_VIEW_FRESHNESS_MS, QueryOptionsUtils::parseLong, -1L);
  public static final Prop<String> QUERY_HASH_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.QUERY_HASH, QueryOptionsUtils::parseString,
          CommonConstants.Broker.DEFAULT_QUERY_HASH);
  public static final Prop<Map<String, Set<FieldConfig.IndexType>>> SKIP_INDEXES_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.SKIP_INDEXES, QueryOptionsUtils::parseSkipIndexes, null);
  public static final Prop<Set<String>> SKIP_PLANNER_RULES_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.SKIP_PLANNER_RULES, QueryOptionsUtils::parsePlannerRules, Set.of());
  public static final Prop<Set<String>> USE_PLANNER_RULES_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.USE_PLANNER_RULES, QueryOptionsUtils::parsePlannerRules, Set.of());
  public static final Prop<Integer> NUM_REPLICA_GROUPS_TO_QUERY_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.NUM_REPLICA_GROUPS_TO_QUERY, QueryOptionsUtils::parsePositiveInt, null);
  public static final Prop<List<Integer>> ORDERED_PREFERRED_POOLS_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.ORDERED_PREFERRED_POOLS, QueryOptionsUtils::parseOrderedPreferredPools,
          Collections.emptyList());
  public static final Prop<Integer> CURSOR_NUM_ROWS_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.CURSOR_NUM_ROWS, QueryOptionsUtils::parsePositiveInt, null);
  public static final Prop<Integer> MAX_EXECUTION_THREADS_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.MAX_EXECUTION_THREADS, QueryOptionsUtils::parsePositiveInt, null);
  public static final Prop<Integer> MIN_SEGMENT_GROUP_TRIM_SIZE_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.MIN_SEGMENT_GROUP_TRIM_SIZE, QueryOptionsUtils::parseUncheckedInt, null);
  public static final Prop<Integer> MIN_SERVER_GROUP_TRIM_SIZE_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.MIN_SERVER_GROUP_TRIM_SIZE, QueryOptionsUtils::parseUncheckedInt, null);
  public static final Prop<Integer> MIN_BROKER_GROUP_TRIM_SIZE_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.MIN_BROKER_GROUP_TRIM_SIZE, QueryOptionsUtils::parseUncheckedInt, null);
  public static final Prop<Integer> MSE_MIN_GROUP_TRIM_SIZE_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.MSE_MIN_GROUP_TRIM_SIZE, QueryOptionsUtils::parseUncheckedInt, null);
  public static final Prop<Integer> GROUP_TRIM_THRESHOLD_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.GROUP_TRIM_THRESHOLD, QueryOptionsUtils::parseUncheckedInt, null);
  public static final Prop<Integer> NUM_THREADS_EXTRACT_FINAL_RESULT_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.NUM_THREADS_EXTRACT_FINAL_RESULT, QueryOptionsUtils::parsePositiveInt, null);
  public static final Prop<Integer> CHUNK_SIZE_EXTRACT_FINAL_RESULT_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.CHUNK_SIZE_EXTRACT_FINAL_RESULT, QueryOptionsUtils::parsePositiveInt, null);
  public static final Prop<Integer> MULTI_STAGE_LEAF_LIMIT_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.MULTI_STAGE_LEAF_LIMIT, QueryOptionsUtils::parseNonNegativeInt, null);
  public static final Prop<Boolean> ERROR_ON_NUM_GROUPS_LIMIT_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.ERROR_ON_NUM_GROUPS_LIMIT, QueryOptionsUtils::parseBoolean, false);
  public static final Prop<Integer> NUM_GROUPS_LIMIT_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.NUM_GROUPS_LIMIT, QueryOptionsUtils::parsePositiveInt, null);
  public static final Prop<Integer> NUM_GROUPS_WARNING_LIMIT_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.NUM_GROUPS_WARNING_LIMIT, QueryOptionsUtils::parsePositiveInt, null);
  public static final Prop<Integer> MAX_INITIAL_RESULT_HOLDER_CAPACITY_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.MAX_INITIAL_RESULT_HOLDER_CAPACITY, QueryOptionsUtils::parsePositiveInt, null);
  public static final Prop<Integer> MSE_MAX_INITIAL_RESULT_HOLDER_CAPACITY_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.MSE_MAX_INITIAL_RESULT_HOLDER_CAPACITY, QueryOptionsUtils::parsePositiveInt,
          null);
  public static final Prop<Integer> MIN_INITIAL_INDEXED_TABLE_CAPACITY_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.MIN_INITIAL_INDEXED_TABLE_CAPACITY, QueryOptionsUtils::parsePositiveInt, null);
  public static final Prop<Integer> SORT_AGGREGATE_LIMIT_THRESHOLD_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.SORT_AGGREGATE_LIMIT_THRESHOLD, QueryOptionsUtils::parsePositiveInt, null);
  public static final Prop<Integer> SORT_AGGREGATE_SINGLE_THREADED_NUM_SEGMENTS_THRESHOLD_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.SORT_AGGREGATE_SINGLE_THREADED_NUM_SEGMENTS_THRESHOLD,
          QueryOptionsUtils::parsePositiveInt, null);
  public static final Prop<Integer> MAX_STREAMING_PENDING_BLOCKS_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.MAX_STREAMING_PENDING_BLOCKS, QueryOptionsUtils::parsePositiveInt, null);
  public static final Prop<Integer> MAX_ROWS_IN_JOIN_PROP =
      Prop.fromNullInteger()
          .withConfig(MultiStageQueryRunner.KEY_OF_MAX_ROWS_IN_JOIN, QueryOptionsUtils::parsePositiveInt)
          .withRuntimeMap(QueryOptionKey.MAX_ROWS_IN_JOIN, QueryOptionsUtils::parsePositiveInt);
  public static final Prop<JoinOverFlowMode> JOIN_OVERFLOW_MODE_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.JOIN_OVERFLOW_MODE, QueryOptionsUtils::parseJoinOverflowMode, null);
  public static final Prop<Integer> MAX_ROWS_IN_WINDOW_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.MAX_ROWS_IN_WINDOW, QueryOptionsUtils::parsePositiveInt, null);
  public static final Prop<WindowOverFlowMode> WINDOW_OVERFLOW_MODE_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.WINDOW_OVERFLOW_MODE, QueryOptionsUtils::parseWindowOverflowMode, null);
  public static final Prop<Integer> LITE_MODE_LEAF_STAGE_LIMIT_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.LITE_MODE_LEAF_STAGE_LIMIT, QueryOptionsUtils::parsePositiveInt, null);
  public static final Prop<Integer> LITE_MODE_LEAF_STAGE_FANOUT_ADJUSTED_LIMIT_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.LITE_MODE_LEAF_STAGE_FANOUT_ADJUSTED_LIMIT,
          QueryOptionsUtils::parsePositiveInt, null);
  public static final Prop<String> WORKLOAD_NAME_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.WORKLOAD_NAME,
          QueryOptionsUtils::parseString,
          CommonConstants.Accounting.DEFAULT_WORKLOAD_NAME);
  public static final Prop<Integer> REGEX_DICT_SIZE_THRESHOLD_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.REGEX_DICT_SIZE_THRESHOLD, QueryOptionsUtils::parseUncheckedInt, null);
  public static final Prop<Integer> SORT_EXCHANGE_COPY_THRESHOLD_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.SORT_EXCHANGE_COPY_THRESHOLD, QueryOptionsUtils::parseUncheckedInt, null);
  public static final Prop<Boolean> AND_SCAN_REORDERING_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.AND_SCAN_REORDERING, QueryOptionsUtils::parseBoolean, false);
  public static final Prop<Boolean> SKIP_UPSERT_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.SKIP_UPSERT, QueryOptionsUtils::parseBoolean, false);
  public static final Prop<Boolean> SKIP_UPSERT_VIEW_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.SKIP_UPSERT_VIEW, QueryOptionsUtils::parseBoolean, false);
  public static final Prop<Boolean> TRACE_RULE_PRODUCTIONS_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.TRACE_RULE_PRODUCTIONS, QueryOptionsUtils::parseBoolean, false);
  public static final Prop<Boolean> SCAN_STAR_TREE_NODES_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.SCAN_STAR_TREE_NODES, QueryOptionsUtils::parseBoolean, false);
  public static final Prop<Boolean> SKIP_STAR_TREE_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.USE_STAR_TREE, QueryOptionsUtils::parseSkipOnFalse, false);
  public static final Prop<Boolean> SKIP_SCAN_FILTER_REORDER_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.USE_SCAN_REORDER_OPTIMIZATION,
          QueryOptionsUtils::parseSkipOnFalse, false);
  public static final Prop<Boolean> COLLECT_GC_STATS_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.COLLECT_GC_STATS, QueryOptionsUtils::parseBoolean, false);
  public static final Prop<Boolean> EXPLAIN_PLAN_VERBOSE_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.EXPLAIN_PLAN_VERBOSE, QueryOptionsUtils::parseBoolean, false);
  public static final Prop<Boolean> USE_MULTISTAGE_ENGINE_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.USE_MULTISTAGE_ENGINE, QueryOptionsUtils::parseBoolean, false);
  public static final Prop<Boolean> GET_CURSOR_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.GET_CURSOR, QueryOptionsUtils::parseBoolean, false);
  public static final Prop<Boolean> ENABLE_NULL_HANDLING_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.ENABLE_NULL_HANDLING, QueryOptionsUtils::parseBoolean, false);
  public static final Prop<Boolean> SERVER_RETURN_FINAL_RESULT_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.SERVER_RETURN_FINAL_RESULT, QueryOptionsUtils::parseBoolean, false);
  public static final Prop<Boolean> SERVER_RETURN_FINAL_RESULT_KEY_UNPARTITIONED_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.SERVER_RETURN_FINAL_RESULT_KEY_UNPARTITIONED, QueryOptionsUtils::parseBoolean,
          false);
  public static final Prop<Boolean> FILTERED_AGGREGATIONS_SKIP_EMPTY_GROUPS_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.FILTERED_AGGREGATIONS_SKIP_EMPTY_GROUPS, QueryOptionsUtils::parseBoolean,
          false);
  public static final Prop<Boolean> USE_FIXED_REPLICA_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.USE_FIXED_REPLICA, QueryOptionsUtils::parseBoolean, null);
  public static final Prop<Boolean> SKIP_UNAVAILABLE_SERVERS_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.SKIP_UNAVAILABLE_SERVERS, QueryOptionsUtils::parseBoolean, false);
  public static final Prop<Boolean> IGNORE_MISSING_SEGMENTS_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.IGNORE_MISSING_SEGMENTS, QueryOptionsUtils::parseBoolean, false);
  public static final Prop<Boolean> IS_SECONDARY_WORKLOAD_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.IS_SECONDARY_WORKLOAD, QueryOptionsUtils::parseBoolean, false);
  public static final Prop<Boolean> ACCURATE_GROUP_BY_WITHOUT_ORDER_BY_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.ACCURATE_GROUP_BY_WITHOUT_ORDER_BY, QueryOptionsUtils::parseBoolean, false);
  public static final Prop<Boolean> USE_MSE_TO_FILL_EMPTY_RESPONSE_SCHEMA_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.USE_MSE_TO_FILL_EMPTY_RESPONSE_SCHEMA, QueryOptionsUtils::parseBoolean, null);
  public static final Prop<Boolean> INFER_INVALID_SEGMENT_PARTITION_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.INFER_INVALID_SEGMENT_PARTITION, QueryOptionsUtils::parseBoolean, false);
  public static final Prop<Boolean> INFER_REALTIME_SEGMENT_PARTITION_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.INFER_REALTIME_SEGMENT_PARTITION, QueryOptionsUtils::parseBoolean, false);
  public static final Prop<Boolean> USE_LEAF_SERVER_FOR_INTERMEDIATE_STAGE_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.USE_LEAF_SERVER_FOR_INTERMEDIATE_STAGE, QueryOptionsUtils::parseBoolean, null);
  public static final Prop<Boolean> USE_PHYSICAL_OPTIMIZER_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.USE_PHYSICAL_OPTIMIZER, QueryOptionsUtils::parseBoolean, null);
  public static final Prop<Boolean> ENABLE_MULTI_CLUSTER_ROUTING_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.ENABLE_MULTI_CLUSTER_ROUTING, QueryOptionsUtils::parseBoolean, null);
  public static final Prop<Boolean> USE_LITE_MODE_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.USE_LITE_MODE, QueryOptionsUtils::parseBoolean, null);
  public static final Prop<Boolean> USE_BROKER_PRUNING_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.USE_BROKER_PRUNING, QueryOptionsUtils::parseBoolean, null);
  public static final Prop<Boolean> RUN_IN_BROKER_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.RUN_IN_BROKER, QueryOptionsUtils::parseBoolean, null);
  public static final Prop<Boolean> ALLOW_REVERSE_ORDER_PROP =
      Prop.fromRuntimeMap(QueryOptionKey.ALLOW_REVERSE_ORDER,
          QueryOptionsUtils::parseBoolean, QueryOptionKey.DEFAULT_ALLOW_REVERSE_ORDER);

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

    CONFIG_RESOLVER = configResolver == null ? null : Map.copyOf(configResolver);
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

  private static Map<String, String> emptyIfNull(@Nullable Map<String, String> queryOptions) {
    return queryOptions != null ? queryOptions : Map.of();
  }

  @Nullable
  public static String resolveCaseInsensitiveKey(Object property) {
    if (property instanceof String) {
      return CONFIG_RESOLVER.get(((String) property).toLowerCase());
    }
    return null;
  }

  @Nullable
  @Deprecated
  public static Long getTimeoutMs(Map<String, String> queryOptions) {
    return TIMEOUT_MS_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static String getTableSampler(@Nullable Map<String, String> queryOptions) {
    return TABLE_SAMPLER_PROP.resolve(emptyIfNull(queryOptions), null);
  }

  @Nullable
  @Deprecated
  public static Long getExtraPassiveTimeoutMs(Map<String, String> queryOptions) {
    return EXTRA_PASSIVE_TIMEOUT_MS_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static Long getMaxServerResponseSizeBytes(Map<String, String> queryOptions) {
    return MAX_SERVER_RESPONSE_SIZE_BYTES_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static Long getMaxQueryResponseSizeBytes(Map<String, String> queryOptions) {
    return MAX_QUERY_RESPONSE_SIZE_BYTES_PROP.resolve(queryOptions, null);
  }

  public static boolean isAndScanReorderingEnabled(Map<String, String> queryOptions) {
    return AND_SCAN_REORDERING_PROP.resolve(queryOptions, null);
  }

  public static boolean isSkipUpsert(Map<String, String> queryOptions) {
    return SKIP_UPSERT_PROP.resolve(queryOptions, null);
  }

  public static boolean isSkipUpsertView(Map<String, String> queryOptions) {
    return SKIP_UPSERT_VIEW_PROP.resolve(queryOptions, null);
  }

  public static boolean isTraceRuleProductions(Map<String, String> queryOptions) {
    return TRACE_RULE_PRODUCTIONS_PROP.resolve(queryOptions, null);
  }

  @Deprecated
  public static long getUpsertViewFreshnessMs(Map<String, String> queryOptions) {
    return UPSERT_VIEW_FRESHNESS_MS_PROP.resolve(queryOptions, null);
  }

  public static boolean isScanStarTreeNodes(Map<String, String> queryOptions) {
    return SCAN_STAR_TREE_NODES_PROP.resolve(queryOptions, null);
  }

  public static boolean isSkipStarTree(Map<String, String> queryOptions) {
    return SKIP_STAR_TREE_PROP.resolve(queryOptions, null);
  }

  public static boolean isSkipScanFilterReorder(Map<String, String> queryOptions) {
    return SKIP_SCAN_FILTER_REORDER_PROP.resolve(queryOptions, null);
  }

  public static boolean isCollectGcStats(Map<String, String> queryOptions) {
    return COLLECT_GC_STATS_PROP.resolve(queryOptions, null);
  }

  @Deprecated
  public static String getQueryHash(Map<String, String> queryOptions) {
    return QUERY_HASH_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static Map<String, Set<FieldConfig.IndexType>> getSkipIndexes(Map<String, String> queryOptions) {
    return SKIP_INDEXES_PROP.resolve(queryOptions, null);
  }

  @Deprecated
  public static Set<String> getSkipPlannerRules(Map<String, String> queryOptions) {
    return SKIP_PLANNER_RULES_PROP.resolve(queryOptions, null);
  }

  @Deprecated
  public static Set<String> getUsePlannerRules(Map<String, String> queryOptions) {
    return USE_PLANNER_RULES_PROP.resolve(queryOptions, null);
  }

  @Nullable
  public static Boolean isUseFixedReplica(Map<String, String> queryOptions) {
    return USE_FIXED_REPLICA_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static Integer getNumReplicaGroupsToQuery(Map<String, String> queryOptions) {
    return NUM_REPLICA_GROUPS_TO_QUERY_PROP.resolve(queryOptions, null);
  }

  @Deprecated
  public static List<Integer> getOrderedPreferredPools(Map<String, String> queryOptions) {
    return ORDERED_PREFERRED_POOLS_PROP.resolve(queryOptions, null);
  }

  public static boolean isExplainPlanVerbose(Map<String, String> queryOptions) {
    return EXPLAIN_PLAN_VERBOSE_PROP.resolve(queryOptions, null);
  }

  public static boolean isUseMultistageEngine(Map<String, String> queryOptions) {
    return USE_MULTISTAGE_ENGINE_PROP.resolve(queryOptions, null);
  }

  public static boolean isGetCursor(Map<String, String> queryOptions) {
    return GET_CURSOR_PROP.resolve(queryOptions, null);
  }

  @Deprecated
  public static Integer getCursorNumRows(Map<String, String> queryOptions) {
    return CURSOR_NUM_ROWS_PROP.resolve(queryOptions, null);
  }

  public static Optional<Boolean> isExplainAskingServers(Map<String, String> queryOptions) {
    String value = queryOptions.get(QueryOptionKey.EXPLAIN_ASKING_SERVERS);
    if (value == null) {
      return Optional.empty();
    }
    return Optional.of(Boolean.parseBoolean(value));
  }

  @Nullable
  @Deprecated
  public static Integer getMaxExecutionThreads(Map<String, String> queryOptions) {
    return MAX_EXECUTION_THREADS_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static Integer getMinSegmentGroupTrimSize(Map<String, String> queryOptions) {
    return MIN_SEGMENT_GROUP_TRIM_SIZE_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static Integer getMinServerGroupTrimSize(Map<String, String> queryOptions) {
    return MIN_SERVER_GROUP_TRIM_SIZE_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static Integer getMinBrokerGroupTrimSize(Map<String, String> queryOptions) {
    return MIN_BROKER_GROUP_TRIM_SIZE_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static Integer getMSEMinGroupTrimSize(Map<String, String> queryOptions) {
    return MSE_MIN_GROUP_TRIM_SIZE_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static Integer getGroupTrimThreshold(Map<String, String> queryOptions) {
    return GROUP_TRIM_THRESHOLD_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static Integer getNumThreadsExtractFinalResult(Map<String, String> queryOptions) {
    return NUM_THREADS_EXTRACT_FINAL_RESULT_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static Integer getChunkSizeExtractFinalResult(Map<String, String> queryOptions) {
    return CHUNK_SIZE_EXTRACT_FINAL_RESULT_PROP.resolve(queryOptions, null);
  }

  public static boolean isNullHandlingEnabled(Map<String, String> queryOptions) {
    return ENABLE_NULL_HANDLING_PROP.resolve(queryOptions, null);
  }

  public static boolean isServerReturnFinalResult(Map<String, String> queryOptions) {
    return SERVER_RETURN_FINAL_RESULT_PROP.resolve(queryOptions, null);
  }

  public static boolean isServerReturnFinalResultKeyUnpartitioned(Map<String, String> queryOptions) {
    return SERVER_RETURN_FINAL_RESULT_KEY_UNPARTITIONED_PROP.resolve(queryOptions, null);
  }

  public static boolean isFilteredAggregationsSkipEmptyGroups(Map<String, String> queryOptions) {
    return FILTERED_AGGREGATIONS_SKIP_EMPTY_GROUPS_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static Integer getMultiStageLeafLimit(Map<String, String> queryOptions) {
    return MULTI_STAGE_LEAF_LIMIT_PROP.resolve(queryOptions, null);
  }

  @Deprecated
  public static boolean getErrorOnNumGroupsLimit(Map<String, String> queryOptions) {
    return ERROR_ON_NUM_GROUPS_LIMIT_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static Integer getNumGroupsLimit(Map<String, String> queryOptions) {
    return NUM_GROUPS_LIMIT_PROP.resolve(queryOptions, null);
  }

  @Deprecated
  public static Integer getNumGroupsWarningLimit(Map<String, String> queryOptions) {
    return NUM_GROUPS_WARNING_LIMIT_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static Integer getMaxInitialResultHolderCapacity(Map<String, String> queryOptions) {
    return MAX_INITIAL_RESULT_HOLDER_CAPACITY_PROP.resolve(queryOptions, null);
  }

  public static boolean optimizeMaxInitialResultHolderCapacityEnabled(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.OPTIMIZE_MAX_INITIAL_RESULT_HOLDER_CAPACITY));
  }

  @Nullable
  @Deprecated
  public static Integer getMSEMaxInitialResultHolderCapacity(Map<String, String> queryOptions) {
    return MSE_MAX_INITIAL_RESULT_HOLDER_CAPACITY_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static Integer getMinInitialIndexedTableCapacity(Map<String, String> queryOptions) {
    return MIN_INITIAL_INDEXED_TABLE_CAPACITY_PROP.resolve(queryOptions, null);
  }

  public static boolean shouldDropResults(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(CommonConstants.Broker.Request.QueryOptionKey.DROP_RESULTS));
  }

  @Nullable
  @Deprecated
  public static Integer getSortAggregateLimitThreshold(Map<String, String> queryOptions) {
    return SORT_AGGREGATE_LIMIT_THRESHOLD_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static Integer getSortAggregateSequentialCombineNumSegmentsThreshold(Map<String, String> queryOptions) {
    return SORT_AGGREGATE_SINGLE_THREADED_NUM_SEGMENTS_THRESHOLD_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static Integer getMaxStreamingPendingBlocks(Map<String, String> queryOptions) {
    return MAX_STREAMING_PENDING_BLOCKS_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static Integer getMaxRowsInJoin(Map<String, String> queryOptions) {
    return MAX_ROWS_IN_JOIN_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static JoinOverFlowMode getJoinOverflowMode(Map<String, String> queryOptions) {
    return JOIN_OVERFLOW_MODE_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static Integer getMaxRowsInWindow(Map<String, String> queryOptions) {
    return MAX_ROWS_IN_WINDOW_PROP.resolve(queryOptions, null);
  }

  @Nullable
  @Deprecated
  public static WindowOverFlowMode getWindowOverflowMode(Map<String, String> queryOptions) {
    return WINDOW_OVERFLOW_MODE_PROP.resolve(queryOptions, null);
  }

  public static boolean isSkipUnavailableServers(Map<String, String> queryOptions) {
    return SKIP_UNAVAILABLE_SERVERS_PROP.resolve(queryOptions, null);
  }

  public static boolean isIgnoreMissingSegments(Map<String, String> queryOptions) {
    return IGNORE_MISSING_SEGMENTS_PROP.resolve(queryOptions, null);
  }

  public static boolean isSecondaryWorkload(Map<String, String> queryOptions) {
    return IS_SECONDARY_WORKLOAD_PROP.resolve(queryOptions, null);
  }

  public static boolean isAccurateGroupByWithoutOrderBy(Map<String, String> queryOptions) {
    return ACCURATE_GROUP_BY_WITHOUT_ORDER_BY_PROP.resolve(queryOptions, null);
  }

  public static boolean isUseMSEToFillEmptySchema(Map<String, String> queryOptions, boolean defaultValue) {
    Boolean value = USE_MSE_TO_FILL_EMPTY_RESPONSE_SCHEMA_PROP.resolve(queryOptions, null);
    return value != null ? value : defaultValue;
  }

  public static boolean isInferInvalidSegmentPartition(Map<String, String> queryOptions) {
    return INFER_INVALID_SEGMENT_PARTITION_PROP.resolve(queryOptions, null);
  }

  public static boolean isInferRealtimeSegmentPartition(Map<String, String> queryOptions) {
    return INFER_REALTIME_SEGMENT_PARTITION_PROP.resolve(queryOptions, null);
  }

  public static boolean isUseLeafServerForIntermediateStage(Map<String, String> queryOptions, boolean defaultValue) {
    Boolean value = USE_LEAF_SERVER_FOR_INTERMEDIATE_STAGE_PROP.resolve(queryOptions, null);
    return value != null ? value : defaultValue;
  }

  public static boolean isUsePhysicalOptimizer(Map<String, String> queryOptions, boolean defaultValue) {
    Boolean value = USE_PHYSICAL_OPTIMIZER_PROP.resolve(queryOptions, null);
    return value != null ? value : defaultValue;
  }

  public static boolean isMultiClusterRoutingEnabled(Map<String, String> queryOptions, boolean defaultValue) {
    Boolean value = ENABLE_MULTI_CLUSTER_ROUTING_PROP.resolve(queryOptions, null);
    return value != null ? value : defaultValue;
  }

  public static boolean isUseLiteMode(Map<String, String> queryOptions, boolean defaultValue) {
    Boolean value = USE_LITE_MODE_PROP.resolve(queryOptions, null);
    return value != null ? value : defaultValue;
  }

  public static boolean isUseBrokerPruning(Map<String, String> queryOptions, boolean defaultValue) {
    Boolean value = USE_BROKER_PRUNING_PROP.resolve(queryOptions, null);
    return value != null ? value : defaultValue;
  }

  public static boolean isRunInBroker(Map<String, String> queryOptions, boolean defaultValue) {
    Boolean value = RUN_IN_BROKER_PROP.resolve(queryOptions, null);
    return value != null ? value : defaultValue;
  }

  @Deprecated
  public static Integer getLiteModeLeafStageLimit(Map<String, String> queryOptions, int defaultValue) {
    Integer value = LITE_MODE_LEAF_STAGE_LIMIT_PROP.resolve(queryOptions, null);
    return value != null ? value : defaultValue;
  }

  @Deprecated
  public static Integer getLiteModeLeafStageFanOutAdjustedLimit(Map<String, String> queryOptions, int defaultValue) {
    Integer value = LITE_MODE_LEAF_STAGE_FANOUT_ADJUSTED_LIMIT_PROP.resolve(queryOptions, null);
    return value != null ? value : defaultValue;
  }

  static Set<String> parsePlannerRules(String optionName, String optionValue) {
    // Example config: skipPlannerRules='FilterIntoJoin,FilterAggregateTranspose'
    String[] rules = StringUtils.split(optionValue, ',');
    return rules == null ? Set.of() : new HashSet<>(List.of(rules));
  }

  static Integer parsePositiveInt(String optionName, String optionValue) {
    return checkedParseIntPositive(optionName, optionValue);
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

  static Long parsePositiveLong(String optionName, String optionValue) {
    return checkedParseLongPositive(optionName, optionValue);
  }

  static Long parseNonNegativeLong(String optionName, String optionValue) {
    return checkedParseLong(optionName, optionValue, 0);
  }

  static Integer parseNonNegativeInt(String optionName, String optionValue) {
    return checkedParseIntNonNegative(optionName, optionValue);
  }

  static Integer parseUncheckedInt(String optionName, String optionValue) {
    return uncheckedParseInt(optionName, optionValue);
  }

  static Long parseLong(String optionName, String optionValue) {
    return Long.parseLong(optionValue);
  }

  static Boolean parseBoolean(String optionName, String optionValue) {
    return Boolean.parseBoolean(optionValue);
  }

  static Boolean parseSkipOnFalse(String optionName, String optionValue) {
    return "false".equalsIgnoreCase(optionValue);
  }

  static String parseString(String optionName, String optionValue) {
    return optionValue;
  }

  static JoinOverFlowMode parseJoinOverflowMode(String optionName, String optionValue) {
    return JoinOverFlowMode.valueOf(optionValue);
  }

  static WindowOverFlowMode parseWindowOverflowMode(String optionName, String optionValue) {
    return WindowOverFlowMode.valueOf(optionValue);
  }

  static List<Integer> parseOrderedPreferredPools(String optionName, String optionValue) {
    if (StringUtils.isEmpty(optionValue)) {
      return Collections.emptyList();
    }
    String[] pools = optionValue.split("\\|");
    List<Integer> preferredPools = new ArrayList<>(pools.length);
    for (String pool : pools) {
      preferredPools.add(Integer.parseInt(pool.trim()));
    }
    return preferredPools;
  }

  static Map<String, Set<FieldConfig.IndexType>> parseSkipIndexes(String optionName, String optionValue) {
    String[] perColumnIndexSkip = StringUtils.split(optionValue, '&');
    if (perColumnIndexSkip == null || perColumnIndexSkip.length == 0) {
      return Collections.emptyMap();
    }
    Map<String, Set<FieldConfig.IndexType>> skipIndexes = new HashMap<>();
    for (String columnConf : perColumnIndexSkip) {
      String[] conf = StringUtils.split(columnConf, '=');
      if (conf.length != 2) {
        throw new RuntimeException("Invalid format for " + optionName
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

  @Deprecated
  public static String getWorkloadName(Map<String, String> queryOptions) {
    return WORKLOAD_NAME_PROP.resolve(queryOptions, null);
  }

  public static boolean isReverseOrderAllowed(Map<String, String> queryOptions) {
    return ALLOW_REVERSE_ORDER_PROP.resolve(queryOptions, null);
  }

  /// When evaluating REGEXP_LIKE predicate on a dictionary encoded column:
  /// - If dictionary size is smaller than this threshold, scan the dictionary to get the matching dictionary ids
  ///   first, where inverted index can be applied if exists
  /// - Otherwise, read dictionary while scanning the forward index, cache the matching/unmatching dictionary ids
  ///   during the scan
  @Nullable
  @Deprecated
  public static Integer getRegexDictSizeThreshold(Map<String, String> queryOptions) {
    return REGEX_DICT_SIZE_THRESHOLD_PROP.resolve(queryOptions, null);
  }

  @Deprecated
  public static int getSortExchangeCopyThreshold(Map<String, String> options, int i) {
    Integer value = SORT_EXCHANGE_COPY_THRESHOLD_PROP.resolve(options, null);
    return value != null ? value : i;
  }
}
