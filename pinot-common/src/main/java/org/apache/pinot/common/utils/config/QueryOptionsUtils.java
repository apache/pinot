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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.JoinOverFlowMode;


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
    if (timeoutMsString != null) {
      long timeoutMs = Long.parseLong(timeoutMsString);
      Preconditions.checkState(timeoutMs > 0, "Query timeout must be positive, got: %s", timeoutMs);
      return timeoutMs;
    } else {
      return null;
    }
  }

  @Nullable
  public static Long getMaxServerResponseSizeBytes(Map<String, String> queryOptions) {
    String responseSize = queryOptions.get(QueryOptionKey.MAX_SERVER_RESPONSE_SIZE_BYTES);
    if (responseSize != null) {
      long maxSize = Long.parseLong(responseSize);
      Preconditions.checkState(maxSize > 0, "maxServerResponseSize must be positive. got %s", maxSize);
      return maxSize;
    }

    return null;
  }

  public static boolean isAndScanReorderingEnabled(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.AND_SCAN_REORDERING));
  }

  public static boolean isSkipUpsert(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.SKIP_UPSERT));
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
  public static Integer getNumReplicaGroupsToQuery(Map<String, String> queryOptions) {
    String numReplicaGroupsToQuery = queryOptions.get(QueryOptionKey.NUM_REPLICA_GROUPS_TO_QUERY);
    return numReplicaGroupsToQuery != null ? Integer.parseInt(numReplicaGroupsToQuery) : null;
  }

  public static boolean isExplainPlanVerbose(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.EXPLAIN_PLAN_VERBOSE));
  }

  @Nullable
  public static Integer getMaxExecutionThreads(Map<String, String> queryOptions) {
    String maxExecutionThreadsString = queryOptions.get(QueryOptionKey.MAX_EXECUTION_THREADS);
    return maxExecutionThreadsString != null ? Integer.parseInt(maxExecutionThreadsString) : null;
  }

  @Nullable
  public static Integer getMinSegmentGroupTrimSize(Map<String, String> queryOptions) {
    String minSegmentGroupTrimSizeString = queryOptions.get(QueryOptionKey.MIN_SEGMENT_GROUP_TRIM_SIZE);
    return minSegmentGroupTrimSizeString != null ? Integer.parseInt(minSegmentGroupTrimSizeString) : null;
  }

  @Nullable
  public static Integer getMinServerGroupTrimSize(Map<String, String> queryOptions) {
    String minServerGroupTrimSizeString = queryOptions.get(QueryOptionKey.MIN_SERVER_GROUP_TRIM_SIZE);
    return minServerGroupTrimSizeString != null ? Integer.parseInt(minServerGroupTrimSizeString) : null;
  }

  public static boolean isNullHandlingEnabled(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.ENABLE_NULL_HANDLING));
  }

  public static boolean isServerReturnFinalResult(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.SERVER_RETURN_FINAL_RESULT));
  }

  @Nullable
  public static String getOrderByAlgorithm(Map<String, String> queryOptions) {
    return queryOptions.get(QueryOptionKey.ORDER_BY_ALGORITHM);
  }

  @Nullable
  public static Integer getMultiStageLeafLimit(Map<String, String> queryOptions) {
    String maxLeafLimitStr = queryOptions.get(QueryOptionKey.MULTI_STAGE_LEAF_LIMIT);
    return maxLeafLimitStr != null ? Integer.parseInt(maxLeafLimitStr) : null;
  }

  @Nullable
  public static Integer getNumGroupsLimit(Map<String, String> queryOptions) {
    String maxNumGroupLimit = queryOptions.get(QueryOptionKey.NUM_GROUPS_LIMIT);
    return maxNumGroupLimit != null ? Integer.parseInt(maxNumGroupLimit) : null;
  }

  @Nullable
  public static Integer getMaxInitialResultHolderCapacity(Map<String, String> queryOptions) {
    String maxInitResultCap = queryOptions.get(QueryOptionKey.MAX_INITIAL_RESULT_HOLDER_CAPACITY);
    return maxInitResultCap != null ? Integer.parseInt(maxInitResultCap) : null;
  }

  @Nullable
  public static Integer getGroupTrimThreshold(Map<String, String> queryOptions) {
    String groupByTrimThreshold = queryOptions.get(QueryOptionKey.GROUP_TRIM_THRESHOLD);
    return groupByTrimThreshold != null ? Integer.parseInt(groupByTrimThreshold) : null;
  }

  public static boolean shouldDropResults(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(CommonConstants.Broker.Request.QueryOptionKey.DROP_RESULTS));
  }

  @Nullable
  public static Integer getMaxStreamingPendingBlocks(Map<String, String> queryOptions) {
    String maxStreamingPendingBlocks = queryOptions.get(QueryOptionKey.MAX_STREAMING_PENDING_BLOCKS);
    return maxStreamingPendingBlocks != null ? Integer.parseInt(maxStreamingPendingBlocks) : null;
  }

  @Nullable
  public static Integer getMaxRowsInJoin(Map<String, String> queryOptions) {
    String maxRowsInJoin = queryOptions.get(QueryOptionKey.MAX_ROWS_IN_JOIN);
    return maxRowsInJoin != null ? Integer.parseInt(maxRowsInJoin) : null;
  }

  @Nullable
  public static JoinOverFlowMode getJoinOverflowMode(Map<String, String> queryOptions) {
    String joinOverflowModeStr = queryOptions.get(QueryOptionKey.JOIN_OVERFLOW_MODE);
    return joinOverflowModeStr != null ? JoinOverFlowMode.valueOf(joinOverflowModeStr) : null;
  }
}
