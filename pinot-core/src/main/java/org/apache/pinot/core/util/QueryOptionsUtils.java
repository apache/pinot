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
package org.apache.pinot.core.util;

import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionValue;


/**
 * Utils to parse query options.
 */
public class QueryOptionsUtils {
  private QueryOptionsUtils() {
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

  public static boolean isAndScanReorderingEnabled(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.AND_SCAN_REORDERING));
  }

  public static boolean isSkipUpsert(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(QueryOptionKey.SKIP_UPSERT));
  }

  public static boolean isSkipStarTree(Map<String, String> queryOptions) {
    return "false".equalsIgnoreCase(queryOptions.get(QueryOptionKey.USE_STAR_TREE));
  }

  public static boolean isRoutingForceHLC(Map<String, String> queryOptions) {
    String routingOptions = queryOptions.get(QueryOptionKey.ROUTING_OPTIONS);
    return routingOptions != null && routingOptions.toUpperCase().contains(QueryOptionValue.ROUTING_FORCE_HLC);
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
}
