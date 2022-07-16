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
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;


/**
 * Utils to parse query options.
 */
public class QueryOptionsUtils {
  private QueryOptionsUtils() {
  }

  @Nullable
  public static Long getTimeoutMs(Map<String, String> queryOptions) {
    String timeoutMsString = queryOptions.get(Request.QueryOptionKey.TIMEOUT_MS);
    if (timeoutMsString != null) {
      long timeoutMs = Long.parseLong(timeoutMsString);
      Preconditions.checkState(timeoutMs > 0, "Query timeout must be positive, got: %s", timeoutMs);
      return timeoutMs;
    } else {
      return null;
    }
  }

  public static boolean isSkipUpsert(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(Request.QueryOptionKey.SKIP_UPSERT));
  }

  public static boolean isSkipStarTree(Map<String, String> queryOptions) {
    return "false".equalsIgnoreCase(queryOptions.get(Request.QueryOptionKey.USE_STAR_TREE));
  }

  public static boolean isRoutingForceHLC(Map<String, String> queryOptions) {
    String routingOptions = queryOptions.get(Request.QueryOptionKey.ROUTING_OPTIONS);
    return routingOptions != null && routingOptions.toUpperCase().contains(Request.QueryOptionValue.ROUTING_FORCE_HLC);
  }

  public static boolean isSkipScanFilterReorder(Map<String, String> queryOptions) {
    return "false".equalsIgnoreCase(queryOptions.get(Request.QueryOptionKey.USE_SCAN_REORDER_OPTIMIZATION));
  }

  @Nullable
  public static Integer getNumReplicaGroupsToQuery(Map<String, String> queryOptions) {
    String numReplicaGroupsToQuery = queryOptions.get(Request.QueryOptionKey.NUM_REPLICA_GROUPS_TO_QUERY);
    return numReplicaGroupsToQuery != null ? Integer.parseInt(numReplicaGroupsToQuery) : null;
  }

  public static boolean isExplainPlanVerbose(Map<String, String> queryOptions) {
    return Boolean.parseBoolean(queryOptions.get(Request.QueryOptionKey.EXPLAIN_PLAN_VERBOSE));
  }

  @Nullable
  public static Integer getMaxExecutionThreads(Map<String, String> queryOptions) {
    String maxExecutionThreadsString = queryOptions.get(Request.QueryOptionKey.MAX_EXECUTION_THREADS);
    return maxExecutionThreadsString != null ? Integer.parseInt(maxExecutionThreadsString) : null;
  }

  @Nullable
  public static Integer getMinSegmentGroupTrimSize(Map<String, String> queryOptions) {
    String minSegmentGroupTrimSizeString = queryOptions.get(Request.QueryOptionKey.MIN_SEGMENT_GROUP_TRIM_SIZE);
    return minSegmentGroupTrimSizeString != null ? Integer.parseInt(minSegmentGroupTrimSizeString) : null;
  }

  @Nullable
  public static Integer getMinServerGroupTrimSize(Map<String, String> queryOptions) {
    String minServerGroupTrimSizeString = queryOptions.get(Request.QueryOptionKey.MIN_SERVER_GROUP_TRIM_SIZE);
    return minServerGroupTrimSizeString != null ? Integer.parseInt(minServerGroupTrimSizeString) : null;
  }

  public static boolean isNullHandlingEnabled(Map<String, String> queryOptions) {
    boolean nullHandlingEnabled = Boolean.parseBoolean(queryOptions.get(Request.QueryOptionKey.ENABLE_NULL_HANDLING));
    if (nullHandlingEnabled) {
      Preconditions.checkState(DataTableFactory.getDataTableVersion() >= DataTableFactory.VERSION_4,
          "Null handling cannot be enabled for data table version smaller than 4");
    }
    return nullHandlingEnabled;
  }
}
