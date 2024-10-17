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
package org.apache.pinot.common.metrics;

import org.apache.pinot.common.Utils;


/**
 * Enumeration containing all the metrics exposed by the Pinot broker.
 */
public enum BrokerMeter implements AbstractMetrics.Meter {
  UNCAUGHT_GET_EXCEPTIONS("exceptions", true),
  UNCAUGHT_POST_EXCEPTIONS("exceptions", true),
  HEALTHCHECK_BAD_CALLS("healthcheck", true),
  HEALTHCHECK_OK_CALLS("healthcheck", true),
  /**
   * Number of queries executed.
   * <p>
   * At this moment this counter does not include queries executed in multi-stage mode.
   */
  QUERIES("queries", false),
  /**
   * Number of single-stage queries that have been started.
   * <p>
   * Unlike {@link #QUERIES}, this metric is global and not attached to a particular table.
   * That means it can be used to know how many single-stage queries have been started in total.
   */
  QUERIES_GLOBAL("queries", true),
  /**
   * Number of multi-stage queries that have been started.
   * <p>
   * Unlike {@link #MULTI_STAGE_QUERIES}, this metric is global and not attached to a particular table.
   * That means it can be used to know how many multi-stage queries have been started in total.
   */
  MULTI_STAGE_QUERIES_GLOBAL("queries", true),
  /**
   * Number of multi-stage queries that have been started touched a given table.
   * <p>
   * In case the query touch multiple tables (ie using joins)1, this metric will be incremented for each table, so the
   * sum of this metric across all tables should be greater or equal than {@link #MULTI_STAGE_QUERIES_GLOBAL}.
   */
  MULTI_STAGE_QUERIES("queries", false),
  /**
   * Number of single-stage queries executed that would not have successfully run on the multi-stage query engine as is.
   */
  SINGLE_STAGE_QUERIES_INVALID_MULTI_STAGE("queries", true),
  /**
   * Number of time-series queries. This metric is not grouped on the table name.
   */
  TIME_SERIES_GLOBAL_QUERIES("queries", true),
  /**
   * Number of time-series queries that failed. This metric is not grouped on the table name.
   */
  TIME_SERIES_GLOBAL_QUERIES_FAILED("queries", true),
  // These metrics track the exceptions caught during query execution in broker side.
  // Query rejected by Jersey thread pool executor
  QUERY_REJECTED_EXCEPTIONS("exceptions", true),
  // Query compile phase.
  REQUEST_COMPILATION_EXCEPTIONS("exceptions", true),
  // Get resource phase.
  RESOURCE_MISSING_EXCEPTIONS("exceptions", true),
  // Query validation phase.
  QUERY_VALIDATION_EXCEPTIONS("exceptions", false),
  // Query validation phase.
  UNKNOWN_COLUMN_EXCEPTIONS("exceptions", false),
  // Queries preempted by accountant
  QUERIES_KILLED("query", true),
  // Scatter phase.
  NO_SERVER_FOUND_EXCEPTIONS("exceptions", false),
  REQUEST_TIMEOUT_BEFORE_SCATTERED_EXCEPTIONS("exceptions", false),
  REQUEST_CHANNEL_LOCK_TIMEOUT_EXCEPTIONS("exceptions", false),
  REQUEST_SEND_EXCEPTIONS("exceptions", false),
  // Gather phase.
  RESPONSE_FETCH_EXCEPTIONS("exceptions", false),
  // Response deserialize phase.
  DATA_TABLE_DESERIALIZATION_EXCEPTIONS("exceptions", false),
  // Reduce responses phase.
  RESPONSE_MERGE_EXCEPTIONS("exceptions", false),
  HEAP_CRITICAL_LEVEL_EXCEEDED("count", true),
  HEAP_PANIC_LEVEL_EXCEEDED("count", true),

  // These metrics track the number of bad broker responses.
  // This metric track the number of broker responses with processing exceptions inside.
  // The processing exceptions could be caught from both server side and broker side.
  BROKER_RESPONSES_WITH_PROCESSING_EXCEPTIONS("badResponses", false),
  // This metric tracks the number of broker responses with unavailable segments.
  BROKER_RESPONSES_WITH_UNAVAILABLE_SEGMENTS("badResponses", false),
  // This metric track the number of broker responses with not all servers responded.
  // (numServersQueried > numServersResponded)
  BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED("badResponses", false),

  SECONDARY_WORKLOAD_BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED("badResponses", false),

  BROKER_RESPONSES_WITH_TIMEOUTS("badResponses", false),

  SECONDARY_WORKLOAD_BROKER_RESPONSES_WITH_TIMEOUTS("badResponses", false),

  // This metric track the number of broker responses with number of groups limit reached (potential bad responses).
  BROKER_RESPONSES_WITH_NUM_GROUPS_LIMIT_REACHED("badResponses", false),

  // These metrics track the cost of the query.
  DOCUMENTS_SCANNED("documents", false),
  ENTRIES_SCANNED_IN_FILTER("documents", false),
  ENTRIES_SCANNED_POST_FILTER("documents", false),

  NUM_RESIZES("numResizes", false),

  HELIX_ZOOKEEPER_RECONNECTS("reconnects", true),

  REQUEST_DROPPED_DUE_TO_ACCESS_ERROR("requestsDropped", false),

  GROUP_BY_SIZE("queries", false),
  TOTAL_SERVER_RESPONSE_SIZE("queries", false),

  QUERY_QUOTA_EXCEEDED("exceptions", false),

  // tracks a case a segment is not hosted by any server
  // this is different from NO_SERVER_FOUND_EXCEPTIONS which tracks unavailability across all segments
  NO_SERVING_HOST_FOR_SEGMENT("badResponses", false),

  // Track the case where selected server is missing in RoutingManager
  SERVER_MISSING_FOR_ROUTING("badResponses", false),

  // Netty connection metrics
  NETTY_CONNECTION_REQUESTS_SENT("nettyConnection", true),
  NETTY_CONNECTION_BYTES_SENT("nettyConnection", true),
  NETTY_CONNECTION_BYTES_RECEIVED("nettyConnection", true),

  PROACTIVE_CLUSTER_CHANGE_CHECK("proactiveClusterChangeCheck", true),
  DIRECT_MEMORY_OOM("directMemoryOOMCount", true),

  /**
   * How many queries with joins have been executed.
   * <p>
   * For each query with at least one join, this meter is increased exactly once.
   */
  QUERIES_WITH_JOINS("queries", true),
  /**
   * How many joins have been executed.
   * <p>
   * For each query with at least one join, this meter is increased as many times as joins in the query.
   */
  JOIN_COUNT("queries", true),
  /**
   * How many queries with window functions have been executed.
   * <p>
   * For each query with at least one window function, this meter is increased exactly once.
   */
  QUERIES_WITH_WINDOW("queries", true),
  /**
   * How many window functions have been executed.
   * <p>
   * For each query with at least one window function, this meter is increased as many times as window functions in the
   * query.
   */
  WINDOW_COUNT("queries", true),

  /**
   * Number of queries executed with cursors. This count includes queries that use SSE and MSE
   */
  CURSOR_QUERIES_GLOBAL("queries", true),

  CURSOR_WRITE_EXCEPTION("exceptions", true),

  CURSOR_READ_EXCEPTION("exceptions", true),

  CURSOR_RESULT_STORE_SIZE("bytes", true);

  private final String _brokerMeterName;
  private final String _unit;
  private final boolean _global;

  BrokerMeter(String unit, boolean global) {
    _unit = unit;
    _global = global;
    _brokerMeterName = Utils.toCamelCase(name().toLowerCase());
  }

  @Override
  public String getMeterName() {
    return _brokerMeterName;
  }

  @Override
  public String getUnit() {
    return _unit;
  }

  /**
   * Returns true if the metric is global (not attached to a particular resource)
   *
   * @return true if the metric is global
   */
  @Override
  public boolean isGlobal() {
    return _global;
  }
}
