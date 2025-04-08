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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.Utils;
import org.apache.pinot.spi.exception.QueryErrorCode;

/**
 * Class containing all the metrics exposed by the Pinot broker.
 * This is implemented as a class rather than an enum to allow for dynamic addition of metrics for all QueryErrorCodes.
 */
public class BrokerMeter implements AbstractMetrics.Meter {
  private static final List<BrokerMeter> BROKER_METERS = new ArrayList<>();

  public static final BrokerMeter UNCAUGHT_GET_EXCEPTIONS = create("UNCAUGHT_GET_EXCEPTIONS", "exceptions", true);
  public static final BrokerMeter UNCAUGHT_POST_EXCEPTIONS = create("UNCAUGHT_POST_EXCEPTIONS", "exceptions", true);
  public static final BrokerMeter WEB_APPLICATION_EXCEPTIONS = create("WEB_APPLICATION_EXCEPTIONS", "exceptions", true);
  public static final BrokerMeter HEALTHCHECK_BAD_CALLS = create("HEALTHCHECK_BAD_CALLS", "healthcheck", true);
  public static final BrokerMeter HEALTHCHECK_OK_CALLS = create("HEALTHCHECK_OK_CALLS", "healthcheck", true);
  /**
   * Number of queries executed.
   * <p>
   * At this moment this counter does not include queries executed in multi-stage mode.
   */
  public static final BrokerMeter QUERIES = create("QUERIES", "queries", false);
  /**
   * Number of single-stage queries that have been started.
   * <p>
   * Unlike {@link #QUERIES}, this metric is global and not attached to a particular table.
   * That means it can be used to know how many single-stage queries have been started in total.
   */
  public static final BrokerMeter QUERIES_GLOBAL = create("QUERIES_GLOBAL", "queries", true);
  /**
   * Number of multi-stage queries that have been started.
   * <p>
   * Unlike {@link #MULTI_STAGE_QUERIES}, this metric is global and not attached to a particular table.
   * That means it can be used to know how many multi-stage queries have been started in total.
   */
  public static final BrokerMeter MULTI_STAGE_QUERIES_GLOBAL = create("MULTI_STAGE_QUERIES_GLOBAL", "queries", true);
  /**
   * Number of multi-stage queries that have been started touched a given table.
   * <p>
   * In case the query touch multiple tables (ie using joins)1, this metric will be incremented for each table, so the
   * sum of this metric across all tables should be greater or equal than {@link #MULTI_STAGE_QUERIES_GLOBAL}.
   */
  public static final BrokerMeter MULTI_STAGE_QUERIES = create("MULTI_STAGE_QUERIES", "queries", false);
  /**
   * Number of single-stage queries executed that would not have successfully run on the multi-stage query engine as is.
   */
  public static final BrokerMeter SINGLE_STAGE_QUERIES_INVALID_MULTI_STAGE = create(
      "SINGLE_STAGE_QUERIES_INVALID_MULTI_STAGE", "queries", true);
  /**
   * Number of time-series queries. This metric is not grouped on the table name.
   */
  public static final BrokerMeter TIME_SERIES_GLOBAL_QUERIES = create("TIME_SERIES_GLOBAL_QUERIES", "queries", true);
  /**
   * Number of time-series queries that failed. This metric is not grouped on the table name.
   */
  public static final BrokerMeter TIME_SERIES_GLOBAL_QUERIES_FAILED = create(
      "TIME_SERIES_GLOBAL_QUERIES_FAILED", "queries", true);
  // These metrics track the exceptions caught during query execution in broker side.
  // Query rejected by Jersey thread pool executor
  public static final BrokerMeter QUERY_REJECTED_EXCEPTIONS = create("QUERY_REJECTED_EXCEPTIONS", "exceptions", true);
  // Query compile phase.
  public static final BrokerMeter REQUEST_COMPILATION_EXCEPTIONS = create(
      "REQUEST_COMPILATION_EXCEPTIONS", "exceptions", true);
  // Get resource phase.
  public static final BrokerMeter RESOURCE_MISSING_EXCEPTIONS = create(
      "RESOURCE_MISSING_EXCEPTIONS", "exceptions", true);
  // Query validation phase.
  public static final BrokerMeter QUERY_VALIDATION_EXCEPTIONS = create(
      "QUERY_VALIDATION_EXCEPTIONS", "exceptions", false);
  // Query validation phase.
  public static final BrokerMeter UNKNOWN_COLUMN_EXCEPTIONS = create("UNKNOWN_COLUMN_EXCEPTIONS", "exceptions", false);
  // Queries preempted by accountant
  public static final BrokerMeter QUERIES_KILLED = create("QUERIES_KILLED", "query", true);
  // Scatter phase.
  public static final BrokerMeter NO_SERVER_FOUND_EXCEPTIONS = create(
      "NO_SERVER_FOUND_EXCEPTIONS", "exceptions", false);
  public static final BrokerMeter REQUEST_TIMEOUT_BEFORE_SCATTERED_EXCEPTIONS = create(
      "REQUEST_TIMEOUT_BEFORE_SCATTERED_EXCEPTIONS", "exceptions", false);
  public static final BrokerMeter REQUEST_CHANNEL_LOCK_TIMEOUT_EXCEPTIONS = create(
      "REQUEST_CHANNEL_LOCK_TIMEOUT_EXCEPTIONS", "exceptions", false);
  public static final BrokerMeter REQUEST_SEND_EXCEPTIONS = create("REQUEST_SEND_EXCEPTIONS", "exceptions", false);
  // Gather phase.
  public static final BrokerMeter RESPONSE_FETCH_EXCEPTIONS = create("RESPONSE_FETCH_EXCEPTIONS", "exceptions", false);
  // Response deserialization phase.
  public static final BrokerMeter DATA_TABLE_DESERIALIZATION_EXCEPTIONS = create(
      "DATA_TABLE_DESERIALIZATION_EXCEPTIONS", "exceptions", false);
  // Reduce responses phase.
  public static final BrokerMeter RESPONSE_MERGE_EXCEPTIONS = create("RESPONSE_MERGE_EXCEPTIONS", "exceptions", false);
  public static final BrokerMeter HEAP_CRITICAL_LEVEL_EXCEEDED = create("HEAP_CRITICAL_LEVEL_EXCEEDED", "count", true);
  public static final BrokerMeter HEAP_PANIC_LEVEL_EXCEEDED = create("HEAP_PANIC_LEVEL_EXCEEDED", "count", true);

  // These metrics track the number of bad broker responses.
  // This metric track the number of broker responses with processing exceptions inside.
  // The processing exceptions could be caught from both server side and broker side.
  public static final BrokerMeter BROKER_RESPONSES_WITH_PROCESSING_EXCEPTIONS = create(
      "BROKER_RESPONSES_WITH_PROCESSING_EXCEPTIONS", "badResponses", false);
  // This metric tracks the number of broker responses with unavailable segments.
  public static final BrokerMeter BROKER_RESPONSES_WITH_UNAVAILABLE_SEGMENTS = create(
      "BROKER_RESPONSES_WITH_UNAVAILABLE_SEGMENTS", "badResponses", false);
  // This metric track the number of broker responses with not all servers responded.
  // (numServersQueried > numServersResponded)
  public static final BrokerMeter BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED = create(
      "BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED", "badResponses", false);

  public static final BrokerMeter SECONDARY_WORKLOAD_BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED = create(
      "SECONDARY_WORKLOAD_BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED", "badResponses", false);

  public static final BrokerMeter BROKER_RESPONSES_WITH_TIMEOUTS = create(
      "BROKER_RESPONSES_WITH_TIMEOUTS", "badResponses", false);

  public static final BrokerMeter SECONDARY_WORKLOAD_BROKER_RESPONSES_WITH_TIMEOUTS = create(
      "SECONDARY_WORKLOAD_BROKER_RESPONSES_WITH_TIMEOUTS", "badResponses", false);

  // This metric track the number of broker responses with number of groups limit reached (potential bad responses).
  public static final BrokerMeter BROKER_RESPONSES_WITH_NUM_GROUPS_LIMIT_REACHED = create(
      "BROKER_RESPONSES_WITH_NUM_GROUPS_LIMIT_REACHED", "badResponses", false);

  // These metrics track the cost of the query.
  public static final BrokerMeter DOCUMENTS_SCANNED = create(
      "DOCUMENTS_SCANNED", "documents", false);
  public static final BrokerMeter ENTRIES_SCANNED_IN_FILTER = create("ENTRIES_SCANNED_IN_FILTER", "documents", false);
  public static final BrokerMeter ENTRIES_SCANNED_POST_FILTER = create(
      "ENTRIES_SCANNED_POST_FILTER", "documents", false);

  public static final BrokerMeter NUM_RESIZES = create("NUM_RESIZES", "numResizes", false);

  public static final BrokerMeter HELIX_ZOOKEEPER_RECONNECTS = create("HELIX_ZOOKEEPER_RECONNECTS", "reconnects", true);

  public static final BrokerMeter REQUEST_DROPPED_DUE_TO_ACCESS_ERROR = create(
      "REQUEST_DROPPED_DUE_TO_ACCESS_ERROR", "requestsDropped", false);

  public static final BrokerMeter GROUP_BY_SIZE = create("GROUP_BY_SIZE", "queries", false);
  public static final BrokerMeter TOTAL_SERVER_RESPONSE_SIZE = create("TOTAL_SERVER_RESPONSE_SIZE", "queries", false);

  public static final BrokerMeter QUERY_QUOTA_EXCEEDED = create("QUERY_QUOTA_EXCEEDED", "exceptions", false);

  // tracks a case a segment is not hosted by any server
  // this is different from NO_SERVER_FOUND_EXCEPTIONS which tracks unavailability across all segments
  public static final BrokerMeter NO_SERVING_HOST_FOR_SEGMENT = create(
      "NO_SERVING_HOST_FOR_SEGMENT", "badResponses", false);

  // Track the case where selected server is missing in RoutingManager
  public static final BrokerMeter SERVER_MISSING_FOR_ROUTING = create(
      "SERVER_MISSING_FOR_ROUTING", "badResponses", false);

  // Netty connection metrics
  public static final BrokerMeter NETTY_CONNECTION_REQUESTS_SENT = create(
      "NETTY_CONNECTION_REQUESTS_SENT", "nettyConnection", true);
  public static final BrokerMeter NETTY_CONNECTION_BYTES_SENT = create(
      "NETTY_CONNECTION_BYTES_SENT", "nettyConnection", true);
  public static final BrokerMeter NETTY_CONNECTION_BYTES_RECEIVED = create(
      "NETTY_CONNECTION_BYTES_RECEIVED", "nettyConnection", true);

  public static final BrokerMeter PROACTIVE_CLUSTER_CHANGE_CHECK = create(
      "PROACTIVE_CLUSTER_CHANGE_CHECK", "proactiveClusterChangeCheck", true);
  public static final BrokerMeter DIRECT_MEMORY_OOM = create("DIRECT_MEMORY_OOM", "directMemoryOOMCount", true);

  /**
   * How many queries with joins have been executed.
   * <p>
   * For each query with at least one join, this meter is increased exactly once.
   */
  public static final BrokerMeter QUERIES_WITH_JOINS = create("QUERIES_WITH_JOINS", "queries", true);
  /**
   * How many joins have been executed.
   * <p>
   * For each query with at least one join, this meter is increased as many times as joins in the query.
   */
  public static final BrokerMeter JOIN_COUNT = create("JOIN_COUNT", "queries", true);
  /**
   * How many queries with window functions have been executed.
   * <p>
   * For each query with at least one window function, this meter is increased exactly once.
   */
  public static final BrokerMeter QUERIES_WITH_WINDOW = create("QUERIES_WITH_WINDOW", "queries", true);
  /**
   * How many window functions have been executed.
   * <p>
   * For each query with at least one window function, this meter is increased as many times as window functions in the
   * query.
   */
  public static final BrokerMeter WINDOW_COUNT = create("WINDOW_COUNT", "queries", true);

  /**
   * Number of queries executed with cursors. This count includes queries that use SSE and MSE
   */
  public static final BrokerMeter CURSOR_QUERIES_GLOBAL = create("CURSOR_QUERIES_GLOBAL", "queries", true);
  /**
   * Number of exceptions when writing a response to the response store
   */
  public static final BrokerMeter CURSOR_WRITE_EXCEPTION = create("CURSOR_WRITE_EXCEPTION", "exceptions", true);
  /**
   * Number of exceptions when reading a response and result table from the response store
   */
  public static final BrokerMeter CURSOR_READ_EXCEPTION = create("CURSOR_READ_EXCEPTION", "exceptions", true);
  /**
   * The number of bytes stored in the response store. Only the size of the result table is tracked.
   */
  public static final BrokerMeter CURSOR_RESPONSE_STORE_SIZE = create("CURSOR_RESPONSE_STORE_SIZE", "bytes", true);

  // GRPC related metrics
  public static final BrokerMeter GRPC_QUERIES = create("GRPC_QUERIES", "grpcQueries", true);
  public static final BrokerMeter GRPC_QUERY_EXCEPTIONS = create("GRPC_QUERY_EXCEPTIONS", "grpcExceptions", true);
  public static final BrokerMeter GRPC_BYTES_RECEIVED = create("GRPC_BYTES_RECEIVED", "grpcBytesReceived", true);
  public static final BrokerMeter GRPC_BYTES_SENT = create("GRPC_BYTES_SENT", "grpcBytesSent", true);
  public static final BrokerMeter GRPC_TRANSPORT_READY = create("GRPC_TRANSPORT_READY", "grpcTransport", true);
  public static final BrokerMeter GRPC_TRANSPORT_TERMINATED = create(
      "GRPC_TRANSPORT_TERMINATED", "grpcTransport", true);

  private static final Map<QueryErrorCode, BrokerMeter> QUERY_ERROR_CODE_METER_MAP;

  // Iterate through all query error codes from QueryErrorCode.getAllValues() and create a metric for each
  static {
    QUERY_ERROR_CODE_METER_MAP = new HashMap<>();
    for (QueryErrorCode queryErrorCode : QueryErrorCode.values()) {
      // Currently, creating a global meter rather than a table specific meter to not add too many new metrics
      // The intention is to add alerts on these metrics, so a global metric is sufficient for now
      BrokerMeter meter = create("QUERY_ERROR_" + queryErrorCode.name(), "queries", true);
      QUERY_ERROR_CODE_METER_MAP.put(queryErrorCode, meter);
    }
  }

  private final String _brokerMeterName;
  private final String _unit;
  private final boolean _global;

  private BrokerMeter(String name, String unit, boolean global) {
    _unit = unit;
    _global = global;
    _brokerMeterName = Utils.toCamelCase(name.toLowerCase());
  }

  private static BrokerMeter create(String name, String unit, boolean global) {
    BrokerMeter brokerMeter = new BrokerMeter(name, unit, global);
    BROKER_METERS.add(brokerMeter);
    return brokerMeter;
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

  public static BrokerMeter[] values() {
    return BROKER_METERS.toArray(new BrokerMeter[0]);
  }

  public static BrokerMeter getQueryErrorMeter(QueryErrorCode queryErrorCode) {
    return QUERY_ERROR_CODE_METER_MAP.get(queryErrorCode);
  }
}
