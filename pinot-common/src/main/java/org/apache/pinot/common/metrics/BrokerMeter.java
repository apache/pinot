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
 */
public class BrokerMeter implements AbstractMetrics.Meter {
  private static final List<BrokerMeter> BROKER_METERS = new ArrayList<>();

  // TODO - Validate
  //  1. the name of the metric with existing metric name
  //  2. units, global flags
  public static final BrokerMeter UNCAUGHT_GET_EXCEPTIONS = create("uncaughtGetExceptions", "exceptions", true);
  public static final BrokerMeter UNCAUGHT_POST_EXCEPTIONS = create("uncaughtPostExceptions", "exceptions", true);
  public static final BrokerMeter WEB_APPLICATION_EXCEPTIONS = create("webApplicationExceptions", "exceptions", true);
  public static final BrokerMeter HEALTHCHECK_BAD_CALLS = create("healthcheckBadCalls", "healthcheck", true);
  public static final BrokerMeter HEALTHCHECK_OK_CALLS = create("healthcheckOkCalls", "healthcheck", true);
  public static final BrokerMeter QUERIES = create("queries", "queries", false);
  public static final BrokerMeter QUERIES_GLOBAL = create("queriesGlobal", "queries", true);
  public static final BrokerMeter MULTI_STAGE_QUERIES_GLOBAL = create("multiStageQueriesGlobal", "queries", true);
  public static final BrokerMeter MULTI_STAGE_QUERIES = create("multiStageQueries", "queries", false);
  public static final BrokerMeter SINGLE_STAGE_QUERIES_INVALID_MULTI_STAGE = create(
      "singleStageQueriesInvalidMultiStage", "queries", true);
  public static final BrokerMeter TIME_SERIES_GLOBAL_QUERIES = create("timeSeriesGlobalQueries", "queries", true);
  public static final BrokerMeter TIME_SERIES_GLOBAL_QUERIES_FAILED = create(
      "timeSeriesGlobalQueriesFailed", "queries", true);
  public static final BrokerMeter QUERY_REJECTED_EXCEPTIONS = create("queryRejectedExceptions", "exceptions", true);
  public static final BrokerMeter REQUEST_COMPILATION_EXCEPTIONS = create(
      "requestCompilationExceptions", "exceptions", true);
  public static final BrokerMeter RESOURCE_MISSING_EXCEPTIONS = create("resourceMissingExceptions", "exceptions", true);
  public static final BrokerMeter QUERY_VALIDATION_EXCEPTIONS = create(
      "queryValidationExceptions", "exceptions", false);
  public static final BrokerMeter UNKNOWN_COLUMN_EXCEPTIONS = create("unknownColumnExceptions", "exceptions", false);
  public static final BrokerMeter QUERIES_KILLED = create("queriesKilled", "query", true);
  public static final BrokerMeter NO_SERVER_FOUND_EXCEPTIONS = create("noServerFoundExceptions", "exceptions", false);
  public static final BrokerMeter REQUEST_TIMEOUT_BEFORE_SCATTERED_EXCEPTIONS = create(
      "requestTimeoutBeforeScatteredExceptions", "exceptions", false);
  public static final BrokerMeter REQUEST_CHANNEL_LOCK_TIMEOUT_EXCEPTIONS = create(
      "requestChannelLockTimeoutExceptions", "exceptions", false);
  public static final BrokerMeter REQUEST_SEND_EXCEPTIONS = create("requestSendExceptions", "exceptions", false);
  public static final BrokerMeter RESPONSE_FETCH_EXCEPTIONS = create("responseFetchExceptions", "exceptions", false);
  public static final BrokerMeter DATA_TABLE_DESERIALIZATION_EXCEPTIONS = create(
      "dataTableDeserializationExceptions", "exceptions", false);
  public static final BrokerMeter RESPONSE_MERGE_EXCEPTIONS = create("responseMergeExceptions", "exceptions", false);
  public static final BrokerMeter HEAP_CRITICAL_LEVEL_EXCEEDED = create("heapCriticalLevelExceeded", "count", true);
  public static final BrokerMeter HEAP_PANIC_LEVEL_EXCEEDED = create("heapPanicLevelExceeded", "count", true);
  public static final BrokerMeter BROKER_RESPONSES_WITH_PROCESSING_EXCEPTIONS = create(
      "brokerResponsesWithProcessingExceptions", "badResponses", false);
  public static final BrokerMeter BROKER_RESPONSES_WITH_UNAVAILABLE_SEGMENTS = create(
      "brokerResponsesWithUnavailableSegments", "badResponses", false);
  public static final BrokerMeter BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED = create(
      "brokerResponsesWithPartialServersResponded", "badResponses", false);
  public static final BrokerMeter SECONDARY_WORKLOAD_BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED = create(
      "secondaryWorkloadBrokerResponsesWithPartialServersResponded", "badResponses", false);
  public static final BrokerMeter BROKER_RESPONSES_WITH_TIMEOUTS = create(
      "brokerResponsesWithTimeouts", "badResponses", false);
  public static final BrokerMeter SECONDARY_WORKLOAD_BROKER_RESPONSES_WITH_TIMEOUTS = create(
      "secondaryWorkloadBrokerResponsesWithTimeouts", "badResponses", false);
  public static final BrokerMeter BROKER_RESPONSES_WITH_NUM_GROUPS_LIMIT_REACHED = create(
      "brokerResponsesWithNumGroupsLimitReached", "badResponses", false);
  public static final BrokerMeter DOCUMENTS_SCANNED = create("documentsScanned", "documents", false);
  public static final BrokerMeter ENTRIES_SCANNED_IN_FILTER = create("entriesScannedInFilter", "documents", false);
  public static final BrokerMeter ENTRIES_SCANNED_POST_FILTER = create("entriesScannedPostFilter", "documents", false);
  public static final BrokerMeter NUM_RESIZES = create("numResizes", "numResizes", false);
  public static final BrokerMeter HELIX_ZOOKEEPER_RECONNECTS = create("helixZookeeperReconnects", "reconnects", true);
  public static final BrokerMeter REQUEST_DROPPED_DUE_TO_ACCESS_ERROR = create(
      "requestDroppedDueToAccessError", "requestsDropped", false);
  public static final BrokerMeter GROUP_BY_SIZE = create("groupBySize", "queries", false);
  public static final BrokerMeter TOTAL_SERVER_RESPONSE_SIZE = create("totalServerResponseSize", "queries", false);
  public static final BrokerMeter QUERY_QUOTA_EXCEEDED = create("queryQuotaExceeded", "exceptions", false);
  public static final BrokerMeter NO_SERVING_HOST_FOR_SEGMENT = create(
      "noServingHostForSegment", "badResponses", false);
  public static final BrokerMeter SERVER_MISSING_FOR_ROUTING = create("serverMissingForRouting", "badResponses", false);
  public static final BrokerMeter NETTY_CONNECTION_REQUESTS_SENT = create(
      "nettyConnectionRequestsSent", "nettyConnection", true);
  public static final BrokerMeter NETTY_CONNECTION_BYTES_SENT = create(
      "nettyConnectionBytesSent", "nettyConnection", true);
  public static final BrokerMeter NETTY_CONNECTION_BYTES_RECEIVED = create(
      "nettyConnectionBytesReceived", "nettyConnection", true);
  public static final BrokerMeter PROACTIVE_CLUSTER_CHANGE_CHECK = create(
      "proactiveClusterChangeCheck", "proactiveClusterChangeCheck", true);
  public static final BrokerMeter DIRECT_MEMORY_OOM = create("directMemoryOom", "directMemoryOOMCount", true);
  public static final BrokerMeter QUERIES_WITH_JOINS = create("queriesWithJoins", "queries", true);
  public static final BrokerMeter JOIN_COUNT = create("joinCount", "queries", true);
  public static final BrokerMeter QUERIES_WITH_WINDOW = create("queriesWithWindow", "queries", true);
  public static final BrokerMeter WINDOW_COUNT = create("windowCount", "queries", true);
  public static final BrokerMeter CURSOR_QUERIES_GLOBAL = create("cursorQueriesGlobal", "queries", true);
  public static final BrokerMeter CURSOR_WRITE_EXCEPTION = create("cursorWriteException", "exceptions", true);
  public static final BrokerMeter CURSOR_READ_EXCEPTION = create("cursorReadException", "exceptions", true);
  public static final BrokerMeter CURSOR_RESPONSE_STORE_SIZE = create("cursorResponseStoreSize", "bytes", true);
  public static final BrokerMeter GRPC_QUERIES = create("grpcQueries", "grpcQueries", true);
  public static final BrokerMeter GRPC_QUERY_EXCEPTIONS = create("grpcQueryExceptions", "grpcExceptions", true);
  public static final BrokerMeter GRPC_BYTES_RECEIVED = create("grpcBytesReceived", "grpcBytesReceived", true);
  public static final BrokerMeter GRPC_BYTES_SENT = create("grpcBytesSent", "grpcBytesSent", true);
  public static final BrokerMeter GRPC_TRANSPORT_READY = create("grpcTransportReady", "grpcTransport", true);
  public static final BrokerMeter GRPC_TRANSPORT_TERMINATED = create("grpcTransportTerminated", "grpcTransport", true);

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
