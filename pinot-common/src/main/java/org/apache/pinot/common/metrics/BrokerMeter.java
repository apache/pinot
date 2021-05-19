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
  QUERIES("queries", false),

  // These metrics track the exceptions caught during query execution in broker side.
  // PQL compile phase.
  REQUEST_COMPILATION_EXCEPTIONS("exceptions", true),
  // Get resource phase.
  RESOURCE_MISSING_EXCEPTIONS("exceptions", true),
  // Query validation phase.
  QUERY_VALIDATION_EXCEPTIONS("exceptions", false),
  // Scatter phase.
  NO_SERVER_FOUND_EXCEPTIONS("exceptions", false),
  REQUEST_TIMEOUT_BEFORE_SCATTERED_EXCEPTIONS("exceptions", false),
  REQUEST_SEND_EXCEPTIONS("exceptions", false),
  // Gather phase.
  RESPONSE_FETCH_EXCEPTIONS("exceptions", false),
  // Response deserialize phase.
  DATA_TABLE_DESERIALIZATION_EXCEPTIONS("exceptions", false),
  // Reduce responses phase.
  RESPONSE_MERGE_EXCEPTIONS("exceptions", false),

  // These metrics track the number of bad broker responses.
  // This metric track the number of broker responses with processing exceptions inside.
  // The processing exceptions could be caught from both server side and broker side.
  BROKER_RESPONSES_WITH_PROCESSING_EXCEPTIONS("badResponses", false),
  // This metric track the number of broker responses with not all servers responded.
  // (numServersQueried > numServersResponded)
  BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED("badResponses", false),
  // This metric track the number of broker responses with number of groups limit reached (potential bad responses).
  BROKER_RESPONSES_WITH_NUM_GROUPS_LIMIT_REACHED("badResponses", false),

  // These metrics track the cost of the query.
  DOCUMENTS_SCANNED("documents", false),
  ENTRIES_SCANNED_IN_FILTER("documents", false),
  ENTRIES_SCANNED_POST_FILTER("documents", false),

  NUM_RESIZES("numResizes", false),

  REQUEST_CONNECTION_TIMEOUTS("timeouts", false),
  HELIX_ZOOKEEPER_RECONNECTS("reconnects", true),

  // This metric tracks the number of requests dropped by the broker after we get a connection to the server.
  // Exceptions resulting when sending a request get counted in this metric. The metric is counted on a per-table
  // basis.
  REQUEST_DROPPED_DUE_TO_SEND_ERROR("requestDropped", false),

  // This metric tracks the number of requests that had to be dropped because we could not get a connection
  // to the server. Note that this may be because we have exhausted the (fixed-size) pool for the server, and
  // also reached the maximum number of waiting requests for the server. The metric is counted on a per-table
  // basis.
  REQUEST_DROPPED_DUE_TO_CONNECTION_ERROR("requestDropped", false),

  REQUEST_DROPPED_DUE_TO_ACCESS_ERROR("requestsDropped", false),

  ROUTING_TABLE_REBUILD_FAILURES("failures", false),

  GROUP_BY_SIZE("queries", false),
  TOTAL_SERVER_RESPONSE_SIZE("queries", false),
  INVALID_COLUMN_NAMES_IN_QUERY("queries", false),

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

  PROACTIVE_CLUSTER_CHANGE_CHECK("proactiveClusterChangeCheck", true);

  private final String brokerMeterName;
  private final String unit;
  private final boolean global;

  BrokerMeter(String unit, boolean global) {
    this.unit = unit;
    this.global = global;
    this.brokerMeterName = Utils.toCamelCase(name().toLowerCase());
  }

  @Override
  public String getMeterName() {
    return brokerMeterName;
  }

  @Override
  public String getUnit() {
    return unit;
  }

  /**
   * Returns true if the metric is global (not attached to a particular resource)
   *
   * @return true if the metric is global
   */
  @Override
  public boolean isGlobal() {
    return global;
  }
}
