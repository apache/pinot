/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.metrics;

import com.linkedin.pinot.common.Utils;


/**
* Enumeration containing all the metrics exposed by the Pinot broker.
*
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

  // These metrics track the cost of the query.
  DOCUMENTS_SCANNED("documents", false),
  ENTRIES_SCANNED_IN_FILTER("documents", false),
  ENTRIES_SCANNED_POST_FILTER("documents", false),

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

  // Number of queries served by LLC and HLC routing tables
  LLC_QUERY_COUNT("queries", false),
  HLC_QUERY_COUNT("queries", false),

  ROUTING_TABLE_REBUILD_FAILURES("failures", false);

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
