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
 * Enumeration containing all the timers exposed by the Pinot server.
 *
 */
public enum ServerTimer implements AbstractMetrics.Timer {
  FRESHNESS_LAG_MS("freshnessLagMs", false, "Tracks the freshness lag for consuming segments. "
      + "Computed as the time-period between when the data was last updated in the table and the current time."),

  NETTY_CONNECTION_SEND_RESPONSE_LATENCY("nettyConnection", false,
      "Latency of sending the response from server to broker. Computed as the time spent in sending "
          + "response to brokers after the results are available."),

  EXECUTION_THREAD_CPU_TIME_NS("nanoseconds", false, "Query cost (execution thread cpu time) "
      + "for query processing on server. Computed as time spent by all threads processing query and results "
      + "(doesn't includes time spent in system activities)"),

  SYSTEM_ACTIVITIES_CPU_TIME_NS("nanoseconds", false, "Query cost (system activities cpu time) "
      + "for query processing on server. Computed as the time spent in processing query on the servers "
      + "(only counts system acitivities such as GC, OS paging etc.)"),

  RESPONSE_SER_CPU_TIME_NS("nanoseconds", false, "Query cost (response serialization cpu time) "
      + "for query processing on server. Computed as the time spent in serializing query response on servers"),

  SEGMENT_UPLOAD_TIME_MS("milliseconds", false),

  TOTAL_CPU_TIME_NS("nanoseconds", false, "Total query cost (thread cpu time + system "
      + "activities cpu time + response serialization cpu time) for query processing on server."),

  UPSERT_PRELOAD_TIME_MS("milliseconds", false,
      "Total time taken to preload a table partition of an upsert table with upsert snapshot"),
  UPSERT_REMOVE_EXPIRED_PRIMARY_KEYS_TIME_MS("milliseconds", false,
      "Total time taken to delete expired primary keys based on metadataTTL or deletedKeysTTL"),
  UPSERT_SNAPSHOT_TIME_MS("milliseconds", false, "Total time taken to take upsert table snapshot");

  private final String _timerName;
  private final boolean _global;
  private final String _description;

  ServerTimer(String unit, boolean global) {
    this(unit, global, "");
  }

  ServerTimer(String unit, boolean global, String description) {
    _global = global;
    _timerName = Utils.toCamelCase(name().toLowerCase());
    _description = description;
  }

  @Override
  public String getTimerName() {
    return _timerName;
  }

  /**
   * Returns true if the timer is global (not attached to a particular resource)
   *
   * @return true if the timer is global
   */
  @Override
  public boolean isGlobal() {
    return _global;
  }

  @Override
  public String getDescription() {
    return _description;
  }
}
