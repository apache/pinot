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
 * Enumeration containing all the timers exposed by the Pinot broker.
 *
 */
public enum BrokerTimer implements AbstractMetrics.Timer {
  ROUTING_TABLE_UPDATE_TIME(true),
  CLUSTER_CHANGE_QUEUE_TIME(true), // metric tracking the freshness lag for consuming segments
  FRESHNESS_LAG_MS(false),
  QUERY_TOTAL_TIME_MS(false),

  // The latency of sending the request from broker to server
  NETTY_CONNECTION_SEND_REQUEST_LATENCY(false),

  // aggregated thread cpu time in nanoseconds for query processing from offline servers
  OFFLINE_THREAD_CPU_TIME_NS(false),
  // aggregated thread cpu time in nanoseconds for query processing from realtime servers
  REALTIME_THREAD_CPU_TIME_NS(false),
  // aggregated system activities cpu time in nanoseconds for query processing from offline servers
  OFFLINE_SYSTEM_ACTIVITIES_CPU_TIME_NS(false),
  // aggregated system activities cpu time in nanoseconds for query processing from realtime servers
  REALTIME_SYSTEM_ACTIVITIES_CPU_TIME_NS(false),
  // aggregated response serialization cpu time in nanoseconds for query processing from offline servers
  OFFLINE_RESPONSE_SER_CPU_TIME_NS(false),
  // aggregated response serialization cpu time in nanoseconds for query processing from realtime servers
  REALTIME_RESPONSE_SER_CPU_TIME_NS(false),
  // aggregated total cpu time(thread + system activities + response serialization) in nanoseconds for query
  // processing from offline servers
  OFFLINE_TOTAL_CPU_TIME_NS(false),
  // aggregated total cpu time(thread + system activities + response serialization) in nanoseconds for query
  // processing from realtime servers
  REALTIME_TOTAL_CPU_TIME_NS(false),
  // How long it took the server to start.
  STARTUP_SUCCESS_DURATION_MS(true);

  private final String _timerName;
  private final boolean _global;

  BrokerTimer(boolean global) {
    _global = global;
    _timerName = Utils.toCamelCase(name().toLowerCase());
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
}
