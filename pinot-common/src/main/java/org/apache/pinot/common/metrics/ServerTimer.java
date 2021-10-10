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
  // metric tracking the freshness lag for consuming segments
  FRESHNESS_LAG_MS("freshnessLagMs", false),

  // The latency of sending the response from server to broker
  NETTY_CONNECTION_SEND_RESPONSE_LATENCY("nettyConnection", false),

  // Query cost (thread cpu time) for query processing on server
  EXECUTION_THREAD_CPU_TIME_NS("nanoseconds", false);

  private final String _timerName;
  private final boolean _global;

  ServerTimer(String unit, boolean global) {
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
