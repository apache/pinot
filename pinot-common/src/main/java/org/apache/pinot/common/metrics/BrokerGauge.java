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
 * Enumeration containing all the gauges exposed by the Pinot broker.
 *
 */
public enum BrokerGauge implements AbstractMetrics.Gauge {
  QUERY_QUOTA_CAPACITY_UTILIZATION_RATE("tables", false),
  QUERY_RATE_LIMIT_DISABLED("queryQuota", true),
  NETTY_CONNECTION_CONNECT_TIME_MS("nettyConnection", true);

  private final String brokerGaugeName;
  private final String unit;
  private final boolean global;

  BrokerGauge(String unit, boolean global) {
    this.unit = unit;
    this.global = global;
    this.brokerGaugeName = Utils.toCamelCase(name().toLowerCase());
  }

  @Override
  public String getGaugeName() {
    return brokerGaugeName;
  }

  @Override
  public String getUnit() {
    return unit;
  }

  /**
   * Returns true if the gauge is global (not attached to a particular resource)
   *
   * @return true if the gauge is global
   */
  @Override
  public boolean isGlobal() {
    return global;
  }
}
