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

import io.netty.buffer.PooledByteBufAllocatorMetric;
import org.apache.pinot.common.Utils;


/**
 * Enumeration containing all the gauges exposed by the Pinot broker.
 *
 */
public enum BrokerGauge implements AbstractMetrics.Gauge {
  VERSION("version", true),
  QUERY_QUOTA_CAPACITY_UTILIZATION_RATE("tables", false),
  MAX_BURST_QPS("tables", false),
  QUERY_RATE_LIMIT_DISABLED("queryQuota", true),
  NETTY_CONNECTION_CONNECT_TIME_MS("nettyConnection", true),
  REQUEST_SIZE("requestSize", false),
  RESIZE_TIME_MS("milliseconds", false),
  UNHEALTHY_SERVERS("servers", true),
  TIME_BOUNDARY_DIFFERENCE("milliseconds", false),
  JVM_HEAP_USED_BYTES("bytes", true),
  NETTY_POOLED_USED_DIRECT_MEMORY("bytes", true),
  NETTY_POOLED_USED_HEAP_MEMORY("bytes", true),
  NETTY_POOLED_ARENAS_DIRECT("arenas", true),
  NETTY_POOLED_ARENAS_HEAP("arenas", true),

  /**
   * The queue size of ServerRoutingStatsManager main executor service.
   */
  STATS_MANAGER_QUEUE_SIZE("statsManagerQueueSize", true),

  /**
   * The size of the small cache.
   * See {@link PooledByteBufAllocatorMetric#smallCacheSize()}
   */
  NETTY_POOLED_CACHE_SIZE_SMALL("bytes", true),
  /**
   * The size of the normal cache.
   * See {@link PooledByteBufAllocatorMetric#normalCacheSize()}
   */
  NETTY_POOLED_CACHE_SIZE_NORMAL("bytes", true),
  /**
   * The cache size used by the allocator for normal arenas
   */
  NETTY_POOLED_THREADLOCALCACHE("bytes", true),
  NETTY_POOLED_CHUNK_SIZE("bytes", true);

  private final String _brokerGaugeName;
  private final String _unit;
  private final boolean _global;

  BrokerGauge(String unit, boolean global) {
    _unit = unit;
    _global = global;
    _brokerGaugeName = Utils.toCamelCase(name().toLowerCase());
  }

  @Override
  public String getGaugeName() {
    return _brokerGaugeName;
  }

  @Override
  public String getUnit() {
    return _unit;
  }

  /**
   * Returns true if the gauge is global (not attached to a particular resource)
   *
   * @return true if the gauge is global
   */
  @Override
  public boolean isGlobal() {
    return _global;
  }
}
