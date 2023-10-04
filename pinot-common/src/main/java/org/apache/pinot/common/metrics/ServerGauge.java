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
 * Enumeration containing all the gauges exposed by the Pinot server.
 *
 */
public enum ServerGauge implements AbstractMetrics.Gauge {
  VERSION("version", true),
  DOCUMENT_COUNT("documents", false),
  SEGMENT_COUNT("segments", false),
  LLC_PARTITION_CONSUMING("state", false),
  HIGHEST_STREAM_OFFSET_CONSUMED("messages", false),
  LAST_REALTIME_SEGMENT_CREATION_DURATION_SECONDS("seconds", false),
  LAST_REALTIME_SEGMENT_CREATION_WAIT_TIME_SECONDS("seconds", false),
  LAST_REALTIME_SEGMENT_INITIAL_CONSUMPTION_DURATION_SECONDS("seconds", false),
  LAST_REALTIME_SEGMENT_CATCHUP_DURATION_SECONDS("seconds", false),
  LAST_REALTIME_SEGMENT_COMPLETION_DURATION_SECONDS("seconds", false),
  REALTIME_OFFHEAP_MEMORY_USED("bytes", false),
  REALTIME_SEGMENT_NUM_PARTITIONS("realtimeSegmentNumPartitions", false),
  LLC_SIMULTANEOUS_SEGMENT_BUILDS("llcSimultaneousSegmentBuilds", true),
  RESIZE_TIME_MS("milliseconds", false),
  // Upsert metrics
  UPSERT_PRIMARY_KEYS_COUNT("upsertPrimaryKeysCount", false),
  // Dedup metrics
  DEDUP_PRIMARY_KEYS_COUNT("dedupPrimaryKeysCount", false),
  CONSUMPTION_QUOTA_UTILIZATION("ratio", false),
  JVM_HEAP_USED_BYTES("bytes", true),
  NETTY_POOLED_USED_DIRECT_MEMORY("bytes", true),
  NETTY_POOLED_USED_HEAP_MEMORY("bytes", true),
  NETTY_POOLED_ARENAS_DIRECT("arenas", true),
  NETTY_POOLED_ARENAS_HEAP("arenas", true),

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
  NETTY_POOLED_CHUNK_SIZE("bytes", true),
  // Ingestion delay metrics
  REALTIME_INGESTION_DELAY_MS("milliseconds", false),
  END_TO_END_REALTIME_INGESTION_DELAY_MS("milliseconds", false),
  // Needed to track if valid doc id snapshots are present for faster restarts
  UPSERT_VALID_DOC_ID_SNAPSHOT_COUNT("upsertValidDocIdSnapshotCount", false),
  UPSERT_PRIMARY_KEYS_IN_SNAPSHOT_COUNT("upsertPrimaryKeysInSnapshotCount", false);

  private final String _gaugeName;
  private final String _unit;
  private final boolean _global;

  private final String _description;

  ServerGauge(String unit, boolean global) {
    this(unit, global, "");
  }

  ServerGauge(String unit, boolean global, String description) {
    _unit = unit;
    _global = global;
    _gaugeName = Utils.toCamelCase(name().toLowerCase());
    _description = description;
  }

  @Override
  public String getGaugeName() {
    return _gaugeName;
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

  @Override
  public String getDescription() {
    return _description;
  }
}
