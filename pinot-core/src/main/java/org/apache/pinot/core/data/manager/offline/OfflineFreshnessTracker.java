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
package org.apache.pinot.core.data.manager.offline;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tracks freshness (staleness) of offline table data on a per-partition basis.
 *
 * <p>Mirrors the {@code IngestionDelayTracker} pattern from realtime tables. One instance per offline table per server.
 * Tracks {@code max(segment_end_time)} per partition and registers callback-based ingestion delay gauges that are
 * evaluated live on every metrics scrape.</p>
 *
 * <p>For non-partitioned tables, uses partition ID {@link #NON_PARTITIONED_SENTINEL} as a sentinel.</p>
 */
public class OfflineFreshnessTracker {
  private static final Logger LOGGER = LoggerFactory.getLogger(OfflineFreshnessTracker.class);

  /** Sentinel partition ID for non-partitioned tables. */
  public static final int NON_PARTITIONED_SENTINEL = -1;

  private final ServerMetrics _serverMetrics;
  private final String _tableNameWithType;
  private final BooleanSupplier _isServerReadyToServeQueries;
  private final Supplier<Long> _clock;

  // Segment name -> partition ID (for removal lookup)
  private final ConcurrentHashMap<String, Integer> _segmentToPartition = new ConcurrentHashMap<>();

  // Per-partition: segment name -> end time millis (for efficient partition-scoped recomputation on removal)
  private final ConcurrentHashMap<Integer, ConcurrentHashMap<String, Long>> _partitionSegmentEndTimes =
      new ConcurrentHashMap<>();

  // Per-partition max segment end time (millis)
  private final ConcurrentHashMap<Integer, Long> _maxEndTimePerPartition = new ConcurrentHashMap<>();

  // Which partitions have metrics registered
  private final ConcurrentHashMap<Integer, Boolean> _partitionsTracked = new ConcurrentHashMap<>();

  // Whether the table-level gauge has been registered
  private final AtomicBoolean _tableGaugeRegistered = new AtomicBoolean(false);

  public OfflineFreshnessTracker(ServerMetrics serverMetrics, String tableNameWithType,
      BooleanSupplier isServerReadyToServeQueries) {
    this(serverMetrics, tableNameWithType, isServerReadyToServeQueries, System::currentTimeMillis);
  }

  @VisibleForTesting
  OfflineFreshnessTracker(ServerMetrics serverMetrics, String tableNameWithType,
      BooleanSupplier isServerReadyToServeQueries, Supplier<Long> clock) {
    _serverMetrics = serverMetrics;
    _tableNameWithType = tableNameWithType;
    _isServerReadyToServeQueries = isServerReadyToServeQueries;
    _clock = clock;

    LOGGER.info("Created OfflineFreshnessTracker for table: {}", tableNameWithType);
  }

  /**
   * Called after a segment finishes loading. Updates per-partition max end time and creates partition gauge if needed.
   * Handles segment replacement: if the segment was previously tracked (e.g. replaced with a new CRC), the old
   * bookkeeping is cleaned up before the new data is recorded.
   *
   * @param segmentName name of the loaded segment
   * @param endTimeMs segment end time in milliseconds
   * @param partitionId partition ID from segment metadata, or {@link #NON_PARTITIONED_SENTINEL}
   */
  public void segmentLoaded(String segmentName, long endTimeMs, int partitionId) {
    // Handle segment replacement: clean up old bookkeeping if the segment was already tracked
    Integer oldPartitionId = _segmentToPartition.get(segmentName);
    if (oldPartitionId != null) {
      segmentRemoved(segmentName);
    }

    _segmentToPartition.put(segmentName, partitionId);

    _partitionSegmentEndTimes.computeIfAbsent(partitionId, k -> new ConcurrentHashMap<>())
        .put(segmentName, endTimeMs);

    _maxEndTimePerPartition.merge(partitionId, endTimeMs, Math::max);

    _partitionsTracked.computeIfAbsent(partitionId, k -> {
      createPartitionMetrics(partitionId);
      return true;
    });

    if (_tableGaugeRegistered.compareAndSet(false, true)) {
      createTableMetrics();
    }

    LOGGER.debug("Segment loaded: {} partition={} endTimeMs={}", segmentName, partitionId, endTimeMs);
  }

  /**
   * Called when a segment is offloaded. Recomputes max end time for the affected partition.
   *
   * @param segmentName name of the segment being removed
   */
  public void segmentRemoved(String segmentName) {
    Integer partitionId = _segmentToPartition.remove(segmentName);

    if (partitionId == null) {
      return;
    }

    // Use compute() to atomically remove the segment and update partition state, preventing a race
    // where a concurrent segmentLoaded() adds a new segment between isEmpty() check and partition removal.
    final int pid = partitionId;
    _partitionSegmentEndTimes.compute(pid, (k, partitionSegments) -> {
      if (partitionSegments == null) {
        return null;
      }
      partitionSegments.remove(segmentName);
      if (partitionSegments.isEmpty()) {
        _maxEndTimePerPartition.remove(pid);
        _partitionsTracked.computeIfPresent(pid, (pk, v) -> {
          removePartitionMetrics(pid);
          return null;
        });
        return null;
      }
      long newMax = Long.MIN_VALUE;
      for (Long endTime : partitionSegments.values()) {
        if (endTime > newMax) {
          newMax = endTime;
        }
      }
      _maxEndTimePerPartition.put(pid, newMax);
      return partitionSegments;
    });

    LOGGER.debug("Segment removed: {} partition={}", segmentName, partitionId);
  }

  /**
   * Removes all registered gauges.
   */
  public void shutdown() {
    for (Integer partitionId : _partitionsTracked.keySet()) {
      removePartitionMetrics(partitionId);
    }
    _partitionsTracked.clear();

    // Remove table-level gauge
    _serverMetrics.removeTableGauge(_tableNameWithType, ServerGauge.OFFLINE_TABLE_INGESTION_DELAY_MS);

    LOGGER.info("Shut down OfflineFreshnessTracker for table: {}", _tableNameWithType);
  }

  /**
   * Returns the ingestion delay in milliseconds for a specific partition.
   * Returns 0 if no data exists for the partition, if the lag would be negative,
   * or if the server is not yet ready to serve queries.
   */
  @VisibleForTesting
  long getPartitionIngestionDelayMs(int partitionId) {
    if (!_isServerReadyToServeQueries.getAsBoolean()) {
      return 0L;
    }
    Long maxEndTime = _maxEndTimePerPartition.get(partitionId);
    if (maxEndTime == null) {
      return 0L;
    }
    return Math.max(0L, _clock.get() - maxEndTime);
  }

  /**
   * Returns the table-level ingestion delay in milliseconds (worst partition lag).
   * The partition with the oldest max end time determines table freshness.
   * Returns 0 if no data exists or if the server is not yet ready to serve queries.
   */
  @VisibleForTesting
  long getTableIngestionDelayMs() {
    if (!_isServerReadyToServeQueries.getAsBoolean()) {
      return 0L;
    }
    if (_maxEndTimePerPartition.isEmpty()) {
      return 0L;
    }
    long minEndTime = Long.MAX_VALUE;
    for (Long endTime : _maxEndTimePerPartition.values()) {
      if (endTime < minEndTime) {
        minEndTime = endTime;
      }
    }
    return Math.max(0L, _clock.get() - minEndTime);
  }

  @VisibleForTesting
  int getTrackedPartitionCount() {
    return _partitionsTracked.size();
  }

  @VisibleForTesting
  int getTrackedSegmentCount() {
    return _segmentToPartition.size();
  }

  @VisibleForTesting
  Long getMaxEndTimeForPartition(int partitionId) {
    return _maxEndTimePerPartition.get(partitionId);
  }

  private void createTableMetrics() {
    _serverMetrics.setOrUpdateTableGauge(_tableNameWithType, ServerGauge.OFFLINE_TABLE_INGESTION_DELAY_MS,
        this::getTableIngestionDelayMs);
    LOGGER.info("Created offline table ingestion delay gauge for table: {}", _tableNameWithType);
  }

  private void createPartitionMetrics(int partitionId) {
    _serverMetrics.setOrUpdatePartitionGauge(_tableNameWithType, partitionId, ServerGauge.OFFLINE_INGESTION_DELAY_MS,
        () -> getPartitionIngestionDelayMs(partitionId));
    LOGGER.info("Created offline ingestion delay metrics for table: {} partition: {}", _tableNameWithType, partitionId);
  }

  private void removePartitionMetrics(int partitionId) {
    _serverMetrics.removePartitionGauge(_tableNameWithType, partitionId, ServerGauge.OFFLINE_INGESTION_DELAY_MS);
    LOGGER.info("Removed offline ingestion delay metrics for table: {} partition: {}", _tableNameWithType, partitionId);
  }
}
