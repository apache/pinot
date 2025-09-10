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
package org.apache.pinot.core.data.manager.realtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Class to track realtime ingestion delay for table partitions on a given server.
 * Highlights:
 * 1-An object of this class is hosted by each RealtimeTableDataManager.
 * 2-The object tracks ingestion delays for all partitions hosted by the current server for the given Realtime table.
 * 3-Partition delays are updated by all RealtimeSegmentDataManager objects hosted in the corresponding
 *   RealtimeTableDataManager.
 * 4-Individual metrics are associated with each partition being tracked.
 * 5-Delays for partitions that do not have events to consume are reported as zero.
 * 6-Partitions whose Segments go from CONSUMING to DROPPED state stop being tracked so their delays do not cloud
 *   delays of active partitions.
 * 7-When a segment goes from CONSUMING to ONLINE, we start a timeout for the corresponding partition.
 *   If no consumption is noticed after the timeout, we then read ideal state to confirm the server still hosts the
 *   partition. If not, we stop tracking the respective partition.
 * 8-A scheduled executor thread is started by this object to track timeouts of partitions and drive the reading
 * of their ideal state.
 *
 *  The following diagram illustrates the object interactions with main external APIs
 *
 *     (CONSUMING -> ONLINE state change)
 *             |
 *      markPartitionForConfirmation(partitionId)
 *            |                         |<-updateIngestionDelay()-{RealtimeSegmentDataManager(Partition 0}}
 *            |                         |
 * ___________V_________________________V_
 * |           (Table X)                |<-updateIngestionDelay()-{RealtimeSegmentDataManager(Partition 1}}
 * | IngestionDelayTracker              |           ...
 * |____________________________________|<-updateIngestionDelay()-{RealtimeSegmentDataManager (Partition n}}
 *              ^                      ^
 *              |                       \
 *   timeoutInactivePartitions()    stopTrackingPartitionIngestionDelay(partitionId)
 *    _________|__________                \
 *   | TimerTrackingTask |          (CONSUMING -> DROPPED state change)
 *   |___________________|
 *
 * TODO: handle bug situations like the one where a partition is not allocated to a given server due to a bug.
 */

public class IngestionDelayTracker {

  private static class IngestionInfo {
    volatile long _ingestionTimeMs;
    volatile StreamPartitionMsgOffset _currentOffset;

    IngestionInfo(long ingestionTimeMs, @Nullable StreamPartitionMsgOffset currentOffset) {
      _ingestionTimeMs = ingestionTimeMs;
      _currentOffset = currentOffset;
    }

    void update(long ingestionTimeMs, @Nullable StreamPartitionMsgOffset currentOffset) {
      _ingestionTimeMs = ingestionTimeMs;
      _currentOffset = currentOffset;
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(IngestionDelayTracker.class);
  // Sleep interval for scheduled executor service thread that triggers read of ideal state
  // 5 minutes +/- precision in timeouts
  private static final long METRICS_CLEANUP_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);
  // 30 seconds +/- precision in timeouts
  private static final long METRICS_TRACKING_INTERVAL_MS = TimeUnit.SECONDS.toMillis(30);
  // Once a partition is marked for verification, we wait 10 minutes to pull its ideal state.
  private static final int PARTITION_TIMEOUT_MS = 600000;
  // Delay scheduled executor service for this amount of time after starting service
  private static final int INITIAL_SCHEDULED_EXECUTOR_THREAD_DELAY_MS = 100;
  // Cache expire time for ignored segment if there is no update from the segment.
  private static final int IGNORED_SEGMENT_CACHE_TIME_MINUTES = 10;

  private final Cache<String, Boolean> _segmentsToIgnore =
      CacheBuilder.newBuilder().expireAfterAccess(IGNORED_SEGMENT_CACHE_TIME_MINUTES, TimeUnit.MINUTES).build();
  private final Map<Integer, Boolean> _partitionsTracked = new ConcurrentHashMap<>();
  private final Map<Integer, IngestionInfo> _ingestionInfoMap = new ConcurrentHashMap<>();
  private final Map<Integer, Long> _partitionsMarkedForVerification = new ConcurrentHashMap<>();
  private final ScheduledExecutorService _metricsCleanupScheduler;
  private final ServerMetrics _serverMetrics;
  private final String _tableNameWithType;
  private final String _metricName;
  private final RealtimeTableDataManager _realTimeTableDataManager;
  private final Supplier<Boolean> _isServerReadyToServeQueries;
  private final StreamMetadataProvider _streamMetadataProvider;

  private ScheduledExecutorService _ingestionDelayTrackingScheduler = null;
  private Clock _clock = Clock.systemUTC();

  private volatile Set<Integer> _partitionsHostedByThisServer = new HashSet<>();

  protected volatile Map<Integer, StreamPartitionMsgOffset> _partitionIdToLatestOffset;

  public IngestionDelayTracker(ServerMetrics serverMetrics, String tableNameWithType,
      RealtimeTableDataManager realtimeTableDataManager, Supplier<Boolean> isServerReadyToServeQueries)
      throws RuntimeException {
    this(serverMetrics, tableNameWithType, realtimeTableDataManager, METRICS_CLEANUP_INTERVAL_MS,
        isServerReadyToServeQueries);
  }

  @VisibleForTesting
  public IngestionDelayTracker(ServerMetrics serverMetrics, String tableNameWithType,
      RealtimeTableDataManager realtimeTableDataManager, long metricsCleanupIntervalMs,
      Supplier<Boolean> isServerReadyToServeQueries) {
    _serverMetrics = serverMetrics;
    _tableNameWithType = tableNameWithType;
    _metricName = tableNameWithType;
    _realTimeTableDataManager = realtimeTableDataManager;
    _isServerReadyToServeQueries = isServerReadyToServeQueries;

    _metricsCleanupScheduler = Executors.newSingleThreadScheduledExecutor(getThreadFactory(
        "IngestionDelayMetricsRemovalThread-" + TableNameBuilder.extractRawTableName(tableNameWithType)));
    _metricsCleanupScheduler.scheduleWithFixedDelay(this::timeoutInactivePartitions,
        INITIAL_SCHEDULED_EXECUTOR_THREAD_DELAY_MS, metricsCleanupIntervalMs, TimeUnit.MILLISECONDS);

    _streamMetadataProvider = createStreamMetadataProvider(tableNameWithType, realtimeTableDataManager);

    if (_streamMetadataProvider.supportsOffsetLag()) {
      _partitionIdToLatestOffset = new HashMap<>();
    }
    _ingestionDelayTrackingScheduler = Executors.newSingleThreadScheduledExecutor(
        getThreadFactory("IngestionDelayTrackingThread-" + TableNameBuilder.extractRawTableName(tableNameWithType)));
    _ingestionDelayTrackingScheduler.scheduleWithFixedDelay(this::trackIngestionDelay,
        INITIAL_SCHEDULED_EXECUTOR_THREAD_DELAY_MS, METRICS_TRACKING_INTERVAL_MS, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  StreamMetadataProvider createStreamMetadataProvider(String tableNameWithType,
      RealtimeTableDataManager realtimeTableDataManager) {
    Map<String, String> streamConfigMap = IngestionConfigUtils.getFirstStreamConfigMap(
        realtimeTableDataManager.getCachedTableConfigAndSchema().getLeft());
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);
    StreamConsumerFactory streamConsumerFactory = StreamConsumerFactoryProvider.create(streamConfig);
    String clientId =
        IngestionConfigUtils.getTableTopicUniqueClientId(IngestionDelayTracker.class.getSimpleName(), streamConfig);
    return streamConsumerFactory.createStreamMetadataProvider(clientId);
  }

  private void trackIngestionDelay() {
    Set<Integer> partitionsHosted = _partitionsHostedByThisServer;

    if (_ingestionInfoMap.size() > partitionsHosted.size()) {
      // in-case new partition got assigned to the server before _partitionsHostedByThisServer was updated.
      partitionsHosted.addAll(_ingestionInfoMap.keySet());
    }

    if (_streamMetadataProvider.supportsOffsetLag()) {
      _partitionIdToLatestOffset = _streamMetadataProvider.fetchLatestStreamOffset(partitionsHosted, 5000);
    }

    for (Integer partitionId: partitionsHosted) {
      _partitionsTracked.computeIfAbsent(partitionId, k -> {
        createMetrics(partitionId);
        return true;
      });
    }
  }

  private ThreadFactory getThreadFactory(String threadName) {
    return new ThreadFactory() {
      private final ThreadFactory _defaultFactory = Executors.defaultThreadFactory();

      @Override
      public Thread newThread(Runnable r) {
        Thread thread = _defaultFactory.newThread(r);
        thread.setName(threadName);
        return thread;
      }
    };
  }

  /**
   * Helper function to get the ingestion delay for a given ingestion time.
   * Ingestion delay == Current Time - Ingestion Time
   *
   * @param ingestionTimeMs original ingestion time in milliseconds.
   */
  private long getIngestionDelayMs(long ingestionTimeMs) {
    if (ingestionTimeMs < 0) {
      return 0;
    }
    // Compute aged delay for current partition
    long agedIngestionDelayMs = _clock.millis() - ingestionTimeMs;
    // Correct to zero for any time shifts due to NTP or time reset.
    agedIngestionDelayMs = Math.max(agedIngestionDelayMs, 0);
    return agedIngestionDelayMs;
  }

  /*
   * Helper function to be called when we should stop tracking a given partition. Removes the partition from
   * all our maps.
   *
   * @param partitionId partition ID which we should stop tracking.
   */
  private void removePartitionId(int partitionId) {
    _partitionsTracked.compute(partitionId, (k, v) -> {
      if (v != null) {
        // Remove all metrics associated with this partition
        if (_streamMetadataProvider.supportsOffsetLag()) {
          _serverMetrics.removePartitionGauge(_metricName, partitionId, ServerGauge.REALTIME_INGESTION_OFFSET_LAG);
          _serverMetrics.removePartitionGauge(_metricName, partitionId, ServerGauge.REALTIME_INGESTION_UPSTREAM_OFFSET);
          _serverMetrics.removePartitionGauge(_metricName, partitionId,
              ServerGauge.REALTIME_INGESTION_CONSUMING_OFFSET);
        }
        _serverMetrics.removePartitionGauge(_metricName, partitionId, ServerGauge.REALTIME_INGESTION_DELAY_MS);
        _serverMetrics.removePartitionGauge(_metricName, partitionId,
            ServerGauge.END_TO_END_REALTIME_INGESTION_DELAY_MS);
        LOGGER.info("Successfully removed ingestion metrics for partition id: {}", partitionId);
      }
      _ingestionInfoMap.remove(partitionId);
      return null;
    });

    // If we are removing a partition we should stop reading its ideal state.
    _partitionsMarkedForVerification.remove(partitionId);
  }

  /*
   * Helper functions that creates a list of all the partitions that are marked for verification and whose
   * timeouts are expired. This helps us optimize checks of the ideal state.
   */
  private List<Integer> getPartitionsToBeVerified() {
    List<Integer> partitionsToVerify = new ArrayList<>();
    for (Map.Entry<Integer, Long> entry : _partitionsMarkedForVerification.entrySet()) {
      long timeMarked = _clock.millis() - entry.getValue();
      if (timeMarked > PARTITION_TIMEOUT_MS) {
        // Partition must be verified
        partitionsToVerify.add(entry.getKey());
      }
    }
    return partitionsToVerify;
  }

  /**
   * Function that enable use to set predictable clocks for testing purposes.
   *
   * @param clock clock to be used by the class
   */
  @VisibleForTesting
  void setClock(Clock clock) {
    _clock = clock;
  }

  public void createMetrics(int partitionId) {
    if (_streamMetadataProvider.supportsOffsetLag()) {
      _serverMetrics.setOrUpdatePartitionGauge(_metricName, partitionId, ServerGauge.REALTIME_INGESTION_OFFSET_LAG,
          () -> getPartitionIngestionOffsetLag(partitionId));

      _serverMetrics.setOrUpdatePartitionGauge(_metricName, partitionId,
          ServerGauge.REALTIME_INGESTION_CONSUMING_OFFSET, () -> getPartitionIngestionConsumingOffset(partitionId));

      _serverMetrics.setOrUpdatePartitionGauge(_metricName, partitionId,
          ServerGauge.REALTIME_INGESTION_UPSTREAM_OFFSET, () -> getPartitionIngestionUpstreamOffset(partitionId));
    }
    _serverMetrics.setOrUpdatePartitionGauge(_metricName, partitionId, ServerGauge.REALTIME_INGESTION_DELAY_MS,
        () -> getPartitionIngestionDelayMs(partitionId));
  }

  /**
   * Called by RealTimeSegmentDataManagers to update the ingestion delay metrics for a given partition.
   *
   * @param segmentName name of the consuming segment
   * @param partitionId partition id of the consuming segment (directly passed in to avoid parsing the segment name)
   * @param ingestionTimeMs ingestion time of the last consumed message (from {@link StreamMessageMetadata})
   * @param currentOffset offset of the last consumed message (from {@link StreamMessageMetadata})
   */
  public void updateMetrics(String segmentName, int partitionId, long ingestionTimeMs,
      @Nullable StreamPartitionMsgOffset currentOffset) {
    if (!_isServerReadyToServeQueries.get() || _realTimeTableDataManager.isShutDown()) {
      // Do not update the ingestion delay metrics during server startup period
      // or once the table data manager has been shutdown.
      return;
    }

    if (ingestionTimeMs < 0 && (currentOffset == null)) {
      // Do not publish metrics if stream does not return valid ingestion time or offset.
      return;
    }

    _ingestionInfoMap.compute(partitionId, (k, v) -> {
      if (_segmentsToIgnore.getIfPresent(segmentName) != null) {
        // Do not update the metrics for the segment that is marked to be ignored.
        return v;
      }
      if (v == null) {
        _partitionsTracked.computeIfAbsent(partitionId, pid -> {
          createMetrics(partitionId);
          return true;
        });
        return new IngestionInfo(ingestionTimeMs, currentOffset);
      }
      v.update(ingestionTimeMs, currentOffset);
      return v;
    });

    // If we are consuming we do not need to track this partition for removal.
    _partitionsMarkedForVerification.remove(partitionId);
  }

  /*
   * Handle partition removal event. This must be invoked when we stop serving a given partition for
   * this table in the current server.
   *
   * @param partitionId partition id that we should stop tracking.
   */
  public void stopTrackingPartition(int partitionId) {
    removePartitionId(partitionId);
  }

  /**
   * Handles all partition removal event. This must be invoked when we stop serving partitions for this table in the
   * current server.
   *
   * @return Set of partitionIds for which ingestion metrics were removed.
   */
  public Set<Integer> stopTrackingForAllPartitions() {
    Set<Integer> removedPartitionIds = new HashSet<>(_ingestionInfoMap.keySet());
    for (Integer partitionId : _ingestionInfoMap.keySet()) {
      removePartitionId(partitionId);
    }
    return removedPartitionIds;
  }

  /**
   * Stops tracking the partition ingestion delay, and also ignores the updates from the given segment. This is useful
   * when we want to stop tracking the ingestion delay for a partition when the segment might still be consuming, e.g.
   * when the new consuming segment is created on a different server.
   */
  public void stopTrackingPartition(String segmentName) {
    _segmentsToIgnore.put(segmentName, true);
    removePartitionId(new LLCSegmentName(segmentName).getPartitionGroupId());
  }

  /*
   * This method is used for timing out inactive partitions, so we don't display their metrics on current server.
   * When the inactive time exceeds some threshold, we read from ideal state to confirm we still host the partition,
   * if not we remove the partition from being tracked locally.
   * This call is to be invoked by a scheduled executor thread that will periodically wake up and invoke this function.
   */
  public void timeoutInactivePartitions() {
    if (!_isServerReadyToServeQueries.get()) {
      // Do not update the tracker state during server startup period
      return;
    }
    // Check if we have any partition to verify, else don't make the call to check ideal state as that
    // involves network traffic and may be inefficient.
    List<Integer> partitionsToVerify = getPartitionsToBeVerified();
    if (partitionsToVerify.isEmpty()) {
      // Don't make the call to getHostedPartitionsGroupIds() as it involves checking ideal state.
      return;
    }
    Set<Integer> partitionsHostedByThisServer;
    try {
      partitionsHostedByThisServer = _realTimeTableDataManager.getHostedPartitionsGroupIds();
    } catch (Exception e) {
      LOGGER.error("Failed to get partitions hosted by this server, table={}, exception={}:{}", _tableNameWithType,
          e.getClass(), e.getMessage());
      return;
    }
    for (int partitionId : partitionsToVerify) {
      if (!partitionsHostedByThisServer.contains(partitionId)) {
        // Partition is not hosted in this server anymore, stop tracking it
        removePartitionId(partitionId);
      }
    }
    _partitionsHostedByThisServer = partitionsHostedByThisServer;
  }

  /**
   * This function is invoked when a segment goes from CONSUMING to ONLINE, so we can assert whether the partition of
   * the segment is still hosted by this server after some interval of time.
   */
  public void markPartitionForVerification(String segmentName) {
    if (!_isServerReadyToServeQueries.get() || _segmentsToIgnore.getIfPresent(segmentName) != null) {
      // Do not update the tracker state during server startup period or if the segment is marked to be ignored
      return;
    }
    _partitionsMarkedForVerification.put(new LLCSegmentName(segmentName).getPartitionGroupId(), _clock.millis());
  }

  /*
   * Method to get timestamp used for the ingestion delay for a given partition.
   *
   * @param partitionId partition for which we are retrieving the delay
   *
   * @return ingestion delay timestamp in milliseconds for the given partition ID.
   */
  public long getPartitionIngestionTimeMs(int partitionId) {
    IngestionInfo ingestionInfo = _ingestionInfoMap.get(partitionId);
    return ingestionInfo != null ? ingestionInfo._ingestionTimeMs : Long.MIN_VALUE;
  }

  /*
   * Method to get ingestion delay for a given partition.
   *
   * @param partitionId partition for which we are retrieving the delay
   *
   * @return ingestion delay in milliseconds for the given partition ID.
   */
  public long getPartitionIngestionDelayMs(int partitionId) {
    IngestionInfo ingestionInfo = _ingestionInfoMap.get(partitionId);
    return ingestionInfo != null ? getIngestionDelayMs(ingestionInfo._ingestionTimeMs) : 0;
  }

  public long getPartitionIngestionOffsetLag(int partitionId) {
    IngestionInfo ingestionInfo = _ingestionInfoMap.get(partitionId);
    if (ingestionInfo == null) {
      return 0;
    }
    StreamPartitionMsgOffset currentOffset = ingestionInfo._currentOffset;
    StreamPartitionMsgOffset latestOffset = _partitionIdToLatestOffset.get(partitionId);
    if (currentOffset == null || latestOffset == null) {
      return 0;
    }
    // TODO: Support other types of offsets
    if (!(currentOffset instanceof LongMsgOffset && latestOffset instanceof LongMsgOffset)) {
      return 0;
    }
    return Math.max(0, ((LongMsgOffset) latestOffset).getOffset() - ((LongMsgOffset) currentOffset).getOffset());
  }

  // Get the consuming offset for a given partition
  public long getPartitionIngestionConsumingOffset(int partitionId) {
    IngestionInfo ingestionInfo = _ingestionInfoMap.get(partitionId);
    if (ingestionInfo == null) {
      return 0;
    }
    StreamPartitionMsgOffset currentOffset = ingestionInfo._currentOffset;
    if (currentOffset == null) {
      return 0;
    }
    // TODO: Support other types of offsets
    if (!(currentOffset instanceof LongMsgOffset)) {
      return 0;
    }
    return ((LongMsgOffset) currentOffset).getOffset();
  }

  // Get the latest offset in upstream data source for a given partition
  public long getPartitionIngestionUpstreamOffset(int partitionId) {
    IngestionInfo ingestionInfo = _ingestionInfoMap.get(partitionId);
    if (ingestionInfo == null) {
      return 0;
    }
    StreamPartitionMsgOffset latestOffset = _partitionIdToLatestOffset.get(partitionId);
    if (latestOffset == null) {
      return 0;
    }
    // TODO: Support other types of offsets
    if (!(latestOffset instanceof LongMsgOffset)) {
      return 0;
    }
    return ((LongMsgOffset) latestOffset).getOffset();
  }

  /*
   * We use this method to clean up when a table is being removed. No updates are expected at this time as all
   * RealtimeSegmentManagers should be down now.
   */
  public void shutdown() {
    // Now that segments can't report metric, destroy metric for this table
    _metricsCleanupScheduler.shutdown(); // ScheduledExecutor is installed in constructor so must always be cancelled
    if (_ingestionDelayTrackingScheduler != null) {
      _ingestionDelayTrackingScheduler.shutdown();
    }
    if (!_isServerReadyToServeQueries.get()) {
      // Do not update the tracker state during server startup period
      return;
    }
    // Remove partitions so their related metrics get uninstalled.
    for (Integer partitionId : _ingestionInfoMap.keySet()) {
      removePartitionId(partitionId);
    }
  }
}
