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
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerTimer;
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
 * 3-The current consumption status of partitions are updated by the RealtimeSegmentDataManager objects hosted in the
 * corresponding RealtimeTableDataManager.
 * 4-Individual metrics are associated with each partition being tracked.
 * 5-Delays for partitions that do not have events to consume are reported as zero.
 * 6-Partitions whose Segments go from CONSUMING to DROPPED state stop being tracked so their delays do not cloud
 *   delays of active partitions.
 * 7-When a segment goes from CONSUMING to ONLINE, we start a timeout for the corresponding partition.
 *   If no consumption is noticed after the timeout, we then read ideal state to confirm the server still hosts the
 *   partition. If not, we stop tracking the respective partition.
 * 8-A scheduled executor thread is started by this object to track timeouts of partitions and drive the reading
 * of their ideal state.
 * 9-A scheduled executor thread is started by this object to fetch the latest stream offset from upstream and create
 * metrics for new partitions for which there is no consumer present on the server.
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
 */

public class IngestionDelayTracker {

  private static class IngestionInfo {
    volatile long _ingestionTimeMs;
    @Nullable
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
  private static final long METRICS_REMOVAL_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);
  // 30 seconds +/- precision in timeouts
  private static final long METRICS_TRACKING_INTERVAL_MS = TimeUnit.SECONDS.toMillis(30);
  // Once a partition is marked for verification, we wait 10 minutes to pull its ideal state.
  private static final long PARTITION_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(10);
  // Delay scheduled executor service for this amount of time after starting service
  private static final int INITIAL_SCHEDULED_EXECUTOR_THREAD_DELAY_MS = 100;
  // Cache expire time for ignored segment if there is no update from the segment.
  private static final int IGNORED_SEGMENT_CACHE_TIME_MINUTES = 10;
  // Timeout after 5 seconds while fetching the latest stream offset.
  private static final long LATEST_STREAM_OFFSET_FETCH_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(5);

  private final ServerMetrics _serverMetrics;
  private final String _tableNameWithType;
  private final String _metricName;
  private final RealtimeTableDataManager _realTimeTableDataManager;
  private final BooleanSupplier _isServerReadyToServeQueries;
  private final Cache<String, Boolean> _segmentsToIgnore =
      CacheBuilder.newBuilder().expireAfterAccess(IGNORED_SEGMENT_CACHE_TIME_MINUTES, TimeUnit.MINUTES).build();
  // Map to describe the partitions for which the metrics are being reported.
  // This map is accessed by:
  // 1. _ingestionDelayTrackingScheduler thread.
  // 2. All threads which can removes metrics - Consumer thread, Helix Thread, Server API Thread, etc.
  // 3. Consumer thread when it updates the ingestion info of the partition for the first time.
  private final Map<Integer, Boolean> _partitionsTracked = new ConcurrentHashMap<>();
  // Map to hold the ingestion info reported by the consumer.
  // This map is accessed by:
  // 1. _ingestionDelayTrackingScheduler thread.
  // 2. All threads which can removes metrics - Consumer thread, Helix Thread, Server API Thread, etc.
  // 3. Consumer thread when it updates the ingestion info of the partition.
  private final Map<Integer, IngestionInfo> _ingestionInfoMap = new ConcurrentHashMap<>();
  private final Map<Integer, Long> _partitionsMarkedForVerification = new ConcurrentHashMap<>();
  private final ScheduledExecutorService _metricsRemovalScheduler;
  private final ScheduledExecutorService _ingestionDelayTrackingScheduler;

  private Clock _clock = Clock.systemUTC();

  protected volatile Map<Integer, StreamPartitionMsgOffset> _partitionIdToLatestOffset;
  protected volatile Set<Integer> _partitionsHostedByThisServer = new HashSet<>();
  // Map of StreamMetadataProvider to fetch upstream latest stream offset (Table can have multiple upstream topics)
  // This map is accessed by:
  // 1. _ingestionDelayTrackingScheduler thread.
  // 2. All threads which can removes metrics - Consumer thread, Helix Thread, Server API Thread, etc.
  protected Map<Integer, StreamMetadataProvider> _streamConfigIndexToStreamMetadataProvider = new ConcurrentHashMap<>();

  public IngestionDelayTracker(ServerMetrics serverMetrics, String tableNameWithType,
      RealtimeTableDataManager realtimeTableDataManager)
      throws RuntimeException {
    this(serverMetrics, tableNameWithType, realtimeTableDataManager, METRICS_REMOVAL_INTERVAL_MS,
        METRICS_TRACKING_INTERVAL_MS, realtimeTableDataManager.getIsServerReadyToServeQueries());
  }

  @VisibleForTesting
  public IngestionDelayTracker(ServerMetrics serverMetrics, String tableNameWithType,
      RealtimeTableDataManager realtimeTableDataManager, long metricsRemovalIntervalMs, long metricsTrackingIntervalMs,
      BooleanSupplier isServerReadyToServeQueries) {
    _serverMetrics = serverMetrics;
    _tableNameWithType = tableNameWithType;
    _metricName = tableNameWithType;
    _realTimeTableDataManager = realtimeTableDataManager;
    _isServerReadyToServeQueries = isServerReadyToServeQueries;

    _metricsRemovalScheduler = Executors.newSingleThreadScheduledExecutor(getThreadFactory(
        "IngestionDelayMetricsRemovalThread-" + TableNameBuilder.extractRawTableName(tableNameWithType)));
    // schedule periodically to remove in-active partitions.
    _metricsRemovalScheduler.scheduleWithFixedDelay(this::timeoutInactivePartitions,
        INITIAL_SCHEDULED_EXECUTOR_THREAD_DELAY_MS, metricsRemovalIntervalMs, TimeUnit.MILLISECONDS);

    createOrUpdateStreamMetadataProvider();

    _ingestionDelayTrackingScheduler = Executors.newSingleThreadScheduledExecutor(
        getThreadFactory("IngestionDelayTrackingThread-" + TableNameBuilder.extractRawTableName(tableNameWithType)));
    // schedule periodically to update latest upstream offset or create metrics for newly added partitions.
    _ingestionDelayTrackingScheduler.scheduleWithFixedDelay(this::trackIngestionDelay,
        INITIAL_SCHEDULED_EXECUTOR_THREAD_DELAY_MS, metricsTrackingIntervalMs, TimeUnit.MILLISECONDS);
  }

  private void createOrUpdateStreamMetadataProvider() {
    List<StreamConfig> streamConfigs =
        IngestionConfigUtils.getStreamConfigs(_realTimeTableDataManager.getCachedTableConfigAndSchema().getLeft());

    for (int streamConfigIndex = 0; streamConfigIndex < streamConfigs.size(); streamConfigIndex++) {
      if (_streamConfigIndexToStreamMetadataProvider.containsKey(streamConfigIndex)) {
        continue;
      }
      StreamConfig streamConfig = null;
      StreamMetadataProvider streamMetadataProvider;
      try {
        streamConfig = streamConfigs.get(streamConfigIndex);
        StreamConsumerFactory streamConsumerFactory = StreamConsumerFactoryProvider.create(streamConfig);
        String clientId =
            IngestionConfigUtils.getTableTopicUniqueClientId(IngestionDelayTracker.class.getSimpleName(), streamConfig);
        streamMetadataProvider = streamConsumerFactory.createStreamMetadataProvider(clientId);
      } catch (Exception e) {
        LOGGER.error("Failed to create stream metadata provider for streamConfig: {}", streamConfig, e);
        continue;
      }

      assert streamMetadataProvider != null;
      _streamConfigIndexToStreamMetadataProvider.put(streamConfigIndex, streamMetadataProvider);

      if ((streamMetadataProvider.supportsOffsetLag()) && (_partitionIdToLatestOffset == null)) {
        _partitionIdToLatestOffset = new ConcurrentHashMap<>();
      }
    }
  }

  private void trackIngestionDelay() {
    long startMs = System.currentTimeMillis();
    try {
      if (!_isServerReadyToServeQueries.getAsBoolean() || _realTimeTableDataManager.isShutDown()) {
        // Do not update the ingestion delay metrics during server startup period
        // or once the table data manager has been shutdown.
        return;
      }
      Set<Integer> partitionsHosted = _partitionsHostedByThisServer;

      if (_ingestionInfoMap.size() > partitionsHosted.size()) {
        // In-case new partition got assigned to the server before _partitionsHostedByThisServer was updated.
        partitionsHosted.addAll(_ingestionInfoMap.keySet());
      }

      if (partitionsHosted.isEmpty()) {
        return;
      }

      updateLatestStreamOffset(partitionsHosted);

      for (Integer partitionId : partitionsHosted) {
        _partitionsTracked.computeIfAbsent(partitionId, k -> {
          createMetrics(partitionId);
          return true;
        });
      }
    } catch (Throwable t) {
      LOGGER.error("Failed to track ingestion delay metrics.", t);
      _serverMetrics.addMeteredTableValue(_realTimeTableDataManager.getTableName(),
          ServerMeter.INGESTION_DELAY_TRACKING_ERRORS, 1);
    } finally {
      _serverMetrics.addTimedTableValue(_metricName, ServerTimer.INGESTION_DELAY_TRACKING_MS,
          System.currentTimeMillis() - startMs, TimeUnit.MILLISECONDS);
    }
  }

  @VisibleForTesting
  void updateLatestStreamOffset(Set<Integer> partitionsHosted) {
    Map<Integer, Set<Integer>> streamIndexToStreamPartitionIds =
        IngestionConfigUtils.getStreamConfigIndexToStreamPartitions(partitionsHosted);

    if (streamIndexToStreamPartitionIds.size() > _streamConfigIndexToStreamMetadataProvider.size()) {
      // There might be a new stream config added or need to retry creation of streamMetadataProvider for a stream
      // which might have failed before.
      createOrUpdateStreamMetadataProvider();
    }

    for (Map.Entry<Integer, StreamMetadataProvider> entry : _streamConfigIndexToStreamMetadataProvider.entrySet()) {
      int streamIndex = entry.getKey();
      StreamMetadataProvider streamMetadataProvider = entry.getValue();

      if (!streamIndexToStreamPartitionIds.containsKey(streamIndex)) {
        // Server is not hosting any partitions of this stream.
        continue;
      }

      if (streamMetadataProvider.supportsOffsetLag()) {
        Set<Integer> streamPartitionIds = streamIndexToStreamPartitionIds.get(streamIndex);
        try {
          Map<Integer, StreamPartitionMsgOffset> streamPartitionIdToLatestOffset =
              streamMetadataProvider.fetchLatestStreamOffset(streamPartitionIds,
                  LATEST_STREAM_OFFSET_FETCH_TIMEOUT_MS);
          if (streamIndex > 0) {
            // Need to convert stream partition Ids back to pinot partition Ids.
            for (Map.Entry<Integer, StreamPartitionMsgOffset> latestOffsetEntry
                : streamPartitionIdToLatestOffset.entrySet()) {
              _partitionIdToLatestOffset.put(
                  IngestionConfigUtils.getPinotPartitionIdFromStreamPartitionId(latestOffsetEntry.getKey(),
                      streamIndex), latestOffsetEntry.getValue());
            }
          } else {
            _partitionIdToLatestOffset.putAll(streamPartitionIdToLatestOffset);
          }
        } catch (Exception e) {
          LOGGER.error("Failed to update latest stream offsets for partitions: {}", streamPartitionIds, e);
        }
      }
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
   * Helper function to be called when we should stop tracking a given partition. Removes the partition from
   * all our maps.
   *
   * @param partitionId partition ID which we should stop tracking.
   */
  private void removePartitionId(int partitionId) {
    _partitionsTracked.compute(partitionId, (k, v) -> {
      if (v != null) {
        int streamConfigIndex = IngestionConfigUtils.getStreamConfigIndexFromPinotPartitionId(partitionId);
        StreamMetadataProvider streamMetadataProvider =
            _streamConfigIndexToStreamMetadataProvider.get(streamConfigIndex);
        // Remove all metrics associated with this partition
        if (streamMetadataProvider != null && streamMetadataProvider.supportsOffsetLag()) {
          _serverMetrics.removePartitionGauge(_metricName, partitionId, ServerGauge.REALTIME_INGESTION_OFFSET_LAG);
          _serverMetrics.removePartitionGauge(_metricName, partitionId, ServerGauge.REALTIME_INGESTION_UPSTREAM_OFFSET);
          _serverMetrics.removePartitionGauge(_metricName, partitionId,
              ServerGauge.REALTIME_INGESTION_CONSUMING_OFFSET);
        }
        _serverMetrics.removePartitionGauge(_metricName, partitionId, ServerGauge.REALTIME_INGESTION_DELAY_MS);
        LOGGER.info("Successfully removed ingestion metrics for partition id: {}", partitionId);
      }
      _ingestionInfoMap.remove(partitionId);
      return null;
    });

    // If we are removing a partition we should stop reading its ideal state.
    _partitionsMarkedForVerification.remove(partitionId);
  }

  /**
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
    int streamConfigIndex = IngestionConfigUtils.getStreamConfigIndexFromPinotPartitionId(partitionId);
    StreamMetadataProvider streamMetadataProvider = _streamConfigIndexToStreamMetadataProvider.get(streamConfigIndex);

    if (streamMetadataProvider != null && streamMetadataProvider.supportsOffsetLag()) {
      _serverMetrics.setOrUpdatePartitionGauge(_metricName, partitionId, ServerGauge.REALTIME_INGESTION_OFFSET_LAG,
          () -> getPartitionIngestionOffsetLag(partitionId));

      _serverMetrics.setOrUpdatePartitionGauge(_metricName, partitionId,
          ServerGauge.REALTIME_INGESTION_CONSUMING_OFFSET, () -> getPartitionIngestionConsumingOffset(partitionId));

      _serverMetrics.setOrUpdatePartitionGauge(_metricName, partitionId,
          ServerGauge.REALTIME_INGESTION_UPSTREAM_OFFSET, () -> getLatestPartitionOffset(partitionId));
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
    if (!_isServerReadyToServeQueries.getAsBoolean() || _realTimeTableDataManager.isShutDown()) {
      // Do not update the ingestion delay metrics during server startup period
      // or once the table data manager has been shutdown.
      return;
    }

    if ((ingestionTimeMs < 0) && (currentOffset == null)) {
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

  /**
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
  public Set<Integer> stopTrackingAllPartitions() {
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

  /**
   * This method is used for timing out inactive partitions, so we don't display their metrics on current server.
   * When the inactive time exceeds some threshold, we read from ideal state to confirm we still host the partition,
   * if not we remove the partition from being tracked locally.
   * This call is to be invoked by a scheduled executor thread that will periodically wake up and invoke this function.
   */
  public void timeoutInactivePartitions() {
    if (!_isServerReadyToServeQueries.getAsBoolean()) {
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
    if (!_isServerReadyToServeQueries.getAsBoolean() || _segmentsToIgnore.getIfPresent(segmentName) != null) {
      // Do not update the tracker state during server startup period or if the segment is marked to be ignored
      return;
    }
    _partitionsMarkedForVerification.put(new LLCSegmentName(segmentName).getPartitionGroupId(), _clock.millis());
  }

  /**
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

  /**
   * Method to get ingestion delay for a given partition.
   *
   * @param partitionId partition for which we are retrieving the delay
   *
   * @return ingestion delay in milliseconds for the given partition ID.
   */
  public long getPartitionIngestionDelayMs(int partitionId) {
    IngestionInfo ingestionInfo = _ingestionInfoMap.get(partitionId);
    long ingestionTimeMs = 0;
    if ((ingestionInfo != null) && (ingestionInfo._ingestionTimeMs > 0)) {
      ingestionTimeMs = ingestionInfo._ingestionTimeMs;
    }
    // Compute aged delay for current partition
    long agedIngestionDelayMs = _clock.millis() - ingestionTimeMs;
    // Correct to zero for any time shifts due to NTP or time reset.
    return Math.max(agedIngestionDelayMs, 0);
  }

  /**
   * Computes the ingestion lag for the given partition based on offset difference.
   * <p>
   * The lag is calculated as the difference between the latest upstream offset
   * and the current consuming offset. Only {@link LongMsgOffset} types are supported.
   *
   * @param partitionId partition for which the ingestion lag is computed
   * @return offset lag for the given partition
   */
  public long getPartitionIngestionOffsetLag(int partitionId) {
    StreamPartitionMsgOffset latestOffset = _partitionIdToLatestOffset.get(partitionId);
    if (latestOffset == null) {
      return 0;
    }
    IngestionInfo ingestionInfo = _ingestionInfoMap.get(partitionId);
    long currentOffset = 0;
    if (ingestionInfo != null) {
      StreamPartitionMsgOffset currentMsgOffset = ingestionInfo._currentOffset;
      if (currentMsgOffset == null) {
        // If currentOffset is set to null, it means:
        // 1. The stream does not support offset lag (example: Kinesis). But if this was true,
        // IngestionOffsetLag gauge will not be created and this method will never be called.
        // 2. Server has caught up. Think of scenario where server restarts and server is already caught up.
        return 0;
      }
      assert currentMsgOffset instanceof LongMsgOffset;
      currentOffset = ((LongMsgOffset) currentMsgOffset).getOffset();
    }
    assert latestOffset instanceof LongMsgOffset;
    return Math.max(0, ((LongMsgOffset) latestOffset).getOffset() - currentOffset);
  }

  /**
   * Retrieves the latest offset consumed for the given partition.
   *
   * @param partitionId partition for which the consuming offset was retrieved
   * @return consuming offset value for the given partition, or {@code 0} if no ingestion info is available
   */
  public long getPartitionIngestionConsumingOffset(int partitionId) {
    IngestionInfo ingestionInfo = _ingestionInfoMap.get(partitionId);
    if (ingestionInfo == null) {
      return 0;
    }
    StreamPartitionMsgOffset currentMsgOffset = ingestionInfo._currentOffset;
    if (currentMsgOffset == null) {
      // If currentOffset is set to null, it means:
      // 1. The stream does not support offset lag (example: Kinesis). But if this was true,
      // IngestionOffsetLag gauge will not be created and this method will never be called.
      // 2. Server has caught up. Think of scenario where server restarts and server is already caught up.
      return 0;
    }
    assert currentMsgOffset instanceof LongMsgOffset;
    return ((LongMsgOffset) currentMsgOffset).getOffset();
  }

  /**
   * Retrieves the latest offset in the upstream data source for the given partition.
   *
   * @param partitionId partition for which the latest upstream offset is retrieved
   * @return latest offset value for the given partition, or {@code 0} if not available
   */
  public long getLatestPartitionOffset(int partitionId) {
    StreamPartitionMsgOffset latestOffset = _partitionIdToLatestOffset.get(partitionId);
    if (latestOffset == null) {
      return 0;
    }
    assert latestOffset instanceof LongMsgOffset;
    return ((LongMsgOffset) latestOffset).getOffset();
  }

  /**
   * We use this method to clean up when a table is being removed. No updates are expected at this time as all
   * RealtimeSegmentManagers should be down now.
   */
  public void shutdown() {
    // Now that segments can't report metric, destroy metric for this table
    _metricsRemovalScheduler.shutdown(); // ScheduledExecutor is installed in constructor so must always be cancelled
    if (_ingestionDelayTrackingScheduler != null) {
      _ingestionDelayTrackingScheduler.shutdown();
    }
    for (StreamMetadataProvider streamMetadataProvider : _streamConfigIndexToStreamMetadataProvider.values()) {
      try {
        streamMetadataProvider.close();
      } catch (IOException e) {
        LOGGER.error("Failed to close streamMetadataProvider", e);
      }
    }
    if (!_isServerReadyToServeQueries.getAsBoolean()) {
      // Do not update the tracker state during server startup period
      return;
    }
    // Remove partitions so their related metrics get uninstalled.
    for (Integer partitionId : _ingestionInfoMap.keySet()) {
      removePartitionId(partitionId);
    }
  }
}
