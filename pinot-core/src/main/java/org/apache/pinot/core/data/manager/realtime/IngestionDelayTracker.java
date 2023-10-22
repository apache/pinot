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
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Class to track realtime ingestion delay for table partitions on a given server.
 * Highlights:
 * 1-An object of this class is hosted by each RealtimeTableDataManager.
 * 2-The object tracks ingestion delays for all partitions hosted by the current server for the given Realtime table.
 * 3-Partition delays are updated by all LLRealtimeSegmentDataManager objects hosted in the corresponding
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
 *            |                         |<-updateIngestionDelay()-{LLRealtimeSegmentDataManager(Partition 0}}
 *            |                         |
 * ___________V_________________________V_
 * |           (Table X)                |<-updateIngestionDelay()-{LLRealtimeSegmentDataManager(Partition 1}}
 * | IngestionDelayTracker              |           ...
 * |____________________________________|<-updateIngestionDelay()-{LLRealtimeSegmentDataManager (Partition n}}
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

  // Class to wrap supported timestamps collected for an ingested event
  private static class IngestionTimestamps {
    IngestionTimestamps(long ingestionTimesMs, long firstStreamIngestionTimeMs) {
      _ingestionTimeMs = ingestionTimesMs;
      _firstStreamIngestionTimeMs = firstStreamIngestionTimeMs;
    }
    private final long _ingestionTimeMs;
    private final long _firstStreamIngestionTimeMs;
  }
  // Sleep interval for scheduled executor service thread that triggers read of ideal state
  private static final int SCHEDULED_EXECUTOR_THREAD_TICK_INTERVAL_MS = 300000; // 5 minutes +/- precision in timeouts
  // Once a partition is marked for verification, we wait 10 minutes to pull its ideal state.
  private static final int PARTITION_TIMEOUT_MS = 600000;          // 10 minutes timeouts
  // Delay scheduled executor service for this amount of time after starting service
  private static final int INITIAL_SCHEDULED_EXECUTOR_THREAD_DELAY_MS = 100;
  private static final Logger _logger = LoggerFactory.getLogger(IngestionDelayTracker.class.getSimpleName());

  // HashMap used to store ingestion time measures for all partitions active for the current table.
  private final Map<Integer, IngestionTimestamps> _partitionToIngestionTimestampsMap = new ConcurrentHashMap<>();
  // We mark partitions that go from CONSUMING to ONLINE in _partitionsMarkedForVerification: if they do not
  // go back to CONSUMING in some period of time, we verify whether they are still hosted in this server by reading
  // ideal state. This is done with the goal of minimizing reading ideal state for efficiency reasons.
  private final Map<Integer, Long> _partitionsMarkedForVerification = new ConcurrentHashMap<>();

  final int _scheduledExecutorThreadTickIntervalMs;
  // TODO: Make thread pool a server/cluster level config
  // ScheduledExecutorService to check partitions that are inactive against ideal state.
  private final ScheduledExecutorService _scheduledExecutor = Executors.newScheduledThreadPool(2);

  private final ServerMetrics _serverMetrics;
  private final String _tableNameWithType;
  private final String _metricName;

  private final RealtimeTableDataManager _realTimeTableDataManager;
  private final Supplier<Boolean> _isServerReadyToServeQueries;

  private Clock _clock;

  public IngestionDelayTracker(ServerMetrics serverMetrics, String tableNameWithType,
      RealtimeTableDataManager realtimeTableDataManager, int scheduledExecutorThreadTickIntervalMs,
      Supplier<Boolean> isServerReadyToServeQueries)
      throws RuntimeException {
    _serverMetrics = serverMetrics;
    _tableNameWithType = tableNameWithType;
    _metricName = tableNameWithType;
    _realTimeTableDataManager = realtimeTableDataManager;
    _clock = Clock.systemUTC();
    _isServerReadyToServeQueries = isServerReadyToServeQueries;
    // Handle negative timer values
    if (scheduledExecutorThreadTickIntervalMs <= 0) {
      throw new RuntimeException(String.format("Illegal timer timeout argument, expected > 0, got=%d for table=%s",
              scheduledExecutorThreadTickIntervalMs, _tableNameWithType));
    }
    _scheduledExecutorThreadTickIntervalMs = scheduledExecutorThreadTickIntervalMs;

    // ThreadFactory to set the thread's name
    ThreadFactory threadFactory = new ThreadFactory() {
      private final ThreadFactory _defaultFactory = Executors.defaultThreadFactory();

      @Override
      public Thread newThread(Runnable r) {
        Thread thread = _defaultFactory.newThread(r);
        thread.setName("IngestionDelayTimerThread-" + TableNameBuilder.extractRawTableName(tableNameWithType));
        return thread;
      }
    };
    ((ScheduledThreadPoolExecutor) _scheduledExecutor).setThreadFactory(threadFactory);

    _scheduledExecutor.scheduleWithFixedDelay(this::timeoutInactivePartitions,
            INITIAL_SCHEDULED_EXECUTOR_THREAD_DELAY_MS, _scheduledExecutorThreadTickIntervalMs, TimeUnit.MILLISECONDS);
  }

  public IngestionDelayTracker(ServerMetrics serverMetrics, String tableNameWithType,
      RealtimeTableDataManager tableDataManager, Supplier<Boolean> isServerReadyToServeQueries) {
    this(serverMetrics, tableNameWithType, tableDataManager, SCHEDULED_EXECUTOR_THREAD_TICK_INTERVAL_MS,
        isServerReadyToServeQueries);
  }

  /*
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
   * @param partitionGroupId partition ID which we should stop tracking.
   */
  private void removePartitionId(int partitionGroupId) {
    _partitionToIngestionTimestampsMap.remove(partitionGroupId);
    // If we are removing a partition we should stop reading its ideal state.
    _partitionsMarkedForVerification.remove(partitionGroupId);
    _serverMetrics.removePartitionGauge(_metricName, partitionGroupId, ServerGauge.REALTIME_INGESTION_DELAY_MS);
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

  /*
   * Called by LLRealTimeSegmentDataManagers to post ingestion time updates to this tracker class.
   *
   * @param ingestionTimeMs ingestion time being recorded.
   * @param firstStreamIngestionTimeMs time the event was ingested in the first stage of the ingestion pipeline.
   * @param partitionGroupId partition ID for which this ingestion time is being recorded.
   */
  public void updateIngestionDelay(long ingestionTimeMs, long firstStreamIngestionTimeMs, int partitionGroupId) {
    // Store new measure and wipe old one for this partition
    if (!_isServerReadyToServeQueries.get()) {
      // Do not update the ingestion delay metrics during server startup period
      return;
    }
    if ((ingestionTimeMs < 0) && (firstStreamIngestionTimeMs < 0)) {
      // If stream does not return a valid ingestion timestamps don't publish a metric
      return;
    }
    IngestionTimestamps previousMeasure = _partitionToIngestionTimestampsMap.put(partitionGroupId,
        new IngestionTimestamps(ingestionTimeMs, firstStreamIngestionTimeMs));
    if (previousMeasure == null) {
      // First time we start tracking a partition we should start tracking it via metric
      // Only publish the metric if supported by the underlying stream. If not supported the stream
      // returns Long.MIN_VALUE
      if (ingestionTimeMs >= 0) {
        _serverMetrics.setOrUpdatePartitionGauge(_metricName, partitionGroupId, ServerGauge.REALTIME_INGESTION_DELAY_MS,
            () -> getPartitionIngestionDelayMs(partitionGroupId));
      }
      if (firstStreamIngestionTimeMs >= 0) {
        // Only publish this metric when creation time is supported by the underlying stream
        // When this timestamp is not supported it always returns the value Long.MIN_VALUE
        _serverMetrics.setOrUpdatePartitionGauge(_metricName, partitionGroupId,
            ServerGauge.END_TO_END_REALTIME_INGESTION_DELAY_MS,
            () -> getPartitionEndToEndIngestionDelayMs(partitionGroupId));
      }
    }
    // If we are consuming we do not need to track this partition for removal.
    _partitionsMarkedForVerification.remove(partitionGroupId);
  }

  /*
   * Handle partition removal event. This must be invoked when we stop serving a given partition for
   * this table in the current server.
   *
   * @param partitionGroupId partition id that we should stop tracking.
   */
  public void stopTrackingPartitionIngestionDelay(int partitionGroupId) {
    removePartitionId(partitionGroupId);
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
    if (partitionsToVerify.size() == 0) {
      // Don't make the call to getHostedPartitionsGroupIds() as it involves checking ideal state.
      return;
    }
    Set<Integer> partitionsHostedByThisServer;
    try {
      partitionsHostedByThisServer = _realTimeTableDataManager.getHostedPartitionsGroupIds();
    } catch (Exception e) {
      _logger.error("Failed to get partitions hosted by this server, table={}, exception={}:{}", _tableNameWithType,
          e.getClass(), e.getMessage());
      return;
    }
    for (int partitionGroupId : partitionsToVerify) {
      if (!partitionsHostedByThisServer.contains(partitionGroupId)) {
        // Partition is not hosted in this server anymore, stop tracking it
        removePartitionId(partitionGroupId);
      }
    }
  }

  /*
   * This function is invoked when a partition goes from CONSUMING to ONLINE, so we can assert whether the
   * partition is still hosted by this server after some interval of time.
   *
   * @param partitionGroupId Partition id that we need confirmed via ideal state as still hosted by this server.
   */
  public void markPartitionForVerification(int partitionGroupId) {
    if (!_isServerReadyToServeQueries.get()) {
      // Do not update the tracker state during server startup period
      return;
    }
    _partitionsMarkedForVerification.put(partitionGroupId, _clock.millis());
  }

  /*
   * Method to get ingestion delay for a given partition.
   *
   * @param partitionGroupId partition for which we are retrieving the delay
   *
   * @return ingestion delay in milliseconds for the given partition ID.
   */
  public long getPartitionIngestionDelayMs(int partitionGroupId) {
    // Not protected as this will only be invoked when metric is installed which happens after server ready
    IngestionTimestamps currentMeasure = _partitionToIngestionTimestampsMap.get(partitionGroupId);
    if (currentMeasure == null) { // Guard just in case we read the metric without initializing it
      return 0;
    }
    return getIngestionDelayMs(currentMeasure._ingestionTimeMs);
  }

  /*
   * Method to get end to end ingestion delay for a given partition.
   *
   * @param partitionGroupId partition for which we are retrieving the delay
   *
   * @return End to end ingestion delay in milliseconds for the given partition ID.
   */
  public long getPartitionEndToEndIngestionDelayMs(int partitionGroupId) {
    // Not protected as this will only be invoked when metric is installed which happens after server ready
    IngestionTimestamps currentMeasure = _partitionToIngestionTimestampsMap.get(partitionGroupId);
    if (currentMeasure == null) { // Guard just in case we read the metric without initializing it
      return 0;
    }
    return getIngestionDelayMs(currentMeasure._firstStreamIngestionTimeMs);
  }

  /*
   * We use this method to clean up when a table is being removed. No updates are expected at this time
   * as all LLRealtimeSegmentManagers should be down now.
   */
  public void shutdown() {
    // Now that segments can't report metric, destroy metric for this table
    _scheduledExecutor.shutdown(); // ScheduledExecutor is installed in constructor so must always be cancelled
    if (!_isServerReadyToServeQueries.get()) {
      // Do not update the tracker state during server startup period
      return;
    }
    // Remove partitions so their related metrics get uninstalled.
    for (Map.Entry<Integer, IngestionTimestamps> entry : _partitionToIngestionTimestampsMap.entrySet()) {
      removePartitionId(entry.getKey());
    }
  }
}
