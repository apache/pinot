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
import java.util.HashSet;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Class to track realtime ingestion delay for a given table on a given server.
 * Highlights:
 * 1-An object of this class is hosted by each RealtimeTableDataManager.
 * 2-The object tracks ingestion delays for all partitions hosted by the current server for the given Realtime table.
 * 3-Partition delays are updated by all LLRealtimeSegmentDataManager objects hosted in the corresponding
 *   RealtimeTableDataManager.
 * 4-A Metric is derived from reading the maximum tracked by this class. In addition, individual metrics are associated
 *   with each partition being tracked.
 * 5-Delays reported for partitions that do not have events to consume are reported as zero.
 * 6-We track the time at which each delay sample was collected so that delay can be increased when partition stops
 *   consuming for any reason other than no events being available for consumption.
 * 7-Partitions whose Segments go from CONSUMING to DROPPED state stop being tracked so their delays do not cloud
 *   delays of active partitions.
 * 8-When a segment goes from CONSUMING to ONLINE, we start a timeout for the corresponding partition.
 *   If no consumption is noticed after the timeout, we then read ideal state to confirm the server still hosts the
 *   partition. If not, we stop tracking the respective partition.
 * 9-A timer thread is started by this object to track timeouts of partitions and drive the reading of their ideal
 *  state.
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
 */

public class IngestionDelayTracker {

  // Sleep interval for timer thread that triggers read of ideal state
  private static final int TIMER_THREAD_TICK_INTERVAL_MS = 300000; // 5 minutes +/- precision in timeouts
  // Once a partition is marked for verification, we wait 10 minutes to pull its ideal state.
  private static final int PARTITION_TIMEOUT_MS = 600000;          // 10 minutes timeouts
  // Delay Timer thread for this amount of time after starting timer
  private static final int INITIAL_TIMER_THREAD_DELAY_MS = 100;

  /*
   * Class to keep an ingestion delay measure and the time when the sample was taken (i.e. sample time)
   * We will use the sample time to increase ingestion delay when a partition stops consuming: the time
   * difference between the sample time and current time will be added to the metric when read.
   */
  static private class DelayMeasure {
    public DelayMeasure(long t, long d) {
      _delayMilliseconds = d;
      _sampleTime = t;
    }
    public final long _delayMilliseconds;
    public final long _sampleTime;
  }

  // HashMap used to store delay measures for all partitions active for the current table.
  private final ConcurrentHashMap<Integer, DelayMeasure> _partitionToDelaySampleMap = new ConcurrentHashMap<>();
  // We mark partitions that go from CONSUMING to ONLINE in _partitionsMarkedForVerification: if they do not
  // go back to CONSUMING in some period of time, we confirm whether they are still hosted in this server by reading
  // ideal state. This is done with the goal of minimizing reading ideal state for efficiency reasons.
  private final ConcurrentHashMap<Integer, Long> _partitionsMarkedForVerification = new ConcurrentHashMap<>();

  final int _timerThreadTickIntervalMs;
  // Timer task to check partitions that are inactive against ideal state.
  private final Timer _timer;

  private final ServerMetrics _serverMetrics;
  private final String _tableNameWithType;
  private final String _metricName;

  private final boolean _enablePerPartitionMetric;
  private final boolean _enableAggregateMetric;
  private final Logger _logger;

  private final RealtimeTableDataManager _realTimeTableDataManager;
  private Clock _clock;

  /*
   * Returns the maximum ingestion delay amongs all partitions we are tracking.
   */
  private DelayMeasure getMaximumDelay() {
    DelayMeasure newMax = null;
    for (int partitionGroupId : _partitionToDelaySampleMap.keySet()) {
      DelayMeasure currentMeasure = _partitionToDelaySampleMap.get(partitionGroupId);
      if ((newMax == null)
          ||
          (currentMeasure != null) && (currentMeasure._delayMilliseconds > newMax._delayMilliseconds)) {
        newMax = currentMeasure;
      }
    }
    return newMax;
  }

  /*
   * Helper function to age a delay measure. Aging means adding the time elapsed since the measure was
   * taken till the measure is being reported.
  *
  * @param currentDelay original sample delay to which we will add the age of the measure.
   */
  private long getAgedDelay(DelayMeasure currentDelay) {
    if (currentDelay == null) {
      return 0; // return 0 when not initialized
    }
    // Add age of measure to the reported value
    long measureAgeInMs = _clock.millis() - currentDelay._sampleTime;
    // Correct to zero for any time shifts due to NTP or time reset.
    measureAgeInMs = Math.max(measureAgeInMs, 0);
    return currentDelay._delayMilliseconds + measureAgeInMs;
  }

  /*
   * Helper function to be called when we should stop tracking a given partition. Removes the partition from
   * all our maps.
   *
   * @param partitionGroupId partition ID which we should stop tracking.
   */
  private void removePartitionId(int partitionGroupId) {
    _partitionToDelaySampleMap.remove(partitionGroupId);
    // If we are removing a partition we should stop reading its ideal state.
    _partitionsMarkedForVerification.remove(partitionGroupId);
    if (_enablePerPartitionMetric) {
      _serverMetrics.removeTableGauge(getPerPartitionMetricName(partitionGroupId),
          ServerGauge.TABLE_INGESTION_DELAY_MS);
    }
  }

  /*
   * Helper function to generate a per partition metric name.
   *
   * @param partitionGroupId the partition group id to be appended to the table name so we
   *        can differentiate between metrics for various partitions.
   *
   * @return a metric name with the following structure: _metricName + partitionGroupId
   */
  private String getPerPartitionMetricName(int partitionGroupId) {
    return _metricName + partitionGroupId;
  }

  /*
   * Helper functions that creates a list of all the partitions that are marked for verification and whose
   * timeouts are expired. This helps us optimize checks of the ideal state.
   */
  private ArrayList<Integer> getPartitionsToBeVerified() {
    ArrayList<Integer> partitionsToVerify = new ArrayList<>();
    for (int partitionGroupId : _partitionsMarkedForVerification.keySet()) {
      long markTime = _partitionsMarkedForVerification.get(partitionGroupId);
      long timeMarked = _clock.millis() - markTime;
      if (timeMarked > PARTITION_TIMEOUT_MS) {
        // Partition must be verified
        partitionsToVerify.add(partitionGroupId);
      }
    }
    return partitionsToVerify;
  }

  // Custom Constructor
  public IngestionDelayTracker(ServerMetrics serverMetrics, String tableNameWithType,
      RealtimeTableDataManager realtimeTableDataManager, int timerThreadTickIntervalMs, String metricNamePrefix,
      boolean enableAggregateMetric, boolean enablePerPartitionMetric)
      throws RuntimeException {
    _logger = LoggerFactory.getLogger(getClass().getSimpleName());
    _serverMetrics = serverMetrics;
    _tableNameWithType = tableNameWithType;
    _metricName = metricNamePrefix + tableNameWithType;
    _realTimeTableDataManager = realtimeTableDataManager;
    _clock = Clock.systemDefaultZone();
    // Handle negative timer values
    if (timerThreadTickIntervalMs <= 0) {
      throw new RuntimeException(String.format("Illegal timer timeout argument, expected > 0, got=%d for table=%s",
          timerThreadTickIntervalMs, _tableNameWithType));
    }
    _enablePerPartitionMetric = enablePerPartitionMetric;
    _enableAggregateMetric = enableAggregateMetric;
    _timerThreadTickIntervalMs = timerThreadTickIntervalMs;
    _timer = new Timer("IngestionDelayTimerThread" + TableNameBuilder.extractRawTableName(tableNameWithType));
    _timer.schedule(new TimerTask() {
        @Override
        public void run() {
          timeoutInactivePartitions();
        }
      }, INITIAL_TIMER_THREAD_DELAY_MS, _timerThreadTickIntervalMs);
    // Install callback metric
    if (_enableAggregateMetric) {
      _serverMetrics.addCallbackTableGaugeIfNeeded(_metricName, ServerGauge.TABLE_INGESTION_DELAY_MS,
          () -> getMaximumIngestionDelay());
    }
  }

  // Constructor that uses defaults
  public IngestionDelayTracker(ServerMetrics serverMetrics, String tableNameWithType,
      RealtimeTableDataManager tableDataManager) {
    this(serverMetrics, tableNameWithType, tableDataManager, TIMER_THREAD_TICK_INTERVAL_MS,
        "", true, true);
  }

  // Constructor that takes a prefix to name the metric, so we can keep multiple trackers for the same table
  public IngestionDelayTracker(ServerMetrics serverMetrics, String tableNameWithType, String metricNamePrefix,
      RealtimeTableDataManager tableDataManager) {
    this(serverMetrics, metricNamePrefix + tableNameWithType, tableDataManager,
        TIMER_THREAD_TICK_INTERVAL_MS, metricNamePrefix, true, true);
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
   * Called by LLRealTimeSegmentDataManagers to post delay updates to this tracker class.
   *
   * @param delayInMilliseconds ingestion delay being recorded.
   * @param sampleTime sample time.
   * @param partitionGroupId partition ID for which this delay is being recorded.
   */
  public void updateIngestionDelay(long delayInMilliseconds, long sampleTime, int partitionGroupId) {
    // Store new measure and wipe old one for this partition
    DelayMeasure previousMeasure = _partitionToDelaySampleMap.put(partitionGroupId,
        new DelayMeasure(sampleTime, delayInMilliseconds));
    if ((previousMeasure == null) && _enablePerPartitionMetric) {
      // First time we start tracking a partition we should start tracking it via metric
      _serverMetrics.addCallbackTableGaugeIfNeeded(getPerPartitionMetricName(partitionGroupId),
          ServerGauge.TABLE_INGESTION_DELAY_MS,
          () -> getPartitionIngestionDelay(partitionGroupId));
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
   * This call is to be invoked by a timer thread that will periodically wake up and invoke this function.
   */
  public void timeoutInactivePartitions() {
    List<Integer> partitionsHostedByThisServer = null;
    // Check if we have any partition to verify, else don't make the call to check ideal state as that
    // involves network traffic and may be inefficient.
    ArrayList<Integer> partitionsToVerify = getPartitionsToBeVerified();
    if (partitionsToVerify.size() == 0) {
      // Don't make the call to getHostedPartitionsGroupIds() as it involves checking ideal state.
      return;
    }
    try {
      partitionsHostedByThisServer = _realTimeTableDataManager.getHostedPartitionsGroupIds();
    } catch (Exception e) {
      _logger.error("Failed to get partitions hosted by this server, table={}", _tableNameWithType);
      return;
    }
    // We create this hash to check for partitionsGroupId in O(1) vs O(n) for a list
    HashSet<Integer> hostedPartitions = new HashSet(partitionsHostedByThisServer);
    for (int partitionGroupId : partitionsToVerify) {
      if (!hostedPartitions.contains(partitionGroupId)) {
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
    _partitionsMarkedForVerification.put(partitionGroupId, _clock.millis());
  }

  /*
   * This is the function to be invoked when reading the metric.
   * It reports the maximum ingestion delay for all partitions of this table being served
   * by current server; it adds the time elapsed since the sample was taken to the measure.
   * If no measures have been taken, then the reported value is zero.
   *
   * @return max ingestion delay in milliseconds.
   */
  public long getMaximumIngestionDelay() {
    DelayMeasure currentMaxDelay = getMaximumDelay();
    return getAgedDelay(currentMaxDelay);
  }

  /*
   * Method to get ingestion delay for a given partition.
   *
   * @param partitionGroupId partition for which we are retrieving the delay
   *
   * @return ingestion delay in milliseconds for the given partition ID.
   */
  public long getPartitionIngestionDelay(int partitionGroupId) {
    DelayMeasure currentMeasure = _partitionToDelaySampleMap.get(partitionGroupId);
    return getAgedDelay(currentMeasure);
  }

  /*
   * We use this method to clean up when a table is being removed. No updates are expected at this time
   * as all LLRealtimeSegmentManagers should be down now.
   */
  public void shutdown() {
    // Now that segments can't report metric, destroy metric for this table
    _timer.cancel();
    if (_enableAggregateMetric) {
      _serverMetrics.removeTableGauge(_metricName, ServerGauge.TABLE_INGESTION_DELAY_MS);
    }
    // Remove partitions so their related metrics get uninstalled.
    if (_enablePerPartitionMetric) {
      for (int partitionGroupId : _partitionToDelaySampleMap.keySet()) {
        removePartitionId(partitionGroupId);
      }
    }
  }
}
