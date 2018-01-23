/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix;

import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metrics.ControllerGauge;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.util.TableSizeReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.helix.ControllerChangeListener;
import org.apache.helix.HelixAdmin;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages the segment status metrics, regarding tables with fewer replicas than requested
 * and segments in error state
 *
 * May 15, 2016
*/

public class SegmentStatusChecker {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentStatusChecker.class);
  private static final int SegmentCheckerDefaultIntervalSeconds = 120;
  private static final int SegmentCheckerDefaultWaitForPushTimeSeconds = 600 * 1000;
  private static final int MaxOfflineSegmentsToLog = 5;
  private static final String CONTROLLER_LEADER_CHANGE = "CONTROLLER LEADER CHANGE";
  public static final String ONLINE = "ONLINE";
  public static final String ERROR = "ERROR";
  public static final String CONSUMING = "CONSUMING";
  private ScheduledExecutorService _executorService;
  private final ControllerMetrics _metricsRegistry;
  private final ControllerConf _config;
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final HelixAdmin _helixAdmin;
  private final long _segmentStatusIntervalSeconds;
  private final int _waitForPushTimeSeconds;
  private final TableSizeReader _tableSizeReader;

  /**
   * Constructs the segment status checker.
   * @param pinotHelixResourceManager The resource checker used to interact with Helix
   * @param config The controller configuration object
   */
  public SegmentStatusChecker(PinotHelixResourceManager pinotHelixResourceManager, ControllerConf config, ControllerMetrics metricsRegistry) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _helixAdmin = pinotHelixResourceManager.getHelixAdmin();
    _config = config;
    _segmentStatusIntervalSeconds = config.getStatusCheckerFrequencyInSeconds();
    _waitForPushTimeSeconds = config.getStatusCheckerWaitForPushTimeInSeconds();
    _metricsRegistry = metricsRegistry;
    HttpConnectionManager httpConnectionManager = new MultiThreadedHttpConnectionManager();
    _tableSizeReader = new TableSizeReader(Executors.newCachedThreadPool(), httpConnectionManager, _pinotHelixResourceManager);
  }

  /**
   * Starts the segment status checker.
   */
  public void start() {
    if (_segmentStatusIntervalSeconds == -1) {
      return;
    }

    setStatusToDefault();
    // Subscribe to leadership changes
    _pinotHelixResourceManager.getHelixZkManager().addControllerListener(new ControllerChangeListener() {
      @Override
      public void onControllerChange(NotificationContext changeContext) {
        processLeaderChange(CONTROLLER_LEADER_CHANGE);
      }
    });
  }

  private void startThread() {
    LOGGER.info("Starting segment status checker");

    if (_executorService == null) {
      _executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable runnable) {
          Thread thread = new Thread(runnable);
          thread.setName("SegStatChecker");
          return thread;
        }
      });
      // Set up an executor that executes segment status tasks periodically
      _executorService.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          try {
            runSegmentMetrics();
          } catch (Exception e) {
            LOGGER.warn("Caught exception while running segment status checker", e);
          }
        }
      }, SegmentCheckerDefaultIntervalSeconds, _segmentStatusIntervalSeconds, TimeUnit.SECONDS);
    } else {
      LOGGER.warn("SegmentStatusChecker already running. Attempt to start a duplicate thread");
    }
  }

  /**
   * Stops the segment status checker.
   */
  public void stop() {
    if (_executorService == null) {
      return;
    }
    stopThread();
  }

  private void stopThread() {
    // Shut down the executor
    _executorService.shutdown();
    try {
      _executorService.awaitTermination(SegmentCheckerDefaultIntervalSeconds, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
    }
    _executorService = null;
  }

  /**
   * Runs a segment status pass over the currently loaded tables.
   */
  public void runSegmentMetrics() {
    if (!_pinotHelixResourceManager.isLeader()) {
      LOGGER.info("Skipping Segment Status check, not leader!");
      setStatusToDefault();
      stop();
      return;
    }

    long startTime = System.nanoTime();

    LOGGER.info("Starting Segment Status check for metrics");

    // Fetch the list of tables
    List<String> allTableNames = _pinotHelixResourceManager.getAllTables();
    String helixClusterName = _pinotHelixResourceManager.getHelixClusterName();
    HelixAdmin helixAdmin = _pinotHelixResourceManager.getHelixAdmin();
    int realTimeTableCount = 0;
    int offlineTableCount = 0;
    ZkHelixPropertyStore<ZNRecord> propertyStore= _pinotHelixResourceManager.getPropertyStore();

    for (String tableName : allTableNames) {
      if (TableNameBuilder.getTableTypeFromTableName(tableName) == TableType.OFFLINE) {
        offlineTableCount++;
      } else {
        realTimeTableCount++;
      }
      IdealState idealState = helixAdmin.getResourceIdealState(helixClusterName, tableName);
      if ((idealState == null) || (idealState.getPartitionSet().isEmpty())) {
        int nReplicasFromIdealState = 1;
        try {
          if (idealState != null) {
            nReplicasFromIdealState = Integer.valueOf(idealState.getReplicas());
          }
        } catch (NumberFormatException e) {
          // Ignore
        }
        _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.NUMBER_OF_REPLICAS, nReplicasFromIdealState);
        _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.PERCENT_OF_REPLICAS, 100);
        _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE, 100);
        continue;
      }
      _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.IDEALSTATE_ZNODE_SIZE, idealState.toString().length());
      ExternalView externalView = helixAdmin.getResourceExternalView(helixClusterName, tableName);

      int nReplicasIdealMax = 0; // Keeps track of maximum number of replicas in ideal state
      int nReplicasExternal = -1; // Keeps track of minimum number of replicas in external view
      int nErrors = 0; // Keeps track of number of segments in error state
      int nOffline = 0; // Keeps track of number segments with no online replicas
      int nSegments = 0; // Counts number of segments
      for (String partitionName : idealState.getPartitionSet()) {
        int nReplicas = 0;
        int nIdeal = 0;
        nSegments++;
        // Skip segments not online in ideal state
        for (Map.Entry<String, String> serverAndState : idealState.getInstanceStateMap(partitionName).entrySet()) {
          if (serverAndState == null) {
            break;
          }
          if (serverAndState.getValue().equals(ONLINE)) {
            nIdeal++;
            break;
          }
        }
        if (nIdeal == 0) {
          // No online segments in ideal state
          continue;
        }
        nReplicasIdealMax = (idealState.getInstanceStateMap(partitionName).size() > nReplicasIdealMax)
            ? idealState.getInstanceStateMap(partitionName).size() : nReplicasIdealMax;
        if ((externalView == null) || (externalView.getStateMap(partitionName) == null)) {
          // No replicas for this segment
          TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
          if ((tableType != null) && (tableType.equals(TableType.OFFLINE))) {
            OfflineSegmentZKMetadata segmentZKMetadata =
                ZKMetadataProvider.getOfflineSegmentZKMetadata(propertyStore, tableName, partitionName);
            if (segmentZKMetadata != null && segmentZKMetadata.getPushTime() > System.currentTimeMillis() - _waitForPushTimeSeconds * 1000) {
              // push not yet finished, skip
              continue;
            }
          }
          nOffline++;
          if (nOffline < MaxOfflineSegmentsToLog) {
            LOGGER.warn("Segment {} of table {} has no replicas", partitionName, tableName);
          }
          nReplicasExternal = 0;
          continue;
        }
        for (Map.Entry<String, String> serverAndState : externalView.getStateMap(partitionName).entrySet()) {
          // Count number of online replicas. Ignore if state is CONSUMING.
          // It is possible for a segment to be ONLINE in idealstate, and CONSUMING in EV for a short period of time.
          // So, ignore this combination. If a segment exists in this combination for a long time, we will get
          // kafka-partition-not-consuming alert anyway.
          if (serverAndState.getValue().equals(ONLINE) || serverAndState.getValue().equals(CONSUMING)) {
            nReplicas++;
          }
          if (serverAndState.getValue().equals(ERROR)) {
            nErrors++;
          }
        }
        if (nReplicas == 0) {
          if (nOffline < MaxOfflineSegmentsToLog) {
            LOGGER.warn("Segment {} of table {} has no online replicas", partitionName, tableName);
          }
          nOffline++;
        }
        nReplicasExternal = ((nReplicasExternal > nReplicas) || (nReplicasExternal == -1)) ? nReplicas : nReplicasExternal;
      }
      if (nReplicasExternal == -1) {
        nReplicasExternal = (nReplicasIdealMax == 0) ? 1 : 0;
      }
      // Synchronization provided by Controller Gauge to make sure that only one thread updates the gauge
      _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.NUMBER_OF_REPLICAS,
          nReplicasExternal);
      _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.PERCENT_OF_REPLICAS,
          (nReplicasIdealMax > 0) ? (nReplicasExternal * 100 / nReplicasIdealMax) : 100);
      _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.SEGMENTS_IN_ERROR_STATE,
          nErrors);
      _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE,
          (nSegments > 0) ? (100 - (nOffline * 100 / nSegments)) : 100);

      // Emit estimated sizes of realtime/offline tables.
      TableSizeReader.TableSizeDetails tableSizeDetails = _tableSizeReader.getTableSizeDetails(tableName,
          _config.getServerAdminRequestTimeoutSeconds() * 1000);
      if (tableSizeDetails != null) {
        if (tableSizeDetails.realtimeSegments != null) {
          long realtimeTableEstimatedSize = tableSizeDetails.realtimeSegments.estimatedSizeInBytes;
          _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.REALTIME_TABLE_ESTIMATED_SIZE, realtimeTableEstimatedSize);
        }
        if (tableSizeDetails.offlineSegments != null) {
          long offlineTableEstimatedSize = tableSizeDetails.offlineSegments.estimatedSizeInBytes;
          _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.OFFLINE_TABLE_ESTIMATED_SIZE, offlineTableEstimatedSize);
        }
      }

      if (nOffline > 0) {
        LOGGER.warn("Table {} has {} segments with no online replicas", tableName, nOffline);
      }
      if (nReplicasExternal < nReplicasIdealMax) {
        LOGGER.warn("Table {} has {} replicas, below replication threshold :{}", tableName,
            nReplicasExternal, nReplicasIdealMax);
      }
    }
    _metricsRegistry.setValueOfGlobalGauge(ControllerGauge.REALTIME_TABLE_COUNT, realTimeTableCount);
    _metricsRegistry.setValueOfGlobalGauge(ControllerGauge.OFFLINE_TABLE_COUNT, offlineTableCount);
    long totalNanos = System.nanoTime() - startTime;
    LOGGER.info("Segment status metrics completed in {}ms",
        TimeUnit.MILLISECONDS.convert(totalNanos, TimeUnit.NANOSECONDS));
  }

  private void processLeaderChange(String path) {
    try {
      LOGGER.info("Processing change notification for path: {}", path);

      if (_pinotHelixResourceManager.isLeader()) {
        if (path.equals(CONTROLLER_LEADER_CHANGE)) {
          startThread();
        }
      } else {
        LOGGER.info("Not the leader of this cluster, stopping Status Checker.");
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception {} while processing leader change for path {}", e, path);
    }
  }

  private void setStatusToDefault() {
    // Fetch the list of tables
    List<String> allTableNames = _pinotHelixResourceManager.getAllTables();
    // Synchronization provided by Controller Gauge to make sure that only one thread updates the gauge
    for (String tableName : allTableNames) {
      _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.NUMBER_OF_REPLICAS, 0);
      _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.PERCENT_OF_REPLICAS, 0);
      _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.SEGMENTS_IN_ERROR_STATE, 0);
      _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE, 0);
    }
  }
}
