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
package org.apache.pinot.controller.helix;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages the segment status metrics, regarding tables with fewer replicas than requested
 * and segments in error state.
 */
public class SegmentStatusChecker extends ControllerPeriodicTask<SegmentStatusChecker.Context> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentStatusChecker.class);
  private static final int MaxOfflineSegmentsToLog = 5;
  public static final String ONLINE = "ONLINE";
  public static final String ERROR = "ERROR";
  public static final String CONSUMING = "CONSUMING";
  private final int _waitForPushTimeSeconds;

  // log messages about disabled tables atmost once a day
  private static final long DISABLED_TABLE_LOG_INTERVAL_MS = TimeUnit.DAYS.toMillis(1);
  private static final ZNRecordSerializer _recordSerializer = new ZNRecordSerializer();
  private long _lastDisabledTableLogTimestamp = 0;

  /**
   * Constructs the segment status checker.
   * @param pinotHelixResourceManager The resource checker used to interact with Helix
   * @param config The controller configuration object
   */
  public SegmentStatusChecker(PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerConf config, ControllerMetrics controllerMetrics) {
    super("SegmentStatusChecker", config.getStatusCheckerFrequencyInSeconds(),
        config.getStatusCheckerInitialDelayInSeconds(), pinotHelixResourceManager, leadControllerManager,
        controllerMetrics);

    _waitForPushTimeSeconds = config.getStatusCheckerWaitForPushTimeInSeconds();
  }

  @Override
  protected void setUpTask() {
    LOGGER.info("Initializing table metrics for all the tables.");
    setStatusToDefault();
  }

  @Override
  protected Context preprocess() {
    Context context = new Context();
    // check if we need to log disabled tables log messages
    long now = System.currentTimeMillis();
    if (now - _lastDisabledTableLogTimestamp >= DISABLED_TABLE_LOG_INTERVAL_MS) {
      context._logDisabledTables = true;
      _lastDisabledTableLogTimestamp = now;
    }
    return context;
  }

  @Override
  protected void processTable(String tableNameWithType, Context context) {
    try {
      updateSegmentMetrics(tableNameWithType, context);
    } catch (Exception e) {
      LOGGER.error("Caught exception while updating segment status for table {}", tableNameWithType, e);
      // Remove the metric for this table
      resetTableMetrics(tableNameWithType);
    }
  }

  @Override
  protected void postprocess(Context context) {
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.REALTIME_TABLE_COUNT, context._realTimeTableCount);
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.OFFLINE_TABLE_COUNT, context._offlineTableCount);
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.DISABLED_TABLE_COUNT, context._disabledTableCount);
  }

  /**
   * Runs a segment status pass over the given table.
   * TODO: revisit the logic and reduce the ZK access
   */
  private void updateSegmentMetrics(String tableNameWithType, Context context) {
    if (TableNameBuilder.getTableTypeFromTableName(tableNameWithType) == TableType.OFFLINE) {
      context._offlineTableCount++;
    } else {
      context._realTimeTableCount++;
    }

    IdealState idealState = _pinotHelixResourceManager.getTableIdealState(tableNameWithType);

    if (idealState == null) {
      LOGGER.warn("Table {} has null ideal state. Skipping segment status checks", tableNameWithType);
      resetTableMetrics(tableNameWithType);
      return;
    }

    if (!idealState.isEnabled()) {
      if (context._logDisabledTables) {
        LOGGER.warn("Table {} is disabled. Skipping segment status checks", tableNameWithType);
      }
      resetTableMetrics(tableNameWithType);
      context._disabledTableCount++;
      return;
    }

    if (idealState.getPartitionSet().isEmpty()) {
      int nReplicasFromIdealState = 1;
      try {
        nReplicasFromIdealState = Integer.valueOf(idealState.getReplicas());
      } catch (NumberFormatException e) {
        // Ignore
      }
      _controllerMetrics
          .setValueOfTableGauge(tableNameWithType, ControllerGauge.NUMBER_OF_REPLICAS, nReplicasFromIdealState);
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.PERCENT_OF_REPLICAS, 100);
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE, 100);
      return;
    }

    _controllerMetrics
        .setValueOfTableGauge(tableNameWithType, ControllerGauge.IDEALSTATE_ZNODE_SIZE, idealState.toString().length());
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.IDEALSTATE_ZNODE_BYTE_SIZE,
        idealState.serialize(_recordSerializer).length);
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.SEGMENT_COUNT,
        (long) (idealState.getPartitionSet().size()));
    ExternalView externalView = _pinotHelixResourceManager.getTableExternalView(tableNameWithType);

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
      nReplicasIdealMax = (idealState.getInstanceStateMap(partitionName).size() > nReplicasIdealMax) ? idealState
          .getInstanceStateMap(partitionName).size() : nReplicasIdealMax;
      if ((externalView == null) || (externalView.getStateMap(partitionName) == null)) {
        // No replicas for this segment
        TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
        if ((tableType != null) && (tableType.equals(TableType.OFFLINE))) {
          OfflineSegmentZKMetadata segmentZKMetadata =
              _pinotHelixResourceManager.getOfflineSegmentZKMetadata(tableNameWithType, partitionName);

          if (segmentZKMetadata != null
              && segmentZKMetadata.getPushTime() > System.currentTimeMillis() - _waitForPushTimeSeconds * 1000) {
            // push not yet finished, skip
            continue;
          }
        }
        nOffline++;
        if (nOffline < MaxOfflineSegmentsToLog) {
          LOGGER.warn("Segment {} of table {} has no replicas", partitionName, tableNameWithType);
        }
        nReplicasExternal = 0;
        continue;
      }
      for (Map.Entry<String, String> serverAndState : externalView.getStateMap(partitionName).entrySet()) {
        // Count number of online replicas. Ignore if state is CONSUMING.
        // It is possible for a segment to be ONLINE in idealstate, and CONSUMING in EV for a short period of time.
        // So, ignore this combination. If a segment exists in this combination for a long time, we will get
        // low level-partition-not-consuming alert anyway.
        if (serverAndState.getValue().equals(ONLINE) || serverAndState.getValue().equals(CONSUMING)) {
          nReplicas++;
        }
        if (serverAndState.getValue().equals(ERROR)) {
          nErrors++;
        }
      }
      if (nReplicas == 0) {
        if (nOffline < MaxOfflineSegmentsToLog) {
          LOGGER.warn("Segment {} of table {} has no online replicas", partitionName, tableNameWithType);
        }
        nOffline++;
      }
      nReplicasExternal =
          ((nReplicasExternal > nReplicas) || (nReplicasExternal == -1)) ? nReplicas : nReplicasExternal;
    }
    if (nReplicasExternal == -1) {
      nReplicasExternal = (nReplicasIdealMax == 0) ? 1 : 0;
    }
    // Synchronization provided by Controller Gauge to make sure that only one thread updates the gauge
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.NUMBER_OF_REPLICAS, nReplicasExternal);
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.PERCENT_OF_REPLICAS,
        (nReplicasIdealMax > 0) ? (nReplicasExternal * 100 / nReplicasIdealMax) : 100);
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.SEGMENTS_IN_ERROR_STATE, nErrors);
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE,
        (nSegments > 0) ? (100 - (nOffline * 100 / nSegments)) : 100);
    if (nOffline > 0) {
      LOGGER.warn("Table {} has {} segments with no online replicas", tableNameWithType, nOffline);
    }
    if (nReplicasExternal < nReplicasIdealMax) {
      LOGGER.warn("Table {} has {} replicas, below replication threshold :{}", tableNameWithType, nReplicasExternal,
          nReplicasIdealMax);
    }
  }

  private void setStatusToDefault() {
    List<String> allTableNames = _pinotHelixResourceManager.getAllTables();

    for (String tableName : allTableNames) {
      resetTableMetrics(tableName);
    }
  }

  private void resetTableMetrics(String tableName) {
    _controllerMetrics.setValueOfTableGauge(tableName, ControllerGauge.NUMBER_OF_REPLICAS, Long.MIN_VALUE);
    _controllerMetrics.setValueOfTableGauge(tableName, ControllerGauge.PERCENT_OF_REPLICAS, Long.MIN_VALUE);
    _controllerMetrics.setValueOfTableGauge(tableName, ControllerGauge.SEGMENTS_IN_ERROR_STATE, Long.MIN_VALUE);
    _controllerMetrics.setValueOfTableGauge(tableName, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE, Long.MIN_VALUE);
  }

  @Override
  public void cleanUpTask() {
    LOGGER.info("Resetting table metrics for all the tables.");
    setStatusToDefault();
  }

  public static final class Context {
    private boolean _logDisabledTables;
    private int _realTimeTableCount;
    private int _offlineTableCount;
    private int _disabledTableCount;
  }
}
