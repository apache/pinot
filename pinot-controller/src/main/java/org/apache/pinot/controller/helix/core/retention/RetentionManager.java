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
package org.apache.pinot.controller.helix.core.retention;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.helix.core.retention.strategy.RetentionStrategy;
import org.apache.pinot.controller.helix.core.retention.strategy.TimeRetentionStrategy;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>RetentionManager</code> class manages retention for all segments and delete expired segments.
 * <p>It is scheduled to run only on leader controller.
 */
public class RetentionManager extends ControllerPeriodicTask<Void> {
  public static final long OLD_LLC_SEGMENTS_RETENTION_IN_MILLIS = TimeUnit.DAYS.toMillis(5L);
  private static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicies.exponentialBackoffRetryPolicy(5, 1000L, 2.0f);

  private static final Logger LOGGER = LoggerFactory.getLogger(RetentionManager.class);

  public RetentionManager(PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerConf config, ControllerMetrics controllerMetrics) {
    super("RetentionManager", config.getRetentionControllerFrequencyInSeconds(),
        config.getRetentionManagerInitialDelayInSeconds(), pinotHelixResourceManager, leadControllerManager,
        controllerMetrics);

    LOGGER.info("Starting RetentionManager with runFrequencyInSeconds: {}", getIntervalInSeconds());
  }

  @Override
  protected void processTable(String tableNameWithType) {
    // Fetch table config
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      LOGGER.error("Failed to get table config for table: {}", tableNameWithType);
      return;
    }

    // Manage normal table retention except segment lineage cleanup.
    // The reason of separating the logic is that REFRESH only table will be skipped in the first part,
    // whereas the segment lineage cleanup needs to be handled.
    manageRetentionForTable(tableConfig);

    // Delete segments based on segment lineage and clean up segment lineage metadata.
    manageSegmentLineageCleanupForTable(tableConfig);
  }

  @Override
  protected void postprocess() {
    LOGGER.info("Removing aged deleted segments for all tables");
    _pinotHelixResourceManager.getSegmentDeletionManager().removeAgedDeletedSegments(_leadControllerManager);
  }

  private void manageRetentionForTable(TableConfig tableConfig) {
    String tableNameWithType = tableConfig.getTableName();
    LOGGER.info("Start managing retention for table: {}", tableNameWithType);

    // For offline tables, ensure that the segmentPushType is APPEND.
    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
    String segmentPushType = IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig);
    if (tableConfig.getTableType() == TableType.OFFLINE && !"APPEND".equalsIgnoreCase(segmentPushType)) {
      LOGGER.info("Segment push type is not APPEND for table: {}, skip managing retention", tableNameWithType);
      return;
    }
    String retentionTimeUnit = validationConfig.getRetentionTimeUnit();
    String retentionTimeValue = validationConfig.getRetentionTimeValue();
    RetentionStrategy retentionStrategy;
    try {
      retentionStrategy = new TimeRetentionStrategy(TimeUnit.valueOf(retentionTimeUnit.toUpperCase()),
          Long.parseLong(retentionTimeValue));
    } catch (Exception e) {
      LOGGER.warn("Invalid retention time: {} {} for table: {}, skip", retentionTimeUnit, retentionTimeValue,
          tableNameWithType);
      return;
    }

    // Scan all segment ZK metadata and purge segments if necessary
    if (TableNameBuilder.isOfflineTableResource(tableNameWithType)) {
      manageRetentionForOfflineTable(tableNameWithType, retentionStrategy);
    } else {
      manageRetentionForRealtimeTable(tableNameWithType, retentionStrategy);
    }
  }

  private void manageRetentionForOfflineTable(String offlineTableName, RetentionStrategy retentionStrategy) {
    List<String> segmentsToDelete = new ArrayList<>();
    for (SegmentZKMetadata segmentZKMetadata : _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName)) {
      if (retentionStrategy.isPurgeable(offlineTableName, segmentZKMetadata)) {
        segmentsToDelete.add(segmentZKMetadata.getSegmentName());
      }
    }
    if (!segmentsToDelete.isEmpty()) {
      LOGGER.info("Deleting {} segments from table: {}", segmentsToDelete.size(), offlineTableName);
      _pinotHelixResourceManager.deleteSegments(offlineTableName, segmentsToDelete);
    }
  }

  private void manageRetentionForRealtimeTable(String realtimeTableName, RetentionStrategy retentionStrategy) {
    List<String> segmentsToDelete = new ArrayList<>();
    IdealState idealState = _pinotHelixResourceManager.getHelixAdmin()
        .getResourceIdealState(_pinotHelixResourceManager.getHelixClusterName(), realtimeTableName);
    for (SegmentZKMetadata segmentZKMetadata : _pinotHelixResourceManager.getSegmentsZKMetadata(realtimeTableName)) {
      String segmentName = segmentZKMetadata.getSegmentName();
      if (segmentZKMetadata.getStatus() == Status.IN_PROGRESS) {
        // Delete old LLC segment that hangs around. Do not delete segment that are current since there may be a race
        // with RealtimeSegmentValidationManager trying to auto-create the LLC segment
        if (shouldDeleteInProgressLLCSegment(segmentName, idealState, segmentZKMetadata)) {
          segmentsToDelete.add(segmentName);
        }
      } else {
        // Sealed segment
        if (retentionStrategy.isPurgeable(realtimeTableName, segmentZKMetadata)) {
          segmentsToDelete.add(segmentName);
        }
      }
    }

    // Remove last sealed segments such that the table can still create new consuming segments if it's paused
    segmentsToDelete.removeAll(_pinotHelixResourceManager.getLastLLCCompletedSegments(realtimeTableName));

    if (!segmentsToDelete.isEmpty()) {
      LOGGER.info("Deleting {} segments from table: {}", segmentsToDelete.size(), realtimeTableName);
      _pinotHelixResourceManager.deleteSegments(realtimeTableName, segmentsToDelete);
    }
  }

  private boolean shouldDeleteInProgressLLCSegment(String segmentName, IdealState idealState,
      SegmentZKMetadata segmentZKMetadata) {
    if (idealState == null) {
      return false;
    }
    // delete a segment only if it is old enough (5 days) or else,
    // 1. latest segment could get deleted in the middle of repair by RealtimeSegmentValidationManager
    // 2. for a brand new segment, if this code kicks in after new metadata is created but ideal state entry is not
    // yet created (between step 2 and 3),
    // the latest segment metadata could get marked for deletion
    if (System.currentTimeMillis() - segmentZKMetadata.getCreationTime() <= OLD_LLC_SEGMENTS_RETENTION_IN_MILLIS) {
      return false;
    }
    Map<String, String> stateMap = idealState.getInstanceStateMap(segmentName);
    if (stateMap == null) {
      // Segment is in property store but not in ideal state, delete it
      return true;
    } else {
      // Delete segment if all of its replicas are OFFLINE
      Set<String> states = new HashSet<>(stateMap.values());
      return states.size() == 1 && states.contains(CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE);
    }
  }

  private void manageSegmentLineageCleanupForTable(TableConfig tableConfig) {
    String tableNameWithType = tableConfig.getTableName();
    try {
      DEFAULT_RETRY_POLICY.attempt(() -> {
        // Fetch segment lineage
        ZNRecord segmentLineageZNRecord =
            SegmentLineageAccessHelper.getSegmentLineageZNRecord(_pinotHelixResourceManager.getPropertyStore(),
                tableNameWithType);
        if (segmentLineageZNRecord == null) {
          return true;
        }
        LOGGER.info("Start cleaning up segment lineage for table: {}", tableNameWithType);
        long cleanupStartTime = System.currentTimeMillis();
        SegmentLineage segmentLineage = SegmentLineage.fromZNRecord(segmentLineageZNRecord);
        int expectedVersion = segmentLineageZNRecord.getVersion();

        List<String> segmentsForTable = _pinotHelixResourceManager.getSegmentsFor(tableNameWithType, false);
        List<String> segmentsToDelete = new ArrayList<>();
        _pinotHelixResourceManager.getLineageManager()
            .updateLineageForRetention(tableConfig, segmentLineage, segmentsForTable, segmentsToDelete,
                _pinotHelixResourceManager.getConsumingSegments(tableNameWithType));

        // Write back to the lineage entry
        if (SegmentLineageAccessHelper.writeSegmentLineage(_pinotHelixResourceManager.getPropertyStore(),
            segmentLineage, expectedVersion)) {
          // Remove last sealed segments such that the table can still create new consuming segments if it's paused
          if (TableNameBuilder.isRealtimeTableResource(tableNameWithType)) {
            segmentsToDelete.removeAll(_pinotHelixResourceManager.getLastLLCCompletedSegments(tableNameWithType));
          }
          // Delete segments based on the segment lineage
          if (!segmentsToDelete.isEmpty()) {
            _pinotHelixResourceManager.deleteSegments(tableNameWithType, segmentsToDelete);
            LOGGER.info("Finished cleaning up segment lineage for table: {} in {}ms, deleted segments: {}",
                tableNameWithType, (System.currentTimeMillis() - cleanupStartTime), segmentsToDelete);
          }
          return true;
        } else {
          LOGGER.warn("Failed to write segment lineage back when cleaning up segment lineage for table: {}",
              tableNameWithType);
          return false;
        }
      });
    } catch (Exception e) {
      String errorMsg = String.format("Failed to clean up the segment lineage. (tableName = %s)", tableNameWithType);
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
    LOGGER.info("Segment lineage metadata clean-up is successfully processed for table: {}", tableNameWithType);
  }
}
