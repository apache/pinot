/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.helix.core.retention;

import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import com.linkedin.pinot.controller.helix.core.retention.strategy.RetentionStrategy;
import com.linkedin.pinot.controller.helix.core.retention.strategy.TimeRetentionStrategy;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>RetentionManager</code> class manages retention for all segments and delete expired segments.
 * <p>It is scheduled to run only on leader controller.
 */
public class RetentionManager extends ControllerPeriodicTask {
  public static final long OLD_LLC_SEGMENTS_RETENTION_IN_MILLIS = TimeUnit.DAYS.toMillis(5L);

  private static final Logger LOGGER = LoggerFactory.getLogger(RetentionManager.class);

  private final int _deletedSegmentsRetentionInDays;

  public RetentionManager(PinotHelixResourceManager pinotHelixResourceManager, int runFrequencyInSeconds,
      int deletedSegmentsRetentionInDays) {
    super("RetentionManager", runFrequencyInSeconds, Math.min(60, runFrequencyInSeconds),
        pinotHelixResourceManager);
    _deletedSegmentsRetentionInDays = deletedSegmentsRetentionInDays;
  }

  @Override
  public void onBecomeLeader() {
    LOGGER.info("Starting RetentionManager with runFrequencyInSeconds: {}, deletedSegmentsRetentionInDays: {}",
        getIntervalInSeconds(), _deletedSegmentsRetentionInDays);
  }

  @Override
  public void process(List<String> allTableNames) {
    execute(allTableNames);
  }

  /**
   * Manages retention for all tables.
   * @param allTableNames List of all the table names
   */
  private void execute(List<String> allTableNames) {
    try {
      for (String tableNameWithType : allTableNames) {
        LOGGER.info("Start managing retention for table: {}", tableNameWithType);
        manageRetentionForTable(tableNameWithType);
      }

      LOGGER.info("Removing aged (more than {} days) deleted segments for all tables", _deletedSegmentsRetentionInDays);
      _pinotHelixResourceManager.getSegmentDeletionManager().removeAgedDeletedSegments(_deletedSegmentsRetentionInDays);
    } catch (Exception e) {
      LOGGER.error("Caught exception while managing retention for all tables", e);
    }
  }

  private void manageRetentionForTable(String tableNameWithType) {
    try {
      // Build retention strategy from table config
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
      if (tableConfig == null) {
        LOGGER.error("Failed to get table config for table: {}", tableNameWithType);
        return;
      }
      SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
      String segmentPushType = validationConfig.getSegmentPushType();
      if (!"APPEND".equalsIgnoreCase(segmentPushType)) {
        LOGGER.info("Segment push type is not APPEND for table: {}, skip", tableNameWithType);
        return;
      }
      String retentionTimeUnit = validationConfig.getRetentionTimeUnit();
      String retentionTimeValue = validationConfig.getRetentionTimeValue();
      RetentionStrategy retentionStrategy;
      try {
        retentionStrategy = new TimeRetentionStrategy(TimeUnit.valueOf(retentionTimeUnit.toUpperCase()),
            Long.parseLong(retentionTimeValue));
      } catch (Exception e) {
        LOGGER.warn("Invalid retention time: {} {} for table: {}, skip", retentionTimeUnit, retentionTimeValue);
        return;
      }

      // Scan all segment ZK metadata and purge segments if necessary
      if (TableNameBuilder.OFFLINE.tableHasTypeSuffix(tableNameWithType)) {
        manageRetentionForOfflineTable(tableNameWithType, retentionStrategy);
      } else {
        manageRetentionForRealtimeTable(tableNameWithType, retentionStrategy);
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while managing retention for table: {}", tableNameWithType, e);
    }
  }

  private void manageRetentionForOfflineTable(String offlineTableName, RetentionStrategy retentionStrategy) {
    List<String> segmentsToDelete = new ArrayList<>();
    for (OfflineSegmentZKMetadata offlineSegmentZKMetadata : _pinotHelixResourceManager.getOfflineSegmentMetadata(
        offlineTableName)) {
      if (retentionStrategy.isPurgeable(offlineSegmentZKMetadata)) {
        segmentsToDelete.add(offlineSegmentZKMetadata.getSegmentName());
      }
    }
    if (!segmentsToDelete.isEmpty()) {
      LOGGER.info("Deleting segments: {} from table: {}", segmentsToDelete, offlineTableName);
      _pinotHelixResourceManager.deleteSegments(offlineTableName, segmentsToDelete);
    }
  }

  private void manageRetentionForRealtimeTable(String realtimeTableName, RetentionStrategy retentionStrategy) {
    List<String> segmentsToDelete = new ArrayList<>();
    IdealState idealState = _pinotHelixResourceManager.getHelixAdmin()
        .getResourceIdealState(_pinotHelixResourceManager.getHelixClusterName(), realtimeTableName);
    for (RealtimeSegmentZKMetadata realtimeSegmentZKMetadata : _pinotHelixResourceManager.getRealtimeSegmentMetadata(
        realtimeTableName)) {
      String segmentName = realtimeSegmentZKMetadata.getSegmentName();
      if (realtimeSegmentZKMetadata.getStatus() == Status.IN_PROGRESS) {
        // In progress segment, only check LLC segment
        if (SegmentName.isLowLevelConsumerSegmentName(segmentName)) {
          // Delete old LLC segment that hangs around. Do not delete segment that are current since there may be a race
          // with ValidationManager trying to auto-create the LLC segment
          if (shouldDeleteInProgressLLCSegment(segmentName, idealState, realtimeSegmentZKMetadata)) {
            segmentsToDelete.add(segmentName);
          }
        }
      } else {
        // Sealed segment
        if (retentionStrategy.isPurgeable(realtimeSegmentZKMetadata)) {
          segmentsToDelete.add(segmentName);
        }
      }
    }
    if (!segmentsToDelete.isEmpty()) {
      LOGGER.info("Deleting segments: {} from table: {}", segmentsToDelete, realtimeTableName);
      _pinotHelixResourceManager.deleteSegments(realtimeTableName, segmentsToDelete);
    }
  }

  private boolean shouldDeleteInProgressLLCSegment(String segmentName, IdealState idealState,
      RealtimeSegmentZKMetadata realtimeSegmentZKMetadata) {
    if (idealState == null) {
      return false;
    }
    // delete a segment only if it is old enough (5 days) or else,
    // 1. latest segment could get deleted in the middle of repair by ValidationManager
    // 2. for a brand new segment, if this code kicks in after new metadata is created but ideal state entry is not yet created (between step 2 and 3),
    // the latest segment metadata could get marked for deletion
    if (System.currentTimeMillis() - realtimeSegmentZKMetadata.getCreationTime()
        <= OLD_LLC_SEGMENTS_RETENTION_IN_MILLIS) {
      return false;
    }
    Map<String, String> stateMap = idealState.getInstanceStateMap(segmentName);
    if (stateMap == null) {
      // Segment is in property store but not in ideal state, delete it
      return true;
    } else {
      // Delete segment if all of its replicas are OFFLINE
      Set<String> states = new HashSet<>(stateMap.values());
      return states.size() == 1 && states.contains(
          CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.OFFLINE);
    }
  }
}
