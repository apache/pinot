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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import junit.framework.Assert;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.lineage.LineageEntry;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.common.utils.SegmentName;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.helix.core.retention.strategy.RetentionStrategy;
import org.apache.pinot.controller.helix.core.retention.strategy.TimeRetentionStrategy;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
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

  public static final long LINEAGE_ENTRY_CLEANUP_RETENTION_IN_MILLIS = TimeUnit.DAYS.toMillis(1L); // 1 day
  private static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicies.exponentialBackoffRetryPolicy(5, 1000L, 2.0f);

  private static final Logger LOGGER = LoggerFactory.getLogger(RetentionManager.class);

  private final int _deletedSegmentsRetentionInDays;

  public RetentionManager(PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerConf config, ControllerMetrics controllerMetrics) {
    super("RetentionManager", config.getRetentionControllerFrequencyInSeconds(),
        config.getRetentionManagerInitialDelayInSeconds(), pinotHelixResourceManager, leadControllerManager,
        controllerMetrics);
    _deletedSegmentsRetentionInDays = config.getDeletedSegmentsRetentionInDays();

    LOGGER.info("Starting RetentionManager with runFrequencyInSeconds: {}, deletedSegmentsRetentionInDays: {}",
        getIntervalInSeconds(), _deletedSegmentsRetentionInDays);
  }

  @Override
  protected void processTable(String tableNameWithType) {
    LOGGER.info("Start managing retention for table: {}", tableNameWithType);
    manageRetentionForTable(tableNameWithType);
  }

  @Override
  protected void postprocess() {
    LOGGER.info("Removing aged (more than {} days) deleted segments for all tables", _deletedSegmentsRetentionInDays);
    _pinotHelixResourceManager.getSegmentDeletionManager().removeAgedDeletedSegments(_deletedSegmentsRetentionInDays);
  }

  private void manageRetentionForTable(String tableNameWithType) {

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
    if (TableNameBuilder.isOfflineTableResource(tableNameWithType)) {
      manageRetentionForOfflineTable(tableNameWithType, retentionStrategy);
    } else {
      manageRetentionForRealtimeTable(tableNameWithType, retentionStrategy);
    }

    // Delete segments based on segment lineage and clean up segment lineage metadata
    manageSegmentLineageCleanupForTable(tableNameWithType);
  }

  private void manageRetentionForOfflineTable(String offlineTableName, RetentionStrategy retentionStrategy) {
    List<String> segmentsToDelete = new ArrayList<>();
    for (OfflineSegmentZKMetadata offlineSegmentZKMetadata : _pinotHelixResourceManager
        .getOfflineSegmentMetadata(offlineTableName)) {
      if (retentionStrategy.isPurgeable(offlineTableName, offlineSegmentZKMetadata)) {
        segmentsToDelete.add(offlineSegmentZKMetadata.getSegmentName());
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
    for (RealtimeSegmentZKMetadata realtimeSegmentZKMetadata : _pinotHelixResourceManager
        .getRealtimeSegmentMetadata(realtimeTableName)) {
      String segmentName = realtimeSegmentZKMetadata.getSegmentName();
      if (realtimeSegmentZKMetadata.getStatus() == Status.IN_PROGRESS) {
        // In progress segment, only check LLC segment
        if (SegmentName.isLowLevelConsumerSegmentName(segmentName)) {
          // Delete old LLC segment that hangs around. Do not delete segment that are current since there may be a race
          // with RealtimeSegmentValidationManager trying to auto-create the LLC segment
          if (shouldDeleteInProgressLLCSegment(segmentName, idealState, realtimeSegmentZKMetadata)) {
            segmentsToDelete.add(segmentName);
          }
        }
      } else {
        // Sealed segment
        if (retentionStrategy.isPurgeable(realtimeTableName, realtimeSegmentZKMetadata)) {
          segmentsToDelete.add(segmentName);
        }
      }
    }
    if (!segmentsToDelete.isEmpty()) {
      LOGGER.info("Deleting {} segments from table: {}", segmentsToDelete.size(), realtimeTableName);
      _pinotHelixResourceManager.deleteSegments(realtimeTableName, segmentsToDelete);
    }
  }

  private boolean shouldDeleteInProgressLLCSegment(String segmentName, IdealState idealState,
      RealtimeSegmentZKMetadata realtimeSegmentZKMetadata) {
    if (idealState == null) {
      return false;
    }
    // delete a segment only if it is old enough (5 days) or else,
    // 1. latest segment could get deleted in the middle of repair by RealtimeSegmentValidationManager
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
      return states.size() == 1 && states.contains(CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE);
    }
  }

  private void manageSegmentLineageCleanupForTable(String tableNameWithType) {
    try {
      DEFAULT_RETRY_POLICY.attempt(() -> {
        // Fetch segment lineage
        ZNRecord segmentLineageZNRecord = SegmentLineageAccessHelper
            .getSegmentLineageZNRecord(_pinotHelixResourceManager.getPropertyStore(), tableNameWithType);
        if (segmentLineageZNRecord == null) {
          LOGGER.info("Segment lineage does not exist for table: {}", tableNameWithType);
          return true;
        }
        SegmentLineage segmentLineage = SegmentLineage.fromZNRecord(segmentLineageZNRecord);
        int expectedVersion = segmentLineageZNRecord.getVersion();

        // Delete segments based on the segment lineage
        PinotResourceManagerResponse response = _pinotHelixResourceManager
            .deleteSegments(tableNameWithType, computeSegmentsToDeleteFromSegmentLineage(segmentLineage));

        Assert.assertTrue(response.isSuccessful());

        // Fetch available segments for the table
        Set<String> segmentsForTable = new HashSet<>(_pinotHelixResourceManager.getSegmentsFor(tableNameWithType));

        // Clean up the segment lineage
        for (String lineageEntryId : segmentLineage.getLineageEntryIds()) {
          LineageEntry lineageEntry = segmentLineage.getLineageEntry(lineageEntryId);
          if (lineageEntry.getState() == LineageEntryState.COMPLETED) {
            // The lineage entry for 'COMPLETED' state can only be safely removed when both segmentFrom & segmentTo
            // are all removed from the table.
            if (Collections.disjoint(segmentsForTable, lineageEntry.getSegmentsFrom()) && Collections
                .disjoint(segmentsForTable, lineageEntry.getSegmentsTo())) {
              segmentLineage.deleteLineageEntry(lineageEntryId);
            }
          } else if (lineageEntry.getState() == LineageEntryState.IN_PROGRESS) {
            // Zombie lineage entry is safe to remove. This will allow the task scheduler to re-schedule the
            // source segments to be merged again.
            segmentLineage.deleteLineageEntry(lineageEntryId);
          }
        }

        // Write back to the lineage entry
        return SegmentLineageAccessHelper
            .writeSegmentLineage(_pinotHelixResourceManager.getPropertyStore(), segmentLineage, expectedVersion);
      });
    } catch (Exception e) {
      String errorMsg = String.format("Failed to clean up the segment lineage. (tableName = %s)", tableNameWithType);
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
    LOGGER.info("Segment lineage metadata clean-up is successfully processed for table: {}", tableNameWithType);
  }

  /**
   * Compute the segments that can be safely deleted based on the segment lineage.
   *
   * 1. The original segments can be deleted once the merged segments are successfully uploaded.
   * 2. If the segmentReplacement operation fails in the middle, there can be a case where partial segments are
   *    uploaded to the table. We should periodically clean up those zombie segments.
   */
  private List<String> computeSegmentsToDeleteFromSegmentLineage(SegmentLineage segmentLineage) {
    if (segmentLineage != null) {
      List<String> segmentsToDelete = new ArrayList<>();
      for (String lineageEntryId : segmentLineage.getLineageEntryIds()) {
        LineageEntry lineageEntry = segmentLineage.getLineageEntry(lineageEntryId);
        if (lineageEntry.getState() == LineageEntryState.COMPLETED) {
          // If the lineage state is 'COMPLETED', it is safe to delete all segments from 'segmentsFrom'
          segmentsToDelete.addAll(lineageEntry.getSegmentsFrom());
        } else if (lineageEntry.getState() == LineageEntryState.IN_PROGRESS) {
          // If the lineage state is 'IN_PROGRESS', we need to clean up the zombie lineage entry and its segments
          if (lineageEntry.getTimestamp()
              < System.currentTimeMillis() - LINEAGE_ENTRY_CLEANUP_RETENTION_IN_MILLIS) {
            segmentsToDelete.addAll(lineageEntry.getSegmentsTo());
            // Zombie lineage entry is safe to remove. This will allow the task scheduler to re-schedule the
            // source segments to be merged again.
            segmentLineage.deleteLineageEntry(lineageEntryId);
          }
        }
      }
      return segmentsToDelete;
    } else {
      return Collections.emptyList();
    }
  }
}
