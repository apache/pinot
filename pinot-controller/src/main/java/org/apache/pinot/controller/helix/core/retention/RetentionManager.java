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
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.lineage.LineageEntry;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.SegmentName;
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
    // Manage normal table retention except segment lineage cleanup.
    // The reason of separating the logic is that REFRESH only table will be skipped in the first part,
    // whereas the segment lineage cleanup needs to be handled.
    manageRetentionForTable(tableNameWithType);

    // Delete segments based on segment lineage and clean up segment lineage metadata.
    manageSegmentLineageCleanupForTable(tableNameWithType);
  }

  @Override
  protected void postprocess() {
    LOGGER.info("Removing aged (more than {} days) deleted segments for all tables", _deletedSegmentsRetentionInDays);
    _pinotHelixResourceManager.getSegmentDeletionManager().removeAgedDeletedSegments(_deletedSegmentsRetentionInDays);
  }

  private void manageRetentionForTable(String tableNameWithType) {
    LOGGER.info("Start managing retention for table: {}", tableNameWithType);

    // Build retention strategy from table config
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      LOGGER.error("Failed to get table config for table: {}", tableNameWithType);
      return;
    }

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
        // In progress segment, only check LLC segment
        if (SegmentName.isLowLevelConsumerSegmentName(segmentName)) {
          // Delete old LLC segment that hangs around. Do not delete segment that are current since there may be a race
          // with RealtimeSegmentValidationManager trying to auto-create the LLC segment
          if (shouldDeleteInProgressLLCSegment(segmentName, idealState, segmentZKMetadata)) {
            segmentsToDelete.add(segmentName);
          }
        }
      } else {
        // Sealed segment
        if (retentionStrategy.isPurgeable(realtimeTableName, segmentZKMetadata)) {
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

  private void manageSegmentLineageCleanupForTable(String tableNameWithType) {
    try {
      DEFAULT_RETRY_POLICY.attempt(() -> {
        // Fetch segment lineage
        ZNRecord segmentLineageZNRecord = SegmentLineageAccessHelper
            .getSegmentLineageZNRecord(_pinotHelixResourceManager.getPropertyStore(), tableNameWithType);
        if (segmentLineageZNRecord == null) {
          return true;
        }
        LOGGER.info("Start cleaning up segment lineage for table: {}", tableNameWithType);
        long cleanupStartTime = System.currentTimeMillis();
        SegmentLineage segmentLineage = SegmentLineage.fromZNRecord(segmentLineageZNRecord);
        int expectedVersion = segmentLineageZNRecord.getVersion();

        // 1. The original segments can be deleted once the merged segments are successfully uploaded
        // 2. The zombie lineage entry & merged segments should be deleted if the segment replacement failed in
        //    the middle
        Set<String> segmentsForTable = new HashSet<>(_pinotHelixResourceManager.getSegmentsFor(tableNameWithType));
        List<String> segmentsToDelete = new ArrayList<>();
        for (String lineageEntryId : segmentLineage.getLineageEntryIds()) {
          LineageEntry lineageEntry = segmentLineage.getLineageEntry(lineageEntryId);
          if (lineageEntry.getState() == LineageEntryState.COMPLETED) {
            Set<String> sourceSegments = new HashSet<>(lineageEntry.getSegmentsFrom());
            sourceSegments.retainAll(segmentsForTable);
            if (sourceSegments.isEmpty()) {
              // If the lineage state is 'COMPLETED' and segmentFrom are removed, it is safe clean up
              // the lineage entry
              segmentLineage.deleteLineageEntry(lineageEntryId);
            } else {
              // If the lineage state is 'COMPLETED', it is safe to delete all segments from 'segmentsFrom'
              segmentsToDelete.addAll(sourceSegments);
            }
          } else if (lineageEntry.getState() == LineageEntryState.IN_PROGRESS) {
            // If the lineage state is 'IN_PROGRESS', we need to clean up the zombie lineage entry and its segments
            if (lineageEntry.getTimestamp() < System.currentTimeMillis() - LINEAGE_ENTRY_CLEANUP_RETENTION_IN_MILLIS) {
              Set<String> destinationSegments = new HashSet<>(lineageEntry.getSegmentsTo());
              destinationSegments.retainAll(segmentsForTable);
              if (destinationSegments.isEmpty()) {
                // If the lineage state is 'IN_PROGRESS' and source segments are already removed, it is safe to clean up
                // the lineage entry. Deleting lineage will allow the task scheduler to re-schedule the source segments
                // to be merged again.
                segmentLineage.deleteLineageEntry(lineageEntryId);
              } else {
                // If the lineage state is 'IN_PROGRESS', it is safe to delete all segments from 'segmentsTo'
                segmentsToDelete.addAll(destinationSegments);
              }
            }
          }
        }

        // Write back to the lineage entry
        if (SegmentLineageAccessHelper
            .writeSegmentLineage(_pinotHelixResourceManager.getPropertyStore(), segmentLineage, expectedVersion)) {
          // Delete segments based on the segment lineage
          _pinotHelixResourceManager.deleteSegments(tableNameWithType, segmentsToDelete);
          LOGGER.info("Finished cleaning up segment lineage for table: {} in {}ms", tableNameWithType,
              (System.currentTimeMillis() - cleanupStartTime));
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
