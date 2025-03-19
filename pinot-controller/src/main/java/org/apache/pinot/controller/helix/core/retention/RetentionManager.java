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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.logging.log4j.util.Strings;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.helix.core.retention.strategy.RetentionStrategy;
import org.apache.pinot.controller.helix.core.retention.strategy.TimeRetentionStrategy;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
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
  public static final int DEFAULT_UNTRACKED_SEGMENTS_DELETION_BATCH_SIZE = 100;
  private static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicies.randomDelayRetryPolicy(20, 100L, 200L);
  private final boolean _untrackedSegmentDeletionEnabled;

  private static final Logger LOGGER = LoggerFactory.getLogger(RetentionManager.class);

  public RetentionManager(PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerConf config, ControllerMetrics controllerMetrics) {
    super("RetentionManager", config.getRetentionControllerFrequencyInSeconds(),
        config.getRetentionManagerInitialDelayInSeconds(), pinotHelixResourceManager, leadControllerManager,
        controllerMetrics);
    _untrackedSegmentDeletionEnabled = config.getUntrackedSegmentDeletionEnabled();
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
    int untrackedSegmentsDeletionBatchSize =
        validationConfig.getUntrackedSegmentsDeletionBatchSize() != null ? Integer.parseInt(
            validationConfig.getUntrackedSegmentsDeletionBatchSize()) : DEFAULT_UNTRACKED_SEGMENTS_DELETION_BATCH_SIZE;

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
      manageRetentionForOfflineTable(tableNameWithType, retentionStrategy, untrackedSegmentsDeletionBatchSize);
    } else {
      manageRetentionForRealtimeTable(tableNameWithType, retentionStrategy, untrackedSegmentsDeletionBatchSize);
    }
  }

  private void manageRetentionForOfflineTable(String offlineTableName, RetentionStrategy retentionStrategy,
      int untrackedSegmentsDeletionBatchSize) {
    List<SegmentZKMetadata> segmentZKMetadataList = _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName);

    // fetch those segments that are beyond the retention period and don't have an entry in ZK i.e.
    // SegmentZkMetadata is missing for those segments
    List<String> segmentsToDelete =
        getSegmentsToDeleteFromDeepstore(offlineTableName, retentionStrategy, segmentZKMetadataList,
            untrackedSegmentsDeletionBatchSize);

    for (SegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      if (retentionStrategy.isPurgeable(offlineTableName, segmentZKMetadata)) {
        segmentsToDelete.add(segmentZKMetadata.getSegmentName());
      }
    }
    if (!segmentsToDelete.isEmpty()) {
      LOGGER.info("Deleting {} segments from table: {}", segmentsToDelete.size(), offlineTableName);
      _pinotHelixResourceManager.deleteSegments(offlineTableName, segmentsToDelete);
    }
  }

  private void manageRetentionForRealtimeTable(String realtimeTableName, RetentionStrategy retentionStrategy,
      int untrackedSegmentsDeletionBatchSize) {
    List<SegmentZKMetadata> segmentZKMetadataList = _pinotHelixResourceManager.getSegmentsZKMetadata(realtimeTableName);

    // fetch those segments that are beyond the retention period and don't have an entry in ZK i.e.
    // SegmentZkMetadata is missing for those segments
    List<String> segmentsToDelete =
        getSegmentsToDeleteFromDeepstore(realtimeTableName, retentionStrategy, segmentZKMetadataList,
            untrackedSegmentsDeletionBatchSize);

    IdealState idealState = _pinotHelixResourceManager.getHelixAdmin()
        .getResourceIdealState(_pinotHelixResourceManager.getHelixClusterName(), realtimeTableName);

    for (SegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
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

  private List<String> getSegmentsToDeleteFromDeepstore(String tableNameWithType, RetentionStrategy retentionStrategy,
      List<SegmentZKMetadata> segmentZKMetadataList, int untrackedSegmentsDeletionBatchSize) {
    List<String> segmentsToDelete = new ArrayList<>();

    if (!_untrackedSegmentDeletionEnabled) {
      LOGGER.info(
          "Not scanning deep store for untracked segments for table: {}", tableNameWithType);
      return segmentsToDelete;
    }

    if (untrackedSegmentsDeletionBatchSize <= 0) {
      // return an empty list in case untracked segment deletion batch size is configured < 0 in table config
      LOGGER.info(
          "Not scanning deep store for untracked segments for table: {} as untrackedSegmentsDeletionBatchSize is set "
              + "to: {}",
          tableNameWithType, untrackedSegmentsDeletionBatchSize);
      return segmentsToDelete;
    }

    List<String> segmentsPresentInZK =
        segmentZKMetadataList.stream().map(SegmentZKMetadata::getSegmentName).collect(Collectors.toList());
    try {
      LOGGER.info("Fetch segments present in deep store that are beyond retention period for table: {}",
          tableNameWithType);
      segmentsToDelete =
          findUntrackedSegmentsToDeleteFromDeepstore(tableNameWithType, retentionStrategy, segmentsPresentInZK);
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.UNTRACKED_SEGMENTS_COUNT,
          segmentsToDelete.size());

      if (segmentsToDelete.size() > untrackedSegmentsDeletionBatchSize) {
        LOGGER.info("Truncating segments to delete from {} to {} for table: {}",
            segmentsToDelete.size(), untrackedSegmentsDeletionBatchSize, tableNameWithType);
        segmentsToDelete = segmentsToDelete.subList(0, untrackedSegmentsDeletionBatchSize);
      }
    } catch (IOException e) {
      LOGGER.warn("Unable to fetch segments from deep store that are beyond retention period for table: {}",
          tableNameWithType);
    }

    return segmentsToDelete;
  }


  /**
   * Identifies segments in deepstore that are ready for deletion based on the retention strategy.
   *
   * This method finds segments that are beyond the retention period and are ready to be purged.
   * It only considers segments that do not have entries in ZooKeeper metadata i.e. untracked segments.
   * The lastModified time of the file in deepstore is used to determine whether the segment
   * should be retained or purged.
   *
   * @param tableNameWithType   Name of the offline table
   * @param retentionStrategy  Strategy to determine if a segment should be purged
   * @param segmentsToExclude  List of segment names that should be excluded from deletion
   * @return List of segment names that should be deleted from deepstore
   * @throws IOException If there's an error accessing the filesystem
   */
  private List<String> findUntrackedSegmentsToDeleteFromDeepstore(String tableNameWithType,
      RetentionStrategy retentionStrategy, List<String> segmentsToExclude)
      throws IOException {

    List<String> segmentsToDelete = new ArrayList<>();
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    URI tableDataUri = URIUtils.getUri(_pinotHelixResourceManager.getDataDir(), rawTableName);
    PinotFS pinotFS = PinotFSFactory.create(tableDataUri.getScheme());

    long startTimeMs = System.currentTimeMillis();

    List<FileMetadata> deepstoreFiles = pinotFS.listFilesWithMetadata(tableDataUri, false);
    long listEndTimeMs = System.currentTimeMillis();
    LOGGER.info("Found: {} segments in deepstore for table: {}. Time taken to list segments: {} ms",
        deepstoreFiles.size(), tableNameWithType, listEndTimeMs - startTimeMs);

    for (FileMetadata fileMetadata : deepstoreFiles) {
      if (fileMetadata.isDirectory()) {
        continue;
      }

      String segmentName = extractSegmentName(fileMetadata.getFilePath());
      if (Strings.isEmpty(segmentName) || segmentsToExclude.contains(segmentName)) {
        continue;
      }

      // determine whether the segment should be purged or not based on the last modified time of the file
      long lastModifiedTime = fileMetadata.getLastModifiedTime();

      if (retentionStrategy.isPurgeable(tableNameWithType, segmentName, lastModifiedTime)) {
        segmentsToDelete.add(segmentName);
      }
    }
    long endTimeMs = System.currentTimeMillis();
    LOGGER.info(
        "Took: {} ms to identify {} segments for deletion from deep store for table: {} as they have no corresponding"
            + " entry in the property store.",
        endTimeMs - startTimeMs, segmentsToDelete.size(), tableNameWithType);
    return segmentsToDelete;
  }

  @Nullable
  private String extractSegmentName(@Nullable String filePath) {
    if (Strings.isEmpty(filePath)) {
      return null;
    }
    String segmentName = filePath.substring(filePath.lastIndexOf("/") + 1);
    if (segmentName.endsWith(TarCompressionUtils.TAR_GZ_FILE_EXTENSION)) {
      segmentName = segmentName.substring(0, segmentName.length() - TarCompressionUtils.TAR_GZ_FILE_EXTENSION.length());
    }
    return segmentName;
  }

  private void manageSegmentLineageCleanupForTable(TableConfig tableConfig) {
    String tableNameWithType = tableConfig.getTableName();
    List<String> segmentsToDelete = new ArrayList<>();
    long cleanupStartTime = System.currentTimeMillis();
    synchronized (_pinotHelixResourceManager.getLineageUpdaterLock(tableNameWithType)) {
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
          SegmentLineage segmentLineage = SegmentLineage.fromZNRecord(segmentLineageZNRecord);
          int expectedVersion = segmentLineageZNRecord.getVersion();

          List<String> segmentsForTable = _pinotHelixResourceManager.getSegmentsFor(tableNameWithType, false);
          _pinotHelixResourceManager.getLineageManager()
              .updateLineageForRetention(tableConfig, segmentLineage, segmentsForTable, segmentsToDelete,
                  _pinotHelixResourceManager.getConsumingSegments(tableNameWithType));

          // Write back to the lineage entry
          if (SegmentLineageAccessHelper.writeSegmentLineage(_pinotHelixResourceManager.getPropertyStore(),
              segmentLineage, expectedVersion)) {
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
    }
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
    LOGGER.info("Segment lineage metadata clean-up is successfully processed for table: {}", tableNameWithType);
  }
}
