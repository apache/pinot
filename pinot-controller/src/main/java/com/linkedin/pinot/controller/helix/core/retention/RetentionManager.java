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
package com.linkedin.pinot.controller.helix.core.retention;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.retention.strategy.RetentionStrategy;
import com.linkedin.pinot.controller.helix.core.retention.strategy.TimeRetentionStrategy;
import javax.annotation.Nonnull;


/**
 * RetentionManager is scheduled to run only on Leader controller.
 * It will first scan the table configs to get segment retention strategy then
 * do data retention..
 *
 *
 */
public class RetentionManager {
  public static final Logger LOGGER = LoggerFactory.getLogger(RetentionManager.class);

  private final PinotHelixResourceManager _pinotHelixResourceManager;

  private final Map<String, RetentionStrategy> _tableDeletionStrategy = new HashMap<>();
  private final Map<String, List<SegmentZKMetadata>> _segmentMetadataMap = new HashMap<>();
  private final Object _lock = new Object();

  private final ScheduledExecutorService _executorService;
  private final int _runFrequencyInSeconds;
  private final int _deletedSegmentsRetentionInDays;

  private static final int RETENTION_TIME_FOR_OLD_LLC_SEGMENTS_DAYS = 5;
  private static final int DEFAULT_RETENTION_FOR_DELETED_SEGMENTS_DAYS = 7;

  public RetentionManager(PinotHelixResourceManager pinotHelixResourceManager, int runFrequencyInSeconds,
      int deletedSegmentsRetentionInDays) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _runFrequencyInSeconds = runFrequencyInSeconds;
    _deletedSegmentsRetentionInDays = deletedSegmentsRetentionInDays;
    _executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(@Nonnull Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setName("PinotRetentionManagerExecutorService");
        return thread;
      }
    });
  }

  public RetentionManager(PinotHelixResourceManager pinotHelixResourceManager, int runFrequencyInSeconds) {
    this(pinotHelixResourceManager, runFrequencyInSeconds, DEFAULT_RETENTION_FOR_DELETED_SEGMENTS_DAYS);
  }

  public static long getRetentionTimeForOldLLCSegmentsDays() {
    return RETENTION_TIME_FOR_OLD_LLC_SEGMENTS_DAYS;
  }

  public void start() {
    scheduleRetentionThreadWithFrequency(_runFrequencyInSeconds);
    LOGGER.info("RetentionManager is started!");
  }

  private void scheduleRetentionThreadWithFrequency(int runFrequencyInSeconds) {
    _executorService.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        synchronized (getLock()) {
          execute();
        }
      }
    }, Math.min(50, runFrequencyInSeconds), runFrequencyInSeconds, TimeUnit.SECONDS);
  }

  private Object getLock() {
    return _lock;
  }

  private void execute() {
    try {
      if (_pinotHelixResourceManager.isLeader()) {
        LOGGER.info("Trying to run retentionManager!");
        updateDeletionStrategiesForEntireCluster();
        LOGGER.info("Finished update deletion strategies for entire cluster!");
        updateSegmentMetadataForEntireCluster();
        LOGGER.info("Finished update segment metadata for entire cluster!");
        scanSegmentMetadataAndPurge();
        LOGGER.info("Finished segment purge for entire cluster!");
        removeAgedDeletedSegments();
        LOGGER.info("Finished remove aged deleted segments!");
      } else {
        LOGGER.info("Not leader of the controller, sleep!");
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while running retention", e);
    }
  }

  private void scanSegmentMetadataAndPurge() {
    for (String tableName : _segmentMetadataMap.keySet()) {
      List<SegmentZKMetadata> segmentZKMetadataList = _segmentMetadataMap.get(tableName);
      List<String> segmentsToDelete = new ArrayList<>(128);
      IdealState idealState = null;
      try {
        if (TableNameBuilder.getTableTypeFromTableName(tableName).equals(TableType.REALTIME)) {
          idealState = _pinotHelixResourceManager.getHelixAdmin().getResourceIdealState(
              _pinotHelixResourceManager.getHelixClusterName(), tableName);
        }
      } catch (Exception e) {
        LOGGER.warn("Could not get idealstate for {}", tableName, e);
        // Ignore, worst case we have some old inactive segments in place.
      }
      for (SegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
        RetentionStrategy deletionStrategy;
        deletionStrategy = _tableDeletionStrategy.get(tableName);
        if (deletionStrategy == null) {
          LOGGER.info("No Retention strategy found for segment: {}", segmentZKMetadata.getSegmentName());
          continue;
        }
        if (segmentZKMetadata instanceof RealtimeSegmentZKMetadata) {
          final RealtimeSegmentZKMetadata realtimeSegmentZKMetadata = (RealtimeSegmentZKMetadata)segmentZKMetadata;
          if (realtimeSegmentZKMetadata.getStatus() == Status.IN_PROGRESS) {
            final String segmentId = realtimeSegmentZKMetadata.getSegmentName();
            if (SegmentName.isHighLevelConsumerSegmentName(segmentId)) {
              continue;
            }
            // This is an in-progress LLC segment. Delete any old ones hanging around. Do not delete
            // segments that are current since there may be a race with the ValidationManager trying to
            // auto-create LLC segments.
            if (shouldDeleteInProgressLLCSegment(segmentId, idealState, realtimeSegmentZKMetadata)) {
              segmentsToDelete.add(segmentId);
            }
            continue;
          }
        }
        if (deletionStrategy.isPurgeable(segmentZKMetadata)) {
          LOGGER.info("Marking segment to delete: {}", segmentZKMetadata.getSegmentName());
          segmentsToDelete.add(segmentZKMetadata.getSegmentName());
        }
      }
      if (segmentsToDelete.size() > 0) {
        LOGGER.info("Trying to delete {} segments for table {}", segmentsToDelete.size(), tableName);
        _pinotHelixResourceManager.deleteSegments(tableName, segmentsToDelete);
      }
    }
  }

  private void removeAgedDeletedSegments() {
    // Trigger clean-up for deleted segments from the deleted directory
    _pinotHelixResourceManager.getSegmentDeletionManager().removeAgedDeletedSegments(_deletedSegmentsRetentionInDays);
  }

  private boolean shouldDeleteInProgressLLCSegment(final String segmentId, final IdealState idealState, RealtimeSegmentZKMetadata segmentZKMetadata) {
    if (idealState == null) {
      return false;
    }
    Map<String, String> stateMap = idealState.getInstanceStateMap(segmentId);
    if (stateMap == null) {
      // segment is there in propertystore but not in idealstate. mark for deletion
      return true;
    } else {
      Set<String> states = new HashSet<>(stateMap.values());
      if (states.size() == 1 && states
          .contains(CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.OFFLINE)) {
        // All replicas of this segment are offline, delete it if it is old enough
        final long now = System.currentTimeMillis();
        if (now - segmentZKMetadata.getCreationTime() >= TimeUnit.DAYS.toMillis(
            RETENTION_TIME_FOR_OLD_LLC_SEGMENTS_DAYS)) {
          return true;
        }
      }
    }
    return false;
  }

  private void updateDeletionStrategiesForEntireCluster() {
    List<String> tableNames = _pinotHelixResourceManager.getAllTables();
    for (String tableName : tableNames) {
      updateDeletionStrategyForTable(tableName);
    }
  }

  private void updateDeletionStrategyForTable(String tableName) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    assert tableType != null;
    switch (tableType) {
      case OFFLINE:
        updateDeletionStrategyForOfflineTable(tableName);
        break;
      case REALTIME:
        updateDeletionStrategyForRealtimeTable(tableName);
        break;
      default:
        throw new IllegalArgumentException("No table type matches table name: " + tableName);
    }
  }

  /**
   * Update deletion strategy for offline table.
   * <ul>
   *   <li>Keep the current deletion strategy when one of the followings happened:
   *     <ul>
   *       <li>Failed to fetch the retention config.</li>
   *       <li>Push type is not valid (neither 'APPEND' nor 'REFRESH').</li>
   *     </ul>
   *   <li>
   *     Remove the deletion strategy when one of the followings happened:
   *     <ul>
   *       <li>Push type is set to 'REFRESH'.</li>
   *       <li>No valid retention time is set.</li>
   *     </ul>
   *   </li>
   *   <li>Update the deletion strategy when push type is set to 'APPEND' and valid retention time is set.</li>
   * </ul>
   */
  private void updateDeletionStrategyForOfflineTable(String offlineTableName) {
    // Fetch table config.
    TableConfig offlineTableConfig;
    try {
      offlineTableConfig = _pinotHelixResourceManager.getOfflineTableConfig(TableNameBuilder.extractRawTableName(offlineTableName));
      if (offlineTableConfig == null) {
        LOGGER.error("Table config is null, skip updating deletion strategy for table: {}.", offlineTableName);
        return;
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while fetching table config, skip updating deletion strategy for table: {}.",
          offlineTableName, e);
      return;
    }

    // Fetch validation config.
    SegmentsValidationAndRetentionConfig validationConfig = offlineTableConfig.getValidationConfig();
    if (validationConfig == null) {
      LOGGER.error("Validation config is null, skip updating deletion strategy for table: {}.", offlineTableName);
      return;
    }

    // Fetch push type.
    String segmentPushType = validationConfig.getSegmentPushType();
    if ((segmentPushType == null)
        || (!segmentPushType.equalsIgnoreCase("APPEND") && !segmentPushType.equalsIgnoreCase("REFRESH"))) {
      LOGGER.error(
          "Segment push type: {} is not valid ('APPEND' or 'REFRESH'), skip updating deletion strategy for table: {}.",
          segmentPushType, offlineTableName);
      return;
    }
    if (segmentPushType.equalsIgnoreCase("REFRESH")) {
      LOGGER.info("Segment push type is set to 'REFRESH', remove deletion strategy for table: {}.", offlineTableName);
      _tableDeletionStrategy.remove(offlineTableName);
      return;
    }

    // Fetch retention time unit and value.
    String retentionTimeUnit = validationConfig.getRetentionTimeUnit();
    String retentionTimeValue = validationConfig.getRetentionTimeValue();
    if (((retentionTimeUnit == null) || retentionTimeUnit.isEmpty())
        || ((retentionTimeValue == null) || retentionTimeValue.isEmpty())) {
      LOGGER.info("Retention time unit/value is not set, remove deletion strategy for table: {}.", offlineTableName);
      _tableDeletionStrategy.remove(offlineTableName);
      return;
    }

    // Update time retention strategy.
    try {
      TimeRetentionStrategy timeRetentionStrategy = new TimeRetentionStrategy(retentionTimeUnit, retentionTimeValue);
      _tableDeletionStrategy.put(offlineTableName, timeRetentionStrategy);
      LOGGER.info("Updated deletion strategy for table: {} using retention time: {} {}.", offlineTableName,
          retentionTimeValue, retentionTimeUnit);
    } catch (Exception e) {
      LOGGER.error(
          "Caught exception while building deletion strategy with retention time: {} {], remove deletion strategy for table: {}.",
          retentionTimeValue, retentionTimeUnit, offlineTableName);
      _tableDeletionStrategy.remove(offlineTableName);
    }
  }

  /**
   * Update deletion strategy for realtime table.
   * <ul>
   *   <li>Keep the current deletion strategy when failed to get a valid retention time</li>
   *   <li>Update the deletion strategy when valid retention time is set.</li>
   * </ul>
   * The reason for this is that we don't allow realtime table without deletion strategy.
   */
  private void updateDeletionStrategyForRealtimeTable(String realtimeTableName) {
    try {
      TableConfig realtimeTableConfig =
          _pinotHelixResourceManager.getRealtimeTableConfig(TableNameBuilder.extractRawTableName(realtimeTableName));
      assert realtimeTableConfig != null;
      SegmentsValidationAndRetentionConfig validationConfig = realtimeTableConfig.getValidationConfig();
      TimeRetentionStrategy timeRetentionStrategy =
          new TimeRetentionStrategy(validationConfig.getRetentionTimeUnit(), validationConfig.getRetentionTimeValue());
      _tableDeletionStrategy.put(realtimeTableName, timeRetentionStrategy);
    } catch (Exception e) {
      LOGGER.error("Caught exception while updating deletion strategy, skip updating deletion strategy for table: {}.",
          realtimeTableName, e);
    }
  }

  private void updateSegmentMetadataForEntireCluster() {
    // Gets table names with type.
    List<String> tableNames = _pinotHelixResourceManager.getAllTables();
    for (String tableNameWithType : tableNames) {
      _segmentMetadataMap.put(tableNameWithType, retrieveSegmentMetadataForTable(tableNameWithType));
    }
  }

  private List<SegmentZKMetadata> retrieveSegmentMetadataForTable(String tableNameWithType) {
    List<SegmentZKMetadata> segmentMetadataList = new ArrayList<>();
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    assert tableType != null;
    switch (tableType) {
      case OFFLINE:
        List<OfflineSegmentZKMetadata> offlineSegmentZKMetadatas = _pinotHelixResourceManager.getOfflineSegmentMetadata(
            tableNameWithType);
        for (OfflineSegmentZKMetadata offlineSegmentZKMetadata : offlineSegmentZKMetadatas) {
          segmentMetadataList.add(offlineSegmentZKMetadata);
        }
        break;
      case REALTIME:
        List<RealtimeSegmentZKMetadata> realtimeSegmentZKMetadatas = _pinotHelixResourceManager.getRealtimeSegmentMetadata(
            tableNameWithType);
        for (RealtimeSegmentZKMetadata realtimeSegmentZKMetadata : realtimeSegmentZKMetadatas) {
          segmentMetadataList.add(realtimeSegmentZKMetadata);
        }
        break;
      default:
        throw new IllegalArgumentException("No table type matches table name: " + tableNameWithType);
    }
    return segmentMetadataList;
  }

  public void stop() {
    _executorService.shutdown();
  }
}
