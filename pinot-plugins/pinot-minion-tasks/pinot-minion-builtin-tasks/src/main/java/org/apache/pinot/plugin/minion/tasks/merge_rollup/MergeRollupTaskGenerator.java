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
package org.apache.pinot.plugin.minion.tasks.merge_rollup;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.I0Itec.zkclient.exception.ZkException;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageUtils;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.minion.MergeRollupTaskMetadata;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.MergeRollupTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link PinotTaskGenerator} implementation for generating tasks of type {@link MergeRollupTask}
 *
 * TODO: Add the support for realtime table
 *
 * Steps:
 *
 *  - Pre-select segments:
 *    - Fetch all segments, select segments based on segment lineage (removing segmentsFrom for COMPLETED lineage entry and
 *      segmentsTo for IN_PROGRESS lineage entry)
 *    - Remove empty segments
 *    - Sort segments based on startTime and endTime in ascending order
 *
 *  For each mergeLevel (from lowest to highest, e.g. Hourly -> Daily -> Monthly -> Yearly):
 *    - Skip scheduling if there's incomplete task for the mergeLevel
 *    - Calculate merge/rollup window:
 *      - Read watermarkMs from the {@link MergeRollupTaskMetadata} ZNode
 *        found at MINION_TASK_METADATA/MergeRollupTask/<tableNameWithType>
 *        In case of cold-start, no ZNode will exist.
 *        A new ZNode will be created, with watermarkMs as the smallest time found in all segments truncated to the
 *        closest bucket start time.
 *      - The execution window for the task is calculated as,
 *        windowStartMs = watermarkMs, windowEndMs = windowStartMs + bucketTimeMs
 *      - Skip scheduling if the window is invalid:
 *        - If the execution window is not older than bufferTimeMs, no task will be generated
 *        - The windowEndMs of higher merge level should be less or equal to the waterMarkMs of lower level
 *      - Bump up target window and watermark if needed.
 *        - If there's no unmerged segments (by checking segment zk metadata {mergeRollupTask.mergeLevel: level}) for current window,
 *          keep bumping up the watermark and target window until unmerged segments are found. Else skip the scheduling.
 *    - Select all segments for the target window
 *    - Create tasks (per partition for partitioned table) based on maxNumRecordsPerTask
 */
@TaskGenerator
public class MergeRollupTaskGenerator implements PinotTaskGenerator {
  private static final int DEFAULT_MAX_NUM_RECORDS_PER_TASK = 50_000_000;
  private static final String REFRESH = "REFRESH";
  private static final Logger LOGGER = LoggerFactory.getLogger(MergeRollupTaskGenerator.class);

  private ClusterInfoAccessor _clusterInfoAccessor;

  @Override
  public void init(ClusterInfoAccessor clusterInfoAccessor) {
    _clusterInfoAccessor = clusterInfoAccessor;
  }

  @Override
  public String getTaskType() {
    return MergeRollupTask.TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    String taskType = MergeRollupTask.TASK_TYPE;
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();

    for (TableConfig tableConfig : tableConfigs) {
      if (!validate(tableConfig, taskType)) {
        continue;
      }
      String offlineTableName = tableConfig.getTableName();
      LOGGER.info("Start generating task configs for table: {} for task: {}", offlineTableName, taskType);

      // Get all segment metadata
      List<SegmentZKMetadata> allSegments = _clusterInfoAccessor.getSegmentsZKMetadata(offlineTableName);

      // Select current segment snapshot based on lineage, filter out empty segments
      SegmentLineage segmentLineage = _clusterInfoAccessor.getSegmentLineage(offlineTableName);
      Set<String> preSelectedSegmentsBasedOnLineage = new HashSet<>();
      for (SegmentZKMetadata segment : allSegments) {
        preSelectedSegmentsBasedOnLineage.add(segment.getSegmentName());
      }
      SegmentLineageUtils.filterSegmentsBasedOnLineageInplace(preSelectedSegmentsBasedOnLineage, segmentLineage);

      List<SegmentZKMetadata> preSelectedSegments = new ArrayList<>();
      for (SegmentZKMetadata segment : allSegments) {
        if (preSelectedSegmentsBasedOnLineage.contains(segment.getSegmentName()) && segment.getTotalDocs() > 0) {
          preSelectedSegments.add(segment);
        }
      }

      if (preSelectedSegments.isEmpty()) {
        LOGGER
            .info("Skip generating task: {} for table: {}, no segment is found to merge.", taskType, offlineTableName);
        continue;
      }

      // Sort segments based on startTimeMs, endTimeMs and segmentName in ascending order
      preSelectedSegments.sort((a, b) -> {
        long aStartTime = a.getStartTimeMs();
        long bStartTime = b.getStartTimeMs();
        if (aStartTime != bStartTime) {
          return Long.compare(aStartTime, bStartTime);
        }
        long aEndTime = a.getEndTimeMs();
        long bEndTime = b.getEndTimeMs();
        return aEndTime != bEndTime ? Long.compare(aEndTime, bEndTime)
            : a.getSegmentName().compareTo(b.getSegmentName());
      });

      // Sort merge levels based on bucket time period
      Map<String, String> taskConfigs = tableConfig.getTaskConfig().getConfigsForTaskType(taskType);
      Map<String, Map<String, String>> mergeLevelToConfigs = MergeRollupTaskUtils.getLevelToConfigMap(taskConfigs);
      List<Map.Entry<String, Map<String, String>>> sortedMergeLevelConfigs =
          new ArrayList<>(mergeLevelToConfigs.entrySet());
      sortedMergeLevelConfigs.sort(Comparator.comparingLong(
          e -> TimeUtils.convertPeriodToMillis(e.getValue().get(MinionConstants.MergeTask.BUCKET_TIME_PERIOD_KEY))));

      // Get incomplete merge levels
      Set<String> inCompleteMergeLevels = new HashSet<>();
      for (Map.Entry<String, TaskState> entry : TaskGeneratorUtils
          .getIncompleteTasks(taskType, offlineTableName, _clusterInfoAccessor).entrySet()) {
        for (PinotTaskConfig taskConfig : _clusterInfoAccessor.getTaskConfigs(entry.getKey())) {
          inCompleteMergeLevels.add(taskConfig.getConfigs().get(MergeRollupTask.MERGE_LEVEL_KEY));
        }
      }

      ZNRecord mergeRollupTaskZNRecord = _clusterInfoAccessor.getMinionMergeRollupTaskZNRecord(offlineTableName);
      int expectedVersion = mergeRollupTaskZNRecord != null ? mergeRollupTaskZNRecord.getVersion() : -1;
      MergeRollupTaskMetadata mergeRollupTaskMetadata =
          mergeRollupTaskZNRecord != null ? MergeRollupTaskMetadata.fromZNRecord(mergeRollupTaskZNRecord)
              : new MergeRollupTaskMetadata(offlineTableName, new TreeMap<>());
      List<PinotTaskConfig> pinotTaskConfigsForTable = new ArrayList<>();

      // Schedule tasks from lowest to highest merge level (e.g. Hourly -> Daily -> Monthly -> Yearly)
      String mergeLevel = null;
      for (Map.Entry<String, Map<String, String>> mergeLevelConfig : sortedMergeLevelConfigs) {
        String lowerMergeLevel = mergeLevel;
        mergeLevel = mergeLevelConfig.getKey();
        Map<String, String> mergeConfigs = mergeLevelConfig.getValue();

        // Skip scheduling if there's incomplete task for current mergeLevel
        if (inCompleteMergeLevels.contains(mergeLevel)) {
          LOGGER.info("Found incomplete task of merge level: {} for the same table: {}, Skipping task generation: {}",
              mergeLevel, offlineTableName, taskType);
          continue;
        }

        // Get the bucket size and buffer
        long bucketMs =
            TimeUtils.convertPeriodToMillis(mergeConfigs.get(MinionConstants.MergeTask.BUCKET_TIME_PERIOD_KEY));
        long bufferMs =
            TimeUtils.convertPeriodToMillis(mergeConfigs.get(MinionConstants.MergeTask.BUFFER_TIME_PERIOD_KEY));

        // Get watermark from MergeRollupTaskMetadata ZNode
        // windowStartMs = watermarkMs, windowEndMs = windowStartMs + bucketTimeMs
        long waterMarkMs =
            getWatermarkMs(preSelectedSegments.get(0).getStartTimeMs(), bucketMs, mergeLevel, mergeRollupTaskMetadata);
        long windowStartMs = waterMarkMs;
        long windowEndMs = windowStartMs + bucketMs;

        if (!isValidMergeWindowEndTime(windowEndMs, bufferMs, lowerMergeLevel, mergeRollupTaskMetadata)) {
          LOGGER.info(
              "Window with start: {} and end: {} of mergeLevel: {} is not a valid merge window, Skipping task generation: {}",
              windowStartMs, windowEndMs, mergeLevel, taskType);
          continue;
        }

        // Find all segments overlapping with the merge window, if all overlapping segments are merged, bump up the target window
        List<SegmentZKMetadata> selectedSegments = new ArrayList<>();
        boolean hasUnmergedSegments = false;
        boolean isValidMergeWindow = true;

        // The for loop terminates in following cases:
        // 1. Found unmerged segments in target merge window, need to bump up watermark if windowStartMs > watermarkMs,
        //    will schedule tasks
        // 2. All segments are merged in the merge window and we have loop through all segments, skip scheduling
        // 3. Merge window is invalid (windowEndMs > System.currentTimeMillis() - bufferMs || windowEndMs > waterMark of
        //    the lower mergeLevel), skip scheduling
        for (SegmentZKMetadata preSelectedSegment : preSelectedSegments) {
          long startTimeMs = preSelectedSegment.getStartTimeMs();
          if (startTimeMs < windowEndMs) {
            long endTimeMs = preSelectedSegment.getEndTimeMs();
            if (endTimeMs >= windowStartMs) {
              // For segments overlapping with current window, add to the result list
              selectedSegments.add(preSelectedSegment);
              if (!isMergedSegment(preSelectedSegment, mergeLevel)) {
                hasUnmergedSegments = true;
              }
            }
            // endTimeMs < windowStartMs
            // Haven't find the first overlapping segment, continue to the next segment
          } else {
            // Has gone through all overlapping segments for current window
            if (hasUnmergedSegments) {
              // Found unmerged segments, schedule merge task for current window
              break;
            } else {
              // No unmerged segments found, clean up selected segments and bump up the merge window
              // TODO: If there are many small merged segments, we should merge them again
              selectedSegments.clear();
              selectedSegments.add(preSelectedSegment);
              if (!isMergedSegment(preSelectedSegment, mergeLevel)) {
                hasUnmergedSegments = true;
              }
              windowStartMs = startTimeMs / bucketMs * bucketMs;
              windowEndMs = windowStartMs + bucketMs;
              if (!isValidMergeWindowEndTime(windowEndMs, bufferMs, lowerMergeLevel, mergeRollupTaskMetadata)) {
                isValidMergeWindow = false;
                break;
              }
            }
          }
        }

        if (!isValidMergeWindow) {
          LOGGER.info(
              "Window with start: {} and end: {} of mergeLevel: {} is not a valid merge window, Skipping task generation: {}",
              windowStartMs, windowEndMs, mergeLevel, taskType);
          continue;
        }

        if (!hasUnmergedSegments) {
          LOGGER.info("No unmerged segments found for mergeLevel:{} for table: {}, Skipping task generation: {}",
              mergeLevel, offlineTableName, taskType);
          continue;
        }

        Long prevWatermarkMs = mergeRollupTaskMetadata.getWatermarkMap().put(mergeLevel, windowStartMs);
        LOGGER.info("Update watermark of mergeLevel: {} for table: {} from: {} to: {}", mergeLevel, offlineTableName,
            prevWatermarkMs, waterMarkMs);

        // Create task configs
        int maxNumRecordsPerTask = mergeConfigs.get(MergeRollupTask.MAX_NUM_RECORDS_PER_TASK_KEY) != null ? Integer
            .parseInt(mergeConfigs.get(MergeRollupTask.MAX_NUM_RECORDS_PER_TASK_KEY))
            : DEFAULT_MAX_NUM_RECORDS_PER_TASK;
        SegmentPartitionConfig segmentPartitionConfig = tableConfig.getIndexingConfig().getSegmentPartitionConfig();
        if (segmentPartitionConfig == null) {
          pinotTaskConfigsForTable.addAll(
              createPinotTaskConfigs(selectedSegments, offlineTableName, maxNumRecordsPerTask, mergeLevel, mergeConfigs,
                  taskConfigs));
        } else {
          // For partitioned table, schedule separate tasks for each partition
          Map<String, ColumnPartitionConfig> columnPartitionMap = segmentPartitionConfig.getColumnPartitionMap();
          Preconditions.checkState(columnPartitionMap.size() == 1, "Cannot partition on multiple columns for table: %s",
              tableConfig.getTableName());
          Map.Entry<String, ColumnPartitionConfig> partitionEntry = columnPartitionMap.entrySet().iterator().next();
          String partitionColumn = partitionEntry.getKey();

          Map<Integer, List<SegmentZKMetadata>> partitionToSegments = new HashMap<>();
          // Handle segments that have multiple partitions or no partition info
          List<SegmentZKMetadata> outlierSegments = new ArrayList<>();
          for (SegmentZKMetadata selectedSegment : selectedSegments) {
            SegmentPartitionMetadata segmentPartitionMetadata = selectedSegment.getPartitionMetadata();
            if (segmentPartitionMetadata == null
                || segmentPartitionMetadata.getPartitions(partitionColumn).size() != 1) {
              outlierSegments.add(selectedSegment);
            } else {
              int partition = segmentPartitionMetadata.getPartitions(partitionColumn).iterator().next();
              partitionToSegments.computeIfAbsent(partition, k -> new ArrayList<>()).add(selectedSegment);
            }
          }

          for (Map.Entry<Integer, List<SegmentZKMetadata>> partitionToSegmentsEntry : partitionToSegments.entrySet()) {
            pinotTaskConfigsForTable.addAll(
                createPinotTaskConfigs(partitionToSegmentsEntry.getValue(), offlineTableName, maxNumRecordsPerTask,
                    mergeLevel, mergeConfigs, taskConfigs));
          }

          if (!outlierSegments.isEmpty()) {
            pinotTaskConfigsForTable.addAll(
                createPinotTaskConfigs(outlierSegments, offlineTableName, maxNumRecordsPerTask, mergeLevel,
                    mergeConfigs, taskConfigs));
          }
        }
      }

      // Write updated watermark map to zookeeper
      try {
        _clusterInfoAccessor.setMergeRollupTaskMetadata(mergeRollupTaskMetadata, expectedVersion);
      } catch (ZkException e) {
        LOGGER.error(
            "Version changed while updating merge/rollup task metadata for table: {}, skip scheduling. There are multiple task schedulers for the same table, need to investigate!",
            offlineTableName);
        continue;
      }
      pinotTaskConfigs.addAll(pinotTaskConfigsForTable);
      LOGGER
          .info("Finished generating task configs for table: {} for task: {}, numTasks: {}", offlineTableName, taskType,
              pinotTaskConfigsForTable.size());
    }
    return pinotTaskConfigs;
  }

  /**
   * Validate table config for merge/rollup task
   */
  private boolean validate(TableConfig tableConfig, String taskType) {
    String offlineTableName = tableConfig.getTableName();
    if (tableConfig.getTableType() != TableType.OFFLINE) {
      LOGGER.warn("Skip generating task: {} for non-OFFLINE table: {}, REALTIME table is not supported yet", taskType,
          offlineTableName);
      return false;
    }

    if (REFRESH.equalsIgnoreCase(IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig))) {
      LOGGER.warn("Skip generating task: {} for non-APPEND table: {}, REFRESH table is not supported", taskType,
          offlineTableName);
      return false;
    }
    return true;
  }

  /**
   * Check if the segment is merged for give merge level
   */
  private boolean isMergedSegment(SegmentZKMetadata segmentZKMetadata, String mergeLevel) {
    Map<String, String> customMap = segmentZKMetadata.getCustomMap();
    return customMap != null && mergeLevel
        .equalsIgnoreCase(customMap.get(MergeRollupTask.SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY));
  }

  /**
   * Check if the merge window end time is valid
   */
  private boolean isValidMergeWindowEndTime(long windowEndMs, long bufferMs, String lowerMergeLevel,
      MergeRollupTaskMetadata mergeRollupTaskMetadata) {
    // Check that execution window endTimeMs <= now - bufferTime
    if (windowEndMs > System.currentTimeMillis() - bufferMs) {
      return false;
    }
    // Check that execution window endTimeMs <= waterMark of the lower mergeLevel
    return lowerMergeLevel == null || mergeRollupTaskMetadata.getWatermarkMap().get(lowerMergeLevel) == null
        || windowEndMs <= mergeRollupTaskMetadata.getWatermarkMap().get(lowerMergeLevel);
  }

  /**
   * Get the watermark from the MergeRollupMetadata ZNode.
   * If the znode is null, computes the watermark using the start time from segment metadata
   */
  private long getWatermarkMs(long minStartTimeMs, long bucketMs, String mergeLevel,
      MergeRollupTaskMetadata mergeRollupTaskMetadata) {
    long watermarkMs;
    if (mergeRollupTaskMetadata.getWatermarkMap().get(mergeLevel) == null) {
      // No ZNode exists. Cold-start.
      // Round off according to the bucket. This ensures we align the merged segments to proper time boundaries
      // For example, if start time millis is 20200813T12:34:59, we want to create the merged segment for window [20200813, 20200814)
      watermarkMs = (minStartTimeMs / bucketMs) * bucketMs;
    } else {
      watermarkMs = mergeRollupTaskMetadata.getWatermarkMap().get(mergeLevel);
    }
    return watermarkMs;
  }

  /**
   * Create pinot task configs with selected segments and configs
   */
  private List<PinotTaskConfig> createPinotTaskConfigs(List<SegmentZKMetadata> selectedSegments,
      String offlineTableName, int maxNumRecordsPerTask, String mergeLevel, Map<String, String> mergeConfigs,
      Map<String, String> taskConfigs) {
    int numRecordsPerTask = 0;
    List<List<String>> segmentNamesList = new ArrayList<>();
    List<List<String>> downloadURLsList = new ArrayList<>();
    List<String> segmentNames = new ArrayList<>();
    List<String> downloadURLs = new ArrayList<>();

    for (int i = 0; i < selectedSegments.size(); i++) {
      SegmentZKMetadata targetSegment = selectedSegments.get(i);
      segmentNames.add(targetSegment.getSegmentName());
      downloadURLs.add(targetSegment.getDownloadUrl());
      numRecordsPerTask += targetSegment.getTotalDocs();
      if (numRecordsPerTask >= maxNumRecordsPerTask || i == selectedSegments.size() - 1) {
        segmentNamesList.add(segmentNames);
        downloadURLsList.add(downloadURLs);
        numRecordsPerTask = 0;
        segmentNames = new ArrayList<>();
        downloadURLs = new ArrayList<>();
      }
    }

    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();
    for (int i = 0; i < segmentNamesList.size(); i++) {
      Map<String, String> configs = new HashMap<>();
      configs.put(MinionConstants.TABLE_NAME_KEY, offlineTableName);
      configs.put(MinionConstants.SEGMENT_NAME_KEY,
          StringUtils.join(segmentNamesList.get(i), MinionConstants.SEGMENT_NAME_SEPARATOR));
      configs.put(MinionConstants.DOWNLOAD_URL_KEY,
          StringUtils.join(downloadURLsList.get(i), MinionConstants.URL_SEPARATOR));
      configs.put(MinionConstants.UPLOAD_URL_KEY, _clusterInfoAccessor.getVipUrl() + "/segments");
      configs.put(MinionConstants.ENABLE_REPLACE_SEGMENTS_KEY, "true");

      for (Map.Entry<String, String> taskConfig : taskConfigs.entrySet()) {
        if (taskConfig.getKey().endsWith(MinionConstants.MergeRollupTask.AGGREGATION_TYPE_KEY_SUFFIX)) {
          configs.put(taskConfig.getKey(), taskConfig.getValue());
        }
      }

      configs.put(MergeRollupTask.MERGE_TYPE_KEY, mergeConfigs.get(MinionConstants.MergeTask.MERGE_TYPE_KEY));
      configs.put(MergeRollupTask.MERGE_LEVEL_KEY, mergeLevel);
      configs.put(MinionConstants.MergeTask.PARTITION_BUCKET_TIME_PERIOD_KEY,
          mergeConfigs.get(MinionConstants.MergeTask.BUCKET_TIME_PERIOD_KEY));
      configs.put(MinionConstants.MergeTask.ROUND_BUCKET_TIME_PERIOD_KEY,
          mergeConfigs.get(MinionConstants.MergeTask.ROUND_BUCKET_TIME_PERIOD_KEY));
      configs.put(MinionConstants.MergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY,
          mergeConfigs.get(MinionConstants.MergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY));

      configs.put(MergeRollupTask.SEGMENT_NAME_PREFIX_KEY,
          MergeRollupTask.MERGED_SEGMENT_NAME_PREFIX + mergeLevel + "_" + System.currentTimeMillis() + "_" + i + "_"
              + TableNameBuilder.extractRawTableName(offlineTableName));
      pinotTaskConfigs.add(new PinotTaskConfig(MergeRollupTask.TASK_TYPE, configs));
    }

    return pinotTaskConfigs;
  }
}
