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
package org.apache.pinot.plugin.minion.tasks.materializedview;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.task.TaskState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.minion.MaterializedViewTaskMetadata;
import org.apache.pinot.common.minion.RealtimeToOfflineSegmentsTaskMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.MaterializedViewTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * A {@link PinotTaskGenerator} implementation for generating tasks of type {@link MaterializedViewTask}
 *
 * These will be generated only for REALTIME tables.
 * At any given time, only 1 task of this type should be generated for a table.
 *
 * Steps:
 *  - The watermarkMs is read from the {@link MaterializedViewTaskMetadata} ZNode
 *  found at MINION_TASK_METADATA/MaterializedViewTask/tableNameWithType
 *  In case of cold-start, no ZNode will exist.
 *  A new ZNode will be created, with watermarkMs as the smallest time found in the COMPLETED segments
 *
 *  - The execution window for the task is calculated as,
 *  windowStartMs = watermarkMs, windowEndMs = windowStartMs + bucketTimeMs,
 *  where bucketTime can be provided in the taskConfigs (default 1d)
 *
 *  - If the execution window is not older than bufferTimeMs, no task will be generated,
 *  where bufferTime can be provided in the taskConfigs (default 2d)
 *
 *  - Segment metadata is scanned for all COMPLETED segments,
 *  to pick those containing data in window [windowStartMs, windowEndMs)
 *
 *  - There are some special considerations for using last completed segment of a partition.
 *  Such segments will be checked for segment endTime, to ensure there's no overflow into CONSUMING segments
 *
 *  - A PinotTaskConfig is created, with segment information, execution window, and any config specific to the task
 */
@TaskGenerator
public class MaterializedViewTaskGenerator extends BaseTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedViewTaskGenerator.class);

  private static final String DEFAULT_BUCKET_PERIOD = "1d";
  private static final String DEFAULT_BUFFER_PERIOD = "2d";

  @Override
  public String getTaskType() {
    return MinionConstants.MaterializedViewTask.TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    String taskType = getTaskType();
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();
    MaterializedViewTaskMetadata materializedViewTaskMetadata = null;
    for (TableConfig tableConfig : tableConfigs) {
      String realtimeTableName = tableConfig.getTableName();

      if (tableConfig.getTableType() != TableType.REALTIME) {
        LOGGER.warn("Skip generating task: {} for non-REALTIME table: {}", taskType, realtimeTableName);
        continue;
      }

      LOGGER.info("Start generating task configs for table: {} for task: {}", realtimeTableName, taskType);

      // Only schedule 1 task of this type, per table
      Map<String, TaskState> incompleteTasks =
          TaskGeneratorUtils.getIncompleteTasks(taskType, realtimeTableName, _clusterInfoAccessor);
      if (!incompleteTasks.isEmpty()) {
        LOGGER.warn("Found incomplete tasks: {} for same table: {}. Skipping task generation.",
            incompleteTasks.keySet(), realtimeTableName);
        continue;
      }
      ZNRecord materializedViewTaskZNRecord = _clusterInfoAccessor
          .getMinionTaskMetadataZNRecord(MaterializedViewTask.TASK_TYPE, realtimeTableName);
      materializedViewTaskMetadata =
          materializedViewTaskZNRecord != null ? MaterializedViewTaskMetadata.fromZNRecord(materializedViewTaskZNRecord)
              : new MaterializedViewTaskMetadata(realtimeTableName, new HashMap<>());

      // Get all segment metadata for completed segments (DONE status).
      List<SegmentZKMetadata> completedSegmentsZKMetadata = new ArrayList<>();
      Map<Integer, String> partitionToLatestCompletedSegmentName = new HashMap<>();
      Set<Integer> allPartitions = new HashSet<>();
      getCompletedSegmentsInfo(realtimeTableName, completedSegmentsZKMetadata, partitionToLatestCompletedSegmentName,
          allPartitions);
      if (completedSegmentsZKMetadata.isEmpty()) {
        LOGGER.info("No realtime-completed segments found for table: {}, skipping task generation: {}",
            realtimeTableName, taskType);
        continue;
      }
      allPartitions.removeAll(partitionToLatestCompletedSegmentName.keySet());
      if (!allPartitions.isEmpty()) {
        LOGGER.info(
            "Partitions: {} have no completed segments. Table: {} is not ready for {}. Skipping task generation.",
            allPartitions, realtimeTableName, taskType);
        continue;
      }

      TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
      Preconditions.checkState(tableTaskConfig != null);
      Map<String, String> taskConfigs = tableTaskConfig.getConfigsForTaskType(taskType);

      // Get the bucket size and buffer
      String bucketTimePeriod =
          taskConfigs.getOrDefault(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, DEFAULT_BUCKET_PERIOD);
      String bufferTimePeriod =
          taskConfigs.getOrDefault(MaterializedViewTask.BUFFER_TIME_PERIOD_KEY, DEFAULT_BUFFER_PERIOD);
      long bucketMs = TimeUtils.convertPeriodToMillis(bucketTimePeriod);
      long bufferMs = TimeUtils.convertPeriodToMillis(bufferTimePeriod);

      String selectedDimensionListStr =
          taskConfigs.get(MaterializedViewTask.SELECTED_DIMENSION_LIST);
      Preconditions.checkState(!StringUtils.isEmpty(selectedDimensionListStr),
          "You have to specify selectedDimensionList in your task config");
      List<String> selectedDimensionList = Arrays.asList(StringUtils.split(selectedDimensionListStr, ","))
          .stream().map(s -> s.trim()).filter(s -> StringUtils.isNotBlank(s)).collect(Collectors.toList());

      selectedDimensionListStr = selectedDimensionList.stream().collect(Collectors.joining(","));
      String mvName = taskConfigs.get(MaterializedViewTask.MATERIALIZED_VIEW_NAME);

      // Get watermark from RealtimeToOfflineSegmentsTaskMetadata ZNode. WindowStart = watermark. WindowEnd =
      // windowStart + bucket.
      long windowStartMs = getWatermarkMs(completedSegmentsZKMetadata, materializedViewTaskMetadata, bucketMs,
          mvName);
      long windowEndMs = windowStartMs + bucketMs;

      // Find all COMPLETED segments with data overlapping execution window: windowStart (inclusive) to windowEnd
      // (exclusive)
      List<String> segmentNames = new ArrayList<>();
      List<String> downloadURLs = new ArrayList<>();
      Set<String> lastCompletedSegmentPerPartition = new HashSet<>(partitionToLatestCompletedSegmentName.values());
      boolean skipGenerate = false;
      while (true) {
        // Check that execution window is older than bufferTime
        if (windowEndMs > System.currentTimeMillis() - bufferMs) {
          LOGGER.info(
              "Window with start: {} and end: {} is not older than buffer time: {} configured as {} ago. Skipping task "
                  + "generation: {}", windowStartMs, windowEndMs, bufferMs, bufferTimePeriod, taskType);
          skipGenerate = true;
          break;
        }

        for (SegmentZKMetadata segmentZKMetadata : completedSegmentsZKMetadata) {
          String segmentName = segmentZKMetadata.getSegmentName();
          long segmentStartTimeMs = segmentZKMetadata.getStartTimeMs();
          long segmentEndTimeMs = segmentZKMetadata.getEndTimeMs();

          // Check overlap with window
          if (windowStartMs <= segmentEndTimeMs && segmentStartTimeMs < windowEndMs) {
            // If last completed segment is being used, make sure that segment crosses over end of window.
            // In the absence of this check, CONSUMING segments could contain some portion of the window. That data
            // would be skipped forever.
            if (lastCompletedSegmentPerPartition.contains(segmentName) && segmentEndTimeMs < windowEndMs) {
              LOGGER.info("Window data overflows into CONSUMING segments for partition of segment: {}. Skipping task "
                  + "generation: {}", segmentName, taskType);
              skipGenerate = true;
              break;
            }
            segmentNames.add(segmentName);
            downloadURLs.add(segmentZKMetadata.getDownloadUrl());
            if (StringUtils.isEmpty(segmentZKMetadata.getDownloadUrl())) {
              LOGGER.error("segment {} doesn't have downloadURls", segmentName);
            }
          }
        }
        if (skipGenerate || !segmentNames.isEmpty()) {
          break;
        }

        LOGGER.info("Found no eligible segments for task: {} with window [{} - {}), moving to the next time bucket",
            taskType, windowStartMs, windowEndMs);
        windowStartMs = windowEndMs;
        windowEndMs += bucketMs;
      }

      if (skipGenerate) {
        continue;
      }
      // update watermarkMs of taskIndex
      materializedViewTaskMetadata.getWatermarkMap().put(mvName, windowEndMs);

      Map<String, String> configs = new HashMap<>();
      configs.put(MinionConstants.TABLE_NAME_KEY, realtimeTableName);
      configs.put(MinionConstants.SEGMENT_NAME_KEY, StringUtils.join(segmentNames, ","));
      configs.put(MinionConstants.DOWNLOAD_URL_KEY, StringUtils.join(downloadURLs, MinionConstants.URL_SEPARATOR));
      configs.put(MinionConstants.UPLOAD_URL_KEY, _clusterInfoAccessor.getVipUrl() + "/segments");


      // Segment processor configs
      configs.put(MaterializedViewTask.WINDOW_START_MS_KEY, String.valueOf(windowStartMs));
      configs.put(MaterializedViewTask.WINDOW_END_MS_KEY, String.valueOf(windowEndMs));
      configs.put(MaterializedViewTask.MATERIALIZED_VIEW_SEGMENTS_TASK_TYPE, taskType);
      configs.put(MaterializedViewTask.FILTER_FUNCTION,
          taskConfigs.get(MaterializedViewTask.FILTER_FUNCTION));
      configs.put(MaterializedViewTask.FILTER_FUNCTION,
          taskConfigs.get(MaterializedViewTask.FILTER_FUNCTION));
      String roundBucketTimePeriod = taskConfigs.get(MaterializedViewTask.ROUND_BUCKET_TIME_PERIOD_KEY);
      if (roundBucketTimePeriod != null) {
        configs.put(MaterializedViewTask.ROUND_BUCKET_TIME_PERIOD_KEY, roundBucketTimePeriod);
      }
      configs.put(MaterializedViewTask.MATERIALIZED_VIEW_MARK, mvName);

      configs.put(MaterializedViewTask.SELECTED_DIMENSION_LIST, selectedDimensionListStr);
      configs.put(MaterializedViewTask.MATERIALIZED_VIEW_NAME, mvName + "_OFFLINE");

      // NOTE: Check and put both keys for backward-compatibility
      String mergeType = taskConfigs.get(MaterializedViewTask.MERGE_TYPE_KEY);
      if (mergeType != null) {
        configs.put(MaterializedViewTask.MERGE_TYPE_KEY, mergeType);
      }
      for (Map.Entry<String, String> entry : taskConfigs.entrySet()) {
        if (entry.getKey().endsWith(MaterializedViewTask.AGGREGATION_TYPE_KEY_SUFFIX)) {
          configs.put(entry.getKey(), entry.getValue());
        }
      }
      String maxNumRecordsPerSegment = taskConfigs.get(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY);
      if (maxNumRecordsPerSegment != null) {
        configs.put(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, maxNumRecordsPerSegment);
      }

      pinotTaskConfigs.add(new PinotTaskConfig(taskType, configs));
      LOGGER.info("Finished generating task configs for table: {} for task: {}", realtimeTableName, taskType);
    }
    return pinotTaskConfigs;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(TableConfig tableConfig, Map<String, String> taskConfigs)
      throws Exception {
    return null;
  }

  /**
   * Fetch completed (non-consuming) segment and partition information
   * @param realtimeTableName the realtime table name
   * @param completedSegmentsZKMetadata list for collecting the completed segments ZK metadata
   * @param partitionToLatestCompletedSegmentName map for collecting the partitionId to the latest completed segment
   *                                              name
   * @param allPartitions set for collecting all partition ids
   */
  private void getCompletedSegmentsInfo(String realtimeTableName, List<SegmentZKMetadata> completedSegmentsZKMetadata,
      Map<Integer, String> partitionToLatestCompletedSegmentName, Set<Integer> allPartitions) {
    List<SegmentZKMetadata> segmentsZKMetadata = _clusterInfoAccessor.getSegmentsZKMetadata(realtimeTableName);

    Map<Integer, LLCSegmentName> latestLLCSegmentNameMap = new HashMap<>();
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentZKMetadata.getSegmentName());
      allPartitions.add(llcSegmentName.getPartitionGroupId());

      if (segmentZKMetadata.getStatus().equals(CommonConstants.Segment.Realtime.Status.DONE)) {
        completedSegmentsZKMetadata.add(segmentZKMetadata);
        latestLLCSegmentNameMap.compute(llcSegmentName.getPartitionGroupId(),
            (partitionGroupId, latestLLCSegmentName) -> {
              if (latestLLCSegmentName == null) {
                return llcSegmentName;
              } else {
                if (llcSegmentName.getSequenceNumber() > latestLLCSegmentName.getSequenceNumber()) {
                  return llcSegmentName;
                } else {
                  return latestLLCSegmentName;
                }
              }
            });
      }
    }

    for (Map.Entry<Integer, LLCSegmentName> entry : latestLLCSegmentNameMap.entrySet()) {
      partitionToLatestCompletedSegmentName.put(entry.getKey(), entry.getValue().getSegmentName());
    }
  }

  /**
   * Get the watermark from the MaterializedViewTaskSegmentsMetadata ZNode.
   * If the znode is null, computes the watermark using either the start time config or the start time from segment
   * metadata
   */
  private long getWatermarkMs(List<SegmentZKMetadata> completedSegmentsZKMetadata,
      MaterializedViewTaskMetadata materializedViewTaskMetadata, long bucketMs, String mvName) {

    if (materializedViewTaskMetadata.getWatermarkMap().get(mvName) == null) {
      // No ZNode exists. Cold-start.
      long watermarkMs;
      // Find the smallest time from all segments
      long minStartTimeMs = Long.MAX_VALUE;
      for (SegmentZKMetadata segmentZKMetadata : completedSegmentsZKMetadata) {
        minStartTimeMs = Math.min(minStartTimeMs, segmentZKMetadata.getStartTimeMs());
      }
      Preconditions.checkState(minStartTimeMs != Long.MAX_VALUE);

      // Round off according to the bucket. This ensures we align the offline segments to proper time boundaries
      // For example, if start time millis is 20200813T12:34:59, we want to create the first segment for window
      // [20200813, 20200814)
      watermarkMs = (minStartTimeMs / bucketMs) * bucketMs;
      materializedViewTaskMetadata.getWatermarkMap().put(mvName, watermarkMs);
      _clusterInfoAccessor.setMinionTaskMetadata(materializedViewTaskMetadata,
          MaterializedViewTask.TASK_TYPE, -1);
    }
    return materializedViewTaskMetadata.getWatermarkMap().get(mvName);
  }
}
