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
package org.apache.pinot.plugin.minion.tasks.realtimetoofflinesegments;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.task.TaskState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.minion.RealtimeToOfflineSegmentsTaskMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.RealtimeToOfflineSegmentsTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.plugin.minion.tasks.MinionTaskUtils;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants.Segment;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link PinotTaskGenerator} implementation for generating tasks of type {@link RealtimeToOfflineSegmentsTask}
 *
 * These will be generated only for REALTIME tables.
 * At any given time, only 1 task of this type should be generated for a table.
 *
 * Steps:
 *  - The watermarkMs is read from the {@link RealtimeToOfflineSegmentsTaskMetadata} ZNode
 *  found at MINION_TASK_METADATA/${tableNameWithType}/RealtimeToOfflineSegmentsTask
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
public class RealtimeToOfflineSegmentsTaskGenerator extends BaseTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeToOfflineSegmentsTaskGenerator.class);

  private static final String DEFAULT_BUCKET_PERIOD = "1d";
  private static final String DEFAULT_BUFFER_PERIOD = "2d";

  @Override
  public String getTaskType() {
    return RealtimeToOfflineSegmentsTask.TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    String taskType = RealtimeToOfflineSegmentsTask.TASK_TYPE;
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();

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

      // Get all segment metadata for completed segments (DONE/UPLOADED status).
      List<SegmentZKMetadata> completedSegmentsZKMetadata = new ArrayList<>();
      Map<Integer, String> partitionToLatestLLCSegmentName = new HashMap<>();
      Set<Integer> allPartitions = new HashSet<>();
      getCompletedSegmentsInfo(realtimeTableName, completedSegmentsZKMetadata, partitionToLatestLLCSegmentName,
          allPartitions);
      if (completedSegmentsZKMetadata.isEmpty()) {
        LOGGER.info("No realtime-completed segments found for table: {}, skipping task generation: {}",
            realtimeTableName, taskType);
        continue;
      }
      allPartitions.removeAll(partitionToLatestLLCSegmentName.keySet());
      if (!allPartitions.isEmpty()) {
        LOGGER.info(
            "Partitions: {} have no completed segments. Table: {} is not ready for {}. Skipping task generation.",
            allPartitions, realtimeTableName, taskType);
        continue;
      }

      TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
      Preconditions.checkState(tableTaskConfig != null);
      Map<String, String> taskConfigs = tableTaskConfig.getConfigsForTaskType(taskType);
      Preconditions.checkState(taskConfigs != null, "Task config shouldn't be null for table: {}", realtimeTableName);

      // Get the bucket size and buffer
      String bucketTimePeriod =
          taskConfigs.getOrDefault(RealtimeToOfflineSegmentsTask.BUCKET_TIME_PERIOD_KEY, DEFAULT_BUCKET_PERIOD);
      String bufferTimePeriod =
          taskConfigs.getOrDefault(RealtimeToOfflineSegmentsTask.BUFFER_TIME_PERIOD_KEY, DEFAULT_BUFFER_PERIOD);
      long bucketMs = TimeUtils.convertPeriodToMillis(bucketTimePeriod);
      long bufferMs = TimeUtils.convertPeriodToMillis(bufferTimePeriod);

      // Get watermark from RealtimeToOfflineSegmentsTaskMetadata ZNode. WindowStart = watermark. WindowEnd =
      // windowStart + bucket.
      long windowStartMs = getWatermarkMs(realtimeTableName, completedSegmentsZKMetadata, bucketMs);
      long windowEndMs = windowStartMs + bucketMs;

      // Find all COMPLETED segments with data overlapping execution window: windowStart (inclusive) to windowEnd
      // (exclusive)
      List<String> segmentNames = new ArrayList<>();
      List<String> downloadURLs = new ArrayList<>();
      Set<String> lastLLCSegmentPerPartition = new HashSet<>(partitionToLatestLLCSegmentName.values());
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
            if (lastLLCSegmentPerPartition.contains(segmentName) && segmentEndTimeMs < windowEndMs) {
              LOGGER.info("Window data overflows into CONSUMING segments for partition of segment: {}. Skipping task "
                  + "generation: {}", segmentName, taskType);
              skipGenerate = true;
              break;
            }
            segmentNames.add(segmentName);
            downloadURLs.add(segmentZKMetadata.getDownloadUrl());
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

      Map<String, String> configs = MinionTaskUtils.getPushTaskConfig(realtimeTableName, taskConfigs,
          _clusterInfoAccessor);
      configs.put(MinionConstants.TABLE_NAME_KEY, realtimeTableName);
      configs.put(MinionConstants.SEGMENT_NAME_KEY, StringUtils.join(segmentNames, ","));
      configs.put(MinionConstants.DOWNLOAD_URL_KEY, StringUtils.join(downloadURLs, MinionConstants.URL_SEPARATOR));
      configs.put(MinionConstants.UPLOAD_URL_KEY, _clusterInfoAccessor.getVipUrl() + "/segments");

      // Segment processor configs
      configs.put(RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY, String.valueOf(windowStartMs));
      configs.put(RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY, String.valueOf(windowEndMs));
      String roundBucketTimePeriod = taskConfigs.get(RealtimeToOfflineSegmentsTask.ROUND_BUCKET_TIME_PERIOD_KEY);
      if (roundBucketTimePeriod != null) {
        configs.put(RealtimeToOfflineSegmentsTask.ROUND_BUCKET_TIME_PERIOD_KEY, roundBucketTimePeriod);
      }
      // NOTE: Check and put both keys for backward-compatibility
      String mergeType = taskConfigs.get(RealtimeToOfflineSegmentsTask.MERGE_TYPE_KEY);
      if (mergeType == null) {
        mergeType = taskConfigs.get(RealtimeToOfflineSegmentsTask.COLLECTOR_TYPE_KEY);
      }
      if (mergeType != null) {
        configs.put(RealtimeToOfflineSegmentsTask.MERGE_TYPE_KEY, mergeType);
        configs.put(RealtimeToOfflineSegmentsTask.COLLECTOR_TYPE_KEY, mergeType);
      }
      for (Map.Entry<String, String> entry : taskConfigs.entrySet()) {
        if (entry.getKey().endsWith(RealtimeToOfflineSegmentsTask.AGGREGATION_TYPE_KEY_SUFFIX)) {
          configs.put(entry.getKey(), entry.getValue());
        }
      }
      String maxNumRecordsPerSegment = taskConfigs.get(RealtimeToOfflineSegmentsTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY);
      if (maxNumRecordsPerSegment != null) {
        configs.put(RealtimeToOfflineSegmentsTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, maxNumRecordsPerSegment);
      }

      pinotTaskConfigs.add(new PinotTaskConfig(taskType, configs));
      LOGGER.info("Finished generating task configs for table: {} for task: {}", realtimeTableName, taskType);
    }
    return pinotTaskConfigs;
  }

  /**
   * Fetch completed (DONE/UPLOADED) segment and partition information
   *
   * @param realtimeTableName the realtime table name
   * @param completedSegmentsZKMetadata list for collecting the completed (DONE/UPLOADED) segments ZK metadata
   * @param partitionToLatestLLCSegmentName map for collecting the partitionId to the latest LLC segment name
   * @param allPartitions set for collecting all partition ids
   */
  private void getCompletedSegmentsInfo(String realtimeTableName, List<SegmentZKMetadata> completedSegmentsZKMetadata,
      Map<Integer, String> partitionToLatestLLCSegmentName, Set<Integer> allPartitions) {
    List<SegmentZKMetadata> segmentsZKMetadata = _clusterInfoAccessor.getSegmentsZKMetadata(realtimeTableName);

    Map<Integer, LLCSegmentName> latestLLCSegmentNameMap = new HashMap<>();
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      Segment.Realtime.Status status = segmentZKMetadata.getStatus();
      if (status.isCompleted()) {
        completedSegmentsZKMetadata.add(segmentZKMetadata);
      }

      // Skip UPLOADED segments that don't conform to the LLC segment name
      LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentZKMetadata.getSegmentName());
      if (llcSegmentName != null) {
        int partitionId = llcSegmentName.getPartitionGroupId();
        allPartitions.add(partitionId);
        if (status.isCompleted()) {
          latestLLCSegmentNameMap.compute(partitionId, (k, latestLLCSegmentName) -> {
            if (latestLLCSegmentName == null
                || llcSegmentName.getSequenceNumber() > latestLLCSegmentName.getSequenceNumber()) {
              return llcSegmentName;
            } else {
              return latestLLCSegmentName;
            }
          });
        }
      }
    }

    for (Map.Entry<Integer, LLCSegmentName> entry : latestLLCSegmentNameMap.entrySet()) {
      partitionToLatestLLCSegmentName.put(entry.getKey(), entry.getValue().getSegmentName());
    }
  }

  /**
   * Get the watermark from the RealtimeToOfflineSegmentsMetadata ZNode.
   * If the znode is null, computes the watermark using either the start time config or the start time from segment
   * metadata
   */
  private long getWatermarkMs(String realtimeTableName, List<SegmentZKMetadata> completedSegmentsZKMetadata,
      long bucketMs) {
    ZNRecord realtimeToOfflineZNRecord =
        _clusterInfoAccessor.getMinionTaskMetadataZNRecord(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE,
            realtimeTableName);
    RealtimeToOfflineSegmentsTaskMetadata realtimeToOfflineSegmentsTaskMetadata =
        realtimeToOfflineZNRecord != null ? RealtimeToOfflineSegmentsTaskMetadata.fromZNRecord(
            realtimeToOfflineZNRecord) : null;

    if (realtimeToOfflineSegmentsTaskMetadata == null) {
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

      // Create RealtimeToOfflineSegmentsTaskMetadata ZNode using watermark calculated above
      realtimeToOfflineSegmentsTaskMetadata = new RealtimeToOfflineSegmentsTaskMetadata(realtimeTableName, watermarkMs);
      _clusterInfoAccessor.setMinionTaskMetadata(realtimeToOfflineSegmentsTaskMetadata,
          MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, -1);
    }
    return realtimeToOfflineSegmentsTaskMetadata.getWatermarkMs();
  }
}
