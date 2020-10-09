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
package org.apache.pinot.controller.helix.core.minion.generator;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.minion.RealtimeToOfflineSegmentsTaskMetadata;
import org.apache.pinot.common.utils.CommonConstants.Segment;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoProvider;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.RealtimeToOfflineSegmentsTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link PinotTaskGenerator} implementation for generating tasks of type {@link RealtimeToOfflineSegmentsTask}
 *
 * These will be generated only for REALTIME tables.
 * At any given time, only 1 task of this type should be generated for a table.
 *
 * Steps:
 *  - The watermarkMillis is read from the {@link RealtimeToOfflineSegmentsTaskMetadata} ZNode found at MINION_TASK_METADATA/realtimeToOfflineSegmentsTask/tableNameWithType
 *      In case of cold-start, no ZNode will exist.
 *      A new ZNode will be created, with watermarkMillis as the smallest time found in the COMPLETED segments (or using start time config)
 *  - The execution window for the task is calculated as, windowStartMillis = waterMarkMillis, windowEndMillis = windowStartMillis + bucketTimeMillis,
 *      where bucketTime can be provided in the taskConfigs (default 1d)
 *  - If the execution window is not older than bufferTimeMillis, no task will be generated,
 *      where bufferTime can be provided in the taskConfigs (default 2d)
 *  - Segment metadata is scanned for all COMPLETED segments, to pick those containing data in window [windowStartMillis, windowEndMillis)
 *  - A PinotTaskConfig is created, with segment information, execution window, and any config specific to the task
 */
public class RealtimeToOfflineSegmentsTaskGenerator implements PinotTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeToOfflineSegmentsTaskGenerator.class);

  private static final String DEFAULT_BUCKET_PERIOD = "1d";
  private static final String DEFAULT_BUFFER_PERIOD = "2d";

  private final ClusterInfoProvider _clusterInfoProvider;

  public RealtimeToOfflineSegmentsTaskGenerator(ClusterInfoProvider clusterInfoProvider) {
    _clusterInfoProvider = clusterInfoProvider;
  }

  @Override
  public String getTaskType() {
    return RealtimeToOfflineSegmentsTask.TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    String taskType = RealtimeToOfflineSegmentsTask.TASK_TYPE;

    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();

    for (TableConfig tableConfig : tableConfigs) {
      String tableName = tableConfig.getTableName();

      if (tableConfig.getTableType() != TableType.REALTIME) {
        LOGGER.warn("Skip generating task: {} for non-REALTIME table: {}", taskType, tableName);
        continue;
      }

      String rawTableName = TableNameBuilder.extractRawTableName(tableName);
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);

      // Only schedule 1 task of this type, per table
      Map<String, TaskState> nonCompletedTasks =
          TaskGeneratorUtils.getNonCompletedTasks(taskType, realtimeTableName, _clusterInfoProvider);
      if (!nonCompletedTasks.isEmpty()) {
        LOGGER.warn("Found non-completed tasks: {} for same table: {}. Skipping scheduling new task.",
            nonCompletedTasks.keySet(), realtimeTableName);
        continue;
      }

      List<LLCRealtimeSegmentZKMetadata> realtimeSegmentsMetadataList =
          _clusterInfoProvider.getLLCRealtimeSegmentsMetadata(realtimeTableName);
      List<LLCRealtimeSegmentZKMetadata> completedSegmentsMetadataList = new ArrayList<>();
      for (LLCRealtimeSegmentZKMetadata metadata : realtimeSegmentsMetadataList) {
        if (metadata.getStatus().equals(Segment.Realtime.Status.DONE)) {
          completedSegmentsMetadataList.add(metadata);
        }
      }
      if (completedSegmentsMetadataList.isEmpty()) {
        LOGGER
            .info("No realtime completed segments found for table: {}, skipping task generation: {}", realtimeTableName,
                taskType);
        continue;
      }

      TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
      Preconditions.checkState(tableTaskConfig != null);
      Map<String, String> taskConfigs = tableTaskConfig.getConfigsForTaskType(taskType);
      Preconditions.checkState(taskConfigs != null, "Task config shouldn't be null for Table: {}", tableName);

      // Get the bucket size and buffer
      String bucketTimeStr =
          taskConfigs.getOrDefault(RealtimeToOfflineSegmentsTask.BUCKET_TIME_PERIOD_KEY, DEFAULT_BUCKET_PERIOD);
      String bufferTimeStr =
          taskConfigs.getOrDefault(RealtimeToOfflineSegmentsTask.BUFFER_TIME_PERIOD_KEY, DEFAULT_BUFFER_PERIOD);
      long bucketMillis = TimeUtils.convertPeriodToMillis(bucketTimeStr);
      long bufferMillis = TimeUtils.convertPeriodToMillis(bufferTimeStr);

      // Fetch RealtimeToOfflineSegmentsTaskMetadata ZNode for reading watermark
      RealtimeToOfflineSegmentsTaskMetadata realtimeToOfflineSegmentsTaskMetadata =
          _clusterInfoProvider.getMinionRealtimeToOfflineSegmentsTaskMetadata(realtimeTableName);

      if (realtimeToOfflineSegmentsTaskMetadata == null) {
        // No ZNode exists. Cold-start.
        long watermarkMillis;

        String startTimeStr = taskConfigs.get(RealtimeToOfflineSegmentsTask.START_TIME_MILLIS_KEY);
        if (startTimeStr != null) {
          // Use startTime config if provided in taskConfigs
          watermarkMillis = Long.parseLong(startTimeStr);
        } else {
          // Find the smallest time from all segments
          RealtimeSegmentZKMetadata minSegmentZkMetadata = null;
          for (LLCRealtimeSegmentZKMetadata realtimeSegmentZKMetadata : completedSegmentsMetadataList) {
            if (minSegmentZkMetadata == null || realtimeSegmentZKMetadata.getStartTime() < minSegmentZkMetadata
                .getStartTime()) {
              minSegmentZkMetadata = realtimeSegmentZKMetadata;
            }
          }
          Preconditions.checkState(minSegmentZkMetadata != null);

          // Convert the segment minTime to millis
          long minSegmentStartTimeMillis =
              minSegmentZkMetadata.getTimeUnit().toMillis(minSegmentZkMetadata.getStartTime());

          // Round off according to the bucket. This ensures we align the offline segments to proper time boundaries
          // For example, if start time millis is 20200813T12:34:59, we want to create the first segment for window [20200813, 20200814)
          watermarkMillis = (minSegmentStartTimeMillis / bucketMillis) * bucketMillis;
        }

        // Create RealtimeToOfflineSegmentsTaskMetadata ZNode using watermark calculated above
        realtimeToOfflineSegmentsTaskMetadata =
            new RealtimeToOfflineSegmentsTaskMetadata(realtimeTableName, watermarkMillis);
        _clusterInfoProvider.setRealtimeToOfflineSegmentsTaskMetadata(realtimeToOfflineSegmentsTaskMetadata);
      }

      // WindowStart = watermark. WindowEnd = windowStart + bucket.
      long windowStartMillis = realtimeToOfflineSegmentsTaskMetadata.getWatermarkMillis();
      long windowEndMillis = windowStartMillis + bucketMillis;

      // Check that execution window is older than bufferTime
      if (windowEndMillis > System.currentTimeMillis() - bufferMillis) {
        LOGGER.info(
            "Window with start: {} and end: {} is not older than buffer time: {} configured as {} ago. Skipping scheduling task: {}",
            windowStartMillis, windowEndMillis, bufferMillis, bufferTimeStr, taskType);
      }

      // Find all COMPLETED segments with data overlapping execution window: windowStart (inclusive) to windowEnd (exclusive)
      List<String> segmentNames = new ArrayList<>();
      List<String> downloadURLs = new ArrayList<>();
      for (LLCRealtimeSegmentZKMetadata realtimeSegmentZKMetadata : completedSegmentsMetadataList) {
        TimeUnit timeUnit = realtimeSegmentZKMetadata.getTimeUnit();
        long segmentStartTimeMillis = timeUnit.toMillis(realtimeSegmentZKMetadata.getStartTime());
        long segmentEndTimeMillis = timeUnit.toMillis(realtimeSegmentZKMetadata.getEndTime());

        if (windowStartMillis <= segmentEndTimeMillis && segmentStartTimeMillis < windowEndMillis) {
          segmentNames.add(realtimeSegmentZKMetadata.getSegmentName());
          downloadURLs.add(realtimeSegmentZKMetadata.getDownloadUrl());
        }
      }

      if (segmentNames.isEmpty()) {
        LOGGER.info("Found no eligible segments for task: {} with window [{} - {}). Skipping task generation", taskType,
            windowStartMillis, windowEndMillis);
        continue;
      }

      Map<String, String> configs = new HashMap<>();
      configs.put(MinionConstants.TABLE_NAME_KEY, realtimeTableName);
      configs.put(MinionConstants.SEGMENT_NAME_KEY, StringUtils.join(segmentNames, ","));
      configs.put(MinionConstants.DOWNLOAD_URL_KEY, StringUtils.join(downloadURLs, MinionConstants.URL_SEPARATOR));
      configs.put(MinionConstants.UPLOAD_URL_KEY, _clusterInfoProvider.getVipUrl() + "/segments");

      // Execution window
      configs.put(RealtimeToOfflineSegmentsTask.WINDOW_START_MILLIS_KEY, String.valueOf(windowStartMillis));
      configs.put(RealtimeToOfflineSegmentsTask.WINDOW_END_MILLIS_KEY, String.valueOf(windowEndMillis));

      // Segment processor configs
      String timeColumnTransformationConfig =
          taskConfigs.get(RealtimeToOfflineSegmentsTask.TIME_COLUMN_TRANSFORM_FUNCTION_KEY);
      if (timeColumnTransformationConfig != null) {
        configs.put(RealtimeToOfflineSegmentsTask.TIME_COLUMN_TRANSFORM_FUNCTION_KEY, timeColumnTransformationConfig);
      }
      String collectorTypeConfig = taskConfigs.get(RealtimeToOfflineSegmentsTask.COLLECTOR_TYPE_KEY);
      if (collectorTypeConfig != null) {
        configs.put(RealtimeToOfflineSegmentsTask.COLLECTOR_TYPE_KEY, collectorTypeConfig);
      }
      for (Map.Entry<String, String> entry : taskConfigs.entrySet()) {
        if (entry.getKey().endsWith(RealtimeToOfflineSegmentsTask.AGGREGATION_TYPE_KEY_SUFFIX)) {
          configs.put(entry.getKey(), entry.getValue());
        }
      }
      String maxNumRecordsPerSegmentConfig =
          taskConfigs.get(RealtimeToOfflineSegmentsTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY);
      if (maxNumRecordsPerSegmentConfig != null) {
        configs.put(RealtimeToOfflineSegmentsTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, maxNumRecordsPerSegmentConfig);
      }

      pinotTaskConfigs.add(new PinotTaskConfig(taskType, configs));
    }
    return pinotTaskConfigs;
  }
}
