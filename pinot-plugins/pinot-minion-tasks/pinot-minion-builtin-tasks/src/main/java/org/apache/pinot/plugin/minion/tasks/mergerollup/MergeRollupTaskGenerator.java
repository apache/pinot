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
package org.apache.pinot.plugin.minion.tasks.mergerollup;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.I0Itec.zkclient.exception.ZkException;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.task.TaskState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageUtils;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.minion.MergeRollupTaskMetadata;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.MergeRollupTask;
import org.apache.pinot.core.common.MinionConstants.MergeTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.plugin.minion.tasks.MergeTaskUtils;
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
 *    - Fetch all segments, select segments based on segment lineage (removing segmentsFrom for COMPLETED lineage
 *      entry and segmentsTo for IN_PROGRESS lineage entry)
 *    - Remove empty segments
 *    - Sort segments based on startTime and endTime in ascending order
 *
 *  For each mergeLevel (from lowest to highest, e.g. Hourly -> Daily -> Monthly -> Yearly):
 *    - Skip scheduling if there's incomplete task for the mergeLevel
 *    - Schedule tasks for at most k time buckets, k is up to maxNumParallelBuckets (by default 1) at best effort
 *    - Repeat until k time buckets get created or we loop through all the candidate segments:
 *      - Calculate merge/roll-up bucket:
 *        - Read watermarkMs from the {@link MergeRollupTaskMetadata} ZNode found at
 *          MINION_TASK_METADATA/${tableNameWithType}/MergeRollupTask
 *          In case of cold-start, no ZNode will exist.
 *          A new ZNode will be created, with watermarkMs as the smallest time found in all segments truncated to the
 *          closest bucket start time.
 *        - The execution window for the task is calculated as,
 *          bucketStartMs = watermarkMs
 *          bucketEndMs = bucketStartMs + bucketTimeMs
 *          - bucketEndMs must be equal or older than the bufferTimeMs
 *          - bucketEndMs of higher merge level should be less or equal to the waterMarkMs of lower level
 *        - Bump up target window and watermark if needed.
 *          - If there's no unmerged segments (by checking segment zk metadata {mergeRollupTask.mergeLevel: level}) for
 *            current window, keep bumping up the watermark and target window until unmerged segments are found.
 *          - Else skip the scheduling.
 *      - Select segments for the bucket:
 *        - Skip buckets which all segments are merged
 *        - If there's no spilled over segments (segments spanning multiple time buckets), schedule buckets in parallel
 *        - Else, schedule buckets till the first one that has spilled over data (included), so the spilled over data
 *          will be merged next round
 *      - Create the tasks for the current bucket (and per partition for partitioned tables) based on
 *        maxNumRecordsPerTask
 */
@TaskGenerator
public class MergeRollupTaskGenerator extends BaseTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MergeRollupTaskGenerator.class);

  private static final int DEFAULT_MAX_NUM_RECORDS_PER_TASK = 50_000_000;
  private static final int DEFAULT_NUM_PARALLEL_BUCKETS = 1;
  private static final String REFRESH = "REFRESH";

  // This is the metric that keeps track of the task delay in the number of time buckets. For example, if we see this
  // number to be 7 and merge task is configured with "bucketTimePeriod = 1d", this means that we have 7 days of
  // delay. When operating merge/roll-up task in production, we should set the alert on this metrics to find out the
  // delay. Setting the alert on 7 time buckets of delay would be a good starting point.
  private static final String MERGE_ROLLUP_TASK_DELAY_IN_NUM_BUCKETS = "mergeRollupTaskDelayInNumBuckets";

  // tableNameWithType -> mergeLevel -> watermarkMs
  private final Map<String, Map<String, Long>> _mergeRollupWatermarks = new HashMap<>();
  // tableNameWithType -> maxValidBucketEndTime
  private final Map<String, Long> _tableMaxValidBucketEndTimeMs = new HashMap<>();

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
      SegmentLineageUtils.filterSegmentsBasedOnLineageInPlace(preSelectedSegmentsBasedOnLineage, segmentLineage);

      List<SegmentZKMetadata> preSelectedSegments = new ArrayList<>();
      for (SegmentZKMetadata segment : allSegments) {
        if (preSelectedSegmentsBasedOnLineage.contains(segment.getSegmentName()) && segment.getTotalDocs() > 0
            && MergeTaskUtils.allowMerge(segment)) {
          preSelectedSegments.add(segment);
        }
      }

      if (preSelectedSegments.isEmpty()) {
        // Reset the watermark time if no segment found. This covers the case where the table is newly created or
        // all segments for the existing table got deleted.
        resetDelayMetrics(offlineTableName);
        LOGGER.info("Skip generating task: {} for table: {}, no segment is found.", taskType, offlineTableName);
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
      for (Map.Entry<String, TaskState> entry : TaskGeneratorUtils.getIncompleteTasks(taskType, offlineTableName,
          _clusterInfoAccessor).entrySet()) {
        for (PinotTaskConfig taskConfig : _clusterInfoAccessor.getTaskConfigs(entry.getKey())) {
          inCompleteMergeLevels.add(taskConfig.getConfigs().get(MergeRollupTask.MERGE_LEVEL_KEY));
        }
      }

      ZNRecord mergeRollupTaskZNRecord = _clusterInfoAccessor
          .getMinionTaskMetadataZNRecord(MinionConstants.MergeRollupTask.TASK_TYPE, offlineTableName);
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

        // Get the bucket size, buffer size and maximum number of parallel buckets (by default 1)
        String bucketPeriod = mergeConfigs.get(MergeTask.BUCKET_TIME_PERIOD_KEY);
        long bucketMs = TimeUtils.convertPeriodToMillis(bucketPeriod);
        if (bucketMs <= 0) {
          LOGGER.error("Bucket time period: {} (table : {}, mergeLevel : {}) must be larger than 0", bucketPeriod,
              offlineTableName, mergeLevel);
          continue;
        }
        String bufferPeriod = mergeConfigs.get(MergeTask.BUFFER_TIME_PERIOD_KEY);
        long bufferMs = TimeUtils.convertPeriodToMillis(bufferPeriod);
        if (bufferMs < 0) {
          LOGGER.error("Buffer time period: {} (table : {}, mergeLevel : {}) must be larger or equal to 0",
              bufferPeriod, offlineTableName, mergeLevel);
          continue;
        }
        String maxNumParallelBucketsStr = mergeConfigs.get(MergeTask.MAX_NUM_PARALLEL_BUCKETS);
        int maxNumParallelBuckets = maxNumParallelBucketsStr != null ? Integer.parseInt(maxNumParallelBucketsStr)
            : DEFAULT_NUM_PARALLEL_BUCKETS;
        if (maxNumParallelBuckets <= 0) {
          LOGGER.error("Maximum number of parallel buckets: {} (table : {}, mergeLevel : {}) must be larger than 0",
              maxNumParallelBuckets, offlineTableName, mergeLevel);
          continue;
        }

        // Get watermark from MergeRollupTaskMetadata ZNode
        // bucketStartMs = watermarkMs
        // bucketEndMs = bucketStartMs + bucketMs
        long watermarkMs =
            getWatermarkMs(preSelectedSegments.get(0).getStartTimeMs(), bucketMs, mergeLevel, mergeRollupTaskMetadata);
        long bucketStartMs = watermarkMs;
        long bucketEndMs = bucketStartMs + bucketMs;
        // Create delay metrics even if there's no task scheduled, this helps the case that the controller is restarted
        // but the metrics are not available until the controller schedules a valid task
        long maxValidBucketEndTimeMs = Long.MIN_VALUE;
        for (SegmentZKMetadata preSelectedSegment : preSelectedSegments) {
          // Compute maxValidBucketEndTimeMs among segments that are ready for merge
          long currentValidBucketEndTimeMs = getValidBucketEndTimeMsForSegment(preSelectedSegment, bucketMs, bufferMs);
          maxValidBucketEndTimeMs = Math.max(maxValidBucketEndTimeMs, currentValidBucketEndTimeMs);
        }
        createOrUpdateDelayMetrics(offlineTableName, mergeLevel, null, watermarkMs, maxValidBucketEndTimeMs,
            bufferMs, bucketMs);
        if (!isValidBucketEndTime(bucketEndMs, bufferMs, lowerMergeLevel, mergeRollupTaskMetadata)) {
          LOGGER.info("Bucket with start: {} and end: {} (table : {}, mergeLevel : {}) cannot be merged yet",
              bucketStartMs, bucketEndMs, offlineTableName, mergeLevel);
          continue;
        }

        // Find overlapping segments for each bucket, skip the buckets that has all segments merged
        List<List<SegmentZKMetadata>> selectedSegmentsForAllBuckets = new ArrayList<>(maxNumParallelBuckets);
        List<SegmentZKMetadata> selectedSegmentsForBucket = new ArrayList<>();
        boolean hasUnmergedSegments = false;
        boolean hasSpilledOverData = false;

        // The for loop terminates in following cases:
        // 1. Found buckets with unmerged segments:
        //    For each bucket find all segments overlapping with the target bucket, skip the bucket if all overlapping
        //    segments are merged. Schedule k (numParallelBuckets) buckets at most, and stops at the first bucket that
        //    contains spilled over data.
        // 2. There's no bucket with unmerged segments, skip scheduling
        for (SegmentZKMetadata preSelectedSegment : preSelectedSegments) {
          long startTimeMs = preSelectedSegment.getStartTimeMs();
          if (startTimeMs < bucketEndMs) {
            long endTimeMs = preSelectedSegment.getEndTimeMs();
            if (endTimeMs >= bucketStartMs) {
              // For segments overlapping with current bucket, add to the result list
              if (!isMergedSegment(preSelectedSegment, mergeLevel)) {
                hasUnmergedSegments = true;
              }
              if (hasSpilledOverData(preSelectedSegment, bucketMs)) {
                hasSpilledOverData = true;
              }
              selectedSegmentsForBucket.add(preSelectedSegment);
            }
            // endTimeMs < bucketStartMs
            // Haven't find the first overlapping segment, continue to the next segment
          } else {
            // Has gone through all overlapping segments for current bucket
            if (hasUnmergedSegments) {
              // Add the bucket if there are unmerged segments
              selectedSegmentsForAllBuckets.add(selectedSegmentsForBucket);
            }

            if (selectedSegmentsForAllBuckets.size() == maxNumParallelBuckets || hasSpilledOverData) {
              // If there are enough buckets or found spilled over data, schedule merge tasks
              break;
            } else {
              // Start with a new bucket
              // TODO: If there are many small merged segments, we should merge them again
              selectedSegmentsForBucket = new ArrayList<>();
              hasUnmergedSegments = false;
              bucketStartMs = (startTimeMs / bucketMs) * bucketMs;
              bucketEndMs = bucketStartMs + bucketMs;
              if (!isValidBucketEndTime(bucketEndMs, bufferMs, lowerMergeLevel, mergeRollupTaskMetadata)) {
                break;
              }
              if (!isMergedSegment(preSelectedSegment, mergeLevel)) {
                hasUnmergedSegments = true;
              }
              if (hasSpilledOverData(preSelectedSegment, bucketMs)) {
                hasSpilledOverData = true;
              }
              selectedSegmentsForBucket.add(preSelectedSegment);
            }
          }
        }

        // Add the last bucket if it contains unmerged segments and is not added before
        if (hasUnmergedSegments && (selectedSegmentsForAllBuckets.isEmpty() || (
            selectedSegmentsForAllBuckets.get(selectedSegmentsForAllBuckets.size() - 1)
                != selectedSegmentsForBucket))) {
          selectedSegmentsForAllBuckets.add(selectedSegmentsForBucket);
        }

        if (selectedSegmentsForAllBuckets.isEmpty()) {
          LOGGER.info("No unmerged segment found for table: {}, mergeLevel: {}", offlineTableName, mergeLevel);
          continue;
        }

        // Bump up watermark to the earliest start time of selected segments truncated to the closest bucket boundary
        long newWatermarkMs = selectedSegmentsForAllBuckets.get(0).get(0).getStartTimeMs() / bucketMs * bucketMs;
        mergeRollupTaskMetadata.getWatermarkMap().put(mergeLevel, newWatermarkMs);
        LOGGER.info("Update watermark for table: {}, mergeLevel: {} from: {} to: {}", offlineTableName, mergeLevel,
            watermarkMs, newWatermarkMs);

        // Update the delay metrics
        createOrUpdateDelayMetrics(offlineTableName, mergeLevel, lowerMergeLevel, newWatermarkMs,
            maxValidBucketEndTimeMs, bufferMs, bucketMs);

        // Create task configs
        int maxNumRecordsPerTask =
            mergeConfigs.get(MergeRollupTask.MAX_NUM_RECORDS_PER_TASK_KEY) != null ? Integer.parseInt(
                mergeConfigs.get(MergeRollupTask.MAX_NUM_RECORDS_PER_TASK_KEY)) : DEFAULT_MAX_NUM_RECORDS_PER_TASK;
        SegmentPartitionConfig segmentPartitionConfig = tableConfig.getIndexingConfig().getSegmentPartitionConfig();
        if (segmentPartitionConfig == null) {
          for (List<SegmentZKMetadata> selectedSegmentsPerBucket : selectedSegmentsForAllBuckets) {
            pinotTaskConfigsForTable.addAll(
                createPinotTaskConfigs(selectedSegmentsPerBucket, offlineTableName, maxNumRecordsPerTask, mergeLevel,
                    mergeConfigs, taskConfigs));
          }
        } else {
          // For partitioned table, schedule separate tasks for each partitionId (partitionId is constructed from
          // partitions of all partition columns. There should be exact match between partition columns of segment and
          // partition columns of table configuration, and there is only partition per column in segment metadata).
          // Other segments which do not meet these conditions are considered as outlier segments, and additional tasks
          // are generated for them.
          Map<String, ColumnPartitionConfig> columnPartitionMap = segmentPartitionConfig.getColumnPartitionMap();
          List<String> partitionColumns = new ArrayList<>(columnPartitionMap.keySet());
          for (List<SegmentZKMetadata> selectedSegmentsPerBucket : selectedSegmentsForAllBuckets) {
            Map<List<Integer>, List<SegmentZKMetadata>> partitionToSegments = new HashMap<>();
            List<SegmentZKMetadata> outlierSegments = new ArrayList<>();
            for (SegmentZKMetadata selectedSegment : selectedSegmentsPerBucket) {
              SegmentPartitionMetadata segmentPartitionMetadata = selectedSegment.getPartitionMetadata();
              List<Integer> partitions = new ArrayList<>();
              if (segmentPartitionMetadata != null && columnPartitionMap.keySet()
                  .equals(segmentPartitionMetadata.getColumnPartitionMap().keySet())) {
                for (String partitionColumn : partitionColumns) {
                  if (segmentPartitionMetadata.getPartitions(partitionColumn).size() == 1) {
                    partitions.add(segmentPartitionMetadata.getPartitions(partitionColumn).iterator().next());
                  } else {
                    partitions.clear();
                    break;
                  }
                }
              }
              if (partitions.isEmpty()) {
                outlierSegments.add(selectedSegment);
              } else {
                partitionToSegments.computeIfAbsent(partitions, k -> new ArrayList<>()).add(selectedSegment);
              }
            }

            for (List<SegmentZKMetadata> partitionedSegments : partitionToSegments.values()) {
              pinotTaskConfigsForTable.addAll(
                  createPinotTaskConfigs(partitionedSegments, offlineTableName, maxNumRecordsPerTask, mergeLevel,
                      mergeConfigs, taskConfigs));
            }

            if (!outlierSegments.isEmpty()) {
              pinotTaskConfigsForTable.addAll(
                  createPinotTaskConfigs(outlierSegments, offlineTableName, maxNumRecordsPerTask, mergeLevel,
                      mergeConfigs, taskConfigs));
            }
          }
        }
      }

      // Write updated watermark map to zookeeper
      try {
        _clusterInfoAccessor
            .setMinionTaskMetadata(mergeRollupTaskMetadata, MinionConstants.MergeRollupTask.TASK_TYPE, expectedVersion);
      } catch (ZkException e) {
        LOGGER.error(
            "Version changed while updating merge/rollup task metadata for table: {}, skip scheduling. There are "
                + "multiple task schedulers for the same table, need to investigate!", offlineTableName);
        continue;
      }
      pinotTaskConfigs.addAll(pinotTaskConfigsForTable);
      LOGGER.info("Finished generating task configs for table: {} for task: {}, numTasks: {}", offlineTableName,
          taskType, pinotTaskConfigsForTable.size());
    }

    // Clean up metrics
    cleanUpDelayMetrics(tableConfigs);

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
   * Get the valid bucket end time before the buffer (now - bufferMs). Consider the segment as multiple contiguous
   * time buckets, this function will return the last bucket end time before the buffer. Return LONG.MIN_VALUE if
   * there's no valid bucket before the buffer.
   */
  private long getValidBucketEndTimeMsForSegment(SegmentZKMetadata segmentZKMetadata, long bucketMs, long bufferMs) {
    // Make sure the segment is ready for merge (the first bucket <= now - bufferTime)
    long currentTimeMs = System.currentTimeMillis();
    long firstBucketEndTimeMs = segmentZKMetadata.getStartTimeMs() / bucketMs * bucketMs + bucketMs;
    if (firstBucketEndTimeMs > currentTimeMs - bufferMs) {
      return Long.MIN_VALUE;
    }
    // The validBucketEndTime is calculated as the min(segment end time, now - bufferTime) rounded to the bucket
    // boundary.
    // Notice bucketEndTime is exclusive while segment end time is inclusive. E.g. if bucketTime = 1d,
    // the rounded segment end time of [10/1 00:00, 10/1 23:59] is 10/2 00:00. The rounded segment end time of
    // [10/1 00:00, 10/2 00:00] is 10/3 00:00
    long validBucketEndTimeMs = (segmentZKMetadata.getEndTimeMs() / bucketMs + 1) * bucketMs;
    validBucketEndTimeMs = Math.min(validBucketEndTimeMs, (currentTimeMs - bucketMs) / bucketMs * bucketMs);
    return validBucketEndTimeMs;
  }

  /**
   * Check if the segment span multiple buckets
   */
  private boolean hasSpilledOverData(SegmentZKMetadata segmentZKMetadata, long bucketMs) {
    return segmentZKMetadata.getStartTimeMs() / bucketMs < segmentZKMetadata.getEndTimeMs() / bucketMs;
  }

  /**
   * Check if the segment is merged for give merge level
   */
  private boolean isMergedSegment(SegmentZKMetadata segmentZKMetadata, String mergeLevel) {
    Map<String, String> customMap = segmentZKMetadata.getCustomMap();
    return customMap != null && mergeLevel.equalsIgnoreCase(
        customMap.get(MergeRollupTask.SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY));
  }

  /**
   * Check if the bucket end time is valid
   */
  private boolean isValidBucketEndTime(long bucketEndMs, long bufferMs, @Nullable String lowerMergeLevel,
      MergeRollupTaskMetadata mergeRollupTaskMetadata) {
    // Check that bucketEndMs <= now - bufferMs
    if (bucketEndMs > System.currentTimeMillis() - bufferMs) {
      return false;
    }
    // Check that bucketEndMs <= waterMark of the lower mergeLevel
    if (lowerMergeLevel != null) {
      Long lowerMergeLevelWatermarkMs = mergeRollupTaskMetadata.getWatermarkMap().get(lowerMergeLevel);
      return lowerMergeLevelWatermarkMs != null && bucketEndMs <= lowerMergeLevelWatermarkMs;
    }
    return true;
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
      // For example, if start time millis is 20200813T12:34:59, we want to create the merged segment for window
      // [20200813, 20200814)
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

      configs.put(MergeRollupTask.MERGE_TYPE_KEY, mergeConfigs.get(MergeTask.MERGE_TYPE_KEY));
      configs.put(MergeRollupTask.MERGE_LEVEL_KEY, mergeLevel);
      configs.put(MergeTask.PARTITION_BUCKET_TIME_PERIOD_KEY, mergeConfigs.get(MergeTask.BUCKET_TIME_PERIOD_KEY));
      configs.put(MergeTask.ROUND_BUCKET_TIME_PERIOD_KEY, mergeConfigs.get(MergeTask.ROUND_BUCKET_TIME_PERIOD_KEY));
      configs.put(MergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY,
          mergeConfigs.get(MergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY));

      configs.put(MergeRollupTask.SEGMENT_NAME_PREFIX_KEY,
          MergeRollupTask.MERGED_SEGMENT_NAME_PREFIX + mergeLevel + "_" + System.currentTimeMillis() + "_" + i + "_"
              + TableNameBuilder.extractRawTableName(offlineTableName));
      pinotTaskConfigs.add(new PinotTaskConfig(MergeRollupTask.TASK_TYPE, configs));
    }

    return pinotTaskConfigs;
  }

  private long getMergeRollupTaskDelayInNumTimeBuckets(long watermarkMs, long maxEndTimeMsOfCurrentLevel,
      long bufferTimeMs, long bucketTimeMs) {
    if (watermarkMs == -1 || maxEndTimeMsOfCurrentLevel == Long.MIN_VALUE) {
      return 0;
    }
    return (Math.min(System.currentTimeMillis() - bufferTimeMs, maxEndTimeMsOfCurrentLevel) - watermarkMs)
        / bucketTimeMs;
  }

  /**
   * Update the delay metrics for the given table and merge level. We create the new gauge metric if the metric is not
   * available.
   * @param tableNameWithType table name with type
   * @param mergeLevel merge level
   * @param lowerMergeLevel lower merge level
   * @param watermarkMs current watermark value
   * @param maxValidBucketEndTimeMs max valid bucket end time of all the segments for the table
   * @param bufferTimeMs buffer time
   * @param bucketTimeMs bucket time
   */
  private void createOrUpdateDelayMetrics(String tableNameWithType, String mergeLevel, String lowerMergeLevel,
      long watermarkMs, long maxValidBucketEndTimeMs, long bufferTimeMs, long bucketTimeMs) {
    ControllerMetrics controllerMetrics = _clusterInfoAccessor.getControllerMetrics();
    if (controllerMetrics == null) {
      return;
    }

    // Update gauge value that indicates the delay in terms of the number of time buckets.
    Map<String, Long> watermarkForTable =
        _mergeRollupWatermarks.computeIfAbsent(tableNameWithType, k -> new ConcurrentHashMap<>());
    _tableMaxValidBucketEndTimeMs.put(tableNameWithType, maxValidBucketEndTimeMs);
    watermarkForTable.compute(mergeLevel, (k, v) -> {
      if (v == null) {
        LOGGER.info(
            "Creating the gauge metric for tracking the merge/roll-up task delay for table: {} and mergeLevel: {}."
                + "(watermarkMs={}, bufferTimeMs={}, bucketTimeMs={}, taskDelayInNumTimeBuckets={})", tableNameWithType,
            mergeLevel, watermarkMs, bucketTimeMs, bucketTimeMs,
            getMergeRollupTaskDelayInNumTimeBuckets(watermarkMs, lowerMergeLevel == null
                    ? _tableMaxValidBucketEndTimeMs.get(tableNameWithType) : watermarkForTable.get(lowerMergeLevel),
                bufferTimeMs, bucketTimeMs));
        controllerMetrics.addCallbackGaugeIfNeeded(getMetricNameForTaskDelay(tableNameWithType, mergeLevel),
            (() -> getMergeRollupTaskDelayInNumTimeBuckets(watermarkForTable.getOrDefault(k, -1L),
                lowerMergeLevel == null ? _tableMaxValidBucketEndTimeMs.get(tableNameWithType)
                    : watermarkForTable.get(lowerMergeLevel), bufferTimeMs, bucketTimeMs)));
      }
      return watermarkMs;
    });
  }

  /**
   * Reset the delay metrics for the given table name.
   *
   * @param tableNameWithType a table name with type
   */
  private void resetDelayMetrics(String tableNameWithType) {
    ControllerMetrics controllerMetrics = _clusterInfoAccessor.getControllerMetrics();
    if (controllerMetrics == null) {
      return;
    }

    // Delete all the watermarks associated with the given table name
    Map<String, Long> watermarksForTable = _mergeRollupWatermarks.remove(tableNameWithType);
    if (watermarksForTable != null) {
      for (String mergeLevel : watermarksForTable.keySet()) {
        controllerMetrics.removeGauge(getMetricNameForTaskDelay(tableNameWithType, mergeLevel));
      }
    }
  }

  /**
   * Reset the delay metrics for the given table name and merge level.
   *
   * @param tableNameWithType table name with type
   * @param mergeLevel merge level
   */
  private void resetDelayMetrics(String tableNameWithType, String mergeLevel) {
    ControllerMetrics controllerMetrics = _clusterInfoAccessor.getControllerMetrics();
    if (controllerMetrics == null) {
      return;
    }

    // Delete all the watermarks associated with the given the table name and the merge level.
    Map<String, Long> watermarksForTable = _mergeRollupWatermarks.get(tableNameWithType);
    if (watermarksForTable != null) {
      if (watermarksForTable.remove(mergeLevel) != null) {
        controllerMetrics.removeGauge(getMetricNameForTaskDelay(tableNameWithType, mergeLevel));
      }
    }
  }

  /**
   * Clean up the metrics that no longer need to be emitted.
   *
   * We clean up the metrics for the following cases:
   *   1. Table got deleted.
   *   2. The current controller is no longer the leader for a table.
   *   3. Merge task config got deleted.
   *   4. Merge task config got modified and some merge levels got deleted.
   *
   * TODO: Current code will remove all metrics in case we invoke the ad-hoc task scheduling on a single table.
   * We will file the follow-up PR to address this issue. We need to separate out APIs for ad-hoc scheduling and
   * periodic scheduling. We will only enable metrics for periodic case.
   *
   * @param tableConfigs list of tables
   */
  private void cleanUpDelayMetrics(List<TableConfig> tableConfigs) {
    Map<String, TableConfig> tableConfigMap = new HashMap<>();
    for (TableConfig tableConfig : tableConfigs) {
      tableConfigMap.put(tableConfig.getTableName(), tableConfig);
    }

    for (String tableNameWithType : new ArrayList<>(_mergeRollupWatermarks.keySet())) {
      TableConfig currentTableConfig = tableConfigMap.get(tableNameWithType);
      // Table does not exist in the cluster or merge task config is removed
      if (currentTableConfig == null) {
        resetDelayMetrics(tableNameWithType);
        continue;
      }

      // The current controller is no longer leader for this table
      if (!_clusterInfoAccessor.getLeaderControllerManager().isLeaderForTable(tableNameWithType)) {
        resetDelayMetrics(tableNameWithType);
        continue;
      }

      // Task config is modified and some merge level got removed
      Map<String, String> taskConfigs = currentTableConfig.getTaskConfig().getConfigsForTaskType(getTaskType());
      Map<String, Map<String, String>> mergeLevelToConfigs = MergeRollupTaskUtils.getLevelToConfigMap(taskConfigs);
      Map<String, Long> tableToWatermark = _mergeRollupWatermarks.get(tableNameWithType);
      for (String mergeLevel : tableToWatermark.keySet()) {
        if (!mergeLevelToConfigs.containsKey(mergeLevel)) {
          resetDelayMetrics(tableNameWithType, mergeLevel);
        }
      }
    }
  }

  private String getMetricNameForTaskDelay(String tableNameWithType, String mergeLevel) {
    // e.g. mergeRollupTaskDelayInNumBuckets.myTable_OFFLINE.daily
    return MERGE_ROLLUP_TASK_DELAY_IN_NUM_BUCKETS + "." + tableNameWithType + "." + mergeLevel;
  }
}
