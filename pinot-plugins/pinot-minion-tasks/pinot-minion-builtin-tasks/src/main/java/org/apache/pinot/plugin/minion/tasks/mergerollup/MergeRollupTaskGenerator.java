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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.I0Itec.zkclient.exception.ZkException;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageUtils;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.minion.MergeRollupTaskMetadata;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.MergeRollupTask;
import org.apache.pinot.core.common.MinionConstants.MergeTask;
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
 *    - Fetch all segments, select segments based on segment lineage (removing segmentsFrom for COMPLETED lineage
 *      entry and segmentsTo for IN_PROGRESS lineage entry)
 *    - Remove empty segments
 *    - Sort segments based on startTime and endTime in ascending order
 *
 *  For each mergeLevel (from lowest to highest, e.g. Hourly -> Daily -> Monthly -> Yearly):
 *    - Skip scheduling if there's incomplete task for the mergeLevel
 *    - Schedule tasks for k time buckets, k is up to numParallelTasks at best effort
 *    - Repeat until k tasks get created or we loop through all the candidate segments:
 *      - Calculate merge/roll-up window:
 *        - Read watermarkMs from the {@link MergeRollupTaskMetadata} ZNode
 *          found at MINION_TASK_METADATA/MergeRollupTask/<tableNameWithType>
 *          In case of cold-start, no ZNode will exist.
 *          A new ZNode will be created, with watermarkMs as the smallest time found in all segments truncated to the
 *          closest bucket start time.
 *        - The execution window for the task is calculated as,
 *          bucketStartMs = watermarkMs
 *          bucketEndMs = windowStartMs + bucketTimeMs
 *          - bucketEndMs must be equal or older than the bufferTimeMs
 *          - bucketEndMs of higher merge level should be less or equal to the waterMarkMs of lower level
 *        - Skip scheduling if the window is invalid (windowEndMs <= windowStartMs)
 *        - Bump up target window and watermark if needed.
 *          - If there's no unmerged segments (by checking segment zk metadata {mergeRollupTask.mergeLevel: level}) for
 *            current window, keep bumping up the watermark and target window until unmerged segments are found.
 *            Else skip the scheduling.
 *      - Select segments for the bucket:
 *        - Skip buckets which all segments are merged
 *        - If there's no spilled over segments (segments spanning multiple time buckets),
 *          schedule all buckets in parallel
 *        - Else, schedule buckets till the first one that has spilled over data (included), so the spilled over data
 *          will be merged next round
 *      - Create the tasks for the current bucket (and per partition for partitioned tables) based on
 *        maxNumRecordsPerTask
 */
@TaskGenerator
public class MergeRollupTaskGenerator implements PinotTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MergeRollupTaskGenerator.class);

  private static final int DEFAULT_MAX_NUM_RECORDS_PER_TASK = 50_000_000;
  private static final int DEFAULT_NUM_PARALLEL_BUCKETS = 1;
  private static final String REFRESH = "REFRESH";

  // This is the metric that keeps track of the task delay in the number of time buckets. For example, if we see this
  // number to be 7 and merge task is configured with "bucketTimePeriod = 1d", this means that we have 7 days of
  // delay. When operating merge/roll-up task in production, we should set the alert on this metrics to find out the
  // delay. Setting the alert on 7 time buckets of delay would be a good starting point.
  //
  // NOTE: Based on the current scheduler logic, we are bumping up the watermark with some delay. (the current round
  // will bump up the watermark for the window that got processed from the previous round). Due to this, we will
  // correctly report the delay with one edge case. When we processed all available time windows, the watermark
  // will not get bumped up until we schedule some task for the table. Due to this, we will always see the delay >= 1.
  private static final String MERGE_ROLLUP_TASK_DELAY_IN_NUM_BUCKETS = "mergeRollupTaskDelayInNumBuckets";

  // tableNameWithType -> mergeLevel -> watermarkMs
  private Map<String, Map<String, Long>> _mergeRollupWatermarks;
  private ClusterInfoAccessor _clusterInfoAccessor;

  @Override
  public void init(ClusterInfoAccessor clusterInfoAccessor) {
    _clusterInfoAccessor = clusterInfoAccessor;
    _mergeRollupWatermarks = new HashMap<>();
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
      SegmentLineageUtils.filterSegmentsBasedOnLineageInPlace(preSelectedSegmentsBasedOnLineage, segmentLineage);

      List<SegmentZKMetadata> preSelectedSegments = new ArrayList<>();
      for (SegmentZKMetadata segment : allSegments) {
        if (preSelectedSegmentsBasedOnLineage.contains(segment.getSegmentName()) && segment.getTotalDocs() > 0) {
          preSelectedSegments.add(segment);
        }
      }

      if (preSelectedSegments.isEmpty()) {
        // Reset the watermark time if no segment found. This covers the case where the table is newly created or
        // all segments for the existing table got deleted.
        resetDelayMetrics(offlineTableName);
        LOGGER
            .info("Skip generating task: {} for table: {}, no segment is found.", taskType, offlineTableName);
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

        // Get the bucket size, buffer and number of parallel buckets (1 as default)
        long bucketMs =
            TimeUtils.convertPeriodToMillis(mergeConfigs.get(MergeTask.BUCKET_TIME_PERIOD_KEY));
        if (bucketMs <= 0) {
          LOGGER.error("Bucket time period (table : {}, mergeLevel : {}) must be larger than 0", offlineTableName,
              mergeLevel);
          continue;
        }
        long bufferMs =
            TimeUtils.convertPeriodToMillis(mergeConfigs.get(MergeTask.BUFFER_TIME_PERIOD_KEY));
        int numParallelBuckets = mergeConfigs.get(MergeTask.NUM_PARALLEL_BUCKETS) != null
            ? Integer.parseInt(mergeConfigs.get(MergeTask.NUM_PARALLEL_BUCKETS)) : DEFAULT_NUM_PARALLEL_BUCKETS;
        Preconditions.checkState(numParallelBuckets > 0, "numParallelBuckets has to be larger than 0");

        // Get watermark from MergeRollupTaskMetadata ZNode
        // bucketStartMs = watermarkMs
        // bucketEndMs = bucketStartMs + bucketTimeMs
        long waterMarkMs =
            getWatermarkMs(preSelectedSegments.get(0).getStartTimeMs(), bucketMs, mergeLevel, mergeRollupTaskMetadata);
        long bucketStartMs = waterMarkMs;
        long bucketEndMs = getBucketEndTime(bucketStartMs, bucketMs, bufferMs, lowerMergeLevel,
            mergeRollupTaskMetadata);

        if (!isValidBucket(bucketStartMs, bucketEndMs)) {
          LOGGER.info(
              "Bucket with start: {} and end: {} of mergeLevel: {} is not a valid merge window, Skipping task "
                  + "generation: {}",
              bucketStartMs, bucketEndMs, mergeLevel, taskType);
          continue;
        }

        // Find overlapping segments for each bucket, skip the buckets that has all segments merged
        List<List<SegmentZKMetadata>> selectedSegmentsForAllBuckets = new ArrayList<>(numParallelBuckets);
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

            if (selectedSegmentsForAllBuckets.size() == numParallelBuckets || hasSpilledOverData) {
              // If there are enough buckets or found spilled over data, schedule merge tasks
              break;
            } else {
              // Start with a new bucket
              // TODO: If there are many small merged segments, we should merge them again
              selectedSegmentsForBucket = new ArrayList<>();
              hasUnmergedSegments = false;
              bucketStartMs = startTimeMs / bucketMs * bucketMs;
              bucketEndMs = getBucketEndTime(bucketStartMs, bucketMs, bufferMs, lowerMergeLevel,
                  mergeRollupTaskMetadata);
              if (!isValidBucket(bucketStartMs, bucketEndMs)) {
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
        if (hasUnmergedSegments && (selectedSegmentsForAllBuckets.isEmpty()
            || (selectedSegmentsForAllBuckets.get(selectedSegmentsForAllBuckets.size() - 1)
            != selectedSegmentsForBucket))) {
          selectedSegmentsForAllBuckets.add(selectedSegmentsForBucket);
        }

        if (selectedSegmentsForAllBuckets.isEmpty()) {
          LOGGER.info("No unmerged segments found for mergeLevel:{} for table: {}, Skipping task generation: {}",
              mergeLevel, offlineTableName, taskType);
          continue;
        }

        // Bump up watermark to the earliest start time of selected segments truncated to the closest bucket boundary
        long newWatermarkMs = selectedSegmentsForAllBuckets.get(0).get(0).getStartTimeMs() / bucketMs * bucketMs;
        Long prevWatermarkMs = mergeRollupTaskMetadata.getWatermarkMap().put(mergeLevel, newWatermarkMs);
        LOGGER.info("Update watermark of mergeLevel: {} for table: {} from: {} to: {}", mergeLevel, offlineTableName,
            prevWatermarkMs, bucketStartMs);

        // Update the delay metrics
        updateDelayMetrics(offlineTableName, mergeLevel, bucketStartMs, bufferMs, bucketMs);

        // Create task configs
        int maxNumRecordsPerTask = mergeConfigs.get(MergeRollupTask.MAX_NUM_RECORDS_PER_TASK_KEY) != null ? Integer
            .parseInt(mergeConfigs.get(MergeRollupTask.MAX_NUM_RECORDS_PER_TASK_KEY))
            : DEFAULT_MAX_NUM_RECORDS_PER_TASK;
        SegmentPartitionConfig segmentPartitionConfig = tableConfig.getIndexingConfig().getSegmentPartitionConfig();
        if (segmentPartitionConfig == null) {
          for (List<SegmentZKMetadata> selectedSegmentsPerBucket : selectedSegmentsForAllBuckets) {
            pinotTaskConfigsForTable.addAll(
                createPinotTaskConfigs(selectedSegmentsPerBucket, offlineTableName, maxNumRecordsPerTask, mergeLevel,
                    mergeConfigs, taskConfigs));
          }
        } else {
          // For partitioned table, schedule separate tasks for each partition
          Map<String, ColumnPartitionConfig> columnPartitionMap = segmentPartitionConfig.getColumnPartitionMap();
          Preconditions.checkState(columnPartitionMap.size() == 1, "Cannot partition on multiple columns for table: %s",
              tableConfig.getTableName());
          Map.Entry<String, ColumnPartitionConfig> partitionEntry = columnPartitionMap.entrySet().iterator().next();
          String partitionColumn = partitionEntry.getKey();

          for (List<SegmentZKMetadata> selectedSegmentsPerBucket : selectedSegmentsForAllBuckets) {
            Map<Integer, List<SegmentZKMetadata>> partitionToSegments = new HashMap<>();
            // Handle segments that have multiple partitions or no partition info
            List<SegmentZKMetadata> outlierSegments = new ArrayList<>();
            for (SegmentZKMetadata selectedSegment : selectedSegmentsPerBucket) {
              SegmentPartitionMetadata segmentPartitionMetadata = selectedSegment.getPartitionMetadata();
              if (segmentPartitionMetadata == null
                  || segmentPartitionMetadata.getPartitions(partitionColumn).size() != 1) {
                outlierSegments.add(selectedSegment);
              } else {
                int partition = segmentPartitionMetadata.getPartitions(partitionColumn).iterator().next();
                partitionToSegments.computeIfAbsent(partition, k -> new ArrayList<>()).add(selectedSegment);
              }
            }

            for (Map.Entry<Integer, List<SegmentZKMetadata>> partitionToSegmentsEntry
                : partitionToSegments.entrySet()) {
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
      }

      // Write updated watermark map to zookeeper
      try {
        _clusterInfoAccessor.setMergeRollupTaskMetadata(mergeRollupTaskMetadata, expectedVersion);
      } catch (ZkException e) {
        LOGGER.error(
            "Version changed while updating merge/rollup task metadata for table: {}, skip scheduling. There are "
                + "multiple task schedulers for the same table, need to investigate!", offlineTableName);
        continue;
      }
      pinotTaskConfigs.addAll(pinotTaskConfigsForTable);
      LOGGER
          .info("Finished generating task configs for table: {} for task: {}, numTasks: {}", offlineTableName, taskType,
              pinotTaskConfigsForTable.size());
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
    return customMap != null && mergeLevel
        .equalsIgnoreCase(customMap.get(MergeRollupTask.SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY));
  }

  /**
   * Get the merge bucket end time
   */
  private long getBucketEndTime(long bucketStartMs, long bucketMs, long bufferMs, String lowerMergeLevel,
      MergeRollupTaskMetadata mergeRollupTaskMetadata) {
    // Make sure bucket endTimeMs <= now - bufferTime
    long bucketEndMs = Math.min(bucketStartMs + bucketMs, System.currentTimeMillis() - bufferMs);
    // Make sure endTimeMs <= waterMark of the lower mergeLevel
    if (lowerMergeLevel != null && mergeRollupTaskMetadata.getWatermarkMap().get(lowerMergeLevel) != null) {
      bucketEndMs = Math.min(bucketEndMs, mergeRollupTaskMetadata.getWatermarkMap().get(lowerMergeLevel));
    }
    // Make sure the end time is time aligned
    bucketEndMs = bucketEndMs / bucketMs * bucketMs;
    return bucketEndMs;
  }

  /**
   * Get the merge window end time
   */
  private long getMergeWindowEndTime(long windowStartMs, long bucketMs, long numParallelBuckets, long bufferMs,
      String lowerMergeLevel, MergeRollupTaskMetadata mergeRollupTaskMetadata) {
    // Make sure window endTimeMs <= now - bufferTime
    long windowEndMs = Math.min(windowStartMs + bucketMs * numParallelBuckets, System.currentTimeMillis() - bufferMs);
    // Make sure endTimeMs <= waterMark of the lower mergeLevel
    if (lowerMergeLevel != null && mergeRollupTaskMetadata.getWatermarkMap().get(lowerMergeLevel) != null) {
      windowEndMs = Math.min(windowEndMs, mergeRollupTaskMetadata.getWatermarkMap().get(lowerMergeLevel));
    }
    // Make sure the end time is time aligned
    windowEndMs = windowEndMs / bucketMs * bucketMs;
    return windowEndMs;
  }

  /**
   * Check if the merge bucket end time is valid
   */
  private boolean isValidBucket(long bucketStartMs, long bucketEndMs) {
    return bucketStartMs < bucketEndMs;
  }

  /**
   * Check if the merge window end time is valid
   */
  private boolean isValidMergeWindow(long windowStartMs, long windowEndMs) {
    return windowEndMs > windowStartMs;
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
      configs.put(MergeTask.PARTITION_BUCKET_TIME_PERIOD_KEY,
          mergeConfigs.get(MergeTask.BUCKET_TIME_PERIOD_KEY));
      configs.put(MergeTask.ROUND_BUCKET_TIME_PERIOD_KEY,
          mergeConfigs.get(MergeTask.ROUND_BUCKET_TIME_PERIOD_KEY));
      configs.put(MergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY,
          mergeConfigs.get(MergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY));

      configs.put(MergeRollupTask.SEGMENT_NAME_PREFIX_KEY,
          MergeRollupTask.MERGED_SEGMENT_NAME_PREFIX + mergeLevel + "_" + System.currentTimeMillis() + "_" + i + "_"
              + TableNameBuilder.extractRawTableName(offlineTableName));
      pinotTaskConfigs.add(new PinotTaskConfig(MergeRollupTask.TASK_TYPE, configs));
    }

    return pinotTaskConfigs;
  }

  private long getMergeRollupTaskDelayInNumTimeBuckets(long watermarkMs, long bufferTimeMs, long bucketTimeMs) {
    if (watermarkMs == -1) {
      return 0;
    }
    return (System.currentTimeMillis() - watermarkMs - bufferTimeMs) / bucketTimeMs;
  }

  /**
   * Update the delay metrics for the given table and merge level. We create the new gauge metric if the metric is not
   * available.
   *
   * @param tableNameWithType table name with type
   * @param mergeLevel merge level
   * @param watermarkMs current watermark value
   * @param bufferTimeMs buffer time
   * @param bucketTimeMs bucket time
   */
  private void updateDelayMetrics(String tableNameWithType, String mergeLevel, long watermarkMs, long bufferTimeMs,
      long bucketTimeMs) {
    ControllerMetrics controllerMetrics = _clusterInfoAccessor.getControllerMetrics();
    if (controllerMetrics == null) {
      return;
    }

    // Update gauge value that indicates the delay in terms of the number of time buckets.
    Map<String, Long> watermarkForTable =
        _mergeRollupWatermarks.computeIfAbsent(tableNameWithType, k -> new ConcurrentHashMap<>());
    watermarkForTable.compute(mergeLevel, (k, v) -> {
      if (v == null) {
        LOGGER.info(
            "Creating the gauge metric for tracking the merge/roll-up task delay for table: {} and mergeLevel: {}."
                + "(watermarkMs={}, bufferTimeMs={}, bucketTimeMs={}, taskDelayInNumTimeBuckets={})", tableNameWithType,
            mergeLevel, watermarkMs, bucketTimeMs, bucketTimeMs,
            getMergeRollupTaskDelayInNumTimeBuckets(watermarkMs, bufferTimeMs, bucketTimeMs));
        controllerMetrics.addCallbackGaugeIfNeeded(getMetricNameForTaskDelay(tableNameWithType, mergeLevel),
            (() -> getMergeRollupTaskDelayInNumTimeBuckets(watermarkForTable.getOrDefault(k, -1L), bufferTimeMs,
                bucketTimeMs)));
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
