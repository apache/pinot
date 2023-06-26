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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
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
import org.apache.pinot.plugin.minion.tasks.MinionTaskUtils;
import org.apache.pinot.plugin.minion.tasks.mergerollup.segmentgroupmananger.MergeRollupTaskSegmentGroupManagerProvider;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link PinotTaskGenerator} implementation for generating tasks of type {@link MergeRollupTask}
 *
 * Assumptions:
 *  - When the MergeRollupTask starts the first time, records older than the min(now ms, max end time ms of all ready to
 *    process segments) - bufferTimeMs have already been ingested. If not, newly ingested records older than that time
 *    may not be properly merged (Due to the latest watermarks advanced too far before records are ingested).
 *  - If it is needed, there are backfill protocols to ingest and replace records older than the latest watermarks.
 *    Those protocols can handle time alignment (according to merge levels configurations) correctly.
 *  - If it is needed, there are reconcile protocols to merge & rollup newly ingested segments that are (1) older than
 *    the latest watermarks, and (2) not time aligned according to merge levels configurations
 *    - For realtime tables, those protocols are needed if streaming records arrive late (older thant the latest
 *      watermarks)
 *    - For offline tables, those protocols are needed if there are non-time-aligned segments ingested accidentally.
 *
 *
 * Steps:
 *  - Pre-select segments:
 *    - Fetch all segments, select segments based on segment lineage (removing segmentsFrom for COMPLETED lineage
 *      entry and segmentsTo for IN_PROGRESS lineage entry)
 *    - For realtime tables, remove
 *      - in-progress segments (Segment.Realtime.Status.IN_PROGRESS), and
 *      - sealed segments with start time later than the earliest start time of all in progress segments
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
  private static final String DELIMITER_IN_SEGMENT_NAME = "_";

  // This is the metric that keeps track of the task delay in the number of time buckets. For example, if we see this
  // number to be 7 and merge task is configured with "bucketTimePeriod = 1d", this means that we have 7 days of
  // delay. When operating merge/roll-up task in production, we should set the alert on this metrics to find out the
  // delay. Setting the alert on 7 time buckets of delay would be a good starting point.
  private static final String MERGE_ROLLUP_TASK_DELAY_IN_NUM_BUCKETS = "mergeRollupTaskDelayInNumBuckets";

  private static final String MERGE_ROLLUP_TASK_NUM_BUCKETS_TO_PROCESS = "mergeRollupTaskNumBucketsToProcess";

  // tableNameWithType -> mergeLevel -> watermarkMs
  private final Map<String, Map<String, Long>> _mergeRollupWatermarks = new HashMap<>();
  // tableNameWithType -> lowestLevelMaxValidBucketEndTime
  private final Map<String, Long> _tableLowestLevelMaxValidBucketEndTimeMs = new HashMap<>();
  // tableNameWithType -> mergeLevel -> numberBucketsToProcess
  private final Map<String, Map<String, Long>> _tableNumberBucketsToProcess = new HashMap<>();

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
      String tableNameWithType = tableConfig.getTableName();
      LOGGER.info("Start generating task configs for table: {} for task: {}", tableNameWithType, taskType);

      // Get all segment metadata
      List<SegmentZKMetadata> allSegments = _clusterInfoAccessor.getSegmentsZKMetadata(tableNameWithType);
      // Filter segments based on status
      List<SegmentZKMetadata> preSelectedSegmentsBasedOnStatus
          = filterSegmentsBasedOnStatus(tableConfig.getTableType(), allSegments);

      // Select current segment snapshot based on lineage, filter out empty segments
      SegmentLineage segmentLineage = _clusterInfoAccessor.getSegmentLineage(tableNameWithType);
      Set<String> preSelectedSegmentsBasedOnLineage = new HashSet<>();
      for (SegmentZKMetadata segment : preSelectedSegmentsBasedOnStatus) {
        preSelectedSegmentsBasedOnLineage.add(segment.getSegmentName());
      }
      SegmentLineageUtils.filterSegmentsBasedOnLineageInPlace(preSelectedSegmentsBasedOnLineage, segmentLineage);

      List<SegmentZKMetadata> preSelectedSegments = new ArrayList<>();
      for (SegmentZKMetadata segment : preSelectedSegmentsBasedOnStatus) {
        if (preSelectedSegmentsBasedOnLineage.contains(segment.getSegmentName()) && segment.getTotalDocs() > 0
            && MergeTaskUtils.allowMerge(segment)) {
          preSelectedSegments.add(segment);
        }
      }

      if (preSelectedSegments.isEmpty()) {
        // Reset the watermark time if no segment found. This covers the case where the table is newly created or
        // all segments for the existing table got deleted.
        resetDelayMetrics(tableNameWithType);
        LOGGER.info("Skip generating task: {} for table: {}, no segment is found.", taskType, tableNameWithType);
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
      for (Map.Entry<String, TaskState> entry : TaskGeneratorUtils.getIncompleteTasks(taskType, tableNameWithType,
          _clusterInfoAccessor).entrySet()) {
        for (PinotTaskConfig taskConfig : _clusterInfoAccessor.getTaskConfigs(entry.getKey())) {
          inCompleteMergeLevels.add(taskConfig.getConfigs().get(MergeRollupTask.MERGE_LEVEL_KEY));
        }
      }

      // Get scheduling mode which is "processFromWatermark" by default. If "processAll" mode is enabled, there will be
      // no watermark, and each round we pick the buckets in chronological order which have unmerged segments.
      boolean processAll = MergeTask.PROCESS_ALL_MODE.equalsIgnoreCase(taskConfigs.get(MergeTask.MODE));

      ZNRecord mergeRollupTaskZNRecord = _clusterInfoAccessor
          .getMinionTaskMetadataZNRecord(MinionConstants.MergeRollupTask.TASK_TYPE, tableNameWithType);
      int expectedVersion = mergeRollupTaskZNRecord != null ? mergeRollupTaskZNRecord.getVersion() : -1;
      MergeRollupTaskMetadata mergeRollupTaskMetadata =
          mergeRollupTaskZNRecord != null ? MergeRollupTaskMetadata.fromZNRecord(mergeRollupTaskZNRecord)
              : new MergeRollupTaskMetadata(tableNameWithType, new TreeMap<>());
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
              mergeLevel, tableNameWithType, taskType);
          continue;
        }

        // Get the bucket size, buffer size and maximum number of parallel buckets (by default 1)
        String bucketPeriod = mergeConfigs.get(MergeTask.BUCKET_TIME_PERIOD_KEY);
        long bucketMs = TimeUtils.convertPeriodToMillis(bucketPeriod);
        if (bucketMs <= 0) {
          LOGGER.error("Bucket time period: {} (table : {}, mergeLevel : {}) must be larger than 0", bucketPeriod,
              tableNameWithType, mergeLevel);
          continue;
        }
        String bufferPeriod = mergeConfigs.get(MergeTask.BUFFER_TIME_PERIOD_KEY);
        long bufferMs = TimeUtils.convertPeriodToMillis(bufferPeriod);
        if (bufferMs < 0) {
          LOGGER.error("Buffer time period: {} (table : {}, mergeLevel : {}) must be larger or equal to 0",
              bufferPeriod, tableNameWithType, mergeLevel);
          continue;
        }
        String maxNumParallelBucketsStr = mergeConfigs.get(MergeTask.MAX_NUM_PARALLEL_BUCKETS);
        int maxNumParallelBuckets = maxNumParallelBucketsStr != null ? Integer.parseInt(maxNumParallelBucketsStr)
            : DEFAULT_NUM_PARALLEL_BUCKETS;
        if (maxNumParallelBuckets <= 0) {
          LOGGER.error("Maximum number of parallel buckets: {} (table : {}, mergeLevel : {}) must be larger than 0",
              maxNumParallelBuckets, tableNameWithType, mergeLevel);
          continue;
        }

        // Get bucket start/end time
        long preSelectedSegStartTimeMs = preSelectedSegments.get(0).getStartTimeMs();
        long bucketStartMs = preSelectedSegStartTimeMs / bucketMs * bucketMs;
        long watermarkMs = 0;
        if (!processAll) {
          // Get watermark from MergeRollupTaskMetadata ZNode
          // bucketStartMs = watermarkMs
          // bucketEndMs = bucketStartMs + bucketMs
          watermarkMs = getWatermarkMs(preSelectedSegStartTimeMs, bucketMs, mergeLevel,
              mergeRollupTaskMetadata);
          bucketStartMs = watermarkMs;
        }
        long bucketEndMs = bucketStartMs + bucketMs;
        if (lowerMergeLevel == null) {
          long lowestLevelMaxValidBucketEndTimeMs = Long.MIN_VALUE;
          for (SegmentZKMetadata preSelectedSegment : preSelectedSegments) {
            // Compute lowestLevelMaxValidBucketEndTimeMs among segments that are ready for merge
            long currentValidBucketEndTimeMs =
                getValidBucketEndTimeMsForSegment(preSelectedSegment, bucketMs, bufferMs);
            lowestLevelMaxValidBucketEndTimeMs =
                Math.max(lowestLevelMaxValidBucketEndTimeMs, currentValidBucketEndTimeMs);
          }
          _tableLowestLevelMaxValidBucketEndTimeMs.put(tableNameWithType, lowestLevelMaxValidBucketEndTimeMs);
        }
        // Create metrics even if there's no task scheduled, this helps the case that the controller is restarted
        // but the metrics are not available until the controller schedules a valid task
        List<String> sortedMergeLevels =
            sortedMergeLevelConfigs.stream().map(e -> e.getKey()).collect(Collectors.toList());
        if (processAll) {
          createOrUpdateNumBucketsToProcessMetrics(tableNameWithType, mergeLevel, lowerMergeLevel, bufferMs, bucketMs,
              preSelectedSegments, sortedMergeLevels);
        } else {
          createOrUpdateDelayMetrics(tableNameWithType, mergeLevel, null, watermarkMs, bufferMs, bucketMs);
        }

        if (!isValidBucketEndTime(bucketEndMs, bufferMs, lowerMergeLevel, mergeRollupTaskMetadata, processAll)) {
          LOGGER.info("Bucket with start: {} and end: {} (table : {}, mergeLevel : {}, mode : {}) cannot be merged yet",
              bucketStartMs, bucketEndMs, tableNameWithType, mergeLevel, processAll ? MergeTask.PROCESS_ALL_MODE
                  : MergeTask.PROCESS_FROM_WATERMARK_MODE);
          continue;
        }

        // Find overlapping segments for each bucket, skip the buckets that has all segments merged
        List<List<SegmentZKMetadata>> selectedSegmentsForAllBuckets = new ArrayList<>(maxNumParallelBuckets);
        List<SegmentZKMetadata> selectedSegmentsForBucket = new ArrayList<>();
        boolean hasUnmergedSegments = false;
        boolean hasSpilledOverData = false;
        boolean areAllSegmentsReadyToMerge = true;

        // The for loop terminates in following cases:
        // 1. Found buckets with unmerged segments:
        //    For each bucket find all segments overlapping with the target bucket, skip the bucket if all overlapping
        //    segments are merged. Schedule k (numParallelBuckets) buckets at most, and stops at the first bucket that
        //    contains spilled over data.
        //    One may wonder how a segment with records spanning different buckets is handled. The short answer is that
        //    it will be cut into multiple segments, each for a separate bucket. This is achieved by setting bucket time
        //    period as PARTITION_BUCKET_TIME_PERIOD when generating PinotTaskConfigs
        // 2. There's no bucket with unmerged segments, skip scheduling
        for (SegmentZKMetadata preSelectedSegment : preSelectedSegments) {
          long startTimeMs = preSelectedSegment.getStartTimeMs();
          if (startTimeMs < bucketEndMs) {
            long endTimeMs = preSelectedSegment.getEndTimeMs();
            if (endTimeMs >= bucketStartMs) {
              // For segments overlapping with current bucket, add to the result list
              if (!isMergedSegment(preSelectedSegment, mergeLevel, sortedMergeLevels)) {
                hasUnmergedSegments = true;
              }
              if (!isMergedSegment(preSelectedSegment, lowerMergeLevel, sortedMergeLevels)) {
                areAllSegmentsReadyToMerge = false;
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
            if (hasUnmergedSegments && areAllSegmentsReadyToMerge) {
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
              areAllSegmentsReadyToMerge = true;
              bucketStartMs = (startTimeMs / bucketMs) * bucketMs;
              bucketEndMs = bucketStartMs + bucketMs;
              if (!isValidBucketEndTime(bucketEndMs, bufferMs, lowerMergeLevel, mergeRollupTaskMetadata, processAll)) {
                break;
              }
              if (!isMergedSegment(preSelectedSegment, mergeLevel, sortedMergeLevels)) {
                hasUnmergedSegments = true;
              }
              if (!isMergedSegment(preSelectedSegment, lowerMergeLevel, sortedMergeLevels)) {
                areAllSegmentsReadyToMerge = false;
              }
              if (hasSpilledOverData(preSelectedSegment, bucketMs)) {
                hasSpilledOverData = true;
              }
              selectedSegmentsForBucket.add(preSelectedSegment);
            }
          }
        }

        // Add the last bucket if it contains unmerged segments and is not added before
        if (hasUnmergedSegments && areAllSegmentsReadyToMerge && (selectedSegmentsForAllBuckets.isEmpty() || (
            selectedSegmentsForAllBuckets.get(selectedSegmentsForAllBuckets.size() - 1)
                != selectedSegmentsForBucket))) {
          selectedSegmentsForAllBuckets.add(selectedSegmentsForBucket);
        }

        if (selectedSegmentsForAllBuckets.isEmpty()) {
          LOGGER.info("No unmerged segment found for table: {}, mergeLevel: {}", tableNameWithType, mergeLevel);
          continue;
        }

        // Bump up watermark to the earliest start time of selected segments truncated to the closest bucket boundary
        long newWatermarkMs = selectedSegmentsForAllBuckets.get(0).get(0).getStartTimeMs() / bucketMs * bucketMs;
        mergeRollupTaskMetadata.getWatermarkMap().put(mergeLevel, newWatermarkMs);
        LOGGER.info("Update watermark for table: {}, mergeLevel: {} from: {} to: {}", tableNameWithType, mergeLevel,
            watermarkMs, newWatermarkMs);

        // Update the delay metrics
        if (!processAll) {
          createOrUpdateDelayMetrics(tableNameWithType, mergeLevel, lowerMergeLevel, newWatermarkMs, bufferMs,
              bucketMs);
        }

        // Create task configs
        int maxNumRecordsPerTask =
            mergeConfigs.get(MergeRollupTask.MAX_NUM_RECORDS_PER_TASK_KEY) != null ? Integer.parseInt(
                mergeConfigs.get(MergeRollupTask.MAX_NUM_RECORDS_PER_TASK_KEY)) : DEFAULT_MAX_NUM_RECORDS_PER_TASK;
        SegmentPartitionConfig segmentPartitionConfig = tableConfig.getIndexingConfig().getSegmentPartitionConfig();
        if (segmentPartitionConfig == null) {
          for (List<SegmentZKMetadata> selectedSegmentsPerBucket : selectedSegmentsForAllBuckets) {
            pinotTaskConfigsForTable.addAll(
                createPinotTaskConfigs(selectedSegmentsPerBucket, tableConfig, maxNumRecordsPerTask, mergeLevel,
                    null, mergeConfigs, taskConfigs));
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

            for (Map.Entry<List<Integer>, List<SegmentZKMetadata>> entry : partitionToSegments.entrySet()) {
              List<Integer> partition = entry.getKey();
              List<SegmentZKMetadata> partitionedSegments = entry.getValue();
              pinotTaskConfigsForTable.addAll(
                  createPinotTaskConfigs(partitionedSegments, tableConfig, maxNumRecordsPerTask, mergeLevel,
                      partition, mergeConfigs, taskConfigs));
            }

            if (!outlierSegments.isEmpty()) {
              pinotTaskConfigsForTable.addAll(
                  createPinotTaskConfigs(outlierSegments, tableConfig, maxNumRecordsPerTask, mergeLevel,
                      null, mergeConfigs, taskConfigs));
            }
          }
        }
      }

      // Write updated watermark map to zookeeper
      if (!processAll) {
        try {
          _clusterInfoAccessor
              .setMinionTaskMetadata(mergeRollupTaskMetadata, MinionConstants.MergeRollupTask.TASK_TYPE,
                  expectedVersion);
        } catch (ZkException e) {
          LOGGER.error(
              "Version changed while updating merge/rollup task metadata for table: {}, skip scheduling. There are "
                  + "multiple task schedulers for the same table, need to investigate!", tableNameWithType);
          continue;
        }
      }
      pinotTaskConfigs.addAll(pinotTaskConfigsForTable);
      LOGGER.info("Finished generating task configs for table: {} for task: {}, numTasks: {}", tableNameWithType,
          taskType, pinotTaskConfigsForTable.size());
    }

    // Clean up metrics
    cleanUpDelayMetrics(tableConfigs);

    return pinotTaskConfigs;
  }

  @VisibleForTesting
  static List<SegmentZKMetadata> filterSegmentsBasedOnStatus(TableType tableType, List<SegmentZKMetadata> allSegments) {
    if (tableType == TableType.REALTIME) {
      // For realtime table, don't process
      // 1. in-progress segments (Segment.Realtime.Status.IN_PROGRESS)
      // 2. sealed segments with start time later than the earliest start time of all in progress segments
      // This prevents those in-progress segments from not being merged.
      //
      // Note that we make the following two assumptions here:
      // 1. streaming data consumer lags are negligible
      // 2. streaming data records are ingested mostly in chronological order (no records are ingested with delay larger
      //    than bufferTimeMS)
      //
      // We don't handle the following cases intentionally because it will be either overkill or too complex
      // 1. New partition added. If new partitions are not picked up timely, the MergeRollupTask will move watermarks
      //    forward, and may not be able to merge some lately-created segments for those new partitions -- users should
      //    configure pinot properly to discover new partitions timely, or they should restart pinot servers manually
      //    for new partitions to be picked up
      // 2. (1) no new in-progress segments are created for some partitions (2) new in-progress segments are created for
      //    partitions, but there is no record consumed (i.e, empty in-progress segments). In those two cases,
      //    if new records are consumed later, the MergeRollupTask may have already moved watermarks forward, and may
      //    not be able to merge those lately-created segments -- we assume that users will have a way to backfill those
      //    records correctly.
      long earliestStartTimeMsOfInProgressSegments = Long.MAX_VALUE;
      for (SegmentZKMetadata segmentZKMetadata : allSegments) {
        if (!segmentZKMetadata.getStatus().isCompleted()
            && segmentZKMetadata.getTotalDocs() > 0
            && segmentZKMetadata.getStartTimeMs() < earliestStartTimeMsOfInProgressSegments) {
          earliestStartTimeMsOfInProgressSegments = segmentZKMetadata.getStartTimeMs();
        }
      }
      final long finalEarliestStartTimeMsOfInProgressSegments = earliestStartTimeMsOfInProgressSegments;
      return allSegments.stream()
          .filter(segmentZKMetadata -> segmentZKMetadata.getStatus().isCompleted()
              && segmentZKMetadata.getStartTimeMs() < finalEarliestStartTimeMsOfInProgressSegments)
          .collect(Collectors.toList());
    } else {
      return allSegments;
    }
  }

  /**
   * Validate table config for merge/rollup task
   */
  @VisibleForTesting
  static boolean validate(TableConfig tableConfig, String taskType) {
    String tableNameWithType = tableConfig.getTableName();
    if (REFRESH.equalsIgnoreCase(IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig))) {
      LOGGER.warn("Skip generating task: {} for non-APPEND table: {}, REFRESH table is not supported", taskType,
          tableNameWithType);
      return false;
    }
    if (tableConfig.getTableType() == TableType.REALTIME) {
      if (tableConfig.isUpsertEnabled()) {
        LOGGER.warn("Skip generating task: {} for table: {}, table with upsert enabled is not supported", taskType,
            tableNameWithType);
        return false;
      }
      if (tableConfig.isDedupEnabled()) {
        LOGGER.warn("Skip generating task: {} for table: {}, table with dedup enabled is not supported", taskType,
            tableNameWithType);
        return false;
      }
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
    validBucketEndTimeMs = Math.min(validBucketEndTimeMs, (currentTimeMs - bufferMs) / bucketMs * bucketMs);
    return validBucketEndTimeMs;
  }

  /**
   * Check if the segment span multiple buckets
   */
  private boolean hasSpilledOverData(SegmentZKMetadata segmentZKMetadata, long bucketMs) {
    return segmentZKMetadata.getStartTimeMs() / bucketMs < segmentZKMetadata.getEndTimeMs() / bucketMs;
  }

  /**
   * Check if the segment is merged at the given merge level
   */
  private boolean isMergedSegment(SegmentZKMetadata segmentZKMetadata, String mergeLevel) {
    Map<String, String> customMap = segmentZKMetadata.getCustomMap();
    return customMap != null && mergeLevel.equalsIgnoreCase(
        customMap.get(MergeRollupTask.SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY));
  }

  /**
   * Check if the segment is merged for given and higher merge levels
   *
   * @param segmentZKMetadata segment zk metadata
   * @param baseMergeLevel base merge level
   * @param sortedMergeLevels sorted merge levels based on buffer time period
   * @return true if the segment is merged for the base merge level or any level higher than the base merge level
   */
  private boolean isMergedSegment(SegmentZKMetadata segmentZKMetadata, String baseMergeLevel,
      List<String> sortedMergeLevels) {
    Map<String, String> customMap = segmentZKMetadata.getCustomMap();
    if (baseMergeLevel == null) {
      return true;
    }
    if (customMap == null || customMap.get(MergeRollupTask.SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY) == null) {
      return false;
    }
    String mergeLevel = customMap.get(MergeRollupTask.SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY);
    boolean isCurLevelLowerThanBase = true;
    for (String currentMergeLevel : sortedMergeLevels) {
      if (currentMergeLevel.equalsIgnoreCase(baseMergeLevel)) {
        isCurLevelLowerThanBase = false;
      }
      if (!isCurLevelLowerThanBase && currentMergeLevel.equalsIgnoreCase(mergeLevel)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if the bucket end time is valid
   */
  private boolean isValidBucketEndTime(long bucketEndMs, long bufferMs, @Nullable String lowerMergeLevel,
      MergeRollupTaskMetadata mergeRollupTaskMetadata, boolean processAll) {
    // Check that bucketEndMs <= now - bufferMs
    if (bucketEndMs > System.currentTimeMillis() - bufferMs) {
      return false;
    }
    // Check that bucketEndMs <= waterMark of the lower mergeLevel
    if (lowerMergeLevel != null && !processAll) {
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
      TableConfig tableConfig, int maxNumRecordsPerTask, String mergeLevel, List<Integer> partition,
      Map<String, String> mergeConfigs, Map<String, String> taskConfigs) {
    String tableNameWithType = tableConfig.getTableName();
    List<List<SegmentZKMetadata>> segmentGroups = MergeRollupTaskSegmentGroupManagerProvider.create(taskConfigs)
        .getSegmentGroups(tableConfig, _clusterInfoAccessor, selectedSegments);
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();
    boolean skipAllMerged = Boolean.TRUE.toString().equalsIgnoreCase(taskConfigs.get(MergeTask.SKIP_ALL_MERGED));

    for (List<SegmentZKMetadata> segments : segmentGroups) {
      int numRecordsPerTask = 0;
      List<List<String>> segmentNamesList = new ArrayList<>();
      List<List<String>> downloadURLsList = new ArrayList<>();
      List<SegmentZKMetadata> segmentZKMetadataList = new ArrayList<>();
      List<String> segmentNames = new ArrayList<>();
      List<String> downloadURLs = new ArrayList<>();

      for (int i = 0; i < segments.size(); i++) {
        SegmentZKMetadata targetSegment = segments.get(i);
        segmentZKMetadataList.add(targetSegment);
        segmentNames.add(targetSegment.getSegmentName());
        downloadURLs.add(targetSegment.getDownloadUrl());
        numRecordsPerTask += targetSegment.getTotalDocs();
        if (numRecordsPerTask >= maxNumRecordsPerTask || i == segments.size() - 1) {
          // If "skipAllMerged" is set to true, skip generating task with all merged segments
          if (!skipAllMerged || !segmentZKMetadataList.stream().allMatch(
              segment -> isMergedSegment(segment, mergeLevel))) {
            segmentNamesList.add(segmentNames);
            downloadURLsList.add(downloadURLs);
          }
          numRecordsPerTask = 0;
          segmentZKMetadataList = new ArrayList<>();
          segmentNames = new ArrayList<>();
          downloadURLs = new ArrayList<>();
        }
      }

      StringBuilder partitionSuffixBuilder = new StringBuilder();
      if (partition != null && !partition.isEmpty()) {
        for (int columnPartition : partition) {
          partitionSuffixBuilder.append(DELIMITER_IN_SEGMENT_NAME).append(columnPartition);
        }
      }
      String partitionSuffix = partitionSuffixBuilder.toString();

      for (int i = 0; i < segmentNamesList.size(); i++) {
        String downloadURL = StringUtils.join(downloadURLsList.get(i), MinionConstants.URL_SEPARATOR);
        Map<String, String> configs = MinionTaskUtils.getPushTaskConfig(tableNameWithType, taskConfigs,
            _clusterInfoAccessor);
        configs.put(MinionConstants.TABLE_NAME_KEY, tableNameWithType);
        configs.put(MinionConstants.SEGMENT_NAME_KEY,
            StringUtils.join(segmentNamesList.get(i), MinionConstants.SEGMENT_NAME_SEPARATOR));
        configs.put(MinionConstants.DOWNLOAD_URL_KEY, downloadURL);
        configs.put(MinionConstants.UPLOAD_URL_KEY, _clusterInfoAccessor.getVipUrl() + "/segments");
        configs.put(MinionConstants.ENABLE_REPLACE_SEGMENTS_KEY, "true");

        for (Map.Entry<String, String> taskConfig : taskConfigs.entrySet()) {
          if (taskConfig.getKey().endsWith(MinionConstants.MergeRollupTask.AGGREGATION_TYPE_KEY_SUFFIX)) {
            configs.put(taskConfig.getKey(), taskConfig.getValue());
          }
        }

        configs.put(BatchConfigProperties.OVERWRITE_OUTPUT,
            taskConfigs.getOrDefault(BatchConfigProperties.OVERWRITE_OUTPUT, "false"));
        configs.put(MergeRollupTask.MERGE_TYPE_KEY, mergeConfigs.get(MergeTask.MERGE_TYPE_KEY));
        configs.put(MergeRollupTask.MERGE_LEVEL_KEY, mergeLevel);
        configs.put(MergeTask.PARTITION_BUCKET_TIME_PERIOD_KEY, mergeConfigs.get(MergeTask.BUCKET_TIME_PERIOD_KEY));
        configs.put(MergeTask.ROUND_BUCKET_TIME_PERIOD_KEY, mergeConfigs.get(MergeTask.ROUND_BUCKET_TIME_PERIOD_KEY));
        configs.put(MergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY,
            mergeConfigs.get(MergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY));

        // Segment name conflict happens when the current method "createPinotTaskConfigs" is invoked more than once
        // within the same epoch millisecond, which may happen when there are multiple partitions.
        // To prevent such name conflict, we include a partitionSeqSuffix to the segment name.
        configs.put(MergeRollupTask.SEGMENT_NAME_PREFIX_KEY,
            MergeRollupTask.MERGED_SEGMENT_NAME_PREFIX + mergeLevel + DELIMITER_IN_SEGMENT_NAME
                + System.currentTimeMillis() + partitionSuffix + DELIMITER_IN_SEGMENT_NAME + i
                + DELIMITER_IN_SEGMENT_NAME + TableNameBuilder.extractRawTableName(tableNameWithType));
        pinotTaskConfigs.add(new PinotTaskConfig(MergeRollupTask.TASK_TYPE, configs));
      }
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
   * @param bufferTimeMs buffer time
   * @param bucketTimeMs bucket time
   */
  private void createOrUpdateDelayMetrics(String tableNameWithType, String mergeLevel, String lowerMergeLevel,
      long watermarkMs, long bufferTimeMs, long bucketTimeMs) {
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
            mergeLevel, watermarkMs, bufferTimeMs, bucketTimeMs,
            getMergeRollupTaskDelayInNumTimeBuckets(watermarkMs, lowerMergeLevel == null
                    ? _tableLowestLevelMaxValidBucketEndTimeMs.get(tableNameWithType)
                    : watermarkForTable.get(lowerMergeLevel),
                bufferTimeMs, bucketTimeMs));
        controllerMetrics.addCallbackGaugeIfNeeded(getMetricNameForTaskDelay(tableNameWithType, mergeLevel),
            (() -> getMergeRollupTaskDelayInNumTimeBuckets(watermarkForTable.getOrDefault(k, -1L),
                lowerMergeLevel == null ? _tableLowestLevelMaxValidBucketEndTimeMs.get(tableNameWithType)
                    : watermarkForTable.get(lowerMergeLevel), bufferTimeMs, bucketTimeMs)));
      }
      return watermarkMs;
    });
  }

  /**
   * Update the number buckets to process for the given table and merge level. We create the new gauge metric if
   * the metric is not available.
   * @param tableNameWithType table name with type
   * @param mergeLevel merge level
   * @param lowerMergeLevel lower merge level
   * @param bufferTimeMs buffer time
   * @param bucketTimeMs bucket time
   * @param sortedSegments sorted segment list
   * @param sortedMergeLevels sorted merge level list
   */
  private void createOrUpdateNumBucketsToProcessMetrics(String tableNameWithType, String mergeLevel,
      String lowerMergeLevel, long bufferTimeMs, long bucketTimeMs,
      List<SegmentZKMetadata> sortedSegments, List<String> sortedMergeLevels) {
    ControllerMetrics controllerMetrics = _clusterInfoAccessor.getControllerMetrics();
    if (controllerMetrics == null) {
      return;
    }

    // Find all buckets and segments that are ready to merge
    List<List<SegmentZKMetadata>> selectedSegmentsForAllBuckets = new ArrayList<>();
    List<SegmentZKMetadata> selectedSegmentsForBucket = new ArrayList<>();
    long bucketStartMs = sortedSegments.get(0).getStartTimeMs() / bucketTimeMs * bucketTimeMs;
    long bucketEndMs = bucketStartMs + bucketTimeMs;
    boolean hasUnmergedSegments = false;
    boolean isAllSegmentsReadyToMerge = true;

    for (SegmentZKMetadata segment : sortedSegments) {
      long startTimeMs = segment.getStartTimeMs();
      if (startTimeMs < bucketEndMs) {
        long endTimeMs = segment.getEndTimeMs();
        if (endTimeMs >= bucketStartMs) {
          // For segments overlapping with current bucket, add to the result list
          if (!isMergedSegment(segment, mergeLevel, sortedMergeLevels)) {
            hasUnmergedSegments = true;
          }
          if (!isMergedSegment(segment, lowerMergeLevel, sortedMergeLevels)) {
            isAllSegmentsReadyToMerge = false;
          }
          selectedSegmentsForBucket.add(segment);
        }
        // endTimeMs < bucketStartMs
        // Haven't find the first overlapping segment, continue to the next segment
      } else {
        // Has gone through all overlapping segments for current bucket
        if (hasUnmergedSegments && isAllSegmentsReadyToMerge) {
          // Add the bucket if there are unmerged segments
          selectedSegmentsForAllBuckets.add(selectedSegmentsForBucket);
        }

        // Start with a new bucket
        selectedSegmentsForBucket = new ArrayList<>();
        hasUnmergedSegments = false;
        isAllSegmentsReadyToMerge = true;
        bucketStartMs = (startTimeMs / bucketTimeMs) * bucketTimeMs;
        bucketEndMs = bucketStartMs + bucketTimeMs;
        if (bucketEndMs > System.currentTimeMillis() - bufferTimeMs) {
          break;
        }
        if (!isMergedSegment(segment, mergeLevel, sortedMergeLevels)) {
          hasUnmergedSegments = true;
        }
        if (!isMergedSegment(segment, lowerMergeLevel, sortedMergeLevels)) {
          isAllSegmentsReadyToMerge = false;
        }
        selectedSegmentsForBucket.add(segment);
      }
    }

    // Add the last bucket if it contains unmerged segments and is not added before
    if (hasUnmergedSegments && isAllSegmentsReadyToMerge && (selectedSegmentsForAllBuckets.isEmpty() || (
        selectedSegmentsForAllBuckets.get(selectedSegmentsForAllBuckets.size() - 1)
            != selectedSegmentsForBucket))) {
      selectedSegmentsForAllBuckets.add(selectedSegmentsForBucket);
    }

    Map<String, Long> numBucketsToProcessForTable =
        _tableNumberBucketsToProcess.computeIfAbsent(tableNameWithType, k -> new ConcurrentHashMap<>());
    long finalCount = selectedSegmentsForAllBuckets.size();
    numBucketsToProcessForTable.compute(mergeLevel, (k, v) -> {
      if (v == null) {
        LOGGER.info(
            "Creating the gauge metric for tracking the merge/roll-up number buckets to process for table: {} "
                + "and mergeLevel: {}.(bufferTimeMs={}, bucketTimeMs={}, numTimeBucketsToProcess={})"
                + tableNameWithType, mergeLevel, bufferTimeMs, bucketTimeMs, finalCount);
        controllerMetrics.setOrUpdateGauge(getMetricNameForNumBucketsToProcess(tableNameWithType, mergeLevel),
            () -> _tableNumberBucketsToProcess.get(tableNameWithType).getOrDefault(mergeLevel, finalCount));
      }
      return finalCount;
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

    Map<String, Long> numBucketsToProcessForTable = _tableNumberBucketsToProcess.remove(tableNameWithType);
    if (numBucketsToProcessForTable != null) {
      for (String mergeLevel : numBucketsToProcessForTable.keySet()) {
        controllerMetrics.removeGauge(getMetricNameForNumBucketsToProcess(tableNameWithType, mergeLevel));
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

    Map<String, Long> numBucketsToProcessForTable = _tableNumberBucketsToProcess.remove(tableNameWithType);
    if (numBucketsToProcessForTable != null) {
      if (numBucketsToProcessForTable.remove(mergeLevel) != null) {
        controllerMetrics.removeGauge(getMetricNameForNumBucketsToProcess(tableNameWithType, mergeLevel));
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

    Set<String> tables = new HashSet<>(_mergeRollupWatermarks.keySet());
    tables.addAll(_tableNumberBucketsToProcess.keySet());

    for (String tableNameWithType : tables) {
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
      if (tableToWatermark != null) {
        for (String mergeLevel : tableToWatermark.keySet()) {
          if (!mergeLevelToConfigs.containsKey(mergeLevel)) {
            resetDelayMetrics(tableNameWithType, mergeLevel);
          }
        }
      }

      Map<String, Long> tableToNumBucketsToProcess = _tableNumberBucketsToProcess.get(tableNameWithType);
      if (tableToNumBucketsToProcess != null) {
        for (String mergeLevel : tableToNumBucketsToProcess.keySet()) {
          if (!mergeLevelToConfigs.containsKey(mergeLevel)) {
            resetDelayMetrics(tableNameWithType, mergeLevel);
          }
        }
      }
    }
  }

  private String getMetricNameForTaskDelay(String tableNameWithType, String mergeLevel) {
    // e.g. mergeRollupTaskDelayInNumBuckets.myTable_OFFLINE.daily
    return MERGE_ROLLUP_TASK_DELAY_IN_NUM_BUCKETS + "." + tableNameWithType + "." + mergeLevel;
  }

  private String getMetricNameForNumBucketsToProcess(String tableNameWithType, String mergeLevel) {
    return MERGE_ROLLUP_TASK_NUM_BUCKETS_TO_PROCESS + "." + tableNameWithType + "." + mergeLevel;
  }
}
