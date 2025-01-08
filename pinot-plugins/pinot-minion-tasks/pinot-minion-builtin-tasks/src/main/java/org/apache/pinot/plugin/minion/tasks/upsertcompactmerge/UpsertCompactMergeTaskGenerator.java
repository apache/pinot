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
package org.apache.pinot.plugin.minion.tasks.upsertcompactmerge;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.restlet.resources.ValidDocIdsMetadataInfo;
import org.apache.pinot.common.restlet.resources.ValidDocIdsType;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorUtils;
import org.apache.pinot.controller.util.ServerSegmentMetadataReader;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@TaskGenerator
public class UpsertCompactMergeTaskGenerator extends BaseTaskGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertCompactMergeTaskGenerator.class);
  private static final String DEFAULT_BUFFER_PERIOD = "2d";
  private static final int DEFAULT_NUM_SEGMENTS_BATCH_PER_SERVER_REQUEST = 500;

  public static class SegmentMergerMetadata {
    private final SegmentZKMetadata _segmentZKMetadata;
    private final long _validDocIds;
    private final long _invalidDocIds;
    private final double _segmentSizeInBytes;

    SegmentMergerMetadata(SegmentZKMetadata segmentZKMetadata, long validDocIds, long invalidDocIds,
        double segmentSizeInBytes) {
      _segmentZKMetadata = segmentZKMetadata;
      _validDocIds = validDocIds;
      _invalidDocIds = invalidDocIds;
      _segmentSizeInBytes = segmentSizeInBytes;
    }

    public SegmentZKMetadata getSegmentZKMetadata() {
      return _segmentZKMetadata;
    }

    public long getValidDocIds() {
      return _validDocIds;
    }

    public long getInvalidDocIds() {
      return _invalidDocIds;
    }

    public double getSegmentSizeInBytes() {
      return _segmentSizeInBytes;
    }
  }

  public static class SegmentSelectionResult {

    private final Map<Integer, List<List<SegmentMergerMetadata>>> _segmentsForCompactMergeByPartition;

    private final List<String> _segmentsForDeletion;

    SegmentSelectionResult(Map<Integer, List<List<SegmentMergerMetadata>>> segmentsForCompactMergeByPartition,
        List<String> segmentsForDeletion) {
      _segmentsForCompactMergeByPartition = segmentsForCompactMergeByPartition;
      _segmentsForDeletion = segmentsForDeletion;
    }

    public Map<Integer, List<List<SegmentMergerMetadata>>> getSegmentsForCompactMergeByPartition() {
      return _segmentsForCompactMergeByPartition;
    }

    public List<String> getSegmentsForDeletion() {
      return _segmentsForDeletion;
    }
  }

  @Override
  public String getTaskType() {
    return MinionConstants.UpsertCompactMergeTask.TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    String taskType = MinionConstants.UpsertCompactMergeTask.TASK_TYPE;
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();
    for (TableConfig tableConfig : tableConfigs) {

      String tableNameWithType = tableConfig.getTableName();
      LOGGER.info("Start generating task configs for table: {}", tableNameWithType);

      if (tableConfig.getTaskConfig() == null) {
        LOGGER.warn("Task config is null for table: {}", tableNameWithType);
        continue;
      }

      // Only schedule 1 task of this type, per table
      Map<String, TaskState> incompleteTasks =
          TaskGeneratorUtils.getIncompleteTasks(taskType, tableNameWithType, _clusterInfoAccessor);
      if (!incompleteTasks.isEmpty()) {
        LOGGER.warn("Found incomplete tasks: {} for same table: {} and task type: {}. Skipping task generation.",
            incompleteTasks.keySet(), tableNameWithType, taskType);
        continue;
      }

      Map<String, String> taskConfigs = tableConfig.getTaskConfig().getConfigsForTaskType(taskType);
      List<SegmentZKMetadata> allSegments = _clusterInfoAccessor.getSegmentsZKMetadata(tableNameWithType);

      // Get completed segments and filter out the segments based on the buffer time configuration
      List<SegmentZKMetadata> candidateSegments =
          getCandidateSegments(taskConfigs, allSegments, System.currentTimeMillis());

      if (candidateSegments.isEmpty()) {
        LOGGER.info("No segments were eligible for compactMerge task for table: {}", tableNameWithType);
        continue;
      }

      // get server to segment mappings
      PinotHelixResourceManager pinotHelixResourceManager = _clusterInfoAccessor.getPinotHelixResourceManager();
      Map<String, List<String>> serverToSegments = pinotHelixResourceManager.getServerToSegmentsMap(tableNameWithType);
      BiMap<String, String> serverToEndpoints;
      try {
        serverToEndpoints = pinotHelixResourceManager.getDataInstanceAdminEndpoints(serverToSegments.keySet());
      } catch (InvalidConfigException e) {
        throw new RuntimeException(e);
      }

      ServerSegmentMetadataReader serverSegmentMetadataReader =
          new ServerSegmentMetadataReader(_clusterInfoAccessor.getExecutor(),
              _clusterInfoAccessor.getConnectionManager());

      // Number of segments to query per server request. If a table has a lot of segments, then we might send a
      // huge payload to pinot-server in request. Batching the requests will help in reducing the payload size.
      int numSegmentsBatchPerServerRequest = Integer.parseInt(
          taskConfigs.getOrDefault(MinionConstants.UpsertCompactMergeTask.NUM_SEGMENTS_BATCH_PER_SERVER_REQUEST,
              String.valueOf(DEFAULT_NUM_SEGMENTS_BATCH_PER_SERVER_REQUEST)));

      Map<String, List<ValidDocIdsMetadataInfo>> validDocIdsMetadataList =
          serverSegmentMetadataReader.getSegmentToValidDocIdsMetadataFromServer(tableNameWithType, serverToSegments,
              serverToEndpoints, null, 60_000, ValidDocIdsType.SNAPSHOT.toString(), numSegmentsBatchPerServerRequest);

      Map<String, SegmentZKMetadata> candidateSegmentsMap =
          candidateSegments.stream().collect(Collectors.toMap(SegmentZKMetadata::getSegmentName, Function.identity()));

      Set<String> alreadyMergedSegments = getAlreadyMergedSegments(allSegments);

      SegmentSelectionResult segmentSelectionResult =
          processValidDocIdsMetadata(tableNameWithType, taskConfigs, candidateSegmentsMap, validDocIdsMetadataList,
              alreadyMergedSegments);

      if (!segmentSelectionResult.getSegmentsForDeletion().isEmpty()) {
        pinotHelixResourceManager.deleteSegments(tableNameWithType, segmentSelectionResult.getSegmentsForDeletion(),
            "0d");
        LOGGER.info(
            "Deleted segments containing only invalid records for table: {}, number of segments to be deleted: {}",
            tableNameWithType, segmentSelectionResult.getSegmentsForDeletion());
      }

      int numTasks = 0;
      int maxTasks = Integer.parseInt(taskConfigs.getOrDefault(MinionConstants.TABLE_MAX_NUM_TASKS_KEY,
          String.valueOf(MinionConstants.DEFAULT_TABLE_MAX_NUM_TASKS)));
      for (Map.Entry<Integer, List<List<SegmentMergerMetadata>>> entry
          : segmentSelectionResult.getSegmentsForCompactMergeByPartition().entrySet()) {
        if (numTasks == maxTasks) {
          break;
        }
        List<List<SegmentMergerMetadata>> groups = entry.getValue();
        // no valid groups found in the partition to merge
        if (groups.isEmpty()) {
          continue;
        }
        // there are no groups with more than 1 segment to merge
        // TODO this can be later removed if we want to just do single-segment compaction from this task
        if (groups.get(0).size() <= 1) {
          continue;
        }
        // TODO see if multiple groups of same partition can be added
        Map<String, String> configs = new HashMap<>(getBaseTaskConfigs(tableConfig,
            groups.get(0).stream().map(x -> x.getSegmentZKMetadata().getSegmentName()).collect(Collectors.toList())));
        configs.put(MinionConstants.DOWNLOAD_URL_KEY, getDownloadUrl(groups.get(0)));
        configs.put(MinionConstants.UPLOAD_URL_KEY, _clusterInfoAccessor.getVipUrl() + "/segments");
        configs.put(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY, getSegmentCrcList(groups.get(0)));
        configs.put(MinionConstants.UpsertCompactMergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, String.valueOf(
            Long.parseLong(
                taskConfigs.getOrDefault(MinionConstants.UpsertCompactMergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY,
                    String.valueOf(MinionConstants.UpsertCompactMergeTask.DEFAULT_MAX_NUM_RECORDS_PER_SEGMENT)))));
        pinotTaskConfigs.add(new PinotTaskConfig(MinionConstants.UpsertCompactMergeTask.TASK_TYPE, configs));
        numTasks++;
      }
      LOGGER.info("Finished generating {} tasks configs for table: {}", numTasks, tableNameWithType);
    }
    return pinotTaskConfigs;
  }

  @VisibleForTesting
  public static SegmentSelectionResult processValidDocIdsMetadata(String tableNameWithType,
      Map<String, String> taskConfigs, Map<String, SegmentZKMetadata> candidateSegmentsMap,
      Map<String, List<ValidDocIdsMetadataInfo>> validDocIdsMetadataInfoMap, Set<String> alreadyMergedSegments) {
    Map<Integer, List<SegmentMergerMetadata>> segmentsEligibleForCompactMerge = new HashMap<>();
    Set<String> segmentsForDeletion = new HashSet<>();

    // task config thresholds
    long validDocsThreshold = Long.parseLong(
        taskConfigs.getOrDefault(MinionConstants.UpsertCompactMergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY,
            String.valueOf(MinionConstants.UpsertCompactMergeTask.DEFAULT_MAX_NUM_RECORDS_PER_SEGMENT)));
    long maxRecordsPerTask = Long.parseLong(
        taskConfigs.getOrDefault(MinionConstants.UpsertCompactMergeTask.MAX_NUM_RECORDS_PER_TASK_KEY,
            String.valueOf(MinionConstants.UpsertCompactMergeTask.DEFAULT_MAX_NUM_RECORDS_PER_TASK)));
    long maxNumSegments = Long.parseLong(
        taskConfigs.getOrDefault(MinionConstants.UpsertCompactMergeTask.MAX_NUM_SEGMENTS_PER_TASK_KEY,
            String.valueOf(MinionConstants.UpsertCompactMergeTask.DEFAULT_MAX_NUM_SEGMENTS_PER_TASK)));

    // default to Long.MAX_VALUE to avoid size-based compaction by default
    long outputSegmentMaxSizeInBytes = Long.MAX_VALUE;
    try {
      if (taskConfigs.containsKey(MinionConstants.UpsertCompactMergeTask.OUTPUT_SEGMENT_MAX_SIZE_KEY)) {
        String configuredOutputSegmentMaxSize =
            taskConfigs.get(MinionConstants.UpsertCompactMergeTask.OUTPUT_SEGMENT_MAX_SIZE_KEY);
        LOGGER.info("Configured outputSegmentMaxSizeInByte: {} for {}", configuredOutputSegmentMaxSize,
            tableNameWithType);
        outputSegmentMaxSizeInBytes = DataSizeUtils.toBytes(configuredOutputSegmentMaxSize);
      } else {
        LOGGER.info("No configured outputSegmentMaxSizeInByte for {}, defaulting to Long.MAX_VALUE", tableNameWithType);
      }
    } catch (Exception e) {
      LOGGER.warn("Invalid value outputSegmentMaxSizeInBytes configured for {}, defaulting to Long.MAX_VALUE",
          tableNameWithType, e);
    }

    for (String segmentName : validDocIdsMetadataInfoMap.keySet()) {
      // check if segment is part of completed segments
      if (!candidateSegmentsMap.containsKey(segmentName)) {
        LOGGER.debug("Segment {} is not found in the candidate segments list, skipping it for {}", segmentName,
            MinionConstants.UpsertCompactMergeTask.TASK_TYPE);
        continue;
      }
      SegmentZKMetadata segment = candidateSegmentsMap.get(segmentName);
      for (ValidDocIdsMetadataInfo validDocIdsMetadata : validDocIdsMetadataInfoMap.get(segmentName)) {
        long totalInvalidDocs = validDocIdsMetadata.getTotalInvalidDocs();
        long totalValidDocs = validDocIdsMetadata.getTotalValidDocs();
        long segmentSizeInBytes = validDocIdsMetadata.getSegmentSizeInBytes();

        // Skip segments if the crc from zk metadata and server does not match. They may be getting reloaded.
        if (segment.getCrc() != Long.parseLong(validDocIdsMetadata.getSegmentCrc())) {
          LOGGER.warn("CRC mismatch for segment: {}, (segmentZKMetadata={}, validDocIdsMetadata={})", segmentName,
              segment.getCrc(), validDocIdsMetadata.getSegmentCrc());
          continue;
        }

        // segments eligible for deletion with no valid records
        long totalDocs = validDocIdsMetadata.getTotalDocs();
        if (totalInvalidDocs == totalDocs) {
          segmentsForDeletion.add(segmentName);
        } else if (alreadyMergedSegments.contains(segmentName)) {
          LOGGER.debug("Segment {} already merged. Skipping it for {}", segmentName,
              MinionConstants.UpsertCompactMergeTask.TASK_TYPE);
          break;
        } else {
          Integer partitionID = SegmentUtils.getPartitionIdFromRealtimeSegmentName(segmentName);
          if (partitionID == null) {
            LOGGER.warn("Partition ID not found for segment: {}, skipping it for {}", segmentName,
                MinionConstants.UpsertCompactMergeTask.TASK_TYPE);
            continue;
          }
          double expectedSegmentSizeAfterCompaction = (segmentSizeInBytes * totalValidDocs * 1.0) / totalDocs;
          segmentsEligibleForCompactMerge.computeIfAbsent(partitionID, k -> new ArrayList<>())
              .add(new SegmentMergerMetadata(segment, totalValidDocs, totalInvalidDocs,
                  expectedSegmentSizeAfterCompaction));
        }
        break;
      }
    }

    segmentsEligibleForCompactMerge.forEach((partitionID, segmentList) -> segmentList.sort(
        Comparator.comparingLong(o -> o.getSegmentZKMetadata().getCreationTime())));

    // Map to store the result: each key (partition) will have a list of groups
    Map<Integer, List<List<SegmentMergerMetadata>>> groupedSegments = new HashMap<>();

    // Iterate over each partition and process its segments list
    for (Map.Entry<Integer, List<SegmentMergerMetadata>> entry : segmentsEligibleForCompactMerge.entrySet()) {
      int partitionID = entry.getKey();
      List<SegmentMergerMetadata> segments = entry.getValue();

      // List to store groups for the current partition
      List<List<SegmentMergerMetadata>> groups = new ArrayList<>();
      List<SegmentMergerMetadata> currentGroup = new ArrayList<>();

      // variables to maintain current group sum
      long currentValidDocsSum = 0;
      long currentTotalDocsSum = 0;
      double currentOutputSegmentSizeInBytes = 0.0;

      for (SegmentMergerMetadata segment : segments) {
        long validDocs = segment.getValidDocIds();
        long invalidDocs = segment.getInvalidDocIds();
        double expectedSegmentSizeInBytes = segment.getSegmentSizeInBytes();

        // Check if adding this segment would keep the validDocs sum within the threshold
        if (currentValidDocsSum + validDocs <= validDocsThreshold && currentGroup.size() < maxNumSegments
            && currentTotalDocsSum + validDocs + invalidDocs < maxRecordsPerTask
            && currentOutputSegmentSizeInBytes + expectedSegmentSizeInBytes < outputSegmentMaxSizeInBytes) {
          // Add the segment to the current group
          currentGroup.add(segment);
          currentValidDocsSum += validDocs;
          currentTotalDocsSum += validDocs + invalidDocs;
          currentOutputSegmentSizeInBytes += expectedSegmentSizeInBytes;
        } else {
          // Finalize the current group and start a new one
          if (!currentGroup.isEmpty()) {
            groups.add(new ArrayList<>(currentGroup));  // Add the finalized group
          }

          // Reset current group, sums and start with the new segment
          currentGroup = new ArrayList<>();
          currentGroup.add(segment);
          currentValidDocsSum = validDocs;
          currentTotalDocsSum = validDocs + invalidDocs;
          currentOutputSegmentSizeInBytes = expectedSegmentSizeInBytes;
        }
      }
      // Add the last group
      if (!currentGroup.isEmpty()) {
        groups.add(new ArrayList<>(currentGroup)); // Add a copy of the current group
      }

      // Sort groups by total invalidDocs in descending order, if invalidDocs count are same, prefer group with
      // higher number of small segments in them
      // remove the groups having only 1 segments in them
      // TODO this check can be later removed if we want single-segment compaction from this task itself
      List<List<SegmentMergerMetadata>> compactMergeGroups =
          groups.stream().filter(x -> x.size() > 1).sorted((group1, group2) -> {
            long invalidDocsSum1 = group1.stream().mapToLong(SegmentMergerMetadata::getInvalidDocIds).sum();
            long invalidDocsSum2 = group2.stream().mapToLong(SegmentMergerMetadata::getInvalidDocIds).sum();
            if (invalidDocsSum2 < invalidDocsSum1) {
              return -1;
            } else if (invalidDocsSum2 == invalidDocsSum1) {
              return Long.compare(group2.size(), group1.size());
            } else {
              return 1;
            }
          }).collect(Collectors.toList());

      if (!compactMergeGroups.isEmpty()) {
        groupedSegments.put(partitionID, compactMergeGroups);
      }
    }
    return new SegmentSelectionResult(groupedSegments, new ArrayList<>(segmentsForDeletion));
  }

  @VisibleForTesting
  public static List<SegmentZKMetadata> getCandidateSegments(Map<String, String> taskConfigs,
      List<SegmentZKMetadata> allSegments, long currentTimeInMillis) {
    List<SegmentZKMetadata> candidateSegments = new ArrayList<>();
    String bufferPeriod =
        taskConfigs.getOrDefault(MinionConstants.UpsertCompactMergeTask.BUFFER_TIME_PERIOD_KEY, DEFAULT_BUFFER_PERIOD);
    long bufferMs = TimeUtils.convertPeriodToMillis(bufferPeriod);
    for (SegmentZKMetadata segment : allSegments) {
      // Skip segments if HDFS download url is empty. This also avoids any race condition with deepstore upload
      // retry task and this task
      if (StringUtils.isBlank(segment.getDownloadUrl())) {
        LOGGER.warn("Skipping segment {} for task as download url is empty", segment.getSegmentName());
        continue;
      }
      // initial segments selection based on status and age
      if (segment.getStatus().isCompleted() && (segment.getEndTimeMs() <= (currentTimeInMillis - bufferMs))) {
        candidateSegments.add(segment);
      }
    }
    return candidateSegments;
  }

  @VisibleForTesting
  protected static Set<String> getAlreadyMergedSegments(List<SegmentZKMetadata> allSegments) {
    Set<String> alreadyMergedSegments = new HashSet<>();
    for (SegmentZKMetadata segment : allSegments) {
      // check if the segment has custom map having list of segments which merged to form this. we will later
      // filter out the merged segments as they will be deleted
      if (segment.getCustomMap() != null && !segment.getCustomMap().isEmpty() && !StringUtils.isBlank(
          segment.getCustomMap().get(MinionConstants.UpsertCompactMergeTask.TASK_TYPE
              + MinionConstants.UpsertCompactMergeTask.MERGED_SEGMENTS_ZK_SUFFIX))) {
        alreadyMergedSegments.addAll(List.of(StringUtils.split(segment.getCustomMap().get(
            MinionConstants.UpsertCompactMergeTask.TASK_TYPE
                + MinionConstants.UpsertCompactMergeTask.MERGED_SEGMENTS_ZK_SUFFIX), ",")));
      }
    }
    return alreadyMergedSegments;
  }

  @Override
  public void validateTaskConfigs(TableConfig tableConfig, Schema schema, Map<String, String> taskConfigs) {
    // check table is realtime
    Preconditions.checkState(tableConfig.getTableType() == TableType.REALTIME,
        String.format("%s only supports realtime tables!", MinionConstants.UpsertCompactMergeTask.TASK_TYPE));
    // check upsert enabled
    Preconditions.checkState(tableConfig.isUpsertEnabled(),
        String.format("Upsert must be enabled for %s", MinionConstants.UpsertCompactMergeTask.TASK_TYPE));
    // check no malformed period
    if (taskConfigs.containsKey(MinionConstants.UpsertCompactMergeTask.BUFFER_TIME_PERIOD_KEY)) {
      TimeUtils.convertPeriodToMillis(taskConfigs.get(MinionConstants.UpsertCompactMergeTask.BUFFER_TIME_PERIOD_KEY));
    }
    // check enableSnapshot = true
    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    Preconditions.checkNotNull(upsertConfig,
        String.format("UpsertConfig must be provided for %s", MinionConstants.UpsertCompactMergeTask.TASK_TYPE));
    Preconditions.checkState(upsertConfig.isEnableSnapshot(),
        String.format("'enableSnapshot' from UpsertConfig must be enabled for %s",
            MinionConstants.UpsertCompactMergeTask.TASK_TYPE));
    // check valid task config for maxOutputSegmentSize
    if (taskConfigs.containsKey(MinionConstants.UpsertCompactMergeTask.OUTPUT_SEGMENT_MAX_SIZE_KEY)) {
      DataSizeUtils.toBytes(taskConfigs.get(MinionConstants.UpsertCompactMergeTask.OUTPUT_SEGMENT_MAX_SIZE_KEY));
    }
  }

  @VisibleForTesting
  protected String getDownloadUrl(List<SegmentMergerMetadata> segmentMergerMetadataList) {
    return StringUtils.join(segmentMergerMetadataList.stream().map(x -> x.getSegmentZKMetadata().getDownloadUrl())
        .collect(Collectors.toList()), ",");
  }

  @VisibleForTesting
  protected String getSegmentCrcList(List<SegmentMergerMetadata> segmentMergerMetadataList) {
    return StringUtils.join(
        segmentMergerMetadataList.stream().map(x -> String.valueOf(x.getSegmentZKMetadata().getCrc()))
            .collect(Collectors.toList()), ",");
  }
}
