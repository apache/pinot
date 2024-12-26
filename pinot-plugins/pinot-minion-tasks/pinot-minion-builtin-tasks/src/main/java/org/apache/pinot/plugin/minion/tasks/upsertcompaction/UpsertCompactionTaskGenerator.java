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
package org.apache.pinot.plugin.minion.tasks.upsertcompaction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.restlet.resources.ValidDocIdsMetadataInfo;
import org.apache.pinot.common.restlet.resources.ValidDocIdsType;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorUtils;
import org.apache.pinot.controller.util.ServerSegmentMetadataReader;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.UpsertCompactionTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@TaskGenerator
public class UpsertCompactionTaskGenerator extends BaseTaskGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertCompactionTaskGenerator.class);
  private static final String DEFAULT_BUFFER_PERIOD = "7d";
  private static final double DEFAULT_INVALID_RECORDS_THRESHOLD_PERCENT = 0.0;
  private static final long DEFAULT_INVALID_RECORDS_THRESHOLD_COUNT = 1;
  private static final int DEFAULT_NUM_SEGMENTS_BATCH_PER_SERVER_REQUEST = 500;

  public static class SegmentSelectionResult {

    private final List<SegmentZKMetadata> _segmentsForCompaction;

    private final List<String> _segmentsForDeletion;

    SegmentSelectionResult(List<SegmentZKMetadata> segmentsForCompaction, List<String> segmentsForDeletion) {
      _segmentsForCompaction = segmentsForCompaction;
      _segmentsForDeletion = segmentsForDeletion;
    }

    public List<SegmentZKMetadata> getSegmentsForCompaction() {
      return _segmentsForCompaction;
    }

    public List<String> getSegmentsForDeletion() {
      return _segmentsForDeletion;
    }
  }

  @Override
  public String getTaskType() {
    return MinionConstants.UpsertCompactionTask.TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    String taskType = MinionConstants.UpsertCompactionTask.TASK_TYPE;
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();
    for (TableConfig tableConfig : tableConfigs) {

      String tableNameWithType = tableConfig.getTableName();
      LOGGER.info("Start generating task configs for table: {}", tableNameWithType);

      if (tableConfig.getTaskConfig() == null) {
        LOGGER.warn("Task config is null for table: {}", tableNameWithType);
        continue;
      }

      Map<String, String> taskConfigs = tableConfig.getTaskConfig().getConfigsForTaskType(taskType);
      List<SegmentZKMetadata> allSegments = _clusterInfoAccessor.getSegmentsZKMetadata(tableNameWithType);

      // Get completed segments and filter out the segments based on the buffer time configuration
      List<SegmentZKMetadata> completedSegments =
          getCompletedSegments(taskConfigs, allSegments, System.currentTimeMillis());

      if (completedSegments.isEmpty()) {
        LOGGER.info("No completed segments were eligible for compaction for table: {}", tableNameWithType);
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

      // By default, we use 'snapshot' for validDocIdsType. This means that we will use the validDocIds bitmap from
      // the snapshot from Pinot segment. This will require 'enableSnapshot' from UpsertConfig to be set to true.
      String validDocIdsTypeStr =
          taskConfigs.getOrDefault(UpsertCompactionTask.VALID_DOC_IDS_TYPE, ValidDocIdsType.SNAPSHOT.toString());
      ValidDocIdsType validDocIdsType = ValidDocIdsType.valueOf(validDocIdsTypeStr.toUpperCase());

      // Number of segments to query per server request. If a table has a lot of segments, then we might send a
      // huge payload to pinot-server in request. Batching the requests will help in reducing the payload size.
      int numSegmentsBatchPerServerRequest = Integer.parseInt(
          taskConfigs.getOrDefault(UpsertCompactionTask.NUM_SEGMENTS_BATCH_PER_SERVER_REQUEST,
              String.valueOf(DEFAULT_NUM_SEGMENTS_BATCH_PER_SERVER_REQUEST)));

      Map<String, List<ValidDocIdsMetadataInfo>> validDocIdsMetadataList =
          serverSegmentMetadataReader.getSegmentToValidDocIdsMetadataFromServer(tableNameWithType, serverToSegments,
              serverToEndpoints, null, 60_000, validDocIdsType.toString(), numSegmentsBatchPerServerRequest);

      Map<String, SegmentZKMetadata> completedSegmentsMap =
          completedSegments.stream().collect(Collectors.toMap(SegmentZKMetadata::getSegmentName, Function.identity()));

      SegmentSelectionResult segmentSelectionResult =
          processValidDocIdsMetadata(taskConfigs, completedSegmentsMap, validDocIdsMetadataList);
      int skippedSegmentsCount = validDocIdsMetadataList.size()
              - segmentSelectionResult.getSegmentsForCompaction().size()
              - segmentSelectionResult.getSegmentsForDeletion().size();
      LOGGER.info("Selected {} segments for compaction, {} segments for deletion and skipped {} segments for table: {}",
          segmentSelectionResult.getSegmentsForCompaction().size(),
              segmentSelectionResult.getSegmentsForDeletion().size(), skippedSegmentsCount, tableNameWithType);

      if (!segmentSelectionResult.getSegmentsForDeletion().isEmpty()) {
        pinotHelixResourceManager.deleteSegments(tableNameWithType, segmentSelectionResult.getSegmentsForDeletion(),
            "0d");
        LOGGER.info(
            "Deleted segments containing only invalid records for table: {}, number of segments to be deleted: {}",
            tableNameWithType, segmentSelectionResult.getSegmentsForDeletion());
      }

      int numTasks = 0;
      int maxTasks = getMaxTasks(taskType, tableNameWithType, taskConfigs);
      for (SegmentZKMetadata segment : segmentSelectionResult.getSegmentsForCompaction()) {
        if (numTasks == maxTasks) {
          break;
        }
        if (StringUtils.isBlank(segment.getDownloadUrl())) {
          LOGGER.warn("Skipping segment {} for task {} as download url is empty", segment.getSegmentName(), taskType);
          continue;
        }
        Map<String, String> configs = new HashMap<>(getBaseTaskConfigs(tableConfig, List.of(segment.getSegmentName())));
        configs.put(MinionConstants.DOWNLOAD_URL_KEY, segment.getDownloadUrl());
        configs.put(MinionConstants.UPLOAD_URL_KEY, _clusterInfoAccessor.getVipUrl() + "/segments");
        configs.put(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY, String.valueOf(segment.getCrc()));
        configs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, validDocIdsType.toString());
        configs.put(UpsertCompactionTask.IGNORE_CRC_MISMATCH_KEY,
            taskConfigs.getOrDefault(UpsertCompactionTask.IGNORE_CRC_MISMATCH_KEY,
            String.valueOf(UpsertCompactionTask.DEFAULT_IGNORE_CRC_MISMATCH)));
        pinotTaskConfigs.add(new PinotTaskConfig(UpsertCompactionTask.TASK_TYPE, configs));
        numTasks++;
      }
      LOGGER.info("Finished generating {} tasks configs for table: {}", numTasks, tableNameWithType);
    }
    return pinotTaskConfigs;
  }

  @VisibleForTesting
  public static SegmentSelectionResult processValidDocIdsMetadata(Map<String, String> taskConfigs,
      Map<String, SegmentZKMetadata> completedSegmentsMap,
      Map<String, List<ValidDocIdsMetadataInfo>> validDocIdsMetadataInfoMap) {
    double invalidRecordsThresholdPercent = Double.parseDouble(
        taskConfigs.getOrDefault(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_PERCENT,
            String.valueOf(DEFAULT_INVALID_RECORDS_THRESHOLD_PERCENT)));
    long invalidRecordsThresholdCount = Long.parseLong(
        taskConfigs.getOrDefault(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_COUNT,
            String.valueOf(DEFAULT_INVALID_RECORDS_THRESHOLD_COUNT)));
    List<Pair<SegmentZKMetadata, Long>> segmentsForCompaction = new ArrayList<>();
    List<String> segmentsForDeletion = new ArrayList<>();
    for (String segmentName : validDocIdsMetadataInfoMap.keySet()) {
      // check if segment is part of completed segments
      if (!completedSegmentsMap.containsKey(segmentName)) {
        LOGGER.warn("Segment {} is not found in the completed segments list, skipping it for compaction", segmentName);
        continue;
      }
      SegmentZKMetadata segment = completedSegmentsMap.get(segmentName);
      for (ValidDocIdsMetadataInfo validDocIdsMetadata : validDocIdsMetadataInfoMap.get(segmentName)) {
        long totalInvalidDocs = validDocIdsMetadata.getTotalInvalidDocs();

        // Skip segments if the crc from zk metadata and server does not match. They may be being reloaded.
        if (segment.getCrc() != Long.parseLong(validDocIdsMetadata.getSegmentCrc())) {
          LOGGER.warn("CRC mismatch for segment: {}, (segmentZKMetadata={}, validDocIdsMetadata={})", segmentName,
              segment.getCrc(), validDocIdsMetadata.getSegmentCrc());
          continue;
        }
        long totalDocs = validDocIdsMetadata.getTotalDocs();
        double invalidRecordPercent = ((double) totalInvalidDocs / totalDocs) * 100;
        if (totalInvalidDocs == totalDocs) {
          LOGGER.debug("Segment {} contains only invalid records, adding it to the deletion list", segmentName);
          segmentsForDeletion.add(segment.getSegmentName());
        } else if (invalidRecordPercent >= invalidRecordsThresholdPercent
            && totalInvalidDocs >= invalidRecordsThresholdCount) {
          LOGGER.debug("Segment {} contains {} invalid records out of {} total records "
                          + "(count threshold: {}, percent threshold: {}), adding it to the compaction list",
                  segmentName, totalInvalidDocs, totalDocs, invalidRecordsThresholdCount,
                  invalidRecordsThresholdPercent);
          segmentsForCompaction.add(Pair.of(segment, totalInvalidDocs));
        } else {
          LOGGER.debug("Segment {} contains {} invalid records out of {} total records "
                          + "(count threshold: {}, percent threshold: {}), skipping it for compaction",
                  segmentName, totalInvalidDocs, totalDocs, invalidRecordsThresholdCount,
                  invalidRecordsThresholdPercent);
        }
        break;
      }
    }
    segmentsForCompaction.sort((o1, o2) -> {
      if (o1.getValue() > o2.getValue()) {
        return -1;
      } else if (o1.getValue().equals(o2.getValue())) {
        return 0;
      }
      return 1;
    });

    return new SegmentSelectionResult(
        segmentsForCompaction.stream().map(Map.Entry::getKey).collect(Collectors.toList()), segmentsForDeletion);
  }

  @VisibleForTesting
  public static List<SegmentZKMetadata> getCompletedSegments(Map<String, String> taskConfigs,
      List<SegmentZKMetadata> allSegments, long currentTimeInMillis) {
    List<SegmentZKMetadata> completedSegments = new ArrayList<>();
    String bufferPeriod = taskConfigs.getOrDefault(UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY, DEFAULT_BUFFER_PERIOD);
    long bufferMs = TimeUtils.convertPeriodToMillis(bufferPeriod);
    for (SegmentZKMetadata segment : allSegments) {
      CommonConstants.Segment.Realtime.Status status = segment.getStatus();
      // initial segments selection based on status and age
      if (status.isCompleted() && (segment.getEndTimeMs() <= (currentTimeInMillis - bufferMs))) {
        completedSegments.add(segment);
      }
    }
    return completedSegments;
  }

  @VisibleForTesting
  public static int getMaxTasks(String taskType, String tableNameWithType, Map<String, String> taskConfigs) {
    int maxTasks = Integer.MAX_VALUE;
    String tableMaxNumTasksConfig = taskConfigs.get(MinionConstants.TABLE_MAX_NUM_TASKS_KEY);
    if (tableMaxNumTasksConfig != null) {
      try {
        maxTasks = Integer.parseInt(tableMaxNumTasksConfig);
      } catch (Exception e) {
        LOGGER.warn("MaxNumTasks have been wrongly set for table : {}, and task {}", tableNameWithType, taskType);
      }
    }
    return maxTasks;
  }

  @Override
  public void validateTaskConfigs(TableConfig tableConfig, Map<String, String> taskConfigs) {
    // check table is realtime
    Preconditions.checkState(tableConfig.getTableType() == TableType.REALTIME,
        "UpsertCompactionTask only supports realtime tables!");
    // check upsert enabled
    Preconditions.checkState(tableConfig.isUpsertEnabled(), "Upsert must be enabled for UpsertCompactionTask");

    // check no malformed period
    if (taskConfigs.containsKey(UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY)) {
      TimeUtils.convertPeriodToMillis(taskConfigs.get(UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY));
    }
    // check invalidRecordsThresholdPercent
    if (taskConfigs.containsKey(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_PERCENT)) {
      Preconditions.checkState(
          Double.parseDouble(taskConfigs.get(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_PERCENT)) >= 0
              && Double.parseDouble(taskConfigs.get(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_PERCENT)) <= 100,
          "invalidRecordsThresholdPercent must be >= 0 and <= 100");
    }
    // check invalidRecordsThresholdCount
    if (taskConfigs.containsKey(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_COUNT)) {
      Preconditions.checkState(
          Long.parseLong(taskConfigs.get(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_COUNT)) >= 1,
          "invalidRecordsThresholdCount must be >= 1");
    }
    // check that either invalidRecordsThresholdPercent or invalidRecordsThresholdCount was provided
    Preconditions.checkState(
        taskConfigs.containsKey(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_PERCENT) || taskConfigs.containsKey(
            UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_COUNT),
        "invalidRecordsThresholdPercent or invalidRecordsThresholdCount or both must be provided");
    String validDocIdsType =
        taskConfigs.getOrDefault(UpsertCompactionTask.VALID_DOC_IDS_TYPE, UpsertCompactionTask.SNAPSHOT);
    if (validDocIdsType.equals(ValidDocIdsType.SNAPSHOT.toString())) {
      UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
      Preconditions.checkNotNull(upsertConfig, "UpsertConfig must be provided for UpsertCompactionTask");
      Preconditions.checkState(upsertConfig.isEnableSnapshot(), String.format(
          "'enableSnapshot' from UpsertConfig must be enabled for UpsertCompactionTask with validDocIdsType = "
              + "%s", validDocIdsType));
    } else if (validDocIdsType.equals(ValidDocIdsType.IN_MEMORY_WITH_DELETE.toString())) {
      UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
      Preconditions.checkNotNull(upsertConfig, "UpsertConfig must be provided for UpsertCompactionTask");
      Preconditions.checkNotNull(upsertConfig.getDeleteRecordColumn(), String.format(
          "deleteRecordColumn must be provided for " + "UpsertCompactionTask with validDocIdsType = %s",
          validDocIdsType));
    }
  }
}
