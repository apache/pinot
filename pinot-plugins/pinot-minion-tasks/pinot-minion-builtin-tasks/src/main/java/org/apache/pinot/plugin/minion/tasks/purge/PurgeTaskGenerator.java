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
package org.apache.pinot.plugin.minion.tasks.purge;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.data.Segment;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@TaskGenerator
public class PurgeTaskGenerator extends BaseTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(PurgeTaskGenerator.class);

  @Override
  public String getTaskType() {
    return MinionConstants.PurgeTask.TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    LOGGER.info("Start generating PurgeTask");
    String taskType = MinionConstants.PurgeTask.TASK_TYPE;
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();

    for (TableConfig tableConfig : tableConfigs) {

      String tableName = tableConfig.getTableName();
      List<SegmentZKMetadata> completedSegmentsZKMetadata = new ArrayList<>();
      if (tableConfig.getTableType() == TableType.REALTIME) {
        completedSegmentsZKMetadata = new ArrayList<>();
        Map<Integer, String> partitionToLatestLLCSegmentName = new HashMap<>();
        Set<Integer> allPartitions = new HashSet<>();
        getCompletedSegmentsInfo(tableName, completedSegmentsZKMetadata, partitionToLatestLLCSegmentName,
            allPartitions);
      }

      Map<String, String> taskConfigs;
      TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
      if (tableTaskConfig == null) {
        LOGGER.warn("Failed to find task config for table: {}", tableName);
        continue;
      }
      taskConfigs = tableTaskConfig.getConfigsForTaskType(MinionConstants.PurgeTask.TASK_TYPE);
      Preconditions.checkNotNull(taskConfigs, "Task config shouldn't be null for Table: {}", tableName);

      String deltaTimePeriod =
          taskConfigs.getOrDefault(MinionConstants.PurgeTask.LAST_PURGE_TIME_THREESOLD_PERIOD,
              MinionConstants.PurgeTask.DEFAULT_LAST_PURGE_TIME_THRESHOLD_PERIOD);
      long purgeDeltaMs = TimeUtils.convertPeriodToMillis(deltaTimePeriod);

      LOGGER.info("Start generating task configs for table: {} for task: {}", tableName, taskType);
      // Get max number of tasks for this table
      int tableMaxNumTasks;
      String tableMaxNumTasksConfig = taskConfigs.get(MinionConstants.TABLE_MAX_NUM_TASKS_KEY);
      if (tableMaxNumTasksConfig != null) {
        try {
          tableMaxNumTasks = Integer.parseInt(tableMaxNumTasksConfig);
        } catch (Exception e) {
          tableMaxNumTasks = Integer.MAX_VALUE;
          LOGGER.warn("MaxNumTasks have been wrongly set for table : {}, and task {}", tableName, taskType);
        }
      } else {
        tableMaxNumTasks = Integer.MAX_VALUE;
      }
      List<SegmentZKMetadata> segmentsZKMetadata;
      if (tableConfig.getTableType() == TableType.REALTIME) {
        segmentsZKMetadata = completedSegmentsZKMetadata;
      } else {
        segmentsZKMetadata = _clusterInfoAccessor.getSegmentsZKMetadata(tableName);
      }

      List<SegmentZKMetadata> purgedSegmentsZKMetadata = new ArrayList<>();
      List<SegmentZKMetadata> notpurgedSegmentsZKMetadata = new ArrayList<>();

      for (SegmentZKMetadata segmentMetadata : segmentsZKMetadata) {

        if (segmentMetadata.getCustomMap() != null && segmentMetadata.getCustomMap()
            .containsKey(MinionConstants.PurgeTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX)) {
          purgedSegmentsZKMetadata.add(segmentMetadata);
        } else {
          notpurgedSegmentsZKMetadata.add(segmentMetadata);
        }
      }
      Collections.sort(purgedSegmentsZKMetadata, Comparator.comparing(
          segmentZKMetadata -> segmentZKMetadata.getCustomMap()
              .get(MinionConstants.PurgeTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX),
          Comparator.nullsFirst(Comparator.naturalOrder())));
      //add already purged segment at the end
      notpurgedSegmentsZKMetadata.addAll(purgedSegmentsZKMetadata);
      int tableNumTasks = 0;
      Set<Segment> runningSegments =
          TaskGeneratorUtils.getRunningSegments(MinionConstants.PurgeTask.TASK_TYPE, _clusterInfoAccessor);
      for (SegmentZKMetadata segmentZKMetadata : notpurgedSegmentsZKMetadata) {
        Map<String, String> configs = new HashMap<>();
        String segmentName = segmentZKMetadata.getSegmentName();
        Long tsLastPurge;
        if (segmentZKMetadata.getCustomMap() != null) {
          tsLastPurge = Long.valueOf(segmentZKMetadata.getCustomMap()
              .get(MinionConstants.PurgeTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX));
        } else {
          tsLastPurge = 0L;
        }

        //skip running segment
        if (runningSegments.contains(new Segment(tableName, segmentName))) {
          continue;
        }
        if ((tsLastPurge != null) && ((System.currentTimeMillis() - tsLastPurge) < purgeDeltaMs)) {
          //skip if purge delay is not reached
          continue;
        }
        if (tableNumTasks == tableMaxNumTasks) {
          break;
        }
        configs.put(MinionConstants.TABLE_NAME_KEY, tableName);
        configs.put(MinionConstants.SEGMENT_NAME_KEY, segmentName);
        configs.put(MinionConstants.DOWNLOAD_URL_KEY, segmentZKMetadata.getDownloadUrl());
        configs.put(MinionConstants.UPLOAD_URL_KEY, _clusterInfoAccessor.getVipUrl() + "/segments");
        configs.put(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY, String.valueOf(segmentZKMetadata.getCrc()));
        pinotTaskConfigs.add(new PinotTaskConfig(taskType, configs));
        tableNumTasks++;
      }
      LOGGER.info("Finished generating {} tasks configs for table: {} " + "for task: {}", tableNumTasks, tableName,
          taskType);
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
      CommonConstants.Segment.Realtime.Status status = segmentZKMetadata.getStatus();
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
}