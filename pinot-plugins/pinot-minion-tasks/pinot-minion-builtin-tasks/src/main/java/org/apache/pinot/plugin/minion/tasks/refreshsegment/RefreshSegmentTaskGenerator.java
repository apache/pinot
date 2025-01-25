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
package org.apache.pinot.plugin.minion.tasks.refreshsegment;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.data.Segment;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.RefreshSegmentTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.plugin.minion.tasks.MinionTaskUtils;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@TaskGenerator
public class RefreshSegmentTaskGenerator extends BaseTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(RefreshSegmentTaskGenerator.class);

  @Override
  public String getTaskType() {
    return RefreshSegmentTask.TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();
    for (TableConfig tableConfig : tableConfigs) {
      // Get the task configs for the table. This is used to restrict the maximum number of allowed tasks per table at
      // any given point.
      Map<String, String> taskConfigs;
      TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
      if (tableTaskConfig == null) {
        LOGGER.warn("Failed to find task config for table: {}", tableConfig.getTableName());
        continue;
      }
      taskConfigs = tableTaskConfig.getConfigsForTaskType(RefreshSegmentTask.TASK_TYPE);
      pinotTaskConfigs.addAll(generateTasksForTable(tableConfig, taskConfigs));
    }

    return pinotTaskConfigs;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(TableConfig tableConfig, Map<String, String> taskConfigs)
      throws Exception {
    return generateTasksForTable(tableConfig, taskConfigs);
  }

  private List<PinotTaskConfig> generateTasksForTable(TableConfig tableConfig, Map<String, String> taskConfigs) {
    String tableNameWithType = tableConfig.getTableName();
    Preconditions.checkNotNull(taskConfigs, "Task config shouldn't be null for Table: %s", tableNameWithType);


    String taskType = RefreshSegmentTask.TASK_TYPE;
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();
    PinotHelixResourceManager pinotHelixResourceManager = _clusterInfoAccessor.getPinotHelixResourceManager();

    LOGGER.info("Start generating RefreshSegment tasks for table: {}", tableNameWithType);

    int tableNumTasks = 0;
    int tableMaxNumTasks = RefreshSegmentTask.MAX_NUM_TASKS_PER_TABLE;
    String tableMaxNumTasksConfig = taskConfigs.get(MinionConstants.TABLE_MAX_NUM_TASKS_KEY);
    if (tableMaxNumTasksConfig != null) {
      try {
        tableMaxNumTasks = Integer.parseInt(tableMaxNumTasksConfig);
      } catch (Exception e) {
        tableMaxNumTasks = RefreshSegmentTask.MAX_NUM_TASKS_PER_TABLE;
        LOGGER.warn("MaxNumTasks have been wrongly set for table : {}, and task {}", tableNameWithType, taskType);
      }
    }

    // Get info about table and schema.
    Stat tableStat = pinotHelixResourceManager.getTableStat(tableNameWithType);
    Schema schema = pinotHelixResourceManager.getSchemaForTableConfig(tableConfig);
    Stat schemaStat = pinotHelixResourceManager.getSchemaStat(schema.getSchemaName());

    // Get the running segments for a table.
    Set<Segment> runningSegments =
        TaskGeneratorUtils.getRunningSegments(RefreshSegmentTask.TASK_TYPE, _clusterInfoAccessor);

    // Make a single ZK call to get the segments.
    List<SegmentZKMetadata> allSegments = _clusterInfoAccessor.getSegmentsZKMetadata(tableNameWithType);

    for (SegmentZKMetadata segmentZKMetadata : allSegments) {
      // Skip if we have reached the maximum number of permissible tasks per iteration.
      if (tableNumTasks >= tableMaxNumTasks) {
        break;
      }

      // Skip consuming segments.
      if (tableConfig.getTableType() == TableType.REALTIME && !segmentZKMetadata.getStatus().isCompleted()) {
        continue;
      }

      // Skip segments for which a task is already running.
      if (runningSegments.contains(new Segment(tableNameWithType, segmentZKMetadata.getSegmentName()))) {
        continue;
      }

      String segmentName = segmentZKMetadata.getSegmentName();

      // Skip if the segment is already up-to-date and doesn't have to be refreshed.
      if (!shouldRefreshSegment(segmentZKMetadata, tableConfig, tableStat, schemaStat)) {
        continue;
      }

      Map<String, String> configs = new HashMap<>(getBaseTaskConfigs(tableConfig, List.of(segmentName)));
      configs.put(MinionConstants.DOWNLOAD_URL_KEY, segmentZKMetadata.getDownloadUrl());
      configs.put(MinionConstants.UPLOAD_URL_KEY, _clusterInfoAccessor.getVipUrl() + "/segments");
      configs.put(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY, String.valueOf(segmentZKMetadata.getCrc()));
      pinotTaskConfigs.add(new PinotTaskConfig(taskType, configs));
      tableNumTasks++;
    }

    LOGGER.info("Finished generating {} tasks configs for table: {} for task: {}", tableNumTasks, tableNameWithType,
        taskType);
    return pinotTaskConfigs;
  }

  /**
   * We need not refresh when: There were no tableConfig or schema updates after the last time the segment was
   * refreshed by this task.
   *
   * Note that newly created segments after the latest tableConfig/schema update will still need to be refreshed. This
   * is because inverted index created is disabled by default during segment generation. This can be added as an
   * additional check in the future, if required.
   */
  private boolean shouldRefreshSegment(SegmentZKMetadata segmentZKMetadata, TableConfig tableConfig, Stat tableStat,
      Stat schemaStat) {
    String tableNameWithType = tableConfig.getTableName();
    String timestampKey = RefreshSegmentTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX;

    long lastProcessedTime = 0L;
    if (segmentZKMetadata.getCustomMap() != null && segmentZKMetadata.getCustomMap().containsKey(timestampKey)) {
      lastProcessedTime = MinionTaskUtils.fromUTCString(segmentZKMetadata.getCustomMap().get(timestampKey));
    }

    if (tableStat == null || schemaStat == null) {
      LOGGER.warn("Table or schema stat is null for table: {}", tableNameWithType);
      return false;
    }

    long tableMTime = tableStat.getMtime();
    long schemaMTime = schemaStat.getMtime();

//    TODO: See comment above - add this later if required.
//    boolean segmentCreatedBeforeUpdate =
//        tableMTime > segmentZKMetadata.getCreationTime() || schemaMTime > segmentZKMetadata.getCreationTime();

    boolean segmentProcessedBeforeUpdate = tableMTime > lastProcessedTime || schemaMTime > lastProcessedTime;
    return segmentProcessedBeforeUpdate;
  }
}
