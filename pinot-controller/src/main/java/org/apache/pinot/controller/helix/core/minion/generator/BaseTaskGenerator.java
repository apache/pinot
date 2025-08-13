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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.model.IdealState;
import org.apache.helix.task.JobConfig;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.api.exception.UnknownTaskTypeException;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.controller.helix.core.minion.TaskSchedulingContext;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base implementation of the {@link PinotTaskGenerator} which reads the 'taskTimeoutMs',
 * 'numConcurrentTasksPerInstance' and 'maxAttemptsPerTask' from the cluster config.
 */
public abstract class BaseTaskGenerator implements PinotTaskGenerator {
  protected static final Logger LOGGER = LoggerFactory.getLogger(BaseTaskGenerator.class);

  protected ClusterInfoAccessor _clusterInfoAccessor;

  @Override
  public void init(ClusterInfoAccessor clusterInfoAccessor) {
    _clusterInfoAccessor = clusterInfoAccessor;
  }

  @Override
  public long getTaskTimeoutMs() {
    String taskType = getTaskType();
    String configKey = taskType + MinionConstants.TIMEOUT_MS_KEY_SUFFIX;
    String configValue = _clusterInfoAccessor.getClusterConfig(configKey);
    if (configValue != null) {
      try {
        return Long.parseLong(configValue);
      } catch (Exception e) {
        LOGGER.error("Invalid cluster config {}: '{}'", configKey, configValue, e);
      }
    }
    return JobConfig.DEFAULT_TIMEOUT_PER_TASK;
  }

  @Override
  public int getNumConcurrentTasksPerInstance() {
    String taskType = getTaskType();
    String configKey = taskType + MinionConstants.NUM_CONCURRENT_TASKS_PER_INSTANCE_KEY_SUFFIX;
    String configValue = _clusterInfoAccessor.getClusterConfig(configKey);
    if (configValue != null) {
      try {
        return Integer.parseInt(configValue);
      } catch (Exception e) {
        LOGGER.error("Invalid config {}: '{}'", configKey, configValue, e);
      }
    }
    return JobConfig.DEFAULT_NUM_CONCURRENT_TASKS_PER_INSTANCE;
  }

  @Override
  public int getMaxAllowedSubTasksPerTask() {
    String configKey = MinionConstants.MAX_ALLOWED_SUB_TASKS_KEY;
    String configValue = _clusterInfoAccessor.getClusterConfig(configKey);
    if (configValue != null) {
      try {
        return Integer.parseInt(configValue);
      } catch (Exception e) {
        LOGGER.error("Invalid config {}: '{}'", configKey, configValue, e);
      }
    }
    return MinionConstants.DEFAULT_MINION_MAX_NUM_OF_SUBTASKS_LIMIT;
  }

  /**
   * Updates the max num tasks parameter with the actual maximum number of subtasks based on
   * 1. configured value for the table subtask
   * 2. default value for the task type
   * 3. if the task is triggered manually or adhoc
   * 4. any cluster default(s)
   * This is required to provide user visibility to the maximum number of subtasks that were used for the task
   * @param defaultNumSubTasks - the default number of subtasks for the task type
   */
  public int getAndUpdateMaxNumSubTasks(Map<String, String> taskConfigs, int defaultNumSubTasks, String tableName) {
    int maxNumSubTasks = getNumSubTasks(taskConfigs, defaultNumSubTasks, tableName);
    taskConfigs.put(MinionConstants.TABLE_MAX_NUM_TASKS_KEY, String.valueOf(maxNumSubTasks));
    return maxNumSubTasks;
  }

  /**
   * Gets the final maximum number of subtasks for the given table based on the
   * 1. configured value for the table subtask
   * 2. default value for the task type
   * 3. if the task is triggered manually or adhoc
   * 4. any cluster default(s)
   * @param defaultNumSubTasks - the default number of subtasks for the task type
   */
  public int getNumSubTasks(Map<String, String> taskConfigs, int defaultNumSubTasks, String tableName) {
    int tableMaxNumTasks = defaultNumSubTasks;
    String tableMaxNumTasksConfig = taskConfigs.get(MinionConstants.TABLE_MAX_NUM_TASKS_KEY);
    if (tableMaxNumTasksConfig != null) {
      try {
        tableMaxNumTasks = Integer.parseInt(tableMaxNumTasksConfig);
      } catch (Exception e) {
        LOGGER.warn("MaxNumTasks {} have been wrongly set for table : {}, and task {}",
            tableMaxNumTasksConfig, tableName, getTaskType());
      }
    }

    // For manual / adhoc triggers, the user expects the task to run upto configured (or task's default) value
    // Based on system constraints, we may not be able to run the task with that many subtasks
    // To identify such cases, we return the configured / default value in this method
    // And the API that calls this method can fail the task generation if the generated tasks exceed the limit
    if (taskConfigs.containsKey(MinionConstants.TRIGGERED_BY)) {
      String triggeredBy = taskConfigs.get(MinionConstants.TRIGGERED_BY);
      if (TaskSchedulingContext.isUserTriggeredTask(triggeredBy)) {
        if (tableMaxNumTasks <= 0) {
          // A negative value for maxNumTasks is generally used to indicate that the intention
          // is to not have a limit on the number of subtasks.
          tableMaxNumTasks = Integer.MAX_VALUE;
        }
        return tableMaxNumTasks;
      }
    }

    // A negative value for maxNumTasks is generally used to indicate that the intention
    // is to not have a limit on the number of subtasks.
    // Thus, rather than throwing an error, or setting it to the default value,
    // we set it to the max allowed subtasks.
    if (tableMaxNumTasks > getMaxAllowedSubTasksPerTask() || tableMaxNumTasks <= 0) {
      LOGGER.warn(
          "MaxNumTasks for table {} for tasktype {} is {} which is greater than the max allowed subtasks {}"
              + ". Setting it to the max allowed subtasks",
          tableName, getTaskType(), tableMaxNumTasks, getMaxAllowedSubTasksPerTask());
      tableMaxNumTasks = getMaxAllowedSubTasksPerTask();
    }
    return tableMaxNumTasks;
  }

  @Override
  public int getMaxAttemptsPerTask() {
    String taskType = getTaskType();
    String configKey = taskType + MinionConstants.MAX_ATTEMPTS_PER_TASK_KEY_SUFFIX;
    String configValue = _clusterInfoAccessor.getClusterConfig(configKey);
    if (configValue != null) {
      try {
        return Integer.parseInt(configValue);
      } catch (Exception e) {
        LOGGER.error("Invalid config {}: '{}'", configKey, configValue, e);
      }
    }
    return MinionConstants.DEFAULT_MAX_ATTEMPTS_PER_TASK;
  }

  /**
   * Returns the list of segment zk metadata for available segments in the table. The list does NOT filter out inactive
   * segments based on the lineage. In order to compute the valid segments, we look at both idealstate and segment
   * zk metadata in the property store and compute the intersection. In this way, we can avoid picking the dangling
   * segments.
   *
   * @param tableNameWithType
   * @return the list of segment zk metadata for available segments in the table.
   */
  public List<SegmentZKMetadata> getSegmentsZKMetadataForTable(String tableNameWithType) {
    IdealState idealState = _clusterInfoAccessor.getIdealState(tableNameWithType);
    Set<String> segmentsForTable = idealState.getPartitionSet();
    List<SegmentZKMetadata> segmentZKMetadataList = _clusterInfoAccessor.getSegmentsZKMetadata(tableNameWithType);
    List<SegmentZKMetadata> selectedSegmentZKMetadataList = new ArrayList<>();
    for (SegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      if (segmentsForTable.contains(segmentZKMetadata.getSegmentName())) {
        selectedSegmentZKMetadataList.add(segmentZKMetadata);
      }
    }
    return selectedSegmentZKMetadataList;
  }

  public List<SegmentZKMetadata> getNonConsumingSegmentsZKMetadataForRealtimeTable(String tableNameWithType) {
    IdealState idealState = _clusterInfoAccessor.getIdealState(tableNameWithType);
    Set<String> idealStateSegments = idealState.getPartitionSet();
    List<SegmentZKMetadata> segmentZKMetadataList = _clusterInfoAccessor.getSegmentsZKMetadata(tableNameWithType);
    List<SegmentZKMetadata> selectedSegmentZKMetadataList = new ArrayList<>();
    for (SegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      String segmentName = segmentZKMetadata.getSegmentName();
      if (idealStateSegments.contains(segmentName)
          && segmentZKMetadata.getStatus().isCompleted() // skip consuming segments
          && !idealState.getInstanceStateMap(segmentName).containsValue(SegmentStateModel.CONSUMING)) {
        // The last check is for an edge case where
        //   1. SegmentZKMetadata was updated to DONE in segment commit protocol, but
        //   2. IdealState for the segment was not updated to ONLINE due to some issue in the controller.
        // We avoid picking up such segments to allow RealtimeSegmentValidationManager to fix them.
        selectedSegmentZKMetadataList.add(segmentZKMetadata);
      }
    }
    return selectedSegmentZKMetadataList;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(TableConfig tableConfig, Map<String, String> taskConfigs)
      throws Exception {
    throw new UnknownTaskTypeException("Adhoc task generation is not supported for task type - " + this.getTaskType());
  }

  @Override
  public void generateTasks(List<TableConfig> tableConfigs, List<PinotTaskConfig> pinotTaskConfigs)
      throws Exception {
    pinotTaskConfigs.addAll(generateTasks(tableConfigs));
  }

  @Override
  public String getMinionInstanceTag(TableConfig tableConfig) {
    return TaskGeneratorUtils.extractMinionInstanceTag(tableConfig, getTaskType());
  }

  public Map<String, String> getBaseTaskConfigs(TableConfig tableConfig, List<String> segmentNames) {
    Map<String, String> baseConfigs = new HashMap<>();
    baseConfigs.put(MinionConstants.TABLE_NAME_KEY, tableConfig.getTableName());
    baseConfigs.put(MinionConstants.SEGMENT_NAME_KEY, StringUtils.join(segmentNames,
          MinionConstants.SEGMENT_NAME_SEPARATOR));
    return baseConfigs;
  }
}
