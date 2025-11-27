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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.data.Segment;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TaskGeneratorUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskGeneratorUtils.class);

  private TaskGeneratorUtils() {
  }

  /**
   * If task is in final state, it will not be running any more. But note that
   * STOPPED is not a final task state in helix task framework, as a stopped task
   * is just paused and can be resumed to rerun.
   */
  private static final EnumSet<TaskState> TASK_FINAL_STATES =
      EnumSet.of(TaskState.COMPLETED, TaskState.FAILED, TaskState.ABORTED, TaskState.TIMED_OUT);

  /**
   * Returns all the segments that have been scheduled but not finished.
   */
  public static Set<Segment> getRunningSegments(String taskType, ClusterInfoAccessor clusterInfoAccessor) {
    Set<Segment> runningSegments = new HashSet<>();
    Map<String, TaskState> taskStates = clusterInfoAccessor.getTaskStates(taskType);
    for (Map.Entry<String, TaskState> entry : taskStates.entrySet()) {
      if (TASK_FINAL_STATES.contains(entry.getValue())) {
        continue;
      }
      String taskName = entry.getKey();
      for (PinotTaskConfig pinotTaskConfig : clusterInfoAccessor.getTaskConfigs(taskName)) {
        Map<String, String> configs = pinotTaskConfig.getConfigs();
        runningSegments.add(
            new Segment(configs.get(MinionConstants.TABLE_NAME_KEY), configs.get(MinionConstants.SEGMENT_NAME_KEY)));
      }
    }
    return runningSegments;
  }

  /**
   * Gets all the tasks for the provided task type and tableName, which have not reached final task state yet.
   */
  public static Map<String, TaskState> getIncompleteTasks(String taskType, String tableNameWithType,
      ClusterInfoAccessor clusterInfoAccessor) {
    Map<String, TaskState> incompleteTasks = new HashMap<>();
    Map<String, TaskState> taskStates = clusterInfoAccessor.getTaskStates(taskType);
    for (Map.Entry<String, TaskState> entry : taskStates.entrySet()) {
      if (TASK_FINAL_STATES.contains(entry.getValue())) {
        continue;
      }
      String taskName = entry.getKey();
      for (PinotTaskConfig pinotTaskConfig : clusterInfoAccessor.getTaskConfigs(taskName)) {
        if (tableNameWithType.equals(pinotTaskConfig.getConfigs().get(MinionConstants.TABLE_NAME_KEY))) {
          incompleteTasks.put(taskName, entry.getValue());
        }
      }
    }
    return incompleteTasks;
  }

  /**
   * Get all the tasks for the provided task type and tableName, which have not reached final task state yet.
   * In general, if the task is not in final state, we can treat it as running, although it may wait to start
   * or is paused. The caller provides a consumer to process the task configs of those tasks.
   */
  public static void forRunningTasks(String tableNameWithType, String taskType, ClusterInfoAccessor clusterInfoAccessor,
      Consumer<Map<String, String>> taskConfigConsumer) {
    Map<String, TaskState> taskStates = clusterInfoAccessor.getTaskStates(taskType);
    for (Map.Entry<String, TaskState> entry : taskStates.entrySet()) {
      if (TASK_FINAL_STATES.contains(entry.getValue())) {
        continue;
      }
      String taskName = entry.getKey();
      for (PinotTaskConfig pinotTaskConfig : clusterInfoAccessor.getTaskConfigs(taskName)) {
        Map<String, String> config = pinotTaskConfig.getConfigs();
        String tableNameFromTaskConfig = config.get(MinionConstants.TABLE_NAME_KEY);
        if (tableNameWithType.equals(tableNameFromTaskConfig)) {
          taskConfigConsumer.accept(config);
        }
      }
    }
  }

  /**
   * Extract minionInstanceTag from the task config map. Returns "minion_untagged" in case of no config found.
   */
  public static String extractMinionInstanceTag(TableConfig tableConfig, String taskType) {
    TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
    if (tableTaskConfig != null) {
      Map<String, String> configs = tableTaskConfig.getConfigsForTaskType(taskType);
      if (configs != null && !configs.isEmpty()) {
        return configs.getOrDefault(PinotTaskManager.MINION_INSTANCE_TAG_CONFIG,
            CommonConstants.Helix.UNTAGGED_MINION_INSTANCE);
      }
    }
    return CommonConstants.Helix.UNTAGGED_MINION_INSTANCE;
  }

  /**
   * Generic method to get configuration values from ZK cluster config with fallback hierarchy:
   * 1. Check minion tenant specific config (if minionTag provided)
   * 2. Check task type specific config
   * 3. Return default value
   *
   * @param taskType The task type to build config keys
   * @param configKeySuffix The suffix to append to task type for config key
   * @param minionTag The minion tag for tenant-specific config (can be null)
   * @param defaultValue The default value to return if no config is found
   * @param returnType The class type for parsing the config value. Currently supported: Integer, Long, Double, Float,
   *                   String
   * @param clusterInfoAccessor The cluster info accessor to get config values
   * @param <T> The return type
   * @return The parsed configuration value or default value
   */
  public static <T> T getClusterMinionConfigValue(String taskType, String configKeySuffix, String minionTag,
      T defaultValue, Class<T> returnType, ClusterInfoAccessor clusterInfoAccessor) {
    // Priority 1: Check minion tenant specific cluster config
    if (minionTag != null) {
      String tenantSpecificConfigKey = taskType + "." + minionTag + configKeySuffix;
      String configValue = clusterInfoAccessor.getClusterConfig(tenantSpecificConfigKey);
      if (configValue != null) {
        try {
          return parseConfigValue(configValue, returnType);
        } catch (Exception e) {
          LOGGER.error("Invalid config {}: '{}'", tenantSpecificConfigKey, configValue, e);
        }
      }
    }

    // Priority 2: Check task type specific cluster config
    String taskTypeConfigKey = taskType + configKeySuffix;
    String configValue = clusterInfoAccessor.getClusterConfig(taskTypeConfigKey);
    if (configValue != null) {
      try {
        return parseConfigValue(configValue, returnType);
      } catch (Exception e) {
        LOGGER.error("Invalid config {}: '{}'", taskTypeConfigKey, configValue, e);
      }
    }

    // Priority 3: Default value
    return defaultValue;
  }

  /**
   * Helper method to parse config values to the appropriate type
   */
  @SuppressWarnings("unchecked")
  private static <T> T parseConfigValue(String configValue, Class<T> returnType) {
    if (returnType == Integer.class || returnType == int.class) {
      return (T) Integer.valueOf(configValue);
    } else if (returnType == Long.class || returnType == long.class) {
      return (T) Long.valueOf(configValue);
    } else if (returnType == Double.class || returnType == double.class) {
      return (T) Double.valueOf(configValue);
    } else if (returnType == Float.class || returnType == float.class) {
      return (T) Float.valueOf(configValue);
    } else if (returnType == String.class) {
      return (T) configValue;
    } else {
      throw new IllegalArgumentException("Unsupported return type: " + returnType.getName());
    }
  }
}
