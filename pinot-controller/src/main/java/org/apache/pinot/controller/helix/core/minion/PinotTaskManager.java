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
package org.apache.pinot.controller.helix.core.minion;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorRegistry;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>PinotTaskManager</code> is the component inside Pinot Controller to periodically check the Pinot
 * cluster status and schedule new tasks.
 * <p><code>PinotTaskManager</code> is also responsible for checking the health status on each type of tasks, detect and
 * fix issues accordingly.
 */
public class PinotTaskManager extends ControllerPeriodicTask<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTaskManager.class);

  private final PinotHelixTaskResourceManager _helixTaskResourceManager;
  private final ClusterInfoAccessor _clusterInfoAccessor;
  private final TaskGeneratorRegistry _taskGeneratorRegistry;

  public PinotTaskManager(PinotHelixTaskResourceManager helixTaskResourceManager,
      PinotHelixResourceManager helixResourceManager, LeadControllerManager leadControllerManager,
      ControllerConf controllerConf, ControllerMetrics controllerMetrics) {
    super("PinotTaskManager", controllerConf.getTaskManagerFrequencyInSeconds(),
        controllerConf.getPinotTaskManagerInitialDelaySeconds(), helixResourceManager, leadControllerManager,
        controllerMetrics);
    _helixTaskResourceManager = helixTaskResourceManager;
    _clusterInfoAccessor = new ClusterInfoAccessor(helixResourceManager, helixTaskResourceManager, controllerConf);
    _taskGeneratorRegistry = new TaskGeneratorRegistry(_clusterInfoAccessor);
  }

  /**
   * Returns the cluster info accessor.
   * <p>Cluster info accessor can be used to initialize the task generator.
   */
  public ClusterInfoAccessor getClusterInfoAccessor() {
    return _clusterInfoAccessor;
  }

  /**
   * Registers a task generator.
   * <p>This method can be used to plug in custom task generators.
   */
  public void registerTaskGenerator(PinotTaskGenerator taskGenerator) {
    _taskGeneratorRegistry.registerTaskGenerator(taskGenerator);
  }

  /**
   * Public API to schedule tasks (all task types) for all tables. It might be called from the non-leader controller.
   * Returns a map from the task type to the task scheduled.
   */
  public synchronized Map<String, String> scheduleTasks() {
    return scheduleTasks(_pinotHelixResourceManager.getAllTables(), false);
  }

  /**
   * Helper method to schedule tasks (all task types) for the given tables that have the tasks enabled. Returns a map
   * from the task type to the task scheduled.
   */
  private synchronized Map<String, String> scheduleTasks(List<String> tableNamesWithType, boolean isLeader) {
    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.NUMBER_TIMES_SCHEDULE_TASKS_CALLED, 1L);

    // Scan all table configs to get the tables with tasks enabled
    Map<String, List<TableConfig>> enabledTableConfigMap = new HashMap<>();
    for (String tableNameWithType : tableNamesWithType) {
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
      if (tableConfig != null && tableConfig.getTaskConfig() != null) {
        Set<String> enabledTaskTypes = tableConfig.getTaskConfig().getTaskTypeConfigsMap().keySet();
        for (String enabledTaskType : enabledTaskTypes) {
          enabledTableConfigMap.computeIfAbsent(enabledTaskType, k -> new ArrayList<>()).add(tableConfig);
        }
      }
    }

    // Generate each type of tasks
    Map<String, String> tasksScheduled = new HashMap<>();
    for (Map.Entry<String, List<TableConfig>> entry : enabledTableConfigMap.entrySet()) {
      String taskType = entry.getKey();
      List<TableConfig> enabledTableConfigs = entry.getValue();
      PinotTaskGenerator taskGenerator = _taskGeneratorRegistry.getTaskGenerator(taskType);
      if (taskGenerator != null) {
        _helixTaskResourceManager.ensureTaskQueueExists(taskType);
        tasksScheduled.put(taskType, scheduleTask(taskGenerator, enabledTableConfigs, isLeader));
      } else {
        List<String> enabledTables = new ArrayList<>(enabledTableConfigs.size());
        for (TableConfig enabledTableConfig : enabledTableConfigs) {
          enabledTables.add(enabledTableConfig.getTableName());
        }
        LOGGER.warn("Task type: {} is not registered, cannot enable it for tables: {}", taskType, enabledTables);
        tasksScheduled.put(taskType, null);
      }
    }

    return tasksScheduled;
  }

  /**
   * Helper method to schedule task with the given task generator for the given tables that have the task enabled.
   * Returns the task name, or {@code null} if no task is scheduled.
   */
  @Nullable
  private String scheduleTask(PinotTaskGenerator taskGenerator, List<TableConfig> enabledTableConfigs,
      boolean isLeader) {
    List<PinotTaskConfig> pinotTaskConfigs = taskGenerator.generateTasks(enabledTableConfigs);
    if (!isLeader) {
      taskGenerator.nonLeaderCleanUp();
    }
    int numTasks = pinotTaskConfigs.size();
    if (numTasks > 0) {
      String taskType = taskGenerator.getTaskType();
      LOGGER.info("Submitting {} tasks for task type: {} with task configs: {}", numTasks, taskType, pinotTaskConfigs);
      _controllerMetrics.addMeteredTableValue(taskType, ControllerMeter.NUMBER_TASKS_SUBMITTED, numTasks);
      return _helixTaskResourceManager.submitTask(pinotTaskConfigs, taskGenerator.getTaskTimeoutMs(),
          taskGenerator.getNumConcurrentTasksPerInstance());
    } else {
      return null;
    }
  }

  /**
   * Public API to schedule tasks (all task types) for the given table. It might be called from the non-leader
   * controller. Returns a map from the task type to the task scheduled.
   */
  @Nullable
  public synchronized Map<String, String> scheduleTasks(String tableNameWithType) {
    return scheduleTasks(Collections.singletonList(tableNameWithType), false);
  }

  /**
   * Public API to schedule task for the given task type. It might be called from the non-leader controller. Returns the
   * task name, or {@code null} if no task is scheduled.
   */
  @Nullable
  public synchronized String scheduleTask(String taskType) {
    PinotTaskGenerator taskGenerator = _taskGeneratorRegistry.getTaskGenerator(taskType);
    Preconditions.checkState(taskGenerator != null, "Task type: %s is not registered", taskType);

    // Scan all table configs to get the tables with task enabled
    List<TableConfig> enabledTableConfigs = new ArrayList<>();
    for (String tableNameWithType : _pinotHelixResourceManager.getAllTables()) {
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
      if (tableConfig != null && tableConfig.getTaskConfig() != null && tableConfig.getTaskConfig()
          .isTaskTypeEnabled(taskType)) {
        enabledTableConfigs.add(tableConfig);
      }
    }

    _helixTaskResourceManager.ensureTaskQueueExists(taskType);
    return scheduleTask(taskGenerator, enabledTableConfigs, false);
  }

  /**
   * Public API to schedule task for the given task type on the given table. It might be called from the non-leader
   * controller. Returns the task name, or {@code null} if no task is scheduled.
   */
  @Nullable
  public synchronized String scheduleTask(String taskType, String tableNameWithType) {
    PinotTaskGenerator taskGenerator = _taskGeneratorRegistry.getTaskGenerator(taskType);
    Preconditions.checkState(taskGenerator != null, "Task type: %s is not registered", taskType);

    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s", tableNameWithType);

    Preconditions
        .checkState(tableConfig.getTaskConfig() != null && tableConfig.getTaskConfig().isTaskTypeEnabled(taskType),
            "Table: %s does not have task type: %s enabled", tableNameWithType, taskType);

    _helixTaskResourceManager.ensureTaskQueueExists(taskType);
    return scheduleTask(taskGenerator, Collections.singletonList(tableConfig), false);
  }

  @Override
  protected void processTables(List<String> tableNamesWithType) {
    scheduleTasks(tableNamesWithType, true);
  }

  @Override
  public void cleanUpTask() {
    LOGGER.info("Cleaning up all task generators");
    for (String taskType : _taskGeneratorRegistry.getAllTaskTypes()) {
      _taskGeneratorRegistry.getTaskGenerator(taskType).nonLeaderCleanUp();
    }
  }
}
