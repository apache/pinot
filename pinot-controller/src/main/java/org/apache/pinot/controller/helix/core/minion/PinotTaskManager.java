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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.pinot.spi.config.table.TableTaskConfig;
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
   * Returns the cluster info provider.
   * <p>
   * Cluster info provider might be useful when initializing task generators.
   *
   * @return Cluster info provider
   */
  public ClusterInfoAccessor getClusterInfoAccessor() {
    return _clusterInfoAccessor;
  }

  /**
   * Registers a task generator.
   * <p>
   * This method can be used to plug in custom task generators.
   *
   * @param pinotTaskGenerator Task generator to be registered
   */
  public void registerTaskGenerator(PinotTaskGenerator pinotTaskGenerator) {
    _taskGeneratorRegistry.registerTaskGenerator(pinotTaskGenerator);
  }

  /**
   * Public API to schedule tasks. It doesn't matter whether current pinot controller is leader.
   */
  public synchronized Map<String, String> scheduleTasks() {
    Map<String, String> tasksScheduled = scheduleTasks(_pinotHelixResourceManager.getAllTables());

    // Reset the task because this method will be called from the Rest API instead of the periodic task scheduler
    // TODO: Clean up only the non-leader tables instead of all tables
    cleanUpTask();
    setUpTask();

    return tasksScheduled;
  }

  /**
   * Check the Pinot cluster status and schedule new tasks for the given tables.
   *
   * @param tableNamesWithType List of table names with type suffix
   * @return Map from task type to task scheduled
   */
  private synchronized Map<String, String> scheduleTasks(List<String> tableNamesWithType) {
    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.NUMBER_TIMES_SCHEDULE_TASKS_CALLED, 1L);

    Set<String> taskTypes = _taskGeneratorRegistry.getAllTaskTypes();
    int numTaskTypes = taskTypes.size();
    Map<String, List<TableConfig>> enabledTableConfigMap = new HashMap<>(numTaskTypes);

    for (String taskType : taskTypes) {
      enabledTableConfigMap.put(taskType, new ArrayList<>());

      // Ensure all task queues exist
      _helixTaskResourceManager.ensureTaskQueueExists(taskType);
    }

    // Scan all table configs to get the tables with tasks enabled
    for (String tableNameWithType : tableNamesWithType) {
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
      if (tableConfig != null) {
        TableTaskConfig taskConfig = tableConfig.getTaskConfig();
        if (taskConfig != null) {
          for (String taskType : taskTypes) {
            if (taskConfig.isTaskTypeEnabled(taskType)) {
              enabledTableConfigMap.get(taskType).add(tableConfig);
            }
          }
        }
      }
    }

    // Generate each type of tasks
    Map<String, String> tasksScheduled = new HashMap<>(numTaskTypes);
    for (String taskType : taskTypes) {
      LOGGER.info("Generating tasks for task type: {}", taskType);
      PinotTaskGenerator pinotTaskGenerator = _taskGeneratorRegistry.getTaskGenerator(taskType);
      List<PinotTaskConfig> pinotTaskConfigs = pinotTaskGenerator.generateTasks(enabledTableConfigMap.get(taskType));
      int numTasks = pinotTaskConfigs.size();
      if (numTasks > 0) {
        LOGGER
            .info("Submitting {} tasks for task type: {} with task configs: {}", numTasks, taskType, pinotTaskConfigs);
        tasksScheduled.put(taskType, _helixTaskResourceManager
            .submitTask(pinotTaskConfigs, pinotTaskGenerator.getTaskTimeoutMs(),
                pinotTaskGenerator.getNumConcurrentTasksPerInstance()));
        _controllerMetrics.addMeteredTableValue(taskType, ControllerMeter.NUMBER_TASKS_SUBMITTED, numTasks);
      }
    }

    return tasksScheduled;
  }

  @Override
  protected void processTables(List<String> tableNamesWithType) {
    scheduleTasks(tableNamesWithType);
  }

  @Override
  public void cleanUpTask() {
    LOGGER.info("Cleaning up all task generators");
    for (String taskType : _taskGeneratorRegistry.getAllTaskTypes()) {
      _taskGeneratorRegistry.getTaskGenerator(taskType).nonLeaderCleanUp();
    }
  }
}
