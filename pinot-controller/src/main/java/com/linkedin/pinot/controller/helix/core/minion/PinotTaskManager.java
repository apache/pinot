/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix.core.minion;

import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableTaskConfig;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import com.linkedin.pinot.controller.helix.core.minion.generator.TaskGeneratorRegistry;
import com.linkedin.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>PinotTaskManager</code> is the component inside Pinot Controller to periodically check the Pinot
 * cluster status and schedule new tasks.
 * <p><code>PinotTaskManager</code> is also responsible for checking the health status on each type of tasks, detect and
 * fix issues accordingly.
 */
public class PinotTaskManager extends ControllerPeriodicTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTaskManager.class);

  private final PinotHelixTaskResourceManager _helixTaskResourceManager;
  private final ClusterInfoProvider _clusterInfoProvider;
  private final TaskGeneratorRegistry _taskGeneratorRegistry;
  private final ControllerMetrics _controllerMetrics;

  public PinotTaskManager(@Nonnull PinotHelixTaskResourceManager helixTaskResourceManager,
      @Nonnull PinotHelixResourceManager helixResourceManager, @Nonnull ControllerConf controllerConf,
      @Nonnull ControllerMetrics controllerMetrics) {
    super("PinotTaskManager", controllerConf.getTaskManagerFrequencyInSeconds(),
        Math.min(60, controllerConf.getTaskManagerFrequencyInSeconds()), helixResourceManager);
    _helixTaskResourceManager = helixTaskResourceManager;
    _clusterInfoProvider = new ClusterInfoProvider(helixResourceManager, helixTaskResourceManager, controllerConf);
    _taskGeneratorRegistry = new TaskGeneratorRegistry(_clusterInfoProvider);
    _controllerMetrics = controllerMetrics;
  }

  /**
   * Get the cluster info provider.
   * <p>Cluster info provider might be needed to initialize task generators.
   *
   * @return Cluster info provider
   */
  @Nonnull
  public ClusterInfoProvider getClusterInfoProvider() {
    return _clusterInfoProvider;
  }

  /**
   * Register a task generator.
   * <p>This is for pluggable task generators.
   *
   * @param pinotTaskGenerator Task generator to be registered
   */
  public void registerTaskGenerator(@Nonnull PinotTaskGenerator pinotTaskGenerator) {
    _taskGeneratorRegistry.registerTaskGenerator(pinotTaskGenerator);
  }

  /**
   * Check the Pinot cluster status and schedule new tasks.
   * @param allTableNames List of all the table names
   * @return Map from task type to task scheduled
   */
  @Nonnull
  private Map<String, String> scheduleTasks(List<String> allTableNames) {
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
    for (String tableName : allTableNames) {
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableName);
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
        LOGGER.info("Submitting {} tasks for task type: {} with task configs: {}", numTasks, taskType,
            pinotTaskConfigs);
        tasksScheduled.put(taskType, _helixTaskResourceManager.submitTask(pinotTaskConfigs,
            pinotTaskGenerator.getNumConcurrentTasksPerInstance()));
        _controllerMetrics.addMeteredTableValue(taskType, ControllerMeter.NUMBER_TASKS_SUBMITTED, numTasks);
      }
    }

    return tasksScheduled;
  }

  /**
   * Public API to schedule tasks. It doesn't matter whether current pinot controller is leader.
   */
  public Map<String, String> scheduleTasks() {
    return scheduleTasks(_pinotHelixResourceManager.getAllTables());
  }

  /**
   * Performs necessary cleanups (e.g. remove metrics) when the controller leadership changes.
   */
  @Override
  public void onBecomeNotLeader() {
    LOGGER.info("Perform task cleanups.");
    // Performs necessary cleanups for each task type.
    for (String taskType : _taskGeneratorRegistry.getAllTaskTypes()) {
      _taskGeneratorRegistry.getTaskGenerator(taskType).nonLeaderCleanUp();
    }
  }

  @Override
  public void process(List<String> allTableNames) {
    scheduleTasks(allTableNames);
  }
}
