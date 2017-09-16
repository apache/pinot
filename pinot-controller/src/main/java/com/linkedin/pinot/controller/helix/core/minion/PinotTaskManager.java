/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableTaskConfig;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import com.linkedin.pinot.controller.helix.core.minion.generator.TaskGeneratorRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>PinotTaskManager</code> is the component inside Pinot Controller to periodically check the Pinot
 * cluster status and schedule new tasks.
 * <p><code>PinotTaskManager</code> is also responsible for checking the health status on each type of tasks, detect and
 * fix issues accordingly.
 */
public class PinotTaskManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTaskManager.class);

  private final PinotHelixResourceManager _helixResourceManager;
  private final PinotHelixTaskResourceManager _helixTaskResourceManager;
  private final ClusterInfoProvider _clusterInfoProvider;
  private final TaskGeneratorRegistry _taskGeneratorRegistry;
  private final ControllerMetrics _controllerMetrics;

  private ScheduledExecutorService _executorService;

  public PinotTaskManager(@Nonnull PinotHelixTaskResourceManager helixTaskResourceManager,
      @Nonnull PinotHelixResourceManager helixResourceManager, @Nonnull ControllerConf controllerConf,
      @Nonnull ControllerMetrics controllerMetrics) {
    _helixResourceManager = helixResourceManager;
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
   * Start the task scheduler with the given running frequency.
   *
   * @param runFrequencyInSeconds Scheduler running frequency in seconds
   */
  public void startScheduler(int runFrequencyInSeconds) {
    _executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(@Nonnull Runnable r) {
        Thread thread = new Thread(r);
        thread.setName("PinotTaskManagerExecutorService");
        return thread;
      }
    });
    _executorService.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        // Only schedule new tasks from leader controller
        if (!_helixResourceManager.isLeader()) {
          LOGGER.info("Skip scheduling new tasks on non-leader controller");
          return;
        }
        scheduleTasks();
      }
    }, Math.min(60, runFrequencyInSeconds), runFrequencyInSeconds, TimeUnit.SECONDS);
  }

  /**
   * Stop the task scheduler.
   */
  public void stopScheduler() {
    if (_executorService != null) {
      _executorService.shutdown();
    }
  }

  /**
   * Check the Pinot cluster status and schedule new tasks.
   *
   * @return Map from task type to task scheduled
   */
  @Nonnull
  public Map<String, String> scheduleTasks() {
    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.NUMBER_TIMES_SCHEDULE_TASKS_CALLED, 1L);

    Set<String> taskTypes = _taskGeneratorRegistry.getAllTaskTypes();
    Map<String, List<TableConfig>> enabledTableConfigMap = new HashMap<>();

    for (String taskType : taskTypes) {
      // Ensure all task queues exist and clean up all finished tasks
      _helixTaskResourceManager.ensureTaskQueueExists(taskType);
      _helixTaskResourceManager.cleanUpTaskQueue(taskType);

      enabledTableConfigMap.put(taskType, new ArrayList<TableConfig>());
    }

    // Scan all table configs to get the tables with tasks enabled
    for (String tableName : _helixResourceManager.getAllTables()) {
      TableConfig tableConfig = _helixResourceManager.getTableConfig(tableName);
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
    Map<String, String> tasksScheduled = new HashMap<>(taskTypes.size());
    // TODO: add config to control the max number of tasks for all task types & each task type
    for (String taskType : taskTypes) {
      LOGGER.info("Generating tasks for task type: {}", taskType);
      PinotTaskGenerator pinotTaskGenerator = _taskGeneratorRegistry.getTaskGenerator(taskType);
      Preconditions.checkNotNull(pinotTaskGenerator);
      List<PinotTaskConfig> pinotTaskConfigs = pinotTaskGenerator.generateTasks(enabledTableConfigMap.get(taskType));
      int numTasks = pinotTaskConfigs.size();
      if (numTasks > 0) {
        LOGGER.info("Submitting {} tasks for task type: {} with task configs: {}", numTasks, taskType,
            pinotTaskConfigs);
        tasksScheduled.put(taskType, _helixTaskResourceManager.submitTask(pinotTaskConfigs));
        _controllerMetrics.addMeteredTableValue(taskType, ControllerMeter.NUMBER_TASKS_SUBMITTED, numTasks);
      }
    }

    return tasksScheduled;
  }
}
