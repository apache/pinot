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
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import com.linkedin.pinot.controller.helix.core.minion.generator.TaskGeneratorRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.WorkflowConfig;
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

  private final TaskDriver _taskDriver;
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;
  private final ClusterInfoProvider _clusterInfoProvider;
  private final TaskGeneratorRegistry _taskGeneratorRegistry;

  private ScheduledExecutorService _executorService;

  public PinotTaskManager(@Nonnull TaskDriver taskDriver, @Nonnull PinotHelixResourceManager pinotHelixResourceManager,
      @Nonnull PinotHelixTaskResourceManager pinotHelixTaskResourceManager) {
    _taskDriver = taskDriver;
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _pinotHelixTaskResourceManager = pinotHelixTaskResourceManager;
    _clusterInfoProvider = new ClusterInfoProvider(pinotHelixResourceManager, pinotHelixTaskResourceManager);
    _taskGeneratorRegistry = new TaskGeneratorRegistry(_clusterInfoProvider);
  }

  /**
   * Get the cluster info provider.
   * <p>Cluster info provider might be needed to initialize task generators.
   *
   * @return Cluster info provider
   */
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
   * Ensure all registered task queues exist.
   * <p>Should be called after all task generators get registered.
   */
  public void ensureTaskQueuesExist() {
    Map<String, WorkflowConfig> helixWorkflows = _taskDriver.getWorkflows();
    for (String taskType : _taskGeneratorRegistry.getAllTaskTypes()) {
      if (!helixWorkflows.containsKey(PinotHelixTaskResourceManager.getHelixJobQueueName(taskType))) {
        _pinotHelixTaskResourceManager.createTaskQueue(taskType);
      }
    }
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
        execute();
      }
    }, Math.min(60, runFrequencyInSeconds), runFrequencyInSeconds, TimeUnit.SECONDS);
  }

  /**
   * Check the Pinot cluster status and schedule new tasks.
   */
  public void execute() {
    // Only schedule new tasks from leader controller
    if (!_pinotHelixResourceManager.isLeader()) {
      LOGGER.info("Skip scheduling new tasks on non-leader controller");
      return;
    }

    // TODO: add JobQueue health check here

    Map<String, List<AbstractTableConfig>> enabledTableConfigMap = new HashMap<>();
    for (String taskType : _taskGeneratorRegistry.getAllTaskTypes()) {
      enabledTableConfigMap.put(taskType, new ArrayList<AbstractTableConfig>());
    }

    // Scan all table configs to get the tables with tasks enabled
    for (String tableName : _pinotHelixResourceManager.getAllPinotTableNames()) {
      AbstractTableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableName);
      if (tableConfig != null) {
        // TODO: add table configs that have certain types of tasks enabled into the map
      }
    }

    // Generate each type of tasks
    for (String taskType : _taskGeneratorRegistry.getAllTaskTypes()) {
      LOGGER.info("Generating tasks for task type: {}", taskType);
      PinotTaskGenerator pinotTaskGenerator = _taskGeneratorRegistry.getTaskGenerator(taskType);
      Preconditions.checkNotNull(pinotTaskGenerator);
      List<PinotTaskConfig> pinotTaskConfigs = pinotTaskGenerator.generateTasks(enabledTableConfigMap.get(taskType));
      for (PinotTaskConfig pinotTaskConfig : pinotTaskConfigs) {
        LOGGER.info("Submitting task for task type: {} with task config: {}", taskType, pinotTaskConfig);
        _pinotHelixTaskResourceManager.submitTask(pinotTaskConfig);
      }
    }
  }

  /**
   * Stop the task scheduler.
   */
  public void stopScheduler() {
    _executorService.shutdown();
  }
}
