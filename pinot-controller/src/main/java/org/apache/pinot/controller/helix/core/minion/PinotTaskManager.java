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

import org.apache.pinot.common.config.PinotTaskConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableTaskConfig;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorRegistry;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorRegistry;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
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

  private Map<String, List<TableConfig>> _enabledTableConfigMap;
  private Set<String> _taskTypes;
  private int _numTaskTypes;
  private Map<String, String> _tasksScheduled;

  public PinotTaskManager(@Nonnull PinotHelixTaskResourceManager helixTaskResourceManager,
      @Nonnull PinotHelixResourceManager helixResourceManager, @Nonnull ControllerConf controllerConf,
      @Nonnull ControllerMetrics controllerMetrics) {
    super("PinotTaskManager", controllerConf.getTaskManagerFrequencyInSeconds(), helixResourceManager);
    _helixTaskResourceManager = helixTaskResourceManager;
    _clusterInfoProvider = new ClusterInfoProvider(helixResourceManager, helixTaskResourceManager, controllerConf);
    _taskGeneratorRegistry = new TaskGeneratorRegistry(_clusterInfoProvider);
    _controllerMetrics = controllerMetrics;
  }

  @Override
  protected void initTask() {

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
   * Public API to schedule tasks. It doesn't matter whether current pinot controller is leader.
   */
  public Map<String, String> scheduleTasks() {
    process(_pinotHelixResourceManager.getAllTables());
    return getTasksScheduled();
  }

  @Override
  protected void preprocess() {
    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.NUMBER_TIMES_SCHEDULE_TASKS_CALLED, 1L);

    _taskTypes = _taskGeneratorRegistry.getAllTaskTypes();
    _numTaskTypes = _taskTypes.size();
    _enabledTableConfigMap = new HashMap<>(_numTaskTypes);

    for (String taskType : _taskTypes) {
      _enabledTableConfigMap.put(taskType, new ArrayList<>());

      // Ensure all task queues exist
      _helixTaskResourceManager.ensureTaskQueueExists(taskType);
    }
  }

  @Override
  protected void processTable(String tableNameWithType) {
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    if (tableConfig != null) {
      TableTaskConfig taskConfig = tableConfig.getTaskConfig();
      if (taskConfig != null) {
        for (String taskType : _taskTypes) {
          if (taskConfig.isTaskTypeEnabled(taskType)) {
            _enabledTableConfigMap.get(taskType).add(tableConfig);
          }
        }
      }
    }
  }

  @Override
  protected void postprocess() {
    // Generate each type of tasks
    _tasksScheduled = new HashMap<>(_numTaskTypes);
    for (String taskType : _taskTypes) {
      LOGGER.info("Generating tasks for task type: {}", taskType);
      PinotTaskGenerator pinotTaskGenerator = _taskGeneratorRegistry.getTaskGenerator(taskType);
      List<PinotTaskConfig> pinotTaskConfigs = pinotTaskGenerator.generateTasks(_enabledTableConfigMap.get(taskType));
      int numTasks = pinotTaskConfigs.size();
      if (numTasks > 0) {
        LOGGER.info("Submitting {} tasks for task type: {} with task configs: {}", numTasks, taskType,
            pinotTaskConfigs);
        _tasksScheduled.put(taskType, _helixTaskResourceManager.submitTask(pinotTaskConfigs,
            pinotTaskGenerator.getNumConcurrentTasksPerInstance()));
        _controllerMetrics.addMeteredTableValue(taskType, ControllerMeter.NUMBER_TASKS_SUBMITTED, numTasks);
      }
    }
  }

  /**
   * Returns the tasks that have been scheduled as part of the postprocess
   * @return
   */
  private Map<String, String> getTasksScheduled() {
    return _tasksScheduled;
  }

  /**
   * Performs necessary cleanups (e.g. remove metrics) when the controller leadership changes.
   */
  @Override
  public void stopTask() {
    LOGGER.info("Perform task cleanups.");
    // Performs necessary cleanups for each task type.
    for (String taskType : _taskGeneratorRegistry.getAllTaskTypes()) {
      _taskGeneratorRegistry.getTaskGenerator(taskType).nonLeaderCleanUp();
    }
  }
}
