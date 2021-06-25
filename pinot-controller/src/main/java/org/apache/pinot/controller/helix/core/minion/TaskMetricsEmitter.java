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

import java.util.Set;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager.TaskCount;
import org.apache.pinot.core.periodictask.BasePeriodicTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TaskMetricsEmitter extends BasePeriodicTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskMetricsEmitter.class);
  private final static String TASK_NAME = "TaskMetricsEmitter";

  private final PinotHelixTaskResourceManager _helixTaskResourceManager;
  private final ControllerMetrics _controllerMetrics;
  private final LeadControllerManager _leadControllerManager;

  public TaskMetricsEmitter(PinotHelixTaskResourceManager helixTaskResourceManager,
      LeadControllerManager leadControllerManager, ControllerConf controllerConf, ControllerMetrics controllerMetrics) {
    super(TASK_NAME, controllerConf.getTaskMetricsEmitterFrequencyInSeconds(),
        controllerConf.getPeriodicTaskInitialDelayInSeconds());
    _helixTaskResourceManager = helixTaskResourceManager;
    _controllerMetrics = controllerMetrics;
    _leadControllerManager = leadControllerManager;
  }

  @Override
  protected final void runTask() {
    // Make it so that only one controller returns the metric for all the tasks.
    if (!_leadControllerManager.isLeaderForTable(TASK_NAME)) {
      return;
    }

    // The call to get task types can take time if there are a lot of tasks.
    // Potential optimization is to call it every (say) 30m if we detect a barrage of
    // zk requests.
    Set<String> taskTypes = _helixTaskResourceManager.getTaskTypes();
    for (String taskType : taskTypes) {
      TaskCount accumulated = new TaskCount();
      Set<String> tasksInProgress = _helixTaskResourceManager.getTasksInProgress(taskType);
      final int numRunningTasks = tasksInProgress.size();
      for (String task : tasksInProgress) {
        TaskCount taskCount = _helixTaskResourceManager.getTaskCount(task);
        accumulated.accumulate(taskCount);
      }
      // Emit metrics for taskType.
      _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.NUM_MINION_TASKS_IN_PROGRESS, taskType, numRunningTasks);
      _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.NUM_MINION_SUBTASKS_RUNNING, taskType, accumulated.getRunning());
      _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.NUM_MINION_SUBTASKS_WAITING, taskType, accumulated.getWaiting());
      _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.NUM_MINION_SUBTASKS_ERROR, taskType, accumulated.getError());
      int total = accumulated.getTotal();
      int percent = total != 0 ? (accumulated.getWaiting() + accumulated.getRunning()) * 100 / total : 0;
      _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.PERCENT_MINION_SUBTASKS_IN_QUEUE, taskType, percent);
      percent = total != 0 ? accumulated.getError() * 100 / total : 0;
      _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.PERCENT_MINION_SUBTASKS_IN_ERROR, taskType, percent);
    }
  }
}
