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

import java.util.List;
import java.util.Set;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager.TaskCount;
import org.apache.pinot.core.periodictask.BasePeriodicTask;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class emits task metrics for each type of minion task that is set up in
 * a Pinot cluster. It is intended to be scheduled with a fairly high frequency,
 * of the order of minutes.
 * See ControllerConf class for the default value.
 */
public class TaskMetricsEmitter extends BasePeriodicTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskMetricsEmitter.class);
  private final static String TASK_NAME = "TaskMetricsEmitter";

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final PinotHelixTaskResourceManager _helixTaskResourceManager;
  private final ControllerMetrics _controllerMetrics;
  private final LeadControllerManager _leadControllerManager;

  public TaskMetricsEmitter(PinotHelixResourceManager pinotHelixResourceManager,
      PinotHelixTaskResourceManager helixTaskResourceManager, LeadControllerManager leadControllerManager,
      ControllerConf controllerConf, ControllerMetrics controllerMetrics) {
    super(TASK_NAME, controllerConf.getTaskMetricsEmitterFrequencyInSeconds(),
        controllerConf.getPeriodicTaskInitialDelayInSeconds());
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _helixTaskResourceManager = helixTaskResourceManager;
    _controllerMetrics = controllerMetrics;
    _leadControllerManager = leadControllerManager;
  }

  /**
   * @param filter Currently not used, but can be used to specify how this task should be run.
   */
  @Override
  protected final void runTask(String filter) {
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
      try {
        Set<String> tasksInProgress = _helixTaskResourceManager.getTasksInProgress(taskType);
        final int numRunningTasks = tasksInProgress.size();
        for (String task : tasksInProgress) {
          TaskCount taskCount = _helixTaskResourceManager.getTaskCount(task);
          accumulated.accumulate(taskCount);
        }
        // Emit metrics for taskType.
        _controllerMetrics
            .setValueOfGlobalGauge(ControllerGauge.NUM_MINION_TASKS_IN_PROGRESS, taskType, numRunningTasks);
        _controllerMetrics
            .setValueOfGlobalGauge(ControllerGauge.NUM_MINION_SUBTASKS_RUNNING, taskType, accumulated.getRunning());
        _controllerMetrics
            .setValueOfGlobalGauge(ControllerGauge.NUM_MINION_SUBTASKS_WAITING, taskType, accumulated.getWaiting());
        _controllerMetrics
            .setValueOfGlobalGauge(ControllerGauge.NUM_MINION_SUBTASKS_ERROR, taskType, accumulated.getError());
        int total = accumulated.getTotal();
        int percent = total != 0 ? (accumulated.getWaiting() + accumulated.getRunning()) * 100 / total : 0;
        _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.PERCENT_MINION_SUBTASKS_IN_QUEUE, taskType, percent);
        percent = total != 0 ? accumulated.getError() * 100 / total : 0;
        _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.PERCENT_MINION_SUBTASKS_IN_ERROR, taskType, percent);
      } catch (Exception e) {
        LOGGER.error("Caught exception while getting metrics for task type {}", taskType, e);
      }
    }

    // Emit metric to count the number of online minion instances.
    List<String> onlineInstances = _pinotHelixResourceManager.getOnlineInstanceList();
    int onlineMinionInstanceCount = 0;
    for (String onlineInstance : onlineInstances) {
      if (onlineInstance.startsWith(CommonConstants.Helix.PREFIX_OF_MINION_INSTANCE)) {
        onlineMinionInstanceCount++;
      }
    }
    _controllerMetrics.addValueToGlobalGauge(ControllerGauge.ONLINE_MINION_INSTANCES, onlineMinionInstanceCount);
  }
}
