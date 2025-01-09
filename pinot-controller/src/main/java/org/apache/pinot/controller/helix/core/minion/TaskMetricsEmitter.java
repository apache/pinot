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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager.TaskCount;
import org.apache.pinot.core.periodictask.BasePeriodicTask;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
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

  private final Set<String> _preReportedTaskTypes;
  private final Map<String, Set<String>> _preReportedTables;

  public TaskMetricsEmitter(PinotHelixResourceManager pinotHelixResourceManager,
      PinotHelixTaskResourceManager helixTaskResourceManager, LeadControllerManager leadControllerManager,
      ControllerConf controllerConf, ControllerMetrics controllerMetrics) {
    super(TASK_NAME, controllerConf.getTaskMetricsEmitterFrequencyInSeconds(),
        controllerConf.getPeriodicTaskInitialDelayInSeconds());
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _helixTaskResourceManager = helixTaskResourceManager;
    _controllerMetrics = controllerMetrics;
    _leadControllerManager = leadControllerManager;
    _preReportedTaskTypes = new HashSet<>();
    _preReportedTables = new HashMap<>();
  }

  @Override
  protected final void runTask(Properties periodicTaskProperties) {
    // Make it so that only one controller returns the metric for all the tasks.
    if (!_leadControllerManager.isLeaderForTable(TASK_NAME)) {
      return;
    }

    Map<String, Map<String, Long>> taskMetadataLastUpdateTime =
        _helixTaskResourceManager.getTaskMetadataLastUpdateTimeMs();
    taskMetadataLastUpdateTime.forEach((tableNameWithType, taskTypeLastUpdateTime) ->
        taskTypeLastUpdateTime.forEach((taskType, lastUpdateTimeMs) ->
            _controllerMetrics.setOrUpdateTableGauge(tableNameWithType, taskType,
                ControllerGauge.TIME_MS_SINCE_LAST_MINION_TASK_METADATA_UPDATE,
                () -> System.currentTimeMillis() - lastUpdateTimeMs)));

    // The call to get task types can take time if there are a lot of tasks.
    // Potential optimization is to call it every (say) 30m if we detect a barrage of
    // zk requests.
    Set<String> taskTypes = _helixTaskResourceManager.getTaskTypes();
    for (String taskType : taskTypes) {
      TaskCount taskTypeAccumulatedCount = new TaskCount();
      Map<String, TaskCount> tableAccumulatedCount = new HashMap<>();
      try {
        Set<String> tasksInProgress = _helixTaskResourceManager.getTasksInProgress(taskType);
        final int numRunningTasks = tasksInProgress.size();
        for (String task : tasksInProgress) {
          Map<String, TaskCount> tableTaskCount = _helixTaskResourceManager.getTableTaskCount(task);
          tableTaskCount.forEach((tableNameWithType, taskCount) -> {
            taskTypeAccumulatedCount.accumulate(taskCount);
            tableAccumulatedCount.compute(tableNameWithType, (name, count) -> {
              if (count == null) {
                count = new TaskCount();
              }
              count.accumulate(taskCount);
              return count;
            });
          });
        }
        // Emit metrics for taskType.
        _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.NUM_MINION_TASKS_IN_PROGRESS, taskType,
            numRunningTasks);
        _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.NUM_MINION_SUBTASKS_RUNNING, taskType,
            taskTypeAccumulatedCount.getRunning());
        _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.NUM_MINION_SUBTASKS_WAITING, taskType,
            taskTypeAccumulatedCount.getWaiting());
        _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.NUM_MINION_SUBTASKS_ERROR, taskType,
            taskTypeAccumulatedCount.getError());
        _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.NUM_MINION_SUBTASKS_UNKNOWN, taskType,
            taskTypeAccumulatedCount.getUnknown());
        _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.NUM_MINION_SUBTASKS_DROPPED, taskType,
            taskTypeAccumulatedCount.getDropped());
        _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.NUM_MINION_SUBTASKS_TIMED_OUT, taskType,
            taskTypeAccumulatedCount.getTimedOut());
        _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.NUM_MINION_SUBTASKS_ABORTED, taskType,
            taskTypeAccumulatedCount.getAborted());
        int total = taskTypeAccumulatedCount.getTotal();
        int percent = total != 0
            ? (taskTypeAccumulatedCount.getWaiting() + taskTypeAccumulatedCount.getRunning()) * 100 / total : 0;
        _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.PERCENT_MINION_SUBTASKS_IN_QUEUE, taskType, percent);
        percent = total != 0 ? taskTypeAccumulatedCount.getError() * 100 / total : 0;
        _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.PERCENT_MINION_SUBTASKS_IN_ERROR, taskType, percent);

        // Emit metrics for table taskType
        tableAccumulatedCount.forEach((tableNameWithType, taskCount) -> {
          _controllerMetrics.setOrUpdateTableGauge(tableNameWithType, taskType,
              ControllerGauge.NUM_MINION_SUBTASKS_RUNNING, () -> (long) taskCount.getRunning());
          _controllerMetrics.setOrUpdateTableGauge(tableNameWithType, taskType,
              ControllerGauge.NUM_MINION_SUBTASKS_WAITING, taskCount.getWaiting());
          _controllerMetrics.setOrUpdateTableGauge(tableNameWithType, taskType,
              ControllerGauge.NUM_MINION_SUBTASKS_ERROR, taskCount.getError());
          _controllerMetrics.setOrUpdateTableGauge(tableNameWithType, taskType,
              ControllerGauge.NUM_MINION_SUBTASKS_UNKNOWN, taskCount.getUnknown());
          _controllerMetrics.setOrUpdateTableGauge(tableNameWithType, taskType,
              ControllerGauge.NUM_MINION_SUBTASKS_DROPPED, taskCount.getDropped());
          _controllerMetrics.setOrUpdateTableGauge(tableNameWithType, taskType,
              ControllerGauge.NUM_MINION_SUBTASKS_TIMED_OUT, taskCount.getTimedOut());
          _controllerMetrics.setOrUpdateTableGauge(tableNameWithType, taskType,
              ControllerGauge.NUM_MINION_SUBTASKS_ABORTED, taskCount.getAborted());
          int tableTotal = taskCount.getTotal();
          int tablePercent = tableTotal != 0 ? (taskCount.getWaiting() + taskCount.getRunning()) * 100 / tableTotal : 0;
          _controllerMetrics.setOrUpdateTableGauge(tableNameWithType, taskType,
              ControllerGauge.PERCENT_MINION_SUBTASKS_IN_QUEUE, tablePercent);
          tablePercent = tableTotal != 0 ? taskCount.getError() * 100 / tableTotal : 0;
          _controllerMetrics.setOrUpdateTableGauge(tableNameWithType, taskType,
              ControllerGauge.PERCENT_MINION_SUBTASKS_IN_ERROR, tablePercent);
        });

        if (_preReportedTables.containsKey(taskType)) {
          Set<String> tableNameWithTypeSet = _preReportedTables.get(taskType);
          tableNameWithTypeSet.removeAll(tableAccumulatedCount.keySet());
          removeTableTaskTypeMetrics(tableNameWithTypeSet, taskType);
        }
        if (!tableAccumulatedCount.isEmpty()) {
          // need to make a copy of the set because we may want to chagne the set later
          Set<String> tableNameWithTypeSet = new HashSet<>(tableAccumulatedCount.keySet());
          _preReportedTables.put(taskType, tableNameWithTypeSet);
        } else {
          _preReportedTables.remove(taskType);
        }
      } catch (Exception e) {
        LOGGER.error("Caught exception while getting metrics for task type {}", taskType, e);
      }
    }

    // clean up metrics for task types that have already been removed
    _preReportedTaskTypes.removeAll(taskTypes);
    for (String taskType : _preReportedTaskTypes) {
      // remove task type level gauges
      _controllerMetrics.removeGlobalGauge(taskType, ControllerGauge.NUM_MINION_TASKS_IN_PROGRESS);
      _controllerMetrics.removeGlobalGauge(taskType, ControllerGauge.NUM_MINION_SUBTASKS_RUNNING);
      _controllerMetrics.removeGlobalGauge(taskType, ControllerGauge.NUM_MINION_SUBTASKS_WAITING);
      _controllerMetrics.removeGlobalGauge(taskType, ControllerGauge.NUM_MINION_SUBTASKS_ERROR);
      _controllerMetrics.removeGlobalGauge(taskType, ControllerGauge.NUM_MINION_SUBTASKS_UNKNOWN);
      _controllerMetrics.removeGlobalGauge(taskType, ControllerGauge.NUM_MINION_SUBTASKS_DROPPED);
      _controllerMetrics.removeGlobalGauge(taskType, ControllerGauge.NUM_MINION_SUBTASKS_TIMED_OUT);
      _controllerMetrics.removeGlobalGauge(taskType, ControllerGauge.NUM_MINION_SUBTASKS_ABORTED);
      _controllerMetrics.removeGlobalGauge(taskType, ControllerGauge.PERCENT_MINION_SUBTASKS_IN_QUEUE);
      _controllerMetrics.removeGlobalGauge(taskType, ControllerGauge.PERCENT_MINION_SUBTASKS_IN_ERROR);
      // remove table task type level gauges
      if (_preReportedTables.containsKey(taskType)) {
        removeTableTaskTypeMetrics(_preReportedTables.get(taskType), taskType);
        _preReportedTables.remove(taskType);
      }
    }

    // update previously reported task types
    _preReportedTaskTypes.clear();
    _preReportedTaskTypes.addAll(taskTypes);

    // Emit metric to count the number of online minion instances.
    List<String> onlineInstances = _pinotHelixResourceManager.getOnlineInstanceList();
    int onlineMinionInstanceCount = 0;
    for (String onlineInstance : onlineInstances) {
      if (InstanceTypeUtils.isMinion(onlineInstance)) {
        onlineMinionInstanceCount++;
      }
    }
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.ONLINE_MINION_INSTANCES, onlineMinionInstanceCount);
  }

  private void removeTableTaskTypeMetrics(Set<String> tableNameWithTypeSet, String taskType) {
    tableNameWithTypeSet.forEach(tableNameWithType -> {
      _controllerMetrics.removeTableGauge(tableNameWithType, taskType, ControllerGauge.NUM_MINION_SUBTASKS_RUNNING);
      _controllerMetrics.removeTableGauge(tableNameWithType, taskType, ControllerGauge.NUM_MINION_SUBTASKS_WAITING);
      _controllerMetrics.removeTableGauge(tableNameWithType, taskType, ControllerGauge.NUM_MINION_SUBTASKS_ERROR);
      _controllerMetrics.removeTableGauge(tableNameWithType, taskType, ControllerGauge.NUM_MINION_SUBTASKS_UNKNOWN);
      _controllerMetrics.removeTableGauge(tableNameWithType, taskType, ControllerGauge.NUM_MINION_SUBTASKS_DROPPED);
      _controllerMetrics.removeTableGauge(tableNameWithType, taskType, ControllerGauge.NUM_MINION_SUBTASKS_TIMED_OUT);
      _controllerMetrics.removeTableGauge(tableNameWithType, taskType, ControllerGauge.NUM_MINION_SUBTASKS_ABORTED);
      _controllerMetrics.removeTableGauge(tableNameWithType, taskType,
          ControllerGauge.PERCENT_MINION_SUBTASKS_IN_QUEUE);
      _controllerMetrics.removeTableGauge(tableNameWithType, taskType,
          ControllerGauge.PERCENT_MINION_SUBTASKS_IN_ERROR);
    });
  }
}
