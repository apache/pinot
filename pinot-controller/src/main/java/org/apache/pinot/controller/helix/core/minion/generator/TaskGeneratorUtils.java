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
import javax.annotation.Nonnull;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.data.Segment;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;


public class TaskGeneratorUtils {
  private TaskGeneratorUtils() {
  }

  private static final long ONE_DAY_IN_MILLIS = 24 * 60 * 60 * 1000L;

  /**
   * If task is in final state, it will not be running any more. But note that
   * STOPPED is not a final task state in helix task framework, as a stopped task
   * is just paused and can be resumed to rerun.
   */
  private static final EnumSet<TaskState> TASK_FINAL_STATES =
      EnumSet.of(TaskState.COMPLETED, TaskState.FAILED, TaskState.ABORTED, TaskState.TIMED_OUT);

  /**
   * Returns all the segments that have been scheduled in one day but not finished.
   * <p>
   * NOTE: we consider tasks not finished in one day as stuck and don't count the segments in them
   *
   * @param taskType Task type
   * @param clusterInfoAccessor Cluster info accessor
   * @return Set of running segments
   */
  public static Set<Segment> getRunningSegments(@Nonnull String taskType,
      @Nonnull ClusterInfoAccessor clusterInfoAccessor) {
    Set<Segment> runningSegments = new HashSet<>();
    Map<String, TaskState> taskStates = clusterInfoAccessor.getTaskStates(taskType);
    for (Map.Entry<String, TaskState> entry : taskStates.entrySet()) {
      // Skip COMPLETED tasks
      if (entry.getValue() == TaskState.COMPLETED) {
        continue;
      }

      // Skip tasks scheduled for more than one day
      String taskName = entry.getKey();
      if (isTaskOlderThanOneDay(taskName)) {
        continue;
      }

      for (PinotTaskConfig pinotTaskConfig : clusterInfoAccessor.getTaskConfigs(entry.getKey())) {
        Map<String, String> configs = pinotTaskConfig.getConfigs();
        runningSegments.add(
            new Segment(configs.get(MinionConstants.TABLE_NAME_KEY), configs.get(MinionConstants.SEGMENT_NAME_KEY)));
      }
    }
    return runningSegments;
  }

  /**
   * Gets all the tasks for the provided task type and tableName, which do not have TaskState COMPLETED
   * @return map containing task name to task state for non-completed tasks
   *
   * NOTE: we consider tasks not finished in one day as stuck and don't count them
   */
  public static Map<String, TaskState> getIncompleteTasks(String taskType, String tableNameWithType,
      ClusterInfoAccessor clusterInfoAccessor) {
    Map<String, TaskState> nonCompletedTasks = new HashMap<>();
    Map<String, TaskState> taskStates = clusterInfoAccessor.getTaskStates(taskType);
    for (Map.Entry<String, TaskState> entry : taskStates.entrySet()) {
      if (entry.getValue() == TaskState.COMPLETED) {
        continue;
      }
      String taskName = entry.getKey();
      if (isTaskOlderThanOneDay(taskName)) {
        continue;
      }
      for (PinotTaskConfig pinotTaskConfig : clusterInfoAccessor.getTaskConfigs(taskName)) {
        if (tableNameWithType.equals(pinotTaskConfig.getConfigs().get(MinionConstants.TABLE_NAME_KEY))) {
          nonCompletedTasks.put(taskName, entry.getValue());
        }
      }
    }
    return nonCompletedTasks;
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
   * Returns true if task's schedule time is older than 1d
   */
  private static boolean isTaskOlderThanOneDay(String taskName) {
    long scheduleTimeMs =
        Long.parseLong(taskName.substring(taskName.lastIndexOf(PinotHelixTaskResourceManager.TASK_NAME_SEPARATOR) + 1));
    return System.currentTimeMillis() - scheduleTimeMs > ONE_DAY_IN_MILLIS;
  }
}
