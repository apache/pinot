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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.data.Segment;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;


public class TaskGeneratorUtils {
  private static final long ONE_DAY_IN_MILLIS = 24 * 60 * 60 * 1000L;

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
      for (PinotTaskConfig pinotTaskConfig : clusterInfoAccessor.getTaskConfigs(entry.getKey())) {
        if (tableNameWithType.equals(pinotTaskConfig.getConfigs().get(MinionConstants.TABLE_NAME_KEY))) {
          nonCompletedTasks.put(entry.getKey(), entry.getValue());
        }
      }
    }
    return nonCompletedTasks;
  }

  /**
   * Returns true if task's schedule time is older than 1d
   */
  private static boolean isTaskOlderThanOneDay(String taskName) {
    long scheduleTimeMs =
        Long.parseLong(taskName.substring(taskName.lastIndexOf(PinotHelixTaskResourceManager.TASK_NAME_SEPARATOR) + 1));
    return System.currentTimeMillis() - scheduleTimeMs > ONE_DAY_IN_MILLIS;
  }

  /**
   * Get all the segments that are scheduled for all existing tables.
   *
   * @param taskType Task type
   * @param clusterInfoAccessor Cluster info accessor
   * @return a map of table name as a key and the list of scheduled segments as the value.
   */
  public static Map<String, Set<String>> getScheduledSegmentsMap(@Nonnull String taskType,
      @Nonnull ClusterInfoAccessor clusterInfoAccessor) {
    Map<String, Set<String>> scheduledSegments = new HashMap<>();
    Map<String, TaskState> taskStates = clusterInfoAccessor.getTaskStates(taskType);
    for (Map.Entry<String, TaskState> entry : taskStates.entrySet()) {
      // Skip COMPLETED tasks
      if (entry.getValue() == TaskState.COMPLETED) {
        continue;
      }
      for (PinotTaskConfig pinotTaskConfig : clusterInfoAccessor.getTaskConfigs(entry.getKey())) {
        Map<String, String> configs = pinotTaskConfig.getConfigs();
        Set<String> scheduledSegmentsForTable =
            scheduledSegments.computeIfAbsent(configs.get(MinionConstants.TABLE_NAME_KEY), v -> new HashSet<>());
        String segmentName = configs.get(MinionConstants.SEGMENT_NAME_KEY);
        Set<String> segmentNames =
            Arrays.stream(segmentName.split(",")).map(String::trim).collect(Collectors.toSet());
        scheduledSegmentsForTable.addAll(segmentNames);
      }
    }
    return scheduledSegments;
  }
}
