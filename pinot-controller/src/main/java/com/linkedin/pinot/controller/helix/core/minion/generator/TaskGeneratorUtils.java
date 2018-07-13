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
package com.linkedin.pinot.controller.helix.core.minion.generator;

import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.data.Segment;
import com.linkedin.pinot.controller.helix.core.minion.ClusterInfoProvider;
import com.linkedin.pinot.core.common.MinionConstants;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.helix.task.TaskState;


public class TaskGeneratorUtils {

  /**
   * Returns all the segments that have been scheduled but not finished
   *
   * @param taskType Task type
   * @param clusterInfoProvider Cluster info provider
   * @return Set of running segments
   */
  public static Set<Segment> getRunningSegments(@Nonnull String taskType,
      @Nonnull ClusterInfoProvider clusterInfoProvider) {
    Set<Segment> runningSegments = new HashSet<>();
    Map<String, TaskState> taskStates = clusterInfoProvider.getTaskStates(taskType);
    for (Map.Entry<String, TaskState> entry : taskStates.entrySet()) {
      TaskState taskState = entry.getValue();
      if (taskState == TaskState.NOT_STARTED || taskState == TaskState.IN_PROGRESS || taskState == TaskState.STOPPED) {
        for (PinotTaskConfig pinotTaskConfig : clusterInfoProvider.getTaskConfigs(entry.getKey())) {
          Map<String, String> configs = pinotTaskConfig.getConfigs();
          runningSegments.add(
              new Segment(configs.get(MinionConstants.TABLE_NAME_KEY), configs.get(MinionConstants.SEGMENT_NAME_KEY)));
        }
      }
    }
    return runningSegments;
  }
}
