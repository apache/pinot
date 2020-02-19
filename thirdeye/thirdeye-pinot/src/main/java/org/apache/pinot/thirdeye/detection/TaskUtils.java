/*
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

package org.apache.pinot.thirdeye.detection;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;

import static org.apache.pinot.thirdeye.util.ThirdEyeUtils.getDetectionExpectedDelay;


/**
 * Holds utility functions related to ThirdEye Tasks
 */
public class TaskUtils {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Build a task with the specified type and properties
   */
  public static TaskDTO buildTask(long id, String taskInfoJson, TaskConstants.TaskType taskType) {
    TaskDTO taskDTO = new TaskDTO();
    taskDTO.setTaskType(taskType);
    taskDTO.setJobName(taskType.toString() + "_" + id);
    taskDTO.setStatus(TaskConstants.TaskStatus.WAITING);
    taskDTO.setTaskInfo(taskInfoJson);
    return taskDTO;
  }

  public static boolean checkTaskAlreadyRun(String jobName, DetectionPipelineTaskInfo taskInfo, long timeout) {
    // check if a task for this detection pipeline is already scheduled
    List<TaskDTO> scheduledTasks = DAORegistry.getInstance().getTaskDAO().findByPredicate(Predicate.AND(
        Predicate.EQ("name", jobName),
        Predicate.OR(
            Predicate.EQ("status", TaskConstants.TaskStatus.RUNNING.toString()),
            Predicate.EQ("status", TaskConstants.TaskStatus.WAITING.toString()))
        )
    );

    List<DetectionPipelineTaskInfo> scheduledTaskInfos = scheduledTasks.stream().map(taskDTO -> {
      try {
        return OBJECT_MAPPER.readValue(taskDTO.getTaskInfo(), DetectionPipelineTaskInfo.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
    Optional<DetectionPipelineTaskInfo> latestScheduledTask = scheduledTaskInfos.stream()
        .reduce((taskInfo1, taskInfo2) -> taskInfo1.getEnd() > taskInfo2.getEnd() ? taskInfo1 : taskInfo2);
    return latestScheduledTask.isPresent()
        && taskInfo.getEnd() - latestScheduledTask.get().getEnd() < timeout;
  }

  public static Long getIdFromJobKey(String jobKey) {
    String[] tokens = jobKey.split("_");
    String id = tokens[tokens.length - 1];
    return Long.valueOf(id);
  }

  public static DetectionPipelineTaskInfo buildTaskInfo(JobExecutionContext jobExecutionContext) {
    JobKey jobKey = jobExecutionContext.getJobDetail().getKey();
    Long id = getIdFromJobKey(jobKey.getName());
    DetectionConfigDTO configDTO = DAORegistry.getInstance().getDetectionConfigManager().findById(id);

    return buildTaskInfoFromDetectionConfig(configDTO, System.currentTimeMillis());
  }

  public static DetectionPipelineTaskInfo buildTaskInfoFromDetectionConfig(DetectionConfigDTO configDTO, long end) {
    long delay = getDetectionExpectedDelay(configDTO);
    long start = Math.max(configDTO.getLastTimestamp(), end  - ThirdEyeUtils.DETECTION_TASK_MAX_LOOKBACK_WINDOW - delay);
    return new DetectionPipelineTaskInfo(configDTO.getId(), start, end);
  }
}
