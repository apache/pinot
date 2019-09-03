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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DetectionPipelineJob implements Job {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionPipelineJob.class);
  private TaskManager taskDAO = DAORegistry.getInstance().getTaskDAO();
  private DetectionConfigManager detectionDAO = DAORegistry.getInstance().getDetectionConfigManager();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final long DETECTION_TASK_TIMEOUT = TimeUnit.DAYS.toMillis(1);
  private static final long DETECTION_TASK_MAX_LOOKBACK_WINDOW = TimeUnit.DAYS.toMillis(7);

  @Override
  public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    JobKey jobKey = jobExecutionContext.getJobDetail().getKey();
    Long id = getIdFromJobKey(jobKey.getName());
    DetectionConfigDTO configDTO = detectionDAO.findById(id);

    // Make sure start time is not out of DETECTION_TASK_MAX_LOOKBACK_WINDOW
    long end = System.currentTimeMillis();
    long start = Math.max(configDTO.getLastTimestamp(), end  - DETECTION_TASK_MAX_LOOKBACK_WINDOW);
    DetectionPipelineTaskInfo taskInfo = new DetectionPipelineTaskInfo(configDTO.getId(), start, end);

    String jobName = String.format("%s_%d", TaskConstants.TaskType.DETECTION, id);

    // if a task is pending and not time out yet, don't schedule more
    if (checkTaskAlreadyRun(jobName, taskInfo)) {
      LOG.info("Skip scheduling detection task for {} with start time {}. Task is already in the queue.", jobName,
          taskInfo.getStart());
      return;
    }

    String taskInfoJson = null;
    try {
      taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);
    } catch (JsonProcessingException e) {
      LOG.error("Exception when converting AlertTaskInfo {} to jsonString", taskInfo, e);
    }

    TaskDTO taskDTO = new TaskDTO();
    taskDTO.setTaskType(TaskConstants.TaskType.DETECTION);
    taskDTO.setJobName(jobName);
    taskDTO.setStatus(TaskConstants.TaskStatus.WAITING);
    taskDTO.setTaskInfo(taskInfoJson);

    long taskId = taskDAO.save(taskDTO);
    LOG.info("Created detection pipeline task {} with taskId {}", taskDTO, taskId);
  }

  private Long getIdFromJobKey(String jobKey) {
    String[] tokens = jobKey.split("_");
    String id = tokens[tokens.length - 1];
    return Long.valueOf(id);
  }

  private boolean checkTaskAlreadyRun(String jobName, DetectionPipelineTaskInfo taskInfo ) {
    // check if a task for this detection pipeline is already scheduled
    List<TaskDTO> scheduledTasks = taskDAO.findByPredicate(Predicate.AND(
        Predicate.EQ("name", jobName),
        Predicate.OR(
            Predicate.EQ("status", TaskConstants.TaskStatus.RUNNING.toString()),
            Predicate.EQ("status", TaskConstants.TaskStatus.WAITING.toString())
        )
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
        && taskInfo.getEnd() - latestScheduledTask.get().getEnd() < DETECTION_TASK_TIMEOUT;
  }
}


