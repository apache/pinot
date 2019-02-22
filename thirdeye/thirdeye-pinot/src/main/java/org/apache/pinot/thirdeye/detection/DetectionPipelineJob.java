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
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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

  @Override
  public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    JobKey jobKey = jobExecutionContext.getJobDetail().getKey();
    Long id = getIdFromJobKey(jobKey.getName());
    DetectionConfigDTO configDTO = detectionDAO.findById(id);
    DetectionPipelineTaskInfo taskInfo = new DetectionPipelineTaskInfo(configDTO.getId(), configDTO.getLastTimestamp(), System.currentTimeMillis());

    // check if a task for this detection pipeline is already scheduled
    String jobName = String.format("%s_%d", TaskConstants.TaskType.DETECTION, id);
    List<TaskDTO> scheduledTasks = taskDAO.findByPredicate(Predicate.AND(
        Predicate.EQ("name", jobName),
        Predicate.EQ("startTime", taskInfo.getStart()),
        Predicate.OR(
            Predicate.EQ("status", TaskConstants.TaskStatus.RUNNING.toString()),
            Predicate.EQ("status", TaskConstants.TaskStatus.WAITING.toString())
        )
      )
    );

    Optional<TaskDTO> latestScheduledTask = scheduledTasks.stream().reduce((task1, task2) -> task1.getEndTime() > task2.getEndTime() ? task1 : task2);
    if (latestScheduledTask.isPresent() && taskInfo.getEnd() - latestScheduledTask.get().getEndTime() < DETECTION_TASK_TIMEOUT){
      // if a task is pending and not time out yet, don't schedule more
      LOG.info("Skip scheduling detection task for {} with start time {}. Task is already in the queue.", jobName, taskInfo.getStart());
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
    taskDTO.setStartTime(taskInfo.getStart());
    taskDTO.setEndTime(taskInfo.getEnd());

    long taskId = taskDAO.save(taskDTO);
    LOG.info("Created detection pipeline task {} with taskId {}", taskDTO, taskId);

  }

  private Long getIdFromJobKey(String jobKey) {
    String[] tokens = jobKey.split("_");
    String id = tokens[tokens.length - 1];
    return Long.valueOf(id);
  }
}


