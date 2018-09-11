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

package com.linkedin.thirdeye.detection.alert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import com.linkedin.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Detection alert job that run by the cron scheduler.
 * This job put detection alert task into database which can be picked up by works later.
 */
public class DetectionAlertJob implements Job {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionAlertJob.class);
  private DetectionAlertConfigManager alertConfigDAO;
  private TaskManager taskDAO;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public DetectionAlertJob() {
    this.alertConfigDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();
    this.taskDAO = DAORegistry.getInstance().getTaskDAO();
  }

  @Override
  public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    LOG.info("Running " + jobExecutionContext.getJobDetail().getKey().toString());
    String jobKey = jobExecutionContext.getJobDetail().getKey().getName();
    long detectionAlertConfigId = getIdFromJobKey(jobKey);
    DetectionAlertConfigDTO configDTO = alertConfigDAO.findById(detectionAlertConfigId);
    if (configDTO == null) {
      LOG.error("Alert config with id {} does not exist", detectionAlertConfigId);
    }

    DetectionAlertTaskInfo taskInfo = new DetectionAlertTaskInfo(detectionAlertConfigId);

    String taskInfoJson = null;
    try {
      taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);
    } catch (JsonProcessingException e) {
      LOG.error("Exception when converting AlertTaskInfo {} to jsonString", taskInfo, e);
    }

    TaskDTO taskDTO = new TaskDTO();
    taskDTO.setTaskType(TaskConstants.TaskType.DETECTION_ALERT);
    taskDTO.setJobName(String.format("%s_%d", TaskConstants.TaskType.DETECTION_ALERT, detectionAlertConfigId));
    taskDTO.setStatus(TaskConstants.TaskStatus.WAITING);
    taskDTO.setStartTime(System.currentTimeMillis());
    taskDTO.setTaskInfo(taskInfoJson);

    long taskId = taskDAO.save(taskDTO);
    LOG.info("Created detection pipeline task {} with taskId {}", taskDTO, taskId);
  }

  private Long getIdFromJobKey(String jobKey) {
    String[] tokens = jobKey.split("_");
    String id = tokens[tokens.length - 1];
    return Long.valueOf(id);
  }
}
